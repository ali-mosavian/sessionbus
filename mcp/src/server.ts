// MCP server on stdio (newline-delimited JSON-RPC 2.0). The tools mirror
// Claude Code's built-in session messaging (ListAgents, SendMessage,
// ReadNotifications) and add topics. Each call uses its own relay connection;
// requests run concurrently.
import type { Readable, Writable } from "node:stream";
import { createInterface } from "node:readline";

import { Conn, type Config, type Frame } from "./client.ts";

const DEFAULT_VERSION = "2025-06-18";
const TAG = "cross-session-message";
const INSTRUCTIONS =
  "Message other Claude sessions, possibly on other machines, through a sessionbus relay. A session's name is " +
  "its address; `#topic` addresses every subscriber of a topic. Incoming messages are not pushed: call " +
  "ReadNotifications when you expect one. Delivery is at-least-once, so dedupe by the message id.";

const schema = (properties: object, required: string[] = []) => ({
  type: "object",
  properties,
  required,
  additionalProperties: false,
});

const topicArg = { topic: { type: "string", description: "Topic name, with or without a leading #" } };

const TOOLS = [
  {
    name: "ListAgents",
    description:
      "List the other sessions reachable through sessionbus, on this machine or others, and whether each is " +
      "online. Each row leads with the session's name: the name IS the address for SendMessage.",
    inputSchema: schema({}),
  },
  {
    name: "SendMessage",
    description:
      "Send a message to another session. `to` is a name from ListAgents, or `#topic` to broadcast to every " +
      "subscriber of the topic (not yourself). Your plain text output is NOT visible to other sessions: to " +
      "communicate you MUST call this tool. A message arrives wrapped as " +
      `<${TAG} from="...">. **To reply to an incoming message, copy its \`from\` attribute as your \`to\`.** ` +
      "A successful send means the relay queued the message, not that the other session read it.",
    inputSchema: schema(
      {
        to: {
          type: "string",
          pattern: "^[^\\n\\r]*$",
          maxLength: 300,
          description: 'Recipient: a name from ListAgents, or "#topic"',
        },
        message: {
          type: "string",
          default: "",
          description:
            "Plain text message content. The recipient sees only the FIRST LINE as a preview, so make it a " +
            "clear, self-contained sentence saying what this is about.",
        },
        summary: {
          type: "string",
          maxLength: 200,
          description: "A 5-10 word label for your own transcript row (not transmitted).",
        },
      },
      ["to", "message"],
    ),
  },
  {
    name: "ReadNotifications",
    description:
      `Read the messages queued for this session, direct and from subscribed topics, oldest first. Each is ` +
      `wrapped as <${TAG} from="..." [topic="..."] id="...">. Messages are marked read once this reply is ` +
      "delivered. Call it when you expect a reply or were told a message is waiting.",
    inputSchema: schema({}),
  },
  {
    name: "Subscribe",
    description: "Subscribe this session to a topic, so messages sent to #topic reach it. Only future messages.",
    inputSchema: schema(topicArg, ["topic"]),
  },
  {
    name: "Unsubscribe",
    description: "Unsubscribe this session from a topic.",
    inputSchema: schema(topicArg, ["topic"]),
  },
  {
    name: "ListTopics",
    description: "List the topics that have subscribers, with their subscriber counts.",
    inputSchema: schema({}),
  },
];

type ToolResult = { text: string; after?: () => Promise<void> };

const esc = (s: string) => s.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;");

/** A body must not be able to close its wrapper or open a forged one. */
const neutralize = (body: string) => body.replace(new RegExp(`<(/?${TAG})`, "gi"), "&lt;$1");

export const wrap = (m: Frame): string =>
  `<${TAG} from="${esc(m.from)}"${m.topic ? ` topic="${esc(m.topic)}"` : ""} id="${m.id}">\n` +
  `${neutralize(m.body)}\n</${TAG}>`;

const ago = (t: number): string => {
  const s = Math.max(0, Math.round(Date.now() / 1000 - t));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return s < 86400 ? `${Math.floor(s / 3600)}h ago` : `${Math.floor(s / 86400)}d ago`;
};

const plural = (n: number, w: string) => `${n} ${w}${n === 1 ? "" : "s"}`;

const topicOf = (args: Record<string, any>): string => {
  const t = args.topic;
  if (typeof t !== "string" || !t.replace(/^#/, "")) throw new Error("missing string argument `topic`");
  return t.replace(/^#/, "");
};

async function once<T>(cfg: Config, f: (c: Conn) => Promise<T>): Promise<T> {
  const c = await Conn.open(cfg, false);
  try {
    return await f(c);
  } finally {
    c.close();
  }
}

async function callTool(cfg: Config, name: string, args: Record<string, any>): Promise<ToolResult> {
  switch (name) {
    case "ListAgents": {
      const r = await once(cfg, (c) => c.call({ op: "list" }));
      const rows = (r.agents as Frame[])
        .filter((a) => a.name !== cfg.name)
        .map((a) => `${a.name} — ${a.online ? "online" : `offline, last seen ${ago(a.last_seen)}`}`);
      return { text: rows.join("\n") || "No other sessions known yet." };
    }
    case "ListTopics": {
      const r = await once(cfg, (c) => c.call({ op: "topics" }));
      const rows = (r.topics as Frame[]).map((t) => `#${t.topic} — ${plural(t.subscribers, "subscriber")}`);
      return { text: rows.join("\n") || "No topics." };
    }
    case "Subscribe":
    case "Unsubscribe": {
      const topic = topicOf(args);
      const sub = name === "Subscribe";
      await once(cfg, (c) => c.call({ op: sub ? "sub" : "unsub", topic }));
      return { text: `${sub ? "Subscribed to" : "Unsubscribed from"} #${topic}.` };
    }
    case "SendMessage": {
      const { to, message } = args;
      if (typeof to !== "string" || !to || to.length > 300 || /[\r\n]/.test(to)) {
        throw new Error("`to` must be a single-line name of at most 300 characters");
      }
      if (typeof message !== "string") throw new Error("missing string argument `message`");
      try {
        const r = await once(cfg, (c) => c.call({ op: "send", to, body: message }));
        if (!to.startsWith("#")) return { text: `Sent to ${to}.` };
        return { text: r.delivered ? `Sent to ${to} (${plural(r.delivered, "subscriber")}).` : `Sent to ${to}, which has no other subscribers.` };
      } catch (e) {
        if ((e as Error).message === "unknown recipient") {
          throw new Error(`No session named "${to}". Use ListAgents to see who is reachable.`);
        }
        throw e;
      }
    }
    case "ReadNotifications": {
      const c = await Conn.open(cfg, true);
      let msgs: Frame[];
      try {
        msgs = await c.inbox();
      } catch (e) {
        c.close();
        throw e;
      }
      if (!msgs.length) {
        c.close();
        return { text: "No pending notifications." };
      }
      const last = msgs[msgs.length - 1]!.id as number;
      return { text: msgs.map(wrap).join("\n\n"), after: () => c.ackSync(last).finally(() => c.close()) };
    }
    default:
      throw new Error(`unknown tool \`${name}\``);
  }
}

type Reply = { body: Record<string, unknown>; after?: () => Promise<void> };

async function handle(cfg: Config, req: any): Promise<Reply> {
  const params = req.params ?? {};
  switch (req.method) {
    case "initialize":
      return {
        body: {
          result: {
            protocolVersion: params.protocolVersion ?? DEFAULT_VERSION,
            capabilities: { tools: {} },
            serverInfo: { name: "sessionbus", version: "0.1.0" },
            instructions: INSTRUCTIONS,
          },
        },
      };
    case "ping":
      return { body: { result: {} } };
    case "tools/list":
      return { body: { result: { tools: TOOLS } } };
    case "tools/call":
      try {
        const r = await callTool(cfg, String(params.name ?? ""), params.arguments ?? {});
        return { body: { result: { content: [{ type: "text", text: r.text }], isError: false } }, after: r.after };
      } catch (e) {
        return { body: { result: { content: [{ type: "text", text: (e as Error).message }], isError: true } } };
      }
    default:
      return { body: { error: { code: -32601, message: `method not found: ${req.method}` } } };
  }
}

const write = (out: Writable, s: string) => new Promise<boolean>((done) => out.write(s, (err) => done(!err)));

async function handleLine(cfg: Config, out: Writable, line: string): Promise<void> {
  let id: unknown = null;
  let reply: Reply;
  try {
    const req = JSON.parse(line);
    if (req.id === undefined) return; // notification
    id = req.id;
    reply = await handle(cfg, req);
  } catch (e) {
    reply = { body: { error: { code: -32700, message: (e as Error).message } } };
  }
  const delivered = await write(out, JSON.stringify({ jsonrpc: "2.0", id, ...reply.body }) + "\n");
  // Ack only what Claude was actually handed; otherwise the messages stay queued.
  if (delivered) await reply.after?.().catch(() => {});
}

/** Serves until `input` ends, then waits for in-flight requests and their acks. */
export async function serve(cfg: Config, input: Readable, output: Writable): Promise<void> {
  output.on("error", () => {});
  const inflight = new Set<Promise<void>>();
  for await (const line of createInterface({ input, crlfDelay: Infinity })) {
    if (!line.trim()) continue;
    const p: Promise<void> = handleLine(cfg, output, line).finally(() => inflight.delete(p));
    inflight.add(p);
  }
  await Promise.all(inflight);
}
