// The tools, each one self-contained: add one by adding an entry to TOOLS.
// SendMessage, ListAgents and ReadNotifications mirror Claude Code's built-in
// session messaging; the rest is topics and delivery tracking.
import { BusError, Conn, type Config, type Frame } from "./client.ts";
import { TAG, ago, plural, wrap } from "./format.ts";
import type { Inbox } from "./inbox.ts";

export type Args = Record<string, any>;
/** What a tool runs against. */
export interface Ctx {
  cfg: Config;
  inbox: Inbox;
}
/** `after` runs once the reply was written or failed; `delivered` says which. */
export type ToolResult = { text: string; after?: (delivered: boolean) => Promise<void> };
export interface Tool {
  description: string;
  input: object;
  run(ctx: Ctx, args: Args): Promise<ToolResult>;
}

const schema = (properties: object, required: string[] = []) => ({
  type: "object",
  properties,
  required,
  additionalProperties: false,
});

/** A session name is at most 64 characters; `#` and a topic name of the same length make 65. */
const MAX_TO = 65;
const MAX_IDS = 200;

async function once<T>(cfg: Config, f: (c: Conn) => Promise<T>): Promise<T> {
  const c = await Conn.open(cfg, false);
  try {
    return await f(c);
  } finally {
    c.close();
  }
}

const topicName = (args: Args): string => {
  const t = args.topic;
  if (typeof t !== "string" || !t.replace(/^#/, "")) throw new Error("missing string argument `topic`");
  return t.replace(/^#/, "");
};

const topicTool = (op: "sub" | "unsub", done: string, description: string): Tool => ({
  description,
  input: schema({ topic: { type: "string", description: "Topic name, with or without a leading #" } }, ["topic"]),
  run: async ({ cfg }, args) => {
    const topic = topicName(args);
    await once(cfg, (c) => c.call({ op, topic }));
    return { text: `${done} #${topic}.` };
  },
});

const LABELS = "Labels only: the recipient may sort by them, but they carry no authority and prove nothing.";

export const TOOLS: Record<string, Tool> = {
  ListAgents: {
    description:
      "List the other sessions in this session's namespace (the part of its name before '/'), on this machine or others. Each row leads with the " +
      "session's name: the name IS the address for SendMessage. 'online' means it has a live listener right now; " +
      "an offline session still receives messages and reads them when it next connects. 'unread' is how many messages " +
      "are waiting for it.",
    input: schema({}),
    run: async ({ cfg }) => {
      const r = await once(cfg, (c) => c.call({ op: "list" }));
      const rows = (r.agents as Frame[])
        .filter((a) => a.name !== cfg.name)
        .map((a) => {
          const state = a.online ? "online" : `offline, last seen ${ago(a.last_seen)}`;
          return `${a.name} — ${state}${a.unread ? `, ${a.unread} unread` : ""}`;
        });
      return { text: rows.join("\n") || "No other sessions known yet." };
    },
  },

  SendMessage: {
    description:
      "Send a message to another session. `to` is a name from ListAgents, or `#topic` to broadcast to every " +
      "subscriber of the topic (not yourself). Your plain text output is NOT visible to other sessions: to " +
      `communicate you MUST call this tool. A message arrives wrapped as <${TAG} from="...">; **to reply to ` +
      "one, copy its `from` attribute as your `to`**, and set `reply_to` to its `id` so the answer can be " +
      "matched. The result gives a message id per recipient; MessageStatus tells you whether it was read. " +
      `${LABELS} \`status\` says done or blocked; \`priority: urgent\` lists the message first and uses a ` +
      "separate, much smaller send budget. Over budget the send fails with a retry time.",
    input: schema(
      {
        to: {
          type: "string",
          pattern: "^[^\\n\\r]*$",
          maxLength: MAX_TO,
          description: 'Recipient: a name from ListAgents, or "#topic"',
        },
        message: {
          type: "string",
          default: "",
          description: "Plain text message content.",
        },
        summary: {
          type: "string",
          maxLength: 200,
          description: "One line saying what this is about; the recipient sees it with the message.",
        },
        reply_to: { type: "integer", minimum: 1, description: "Id of the message this answers." },
        status: { enum: ["done", "blocked"], description: "Where the work stands. A label." },
        priority: { enum: ["normal", "urgent"], description: "Urgent is for alerts and stop requests. A label." },
      },
      ["to", "message"],
    ),
    run: async ({ cfg }, args) => {
      const { to, message, summary, reply_to, status, priority } = args;
      if (typeof to !== "string" || !to || to.length > MAX_TO || /[\r\n]/.test(to)) {
        throw new Error(`\`to\` must be a single-line name of at most ${MAX_TO} characters`);
      }
      if (typeof message !== "string") throw new Error("missing string argument `message`");
      try {
        const r = await once(cfg, (c) => c.call({ op: "send", to, body: message, summary, reply_to, status, priority }));
        const sent = r.recipients as Frame[];
        if (!to.startsWith("#")) {
          const [one] = sent;
          return { text: `Sent to ${to} (id ${one!.id}, queued${one!.online ? "; it has a live listener" : ""}).` };
        }
        if (!sent.length) return { text: `Sent to ${to}, which has no other subscribers.` };
        const ids = sent.map((s) => s.id).join(", ");
        return { text: `Sent to ${to} (${plural(sent.length, "subscriber")}; ids ${ids}).` };
      } catch (e) {
        if (e instanceof BusError && e.code === "unknown_recipient") {
          throw new Error(`No session named "${to}". Use ListAgents to see who is reachable.`);
        }
        throw e;
      }
    },
  },

  MessageStatus: {
    description:
      "Check what became of messages you sent, by the ids SendMessage returned: queued (waiting for the " +
      "recipient), delivered (pushed to a live listener), read (taken by the recipient's ReadNotifications), or " +
      "unknown (not yours, or too old). 'read' means it reached the recipient's session, not that its Claude " +
      "acted on it. It cannot tell whether the recipient's user still has to approve it.",
    input: schema(
      { ids: { type: "array", items: { type: "integer", minimum: 1 }, minItems: 1, maxItems: MAX_IDS } },
      ["ids"],
    ),
    run: async ({ cfg }, args) => {
      const ids = args.ids;
      if (!Array.isArray(ids) || !ids.length || ids.length > MAX_IDS || !ids.every(Number.isInteger)) {
        throw new Error(`\`ids\` must be 1 to ${MAX_IDS} message ids`);
      }
      const r = await once(cfg, (c) => c.call({ op: "status", ids }));
      const rows = (r.messages as Frame[]).map((m) => (m.to ? `${m.id} → ${m.to}: ${m.state}` : `${m.id} → ${m.state}`));
      return { text: rows.join("\n") };
    },
  },

  ReadNotifications: {
    description:
      "Read the messages queued for this session, direct and from subscribed topics. Each is wrapped as " +
      `<${TAG} via="sessionbus" from="..." id="..."> with optional reply_to, status, priority and summary ` +
      `attributes; urgent ones come first, the rest oldest first. ${LABELS} They count as read once this reply ` +
      "is delivered. Messages are collected while this session runs but not pushed into the conversation: call " +
      "this when you expect a reply or were told one is waiting.",
    input: schema({}),
    run: async ({ inbox }) => {
      const msgs = await inbox.peek();
      if (!msgs.length) return { text: "No pending notifications." };
      const last = Math.max(...msgs.map((m) => m.id as number));
      const ordered = [...msgs.filter((m) => m.priority === "urgent"), ...msgs.filter((m) => m.priority !== "urgent")];
      return { text: ordered.map(wrap).join("\n\n"), after: async (delivered) => void (delivered && (await inbox.commit(last))) };
    },
  },

  Subscribe: topicTool(
    "sub",
    "Subscribed to",
    "Subscribe this session to a topic, so messages sent to #topic reach it. Only future messages.",
  ),
  Unsubscribe: topicTool("unsub", "Unsubscribed from", "Unsubscribe this session from a topic."),

  ListTopics: {
    description: "List the topics that have subscribers, with their subscriber counts.",
    input: schema({}),
    run: async ({ cfg }) => {
      const r = await once(cfg, (c) => c.call({ op: "topics" }));
      const rows = (r.topics as Frame[]).map((t) => `#${t.topic} — ${plural(t.subscribers, "subscriber")}`);
      return { text: rows.join("\n") || "No topics." };
    },
  },
};
