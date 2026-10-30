// The tools, each one self-contained: add one by adding an entry to TOOLS.
// SendMessage, ListAgents and ReadNotifications mirror Claude Code's built-in
// session messaging; the rest is topics and delivery tracking.
import { AgentError, type Frame } from "./agent.ts";
import { TAG, ago, plural, wrap } from "./format.ts";

export type Args = Record<string, any>;
/** What a tool runs against. */
export interface Ctx {
  project: string;
  /** One request to the local agent, in this session's name. */
  call(req: Frame, timeoutMs?: number): Promise<Frame>;
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

const MAX_TO = 130;
const MAX_IDS = 200;

const topicName = (args: Args): string => {
  const t = args.topic;
  if (typeof t !== "string" || !t.replace(/^#/, "")) throw new Error("missing string argument `topic`");
  return t.replace(/^#/, "");
};

const topicTool = (op: "sub" | "unsub", done: string, description: string): Tool => ({
  description,
  input: schema({ topic: { type: "string", description: "Topic name, with or without a leading #" } }, ["topic"]),
  run: async ({ call, project }, args) => {
    const topic = topicName(args);
    await call({ op, project, topic });
    return { text: `${done} #${topic}.` };
  },
});

const LABELS = "Labels only: the recipient may sort by them, but they carry no authority and prove nothing.";

export const TOOLS: Record<string, Tool> = {
  ListAgents: {
    description:
      "List the sessions in the project, on this machine or others; other projects are invisible. Each row is " +
      "`alias@host`, which is also the address for SendMessage (the bare alias works when it is unique). A session " +
      "that is not running still receives messages for a while and reads them when it is back. 'unread' is only " +
      "known for sessions on this host. Hosts that were never reachable are not listed.",
    input: schema({}),
    run: async ({ call, project }) => {
      const r = await call({ op: "list", project });
      const rows = (r.sessions as Frame[]).map((s) => {
        const notes = [s.last_seen ? `seen ${ago(s.last_seen)}` : "", s.unread ? `${s.unread} unread` : ""].filter(Boolean);
        return `${s.alias}@${s.host}${notes.length ? ` — ${notes.join(", ")}` : ""}`;
      });
      return { text: rows.join("\n") || "No sessions have taken a name in this project yet." };
    },
  },

  SendMessage: {
    description:
      "Send a message to another session. `to` is an address from ListAgents (`alias` or `alias@host`), or `#topic` " +
      "to broadcast to every subscriber of the topic. Your plain text output is NOT visible to other sessions: to " +
      `communicate you MUST call this tool. A message arrives wrapped as <${TAG} from="...">; **to reply to ` +
      "one, copy its `from` attribute as your `to`**, and set `reply_to` to its `id` so the answer can be " +
      "matched. The send is accepted at once and delivered when the recipient's host is reachable; it is dropped " +
      "if undelivered after its lifetime (`ttl_minutes`, default 60, at most 1440), so do not send news that " +
      "will be stale by then. The result gives a message id per recipient; MessageStatus tells you whether it " +
      `was delivered, read or expired. ${LABELS} \`status\` says done or blocked; \`priority: urgent\` lists the ` +
      "message first.",
    input: schema(
      {
        to: {
          type: "string",
          pattern: "^[^\\n\\r]*$",
          maxLength: MAX_TO,
          description: 'Recipient: "alias", "alias@host" or "#topic"',
        },
        message: { type: "string", default: "", description: "Plain text message content." },
        summary: {
          type: "string",
          maxLength: 200,
          description: "One line saying what this is about; the recipient sees it with the message.",
        },
        reply_to: { type: "string", description: "Id of the message this answers." },
        status: { enum: ["done", "blocked"], description: "Where the work stands. A label." },
        priority: { enum: ["normal", "urgent"], description: "Urgent is for alerts and stop requests. A label." },
        ttl_minutes: { type: "integer", minimum: 1, maximum: 1440, description: "How long the message stays worth delivering." },
      },
      ["to", "message"],
    ),
    run: async ({ call, project }, args) => {
      const { to, message, summary, reply_to, status, priority, ttl_minutes } = args;
      if (typeof to !== "string" || !to || to.length > MAX_TO || /[\r\n]/.test(to)) {
        throw new Error(`\`to\` must be a single-line address of at most ${MAX_TO} characters`);
      }
      if (typeof message !== "string") throw new Error("missing string argument `message`");
      const r = await call({
        op: "send", project, to, body: message, summary, reply_to, status, priority,
        ttl_s: ttl_minutes ? ttl_minutes * 60 : undefined,
      });
      const sent = r.sent as Frame[];
      if (!sent.length) return { text: `NOT delivered: nobody is subscribed to ${to}. Subscribe sessions first (Subscribe), or message them directly.` };
      const rows = sent.map((s) => `${s.to_alias}@${s.to_host} (id ${s.id}, ${s.state})`);
      const waiting = sent.some((s) => s.state === "queued");
      const who = to.startsWith("#") ? ` ${to} has ${plural(sent.length, "subscriber")}:` : "";
      return { text: `Sent.${who} ${rows.join("; ")}${waiting ? ". queued: that host is not reachable yet; it keeps trying until the message expires." : ""}` };
    },
  },

  MessageStatus: {
    description:
      "Check what became of messages you sent, by the ids SendMessage returned: queued (recipient's host not " +
      "reached yet), delivered (stored on the recipient's host), read (taken by the recipient's ReadNotifications), " +
      "expired (never delivered in time, and dropped) or unknown (not yours, or too old). 'read' means it reached " +
      "the recipient's session, not that its Claude acted on it.",
    input: schema({ ids: { type: "array", items: { type: "string" }, minItems: 1, maxItems: MAX_IDS } }, ["ids"]),
    run: async ({ call }, args) => {
      const ids = args.ids;
      if (!Array.isArray(ids) || !ids.length || ids.length > MAX_IDS || !ids.every((i) => typeof i === "string")) {
        throw new Error(`\`ids\` must be 1 to ${MAX_IDS} message ids`);
      }
      const r = await call({ op: "status", ids });
      const known = new Map((r.messages as Frame[]).map((m) => [m.id, m]));
      const rows = ids.map((id) => {
        const m = known.get(id);
        return m ? `${id} → ${m.to_alias}@${m.to_host}: ${m.state}` : `${id} → unknown`;
      });
      return { text: rows.join("\n") };
    },
  },

  ReadNotifications: {
    description:
      "Read the messages waiting for this session, direct and from subscribed topics. Each is wrapped as " +
      `<${TAG} via="sessionbus" from="..." id="..."> with optional reply_to, status, priority and summary ` +
      `attributes; urgent ones come first, the rest oldest first. ${LABELS} They count as read once this reply ` +
      "is delivered. Idle sessions are woken by new messages when the sessionbus hooks are installed; otherwise " +
      "call this when you expect a reply or were told one is waiting.",
    input: schema({}),
    run: async ({ call, project }) => {
      const r = await call({ op: "peek", project });
      const msgs = r.messages as Frame[];
      if (!msgs.length) return { text: "No pending notifications." };
      return {
        text: msgs.map(wrap).join("\n\n"),
        after: async (delivered) => void (delivered && (await call({ op: "commit", ids: msgs.map((m) => m.id) }))),
      };
    },
  },

  ListProjects: {
    description:
      "List the registered projects. Only these names can be used as `project`; if a call fails because its " +
      "project is not registered, pick one of these (or fix the typo).",
    input: schema({}),
    run: async ({ call }) => {
      const r = await call({ op: "projects" });
      return { text: (r.projects as string[]).join("\n") || "No projects registered." };
    },
  },

  RegisterProject: {
    description:
      "Register a NEW project name, once, for everybody. Do this ONLY when your user explicitly told " +
      "you to create that project. NEVER register a project to get past an 'unknown project' error or because a " +
      "name looks right: that error means a typo or the wrong name, so call ListProjects and use an existing " +
      "project. `project` here is the new name.",
    input: schema({}),
    run: async ({ call, project }) => {
      const before = (await call({ op: "projects" })).projects as string[];
      await call({ op: "addproject", name: project });
      return { text: before.includes(project) ? `Project ${project} was already registered.` : `Registered project ${project}.` };
    },
  },

  Subscribe: topicTool("sub", "Subscribed to", "Subscribe this session to a topic, so messages sent to #topic reach it. Only future messages."),
  Unsubscribe: topicTool("unsub", "Unsubscribed from", "Unsubscribe this session from a topic."),

  ListTopics: {
    description: "List the topics that have subscribers.",
    input: schema({}),
    run: async ({ call, project }) => {
      const r = await call({ op: "topics", project });
      return { text: (r.topics as string[]).map((t) => `#${t}`).join("\n") || "No topics." };
    },
  },
};

export { AgentError, plural };
