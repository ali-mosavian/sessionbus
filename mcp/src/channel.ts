// Claude Code "channels": pushes messages from trusted senders into the running
// conversation. Everything else waits for ReadNotifications.
import type { Frame } from "./client.ts";
import { wrap } from "./format.ts";
import type { Inbox } from "./inbox.ts";

export const CHANNEL_METHOD = "notifications/claude/channel";

/** Sender names allowed to push, from a comma-separated list; empty means channels are off. */
export const allowedSenders = (list: string | undefined): Set<string> =>
  new Set((list ?? "").split(",").map((n) => n.trim()).filter(Boolean));

/** Pushes each new message whose sender is in `allow`; a push that fails leaves the message for ReadNotifications. */
export function pushTrusted(inbox: Inbox, allow: Set<string>, emit: (params: object) => Promise<boolean>): void {
  inbox.onMessage((m: Frame) => {
    if (!allow.has(m.from) || m.topic) return; // topic messages name their sender but are not individually vouched for
    const params = { content: wrap(m), meta: { from: String(m.from), id: String(m.id) } };
    inbox.hold(m.id); // before the write: a ReadNotifications racing the push must not return it too
    void emit(params)
      .then(async (ok) => (ok ? await inbox.markPushed(m.id) : inbox.release(m.id)))
      .catch(() => inbox.release(m.id));
  });
}
