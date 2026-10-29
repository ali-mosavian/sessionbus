// A local socket that lets a hook process reach this session's inboxes. Claude
// Code runs the hook (`sessionbus-wait`) as a sibling of this MCP server: it
// finds the socket by the pid of their common ancestor, the claude process.
import { mkdirSync, rmSync } from "node:fs";
import { createServer, type Server, type Socket } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";

import type { Frame } from "./client.ts";
import type { Inbox } from "./inbox.ts";
import type { Inboxes } from "./inboxes.ts";

export const runDir = (env: NodeJS.ProcessEnv = process.env): string =>
  env.SESSIONBUS_RUN_DIR ?? join(homedir(), ".sessionbus", "run");

export const socketPath = (dir: string, pid: number): string => join(dir, `${pid}.sock`);

type Held = Array<{ inbox: Inbox; id: number }>;

/**
 * Serves `wait`: block until some inbox has an unread message, hand over every
 * pending one and hold them; `commit` acks what the hook printed, and a hook
 * that vanishes first releases them. One waiter at a time: a second is `busy`.
 */
export function startControl(inboxes: Inboxes, dir = runDir(), pid = process.ppid): Server {
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const path = socketPath(dir, pid);
  rmSync(path, { force: true }); // a dead session's leftover
  let waiting = false;
  let nudge: (() => void) | undefined;
  inboxes.tap(() => nudge?.());

  const pendingNow = () => inboxes.all().flatMap(([project, inbox]) => inbox.pending().map((m) => ({ project, inbox, m })));

  const serve = async (sock: Socket) => {
    sock.on("error", () => {});
    let held: Held = [];
    let closed = false;
    sock.on("close", () => {
      closed = true;
      if (waiting && !held.length) nudge?.();
      held.forEach(({ inbox, id }) => inbox.release(id));
    });
    const send = (v: object) => sock.write(JSON.stringify(v) + "\n");
    for await (const line of createInterface({ input: sock, crlfDelay: Infinity })) {
      const req = JSON.parse(line) as { op: string };
      if (req.op === "wait") {
        if (waiting) {
          send({ op: "busy" });
          continue;
        }
        waiting = true;
        try {
          let found = pendingNow();
          while (!found.length && !closed) {
            await new Promise<void>((resolve) => (nudge = resolve));
            found = pendingNow();
          }
          nudge = undefined;
          if (closed) return;
          found.forEach(({ inbox, m }) => inbox.hold(m.id));
          held = found.map(({ inbox, m }) => ({ inbox, id: m.id as number }));
          send({ op: "messages", messages: found.map(({ project, m }) => ({ ...m, project }) as Frame) });
        } finally {
          waiting = false;
        }
      } else if (req.op === "commit") {
        for (const { inbox, id } of held) await inbox.markPushed(id);
        held = [];
        send({ op: "ok" });
      }
    }
  };

  const server = createServer((s) => void serve(s).catch(() => s.destroy()));
  server.on("error", () => {}); // no control socket: the hooks simply find nothing to wait on
  server.listen(path);
  server.on("close", () => rmSync(path, { force: true }));
  return server;
}
