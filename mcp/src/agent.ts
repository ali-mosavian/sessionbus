// Client for the local sessionbus agent: one request per unix-socket connection.
// The agent finds the calling session from this process's ancestry, so nothing
// here says who the caller is.
import { connect } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";

export type Frame = Record<string, any>;

export class AgentError extends Error {
  code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

/** Where to reach the agent, and (tests, debugging) which session to speak as instead of the one found from the process tree. */
export interface Target {
  sock?: string;
  session?: string;
  timeoutMs?: number;
}

export const socketPath = (): string => process.env.SESSIONBUS_SOCK ?? join(homedir(), ".sessionbus", "agent.sock");

/** Sends `req` and returns the reply; `timeoutMs` bounds the wait for it. */
export function call(req: Frame, { sock = socketPath(), session = process.env.SESSIONBUS_SESSION, timeoutMs = 5000 }: Target = {}): Promise<Frame> {
  return new Promise((resolve, reject) => {
    const s = connect(sock);
    let buf = "";
    const timer = setTimeout(() => fail(new AgentError("timeout", "the sessionbus agent did not answer in time")), timeoutMs);
    const fail = (e: Error) => {
      clearTimeout(timer);
      s.destroy();
      reject(e);
    };
    s.setEncoding("utf8");
    s.on("connect", () => s.write(JSON.stringify(session ? { session, ...req } : req) + "\n"));
    s.on("error", (e: NodeJS.ErrnoException) =>
      fail(
        e.code === "ENOENT" || e.code === "ECONNREFUSED"
          ? new AgentError("agent_down", `the sessionbus agent is not running (no socket at ${sock}): start it with \`sessionbus agent\``)
          : e,
      ),
    );
    s.on("close", () => fail(new AgentError("closed", "the sessionbus agent closed the connection")));
    s.on("data", (d: string) => {
      buf += d;
      const nl = buf.indexOf("\n");
      if (nl < 0) return;
      clearTimeout(timer);
      s.destroy();
      const r = JSON.parse(buf.slice(0, nl));
      if (r.error) reject(new AgentError(r.error.code, r.error.message));
      else resolve(r);
    });
  });
}
