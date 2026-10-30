#!/usr/bin/env node
// Claude Code hook (async, asyncRewake): registers this session with the local
// agent, then waits for a message for it, prints it on stderr and exits 2,
// which wakes Claude even when idle. Exits 0 quietly when there is nothing to
// wait on or a newer waiter took over.
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { AgentError, call, type Target } from "./agent.ts";
import { render } from "./format.ts";

const stdin = async (): Promise<Record<string, any>> => {
  let s = "";
  for await (const c of process.stdin) s += c;
  try {
    return JSON.parse(s);
  } catch {
    return {};
  }
};

const WAIT_MS = 10 * 60 * 1000;

/** What to do after the hook ran: the exit code, and text for Claude when it is 2. */
export async function main(hook: Record<string, any>, target: Target = {}): Promise<{ code: number; text?: string }> {
  try {
    if (typeof hook.session_id === "string") await call({ op: "register", session: hook.session_id, cwd: hook.cwd }, target);
    for (;;) {
      const r = await call({ op: "wait", exclusive: true, timeout_ms: WAIT_MS }, { ...target, timeoutMs: WAIT_MS + 5000 });
      if (r.superseded) return { code: 0 };
      const messages = r.messages as Array<Record<string, any>>;
      if (!messages.length) continue;
      await call({ op: "commit", ids: messages.map((m) => m.id) }, target);
      return { code: 2, text: render(messages) };
    }
  } catch (e) {
    if (e instanceof AgentError && (e.code === "agent_down" || e.code === "closed")) return { code: 0 };
    throw e;
  }
}

// `import.meta.main` needs a newer Node than we support
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { code, text } = await main(await stdin());
  if (text) process.stderr.write(text + "\n");
  process.exit(code);
}
