// Starts the real agent binary for tests: build it first with `cargo build` (or set SESSIONBUS_BIN).
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dirname, "../..");
const bin = [process.env.SESSIONBUS_BIN, join(root, "target/debug/sessionbus"), join(root, "target/release/sessionbus")]
  .filter((p): p is string => !!p)
  .find(existsSync);
assert.ok(bin, "binary not found: run `cargo build` in the repo root");

export interface TestAgent {
  sock: string;
  stop(): void;
}

/** An agent whose clients may name their own session. */
export async function startAgent(host: string): Promise<TestAgent> {
  const dir = mkdtempSync(join(tmpdir(), "sb-"));
  const sock = join(dir, "agent.sock");
  const p: ChildProcess = spawn(bin!, ["agent", "--dir", dir, "--host", host, "--listen", "127.0.0.1:0", "--trust-clients"], { stdio: "ignore" });
  for (let i = 0; !existsSync(sock); i++) {
    assert.ok(i < 100, "agent did not start");
    await new Promise((r) => setTimeout(r, 20));
  }
  return { sock, stop: () => void p.kill() };
}
