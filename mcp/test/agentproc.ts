// Starts the real agent binary for tests: build it first with `cargo build` (or set SESSIONBUS_BIN).
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dirname, "../..");
const bin = [process.env.SESSIONBUS_BIN, join(root, "target/debug/sessionbus"), join(root, "target/release/sessionbus")]
  .filter((p): p is string => !!p)
  .find(existsSync);
assert.ok(bin, "binary not found: run `cargo build` in the repo root");

export interface TestAgent {
  sock: string;
  /** host:port other agents reach this one at */
  addr: string;
  stop(): void;
  /** Starts it again on the same state and address. */
  restart(): Promise<void>;
}

export const freePort = () =>
  new Promise<number>((res) => {
    const s = createServer().listen(0, "127.0.0.1", () => {
      const p = (s.address() as { port: number }).port;
      s.close(() => res(p));
    });
  });

/** An agent whose clients may name their own session; `peers` are `host=addr` pairs. */
export async function startAgent(host: string, peers: string[] = [], port?: number): Promise<TestAgent> {
  const dir = mkdtempSync(join(tmpdir(), "sb-"));
  const sock = join(dir, "agent.sock");
  const addr = `127.0.0.1:${port ?? (await freePort())}`;
  let p: ChildProcess;
  const start = async () => {
    const args = ["agent", "--dir", dir, "--host", host, "--listen", addr, "--advertise", addr, "--trust-clients", "--retry-ms", "100", ...peers.flatMap((x) => ["--peer", x])];
    p = spawn(bin!, args, { stdio: "ignore" });
    for (let i = 0; !existsSync(sock); i++) {
      assert.ok(i < 100, "agent did not start");
      await new Promise((r) => setTimeout(r, 20));
    }
  };
  await start();
  const stop = () => {
    p.kill();
    rmSync(sock, { force: true });
  };
  return { sock, addr, stop, restart: start };
}
