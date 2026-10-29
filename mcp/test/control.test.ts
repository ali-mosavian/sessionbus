import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { after, test } from "node:test";

import type { Config } from "../src/client.ts";
import { socketPath, startControl } from "../src/control.ts";
import { Inbox } from "../src/inbox.ts";
import { Inboxes } from "../src/inboxes.ts";
import { FakeWire, opener } from "./fakewire.ts";

const dir = mkdtempSync(join(tmpdir(), "sessionbus-ctl-"));
const cfg: Config = { addr: "x", name: "me", keyPath: "x" };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const servers: Array<{ close(): void }> = [];
after(() => servers.forEach((s) => s.close()));

/** A session whose project inbox is `wire`, with its control socket under `pid`. */
function session(pid: number) {
  const wire = new FakeWire();
  const acked: number[] = [];
  const inboxes = new Inboxes(cfg, () => new Inbox(opener([wire]), async (id) => void acked.push(id), { backoffMs: [5, 20], readyWaitMs: 500 }));
  inboxes.get("p", "me");
  servers.push(startControl(inboxes, dir, pid));
  return { wire, acked, inboxes };
}

/** One control connection. */
async function client(pid: number) {
  const sock = connect(socketPath(dir, pid));
  sock.on("error", () => {});
  const lines = createInterface({ input: sock })[Symbol.asyncIterator]();
  return {
    sock,
    ask: async (op: string) => (sock.write(JSON.stringify({ op }) + "\n"), JSON.parse((await lines.next()).value)),
  };
}

test("wait returns once a message arrives, commit acks it, and it is not offered again", { timeout: 5000 }, async () => {
  const s = session(910001);
  s.wire.ready();
  const c = await client(910001);
  const waiting = c.ask("wait"); // nothing yet: it blocks
  await sleep(30);
  s.wire.msg(7, "hello");
  const got = await waiting;
  assert.equal(got.op, "messages");
  assert.deepEqual(got.messages.map((m: any) => [m.id, m.project, m.body]), [[7, "p", "hello"]]);
  assert.deepEqual(s.inboxes.all()[0]![1].pending(), [], "a message handed to the hook was offered to a read too");
  await c.ask("commit");
  assert.deepEqual(s.acked, [7]);
  c.sock.destroy();
});

test("a hook that disappears before commit gives the message back", { timeout: 5000 }, async () => {
  const s = session(910002);
  s.wire.msg(3).ready();
  const c = await client(910002);
  assert.equal((await c.ask("wait")).messages[0].id, 3);
  c.sock.destroy(); // the hook died before printing
  await sleep(50);
  assert.deepEqual(s.inboxes.all()[0]![1].pending().map((m) => m.id), [3], "the message was lost with the hook");
  assert.deepEqual(s.acked, []);
});

test("a second waiter is told it is busy", { timeout: 5000 }, async () => {
  const s = session(910003);
  s.wire.ready();
  const [a, b] = [await client(910003), await client(910003)];
  void a.ask("wait");
  await sleep(30);
  assert.equal((await b.ask("wait")).op, "busy");
  a.sock.destroy();
  b.sock.destroy();
});

test("sessionbus-wait finds the session's socket by its ancestor, prints the message on stderr and exits 2", { timeout: 15000 }, async () => {
  const s = session(process.pid); // the waiter's parent is this test process
  s.wire.ready();
  const child = spawn(process.execPath, [join(import.meta.dirname, "../src/wait.ts")], {
    env: { ...process.env, SESSIONBUS_RUN_DIR: dir },
    stdio: ["ignore", "ignore", "pipe"],
  });
  let err = "";
  child.stderr.on("data", (d) => (err += d));
  const exit = new Promise<number | null>((res) => child.on("close", res));
  await sleep(300);
  s.wire.msg(11, "wake up");
  assert.equal(await exit, 2, "the waiter must exit 2, which is what wakes an idle session");
  assert.match(err, /1 new message for this session/);
  assert.match(err, /<cross-session-message via="sessionbus" project="p" from="a" id="11">\nwake up\n/);
  await sleep(50);
  assert.deepEqual(s.acked, [11]);
});

test("the MCP process exits when its stdin closes, and removes its socket", { timeout: 8000 }, async () => {
  const run = mkdtempSync(join(tmpdir(), "sessionbus-exit-"));
  const child = spawn(process.execPath, [join(import.meta.dirname, "../src/index.ts")], {
    env: { ...process.env, SESSIONBUS_RUN_DIR: run, SESSIONBUS_ADDR: "127.0.0.1:1" },
    stdio: ["pipe", "ignore", "ignore"],
  });
  const exited = new Promise((res) => child.on("close", res));
  await sleep(500);
  child.stdin.end();
  const code = await Promise.race([exited, sleep(4000).then(() => "still running after stdin closed")]);
  if (code === "still running after stdin closed") child.kill();
  assert.equal(code, 0, "an orphaned MCP process stays alive holding its control socket");
  assert.deepEqual((await import("node:fs")).readdirSync(run), [], "the control socket was left behind");
});
