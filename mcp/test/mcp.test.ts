import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { createInterface } from "node:readline";
import { after, before, test } from "node:test";

import { call } from "../src/agent.ts";
import { runtime, serve } from "../src/server.ts";
import { main as waitHook } from "../src/wait.ts";
import { startAgent, type TestAgent } from "./agentproc.ts";

let agent: TestAgent;
before(async () => {
  agent = await startAgent("t1");
  await call({ op: "addproject", name: "t" }, { sock: agent.sock, session: "setup" });
});
after(() => agent.stop());

/** The ack follows the reply on the same connection, so effects of a read show a moment later. */
async function eventually(f: () => Promise<boolean>) {
  for (let i = 0; i < 50 && !(await f()); i++) await new Promise((r) => setTimeout(r, 20));
  assert.ok(await f(), "condition not reached");
}

/** An MCP session speaking as `session`; `rpc` returns the text of a tool reply. */
function mcp(session: string, sock = agent.sock) {
  const input = new PassThrough();
  const output = new PassThrough();
  const done = serve(runtime({ sock, session }), input, output);
  const lines = createInterface({ input: output })[Symbol.asyncIterator]();
  let id = 0;
  const raw = async (method: string, params: object = {}) => {
    input.write(JSON.stringify({ jsonrpc: "2.0", id: ++id, method, params }) + "\n");
    return JSON.parse((await lines.next()).value);
  };
  const tool = async (name: string, args: object) => {
    const r = await raw("tools/call", { name, arguments: args });
    return { text: r.result.content[0].text as string, isError: r.result.isError as boolean };
  };
  return { raw, tool, close: () => (input.end(), done) };
}

test("every tool requires a project, and `as` is optional", async () => {
  const m = mcp("l1");
  const { result } = await m.raw("tools/list");
  for (const t of result.tools) {
    assert.ok(t.inputSchema.required.includes("project"), `${t.name} does not require project`);
    assert.ok(!t.inputSchema.required.includes("as"));
  }
  await m.close();
});

test("a message goes from one session to another, wrapped with a full reply address, and is read once", async () => {
  const [a, b] = [mcp("s-a"), mcp("s-b")];
  await a.tool("ListAgents", { project: "t", as: "alice" });
  await b.tool("ListAgents", { project: "t", as: "bob" });
  const sent = await a.tool("SendMessage", { project: "t", to: "bob", message: "hello <b>", summary: "greeting" });
  assert.equal(sent.isError, false, sent.text);
  const id = sent.text.match(/id ([0-9a-f-]{36})/)![1];
  const got = await b.tool("ReadNotifications", { project: "t" });
  assert.match(got.text, /via="sessionbus"/);
  assert.match(got.text, /from="alice@t1"/);
  assert.match(got.text, new RegExp(`id="${id}"`));
  assert.ok(got.text.includes("hello &lt;b>"), "markup in a body must be neutralized");
  await eventually(async () => (await b.tool("ReadNotifications", { project: "t" })).text === "No pending notifications.");
  await eventually(async () => /bob@t1: read/.test((await a.tool("MessageStatus", { project: "t", ids: [id] })).text));
  await Promise.all([a.close(), b.close()]);
});

/** A reply that never reached Claude must not count as read. */
test("messages stay unread when the reply could not be written", async () => {
  const [a, b] = [mcp("s-c"), mcp("s-d")];
  await a.tool("ListAgents", { project: "t", as: "carol" });
  await b.tool("ListAgents", { project: "t", as: "dave" });
  await a.tool("SendMessage", { project: "t", to: "dave", message: "keep" });
  const input = new PassThrough();
  const output = new PassThrough();
  output.destroy(); // the reader is gone
  const dead = serve(runtime({ sock: agent.sock, session: "s-d" }), input, output);
  input.write(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "ReadNotifications", arguments: { project: "t" } } }) + "\n");
  input.end();
  await dead;
  assert.match((await b.tool("ReadNotifications", { project: "t" })).text, /keep/);
  await Promise.all([a.close(), b.close()]);
});

test("a project that is not registered is refused with the registered ones listed", async () => {
  const m = mcp("l2");
  const r = await m.tool("SendMessage", { project: "typo", to: "x", message: "hi" });
  assert.equal(r.isError, true);
  assert.match(r.text, /not registered; registered: \[.*\bt\b/);
  await m.close();
});

test("a taken name is refused", async () => {
  const [a, b] = [mcp("s-e"), mcp("s-f")];
  await a.tool("ListAgents", { project: "t", as: "erin" });
  const r = await b.tool("ListAgents", { project: "t", as: "erin" });
  assert.equal(r.isError, true);
  assert.match(r.text, /taken/);
  await Promise.all([a.close(), b.close()]);
});

test("with no agent running the tool says how to start it", async () => {
  const m = mcp("l3", "/tmp/sb-nonexistent.sock");
  const r = await m.tool("ListAgents", { project: "t" });
  assert.equal(r.isError, true);
  assert.match(r.text, /agent is not running.*sessionbus agent/);
  await m.close();
});

test("the wake hook registers the session, waits, hands over the message and marks it read", async () => {
  const [a, b] = [mcp("s-g"), mcp("s-h")];
  await a.tool("ListAgents", { project: "t", as: "gina" });
  await b.tool("ListAgents", { project: "t", as: "hank" });
  const waiting = waitHook({ session_id: "s-h" }, { sock: agent.sock, session: "s-h" });
  await new Promise((r) => setTimeout(r, 100));
  await a.tool("SendMessage", { project: "t", to: "hank", message: "wake up" });
  const { code, text } = await waiting;
  assert.equal(code, 2);
  assert.match(text!, /wake up/);
  assert.equal((await b.tool("ReadNotifications", { project: "t" })).text, "No pending notifications.");
  await Promise.all([a.close(), b.close()]);
});

test("the wake hook exits quietly when no agent runs", async () => {
  const r = await waitHook({ session_id: "s-i" }, { sock: "/tmp/sb-nonexistent.sock" });
  assert.deepEqual(r, { code: 0 });
});

/** Two real agents on two "hosts": a message sent while the other host is down is delivered, once, when it returns. */
test("a message to a host that is down is delivered when the host comes back", async () => {
  const port2 = 43000 + Math.floor(Math.random() * 2000);
  const h2 = await startAgent("h2", [], port2);
  const h1 = await startAgent("h1", [`h2=127.0.0.1:${port2}`]);
  for (const a of [h1, h2]) await call({ op: "addproject", name: "t" }, { sock: a.sock, session: "setup" });
  try {
    const [alice, bob] = [mcp("e-a", h1.sock), mcp("e-b", h2.sock)];
    await alice.tool("ListAgents", { project: "t", as: "alice" });
    await bob.tool("ListAgents", { project: "t", as: "bob" });
    await eventually(async () => /bob@h2/.test((await alice.tool("ListAgents", { project: "t" })).text));
    h2.stop();
    const sent = await alice.tool("SendMessage", { project: "t", to: "bob", message: "while you were out" });
    assert.match(sent.text, /bob@h2 \(id [0-9a-f-]{36}, queued\)/);
    const id = sent.text.match(/id ([0-9a-f-]{36})/)![1];
    await h2.restart();
    await eventually(async () => /while you were out/.test((await bob.tool("ReadNotifications", { project: "t" })).text));
    await eventually(async () => /bob@h2: read/.test((await alice.tool("MessageStatus", { project: "t", ids: [id] })).text));
    assert.equal((await bob.tool("ReadNotifications", { project: "t" })).text, "No pending notifications.");
    await Promise.all([alice.close(), bob.close()]);
  } finally {
    h1.stop();
    h2.stop();
  }
});

/** Every successful send carried a sentence explaining "queued", which read like a failure. */
test("a delivered send does not explain queueing, and a topic send names its subscribers", async () => {
  const [a, b, c] = [mcp("s-j"), mcp("s-k"), mcp("s-l")];
  await a.tool("ListAgents", { project: "t", as: "jo" });
  await b.tool("ListAgents", { project: "t", as: "kim" });
  await c.tool("ListAgents", { project: "t", as: "lee" });
  const direct = await a.tool("SendMessage", { project: "t", to: "kim", message: "hi" });
  assert.match(direct.text, /delivered/);
  assert.doesNotMatch(direct.text, /keeps trying/);
  const none = await a.tool("SendMessage", { project: "t", to: "#empty", message: "anyone?" });
  assert.match(none.text, /NOT delivered: nobody is subscribed/);
  await b.tool("Subscribe", { project: "t", topic: "news" });
  await c.tool("Subscribe", { project: "t", topic: "news" });
  const fan = await a.tool("SendMessage", { project: "t", to: "#news", message: "all" });
  assert.match(fan.text, /#news has 2 subscribers/);
  await Promise.all([a.close(), b.close(), c.close()]);
});

test("ListAgents says when each session was last seen", async () => {
  const [a, b] = [mcp("s-m"), mcp("s-n")];
  await a.tool("ListAgents", { project: "t", as: "mia" });
  await b.tool("ListAgents", { project: "t", as: "ned" });
  assert.match((await a.tool("ListAgents", { project: "t" })).text, /ned@t1 — seen \d+s ago/);
  await Promise.all([a.close(), b.close()]);
});

/** A timeout used to say only "did not answer", leaving the sender to guess whether the message was sent. */
test("a timeout says the request may have been applied", async () => {
  const sock = join(mkdtempSync(join(tmpdir(), "sb-")), "mute.sock");
  const mute = createServer(() => {}).listen(sock);
  try {
    await assert.rejects(call({ op: "projects" }, { sock, timeoutMs: 100 }), /may or may not have been applied.*MessageStatus/);
  } finally {
    mute.close();
  }
});
