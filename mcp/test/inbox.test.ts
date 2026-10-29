import assert from "node:assert/strict";
import { test } from "node:test";

import type { Frame } from "../src/client.ts";
import { Inbox, type Wire } from "../src/inbox.ts";

/** A connection the test drives by hand. */
class FakeWire implements Wire {
  queue: Array<Frame | null> = [];
  waiting?: (f: Frame | null) => void;
  sent: any[] = [];
  closed = false;

  push(f: Frame | null): void {
    const w = this.waiting;
    if (w) {
      this.waiting = undefined;
      w(f);
    } else this.queue.push(f);
  }
  next(): Promise<Frame | null> {
    if (this.queue.length) return Promise.resolve(this.queue.shift()!);
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => (this.waiting = resolve));
  }
  send(v: object): void {
    this.sent.push(v);
  }
  close(): void {
    this.closed = true;
    const w = this.waiting;
    this.waiting = undefined;
    w?.(null);
  }
  msg(id: number, body = `m${id}`): this {
    this.push({ op: "msg", id, from: "a", body });
    return this;
  }
  ready(): this {
    this.push({ op: "ready" });
    return this;
  }
}

/** Opens the given wires in turn; further opens wait. */
const opener = (wires: FakeWire[]) => {
  let i = 0;
  return () => (i < wires.length ? Promise.resolve(wires[i++]!) : new Promise<Wire>(() => {}));
};

const ids = (ms: Frame[]) => ms.map((m) => m.id);
const FAST = { backoffMs: [5, 20] as [number, number], readyWaitMs: 500 };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const noAck = async () => {};

test("messages buffered across a dropped connection arrive once", { timeout: 5000 }, async () => {
  const [w1, w2] = [new FakeWire(), new FakeWire()];
  const inbox = new Inbox(opener([w1, w2]), noAck, FAST);
  w1.msg(1).ready();
  assert.deepEqual(ids(await inbox.peek()), [1]);

  w1.push(null); // the connection drops; the relay will replay everything unacked
  w2.msg(1).msg(2).ready();
  await sleep(60);
  assert.deepEqual(ids(await inbox.peek()), [1, 2], "a replayed message was duplicated or lost");

  await inbox.commit(1);
  w2.msg(1).msg(3);
  await sleep(20);
  assert.deepEqual(ids(await inbox.peek()), [2, 3], "a message already handed to Claude came back");
  inbox.close();
});

test("a full buffer loses nothing", { timeout: 5000 }, async () => {
  const [w1, w2] = [new FakeWire(), new FakeWire()];
  const acked: number[] = [];
  const inbox = new Inbox(opener([w1, w2]), async (id) => void acked.push(id), { ...FAST, cap: 2 });
  w1.msg(1).msg(2).msg(3).ready();
  await sleep(20);
  w1.msg(4); // must not be stored past the gap at 3: acking 4 would drop 3
  await sleep(20);
  assert.deepEqual(ids(await inbox.peek()), [1, 2]);

  await inbox.commit(2);
  assert.deepEqual(acked, [2]);
  assert.ok(w1.closed, "the inbox must reconnect so the relay replays what did not fit");
  w2.msg(3).msg(4).ready();
  await sleep(40);
  assert.deepEqual(ids(await inbox.peek()), [3, 4]);
  inbox.close();
});

test("a failed ack keeps the messages for the next read", { timeout: 5000 }, async () => {
  const w = new FakeWire();
  const inbox = new Inbox(opener([w]), async () => Promise.reject(new Error("relay down")), FAST);
  w.msg(1).ready();
  assert.deepEqual(ids(await inbox.peek()), [1]);
  await assert.rejects(inbox.commit(1), /relay down/);
  assert.deepEqual(ids(await inbox.peek()), [1], "dropped before the relay had them acked");
  inbox.close();
});

test("a missing pong drops the connection; a pong keeps it", { timeout: 5000 }, async () => {
  const [quiet, lively] = [new FakeWire(), new FakeWire()];
  const a = new Inbox(opener([quiet]), noAck, { ...FAST, pingMs: 30, pongMs: 60 });
  quiet.ready();
  await a.peek();
  await sleep(250);
  assert.ok(quiet.sent.some((v) => v.op === "ping"), "never pinged");
  assert.ok(quiet.closed, "a connection that never answers was kept");
  a.close();

  const b = new Inbox(opener([lively]), noAck, { ...FAST, pingMs: 30, pongMs: 60 });
  lively.ready();
  await b.peek();
  for (let i = 0; i < 8; i++) {
    await sleep(40);
    lively.push({ op: "pong" });
  }
  assert.ok(!lively.closed, "a connection that answers was dropped");
  b.close();
});

test("a name that stays taken surfaces its reason, and recovers when freed", { timeout: 5000 }, async () => {
  const w = new FakeWire();
  const keepAlive = setTimeout(() => {}, 3000); // inbox timers are unref'd
  let calls = 0;
  const inbox = new Inbox(
    () => (++calls < 4 ? Promise.reject(Object.assign(new Error("already connected"), { code: "name_in_use" })) : Promise.resolve(w)),
    noAck,
    { ...FAST, readyWaitMs: 15 },
  );
  await assert.rejects(inbox.peek(), /already connected/);
  w.msg(7).ready();
  await sleep(80);
  assert.deepEqual(ids(await inbox.peek()), [7]);
  clearTimeout(keepAlive);
  inbox.close();
});

test("a message too big for the buffer is not skipped over by a smaller one", { timeout: 5000 }, async () => {
  const w = new FakeWire();
  const inbox = new Inbox(opener([w]), noAck, { ...FAST, maxBytes: 600 });
  w.msg(1).msg(2, "x".repeat(1000)).msg(3).ready();
  await sleep(20);
  assert.deepEqual(ids(await inbox.peek()), [1], "stored 3 past the gap at 2: acking 3 would lose 2");
  inbox.close();
});

test("a pushed message is hidden from reads and acked only behind nothing unread", { timeout: 5000 }, async () => {
  const w = new FakeWire();
  const acked: number[] = [];
  const inbox = new Inbox(opener([w]), async (id) => void acked.push(id), FAST);
  w.msg(1).msg(2).msg(3).ready();
  assert.deepEqual(ids(await inbox.peek()), [1, 2, 3]);

  await inbox.markPushed(2);
  assert.deepEqual(acked, [], "acked 2 while 1 was still unread: 1 would be lost");
  assert.deepEqual(ids(await inbox.peek()), [1, 3], "a pushed message was offered again");

  await inbox.markPushed(1);
  assert.deepEqual(acked, [2], "acked later than needed");
  assert.deepEqual(ids(await inbox.peek()), [3]);
  inbox.close();
});
