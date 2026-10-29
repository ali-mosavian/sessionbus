import type { Frame } from "../src/client.ts";
import type { Wire } from "../src/inbox.ts";

/** A connection the test drives by hand. */
export class FakeWire implements Wire {
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
export const opener = (wires: FakeWire[]) => {
  let i = 0;
  return () => (i < wires.length ? Promise.resolve(wires[i++]!) : new Promise<Wire>(() => {}));
};

