// The session's standing connection to the relay. While it is up the session
// shows online, and messages are pushed into this buffer as they arrive.
// ReadNotifications hands the buffer to Claude and only then acks it: until
// then the relay keeps every message, so a crash loses nothing.
import { Conn, type Config, type Frame } from "./client.ts";

/** What the inbox needs of a connection: a `Conn` fits. */
export interface Wire {
  next(): Promise<Frame | null>;
  send(v: object): void;
  close(): void;
}

export interface InboxOptions {
  /** Messages held before the inbox stops storing and leaves the rest on the relay. */
  cap: number;
  maxBytes: number;
  /** Keep-alive: ping this often; a missing pong this long after drops the connection. */
  pingMs: number;
  pongMs: number;
  backoffMs: [min: number, max: number];
  /** How long `peek` waits for a first connection when it has nothing to return. */
  readyWaitMs: number;
}

const DEFAULTS: InboxOptions = {
  cap: 500,
  maxBytes: 16 << 20,
  pingMs: 15_000,
  pongMs: 10_000,
  backoffMs: [100, 5_000],
  readyWaitMs: 10_000,
};

export class Inbox {
  private open: () => Promise<Wire>;
  private ack: (id: number) => Promise<void>;
  private opts: InboxOptions;
  /** Unread messages by id: a replay after a reconnect lands on the same key. */
  private buf = new Map<number, Frame>();
  private bytes = 0;
  /** Highest id handed to Claude; anything at or below it is not stored again. */
  private watermark = 0;
  /** Set when the buffer was full: nothing more is stored until a reconnect replays the rest. */
  private overflowed = false;
  private wire?: Wire;
  private ready = false;
  private lastError?: Error;
  private closed = false;
  private started = false;
  /** Pushed into the conversation already: hidden from `peek`, acked once nothing unpushed precedes them. */
  private pushed = new Set<number>();
  private onStore?: (m: Frame) => void;
  private waiters: Array<() => void> = [];
  private wake?: () => void;

  constructor(open: () => Promise<Wire>, ack: (id: number) => Promise<void>, opts: Partial<InboxOptions> = {}) {
    this.open = open;
    this.ack = ack;
    this.opts = { ...DEFAULTS, ...opts };
  }

  /** Connects and keeps reconnecting until `close`. */
  start(): void {
    if (this.started) return;
    this.started = true;
    void this.run();
  }

  /** Calls `fn` for each message already held and unpushed, then for each one stored later. */
  onMessage(fn: (m: Frame) => void): void {
    this.onStore = fn;
    [...this.buf.values()].filter((m) => !this.pushed.has(m.id)).sort((a, b) => a.id - b.id).forEach(fn);
  }

  /** The unread messages, oldest first. Nothing is removed: call `commit` once Claude has them. */
  async peek(): Promise<Frame[]> {
    this.start();
    if (!this.buf.size && !this.ready) await this.untilReady();
    if (!this.buf.size && !this.ready) throw this.lastError ?? new Error("not connected to the relay");
    return [...this.buf.values()].filter((m) => !this.pushed.has(m.id)).sort((a, b) => a.id - b.id);
  }

  /** Hides `id` from `peek` while a push of it is in flight, so a concurrent read cannot hand it out too. */
  hold(id: number): void {
    this.pushed.add(id);
  }

  /** The push of `id` failed: offer it to `peek` again. */
  release(id: number): void {
    this.pushed.delete(id);
  }

  /** `id` reached Claude by push. The relay is told once no unread message precedes it. */
  async markPushed(id: number): Promise<void> {
    this.hold(id);
    const ids = [...this.buf.keys()].sort((a, b) => a - b);
    const firstUnpushed = ids.find((i) => !this.pushed.has(i));
    const covered = ids.filter((i) => firstUnpushed === undefined || i < firstUnpushed);
    if (covered.length) await this.commit(covered[covered.length - 1]!);
  }

  /** Claude has everything up to `last`: ack it on the relay, then forget it here. */
  async commit(last: number): Promise<void> {
    await this.ack(last);
    for (const [id, m] of this.buf) {
      if (id > last) continue;
      this.bytes -= size(m);
      this.buf.delete(id);
      this.pushed.delete(id);
    }
    this.watermark = Math.max(this.watermark, last);
    if (this.overflowed) {
      this.overflowed = false;
      this.wire?.close(); // the reconnect replays whatever was not stored
    }
  }

  close(): void {
    this.closed = true;
    this.wire?.close();
    this.wake?.();
    this.settle();
  }

  private store(m: Frame): void {
    if (m.id <= this.watermark || this.buf.has(m.id) || this.overflowed) return;
    if (this.buf.size >= this.opts.cap || this.bytes + size(m) > this.opts.maxBytes) {
      this.overflowed = true;
      return;
    }
    this.buf.set(m.id, m);
    this.bytes += size(m);
    this.onStore?.(m);
  }

  private async run(): Promise<void> {
    const [min, max] = this.opts.backoffMs;
    let delay = min;
    while (!this.closed) {
      try {
        const wire = await this.open();
        if (this.closed) return wire.close();
        this.wire = wire;
        this.lastError = undefined;
        delay = min;
        await this.pump(wire);
      } catch (e) {
        this.lastError = e as Error;
      }
      this.wire = undefined;
      this.ready = false;
      if (this.closed) return;
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, delay).unref();
        this.wake = () => (clearTimeout(t), resolve());
      });
      delay = Math.min(delay * 2, max);
    }
  }

  private async pump(wire: Wire): Promise<void> {
    let pong: NodeJS.Timeout | undefined;
    const ping = setInterval(() => {
      wire.send({ op: "ping" });
      pong ??= setTimeout(() => wire.close(), this.opts.pongMs).unref();
    }, this.opts.pingMs).unref();
    try {
      for (;;) {
        const f = await wire.next();
        if (!f) return;
        if (f.op === "msg") this.store(f);
        else if (f.op === "ready") (this.ready = true), this.settle();
        else if (f.op === "pong") (clearTimeout(pong), (pong = undefined));
        else if (f.op === "error") throw Object.assign(new Error(f.error), { code: f.code });
      }
    } finally {
      clearInterval(ping);
      clearTimeout(pong);
      wire.close();
    }
  }

  private untilReady(): Promise<void> {
    return new Promise((resolve) => {
      const t = setTimeout(resolve, this.opts.readyWaitMs).unref();
      this.waiters.push(() => (clearTimeout(t), resolve()));
    });
  }

  private settle(): void {
    this.waiters.splice(0).forEach((resolve) => resolve());
  }
}

const size = (m: Frame): number => String(m.body ?? "").length + String(m.summary ?? "").length + 200;

/** The inbox of `cfg`'s session: a listening connection, acked over short plain ones. */
export const relayInbox = (cfg: Config, opts: Partial<InboxOptions> = {}): Inbox =>
  new Inbox(
    () => Conn.open(cfg, true),
    async (id) => {
      const c = await Conn.open(cfg, false);
      try {
        await c.ackSync(id);
      } finally {
        c.close();
      }
    },
    opts,
  );
