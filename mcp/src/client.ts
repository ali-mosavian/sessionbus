// Relay client: newline-delimited JSON over TCP, authenticated by signing the
// relay's nonce with an ssh key.
import { connect as tcp, type Socket } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";

import { loadKey, sshSign } from "./ssh.ts";

export interface Config {
  addr: string;
  name?: string;
  keyPath: string;
}

export function configFromEnv(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    addr: env.SESSIONBUS_ADDR ?? "127.0.0.1:8787",
    name: env.SESSIONBUS_NAME,
    keyPath: env.SESSIONBUS_KEY ?? join(homedir(), ".ssh", "id_ed25519"),
  };
}

export type Frame = { op: string; [k: string]: any };

const NAMESPACE = "sessionbus";
const CONNECT_TIMEOUT_MS = 10_000;

export class Conn {
  sock: Socket;
  lines: AsyncIterator<string>;

  constructor(sock: Socket) {
    this.sock = sock;
    this.lines = createInterface({ input: sock, crlfDelay: Infinity })[Symbol.asyncIterator]();
    sock.on("error", () => {}); // surfaces as a closed connection on the next read
  }

  /** Connects and authenticates. `listen` connections also receive queued messages. */
  static async open(cfg: Config, listen: boolean): Promise<Conn> {
    if (!cfg.name) throw new Error("SESSIONBUS_NAME not set");
    const key = loadKey(cfg.keyPath);
    const i = cfg.addr.lastIndexOf(":");
    const host = cfg.addr.slice(0, i).replace(/^\[|\]$/g, "");
    const sock = await new Promise<Socket>((resolve, reject) => {
      const s = tcp({ host, port: Number(cfg.addr.slice(i + 1)) });
      const t = setTimeout(() => s.destroy(new Error("connect timeout")), CONNECT_TIMEOUT_MS);
      s.once("connect", () => {
        clearTimeout(t);
        resolve(s);
      });
      s.once("error", (e) => {
        clearTimeout(t);
        reject(new Error(`${cfg.addr}: ${e.message}`));
      });
    });
    const c = new Conn(sock);
    try {
      const ch = await c.next();
      if (!ch?.nonce) throw new Error("closed before challenge");
      const sig = sshSign(key, NAMESPACE, `sessionbus-v1:${ch.nonce}:${cfg.name}`);
      c.send({ op: "hello", name: cfg.name, sig, listen });
      const r = await c.next();
      if (r?.op !== "ok") throw new Error(r?.error ?? "auth failed");
      return c;
    } catch (e) {
      c.close();
      throw e;
    }
  }

  send(v: object): void {
    this.sock.write(JSON.stringify(v) + "\n");
  }

  /** Next frame, or null once the relay closed the connection. */
  async next(): Promise<Frame | null> {
    const r = await this.lines.next();
    return r.done ? null : (JSON.parse(r.value) as Frame);
  }

  /** One request, one reply. Not for `listen` connections, whose stream also carries pushes. */
  async call(req: object): Promise<Frame> {
    this.send(req);
    const r = await this.next();
    if (!r) throw new Error("connection closed");
    if (r.op === "error") throw new Error(r.error);
    return r;
  }

  /** Queued messages, oldest first. They stay queued until acked. */
  async inbox(): Promise<Frame[]> {
    const out: Frame[] = [];
    for (;;) {
      const f = await this.next();
      if (!f) throw new Error("connection closed");
      if (f.op === "msg") out.push(f);
      else if (f.op === "ready") return out;
      else if (f.op === "error") throw new Error(f.error);
    }
  }

  /** Acks up to `id` and returns once the relay applied it: a later reply on one connection proves the earlier ack. */
  async ackSync(id: number): Promise<void> {
    this.send({ op: "ack", id });
    this.send({ op: "topics" });
    for (;;) {
      const f = await this.next();
      if (!f) throw new Error("connection closed");
      if (f.op === "topics") return;
      if (f.op === "error") throw new Error(f.error);
    }
  }

  close(): void {
    this.sock.destroy();
  }
}
