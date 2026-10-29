// OpenSSH ed25519 private key loading and SSHSIG signing, matching what the
// relay verifies (namespace "sessionbus", sha512).
import { createHash, createPrivateKey, sign, type KeyObject } from "node:crypto";
import { readFileSync } from "node:fs";

const u32 = (n: number): Buffer => {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(n);
  return b;
};

const str = (v: Buffer | string): Buffer => {
  const d = typeof v === "string" ? Buffer.from(v) : v;
  return Buffer.concat([u32(d.length), d]);
};

class Reader {
  buf: Buffer;
  off = 0;
  constructor(buf: Buffer) {
    this.buf = buf;
  }
  u32(): number {
    const v = this.buf.readUInt32BE(this.off);
    this.off += 4;
    return v;
  }
  str(): Buffer {
    const n = this.u32();
    const s = this.buf.subarray(this.off, this.off + n);
    if (s.length < n) throw new Error("truncated key");
    this.off += n;
    return s;
  }
}

export interface Ed25519Key {
  pub: Buffer;
  priv: KeyObject;
}

const MAGIC = "openssh-key-v1\0";

export function loadKey(path: string): Ed25519Key {
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (e) {
    throw new Error(`${path}: ${(e as Error).message}`);
  }
  const m = /-----BEGIN OPENSSH PRIVATE KEY-----([\s\S]+?)-----END OPENSSH PRIVATE KEY-----/.exec(text);
  if (!m) throw new Error(`${path}: not an OpenSSH private key`);
  const r = new Reader(Buffer.from(m[1]!.replace(/\s+/g, ""), "base64"));
  if (r.buf.subarray(0, MAGIC.length).toString("latin1") !== MAGIC) throw new Error(`${path}: bad key header`);
  r.off = MAGIC.length;
  const cipher = r.str().toString();
  r.str(); // kdf name
  r.str(); // kdf options
  if (cipher !== "none") throw new Error(`${path}: encrypted keys are not supported`);
  if (r.u32() !== 1) throw new Error(`${path}: expected exactly one key`);
  r.str(); // public blob
  const p = new Reader(r.str());
  p.u32(); // check ints
  p.u32();
  const type = p.str().toString();
  if (type !== "ssh-ed25519") throw new Error(`${path}: only ed25519 keys are supported, got ${type}`);
  const pub = Buffer.from(p.str());
  const seed = p.str().subarray(0, 32); // stored as seed || pub
  const priv = createPrivateKey({
    key: { kty: "OKP", crv: "Ed25519", d: seed.toString("base64url"), x: pub.toString("base64url") },
    format: "jwk",
  });
  return { pub, priv };
}

/** PEM-armored SSHSIG over `message`, as `ssh-keygen -Y sign` would produce. */
export function sshSign(key: Ed25519Key, namespace: string, message: Buffer | string): string {
  const hash = createHash("sha512").update(message).digest();
  const signed = Buffer.concat([Buffer.from("SSHSIG"), str(namespace), str(""), str("sha512"), str(hash)]);
  const sig = sign(null, signed, key.priv);
  const blob = Buffer.concat([
    Buffer.from("SSHSIG"),
    u32(1),
    str(Buffer.concat([str("ssh-ed25519"), str(key.pub)])),
    str(namespace),
    str(""),
    str("sha512"),
    str(Buffer.concat([str("ssh-ed25519"), str(sig)])),
  ]);
  const b64 = blob.toString("base64").replace(/(.{70})/g, "$1\n");
  return `-----BEGIN SSH SIGNATURE-----\n${b64.replace(/\n$/, "")}\n-----END SSH SIGNATURE-----\n`;
}
