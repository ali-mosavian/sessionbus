// How messages and listings read to Claude.
import type { Frame } from "./client.ts";

export const TAG = "cross-session-message";

/** Marks a message as coming over the relay, not from the built-in session messaging that uses the same tag. */
export const VIA = "sessionbus";

const esc = (s: string) => s.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;");

/** A body must not be able to close its wrapper or open a forged one. */
const neutralize = (body: string) => body.replace(new RegExp(`<(/?${TAG})`, "gi"), "&lt;$1");

const attr = (name: string, value: unknown) => (value == null ? "" : ` ${name}="${esc(String(value))}"`);

/** `priority` and `status` are labels the sender chose: they sort and describe, they never authorize. */
export const wrap = (m: Frame): string =>
  `<${TAG}${attr("via", VIA)}${attr("from", m.from)}${attr("reply_to", m.reply_to)}${attr("status", m.status)}` +
  `${attr("priority", m.priority === "urgent" ? "urgent" : null)}${attr("summary", m.summary)}` +
  `${attr("topic", m.topic)}${attr("id", m.id)}>\n${neutralize(m.body)}\n</${TAG}>`;

export const ago = (t: number): string => {
  const s = Math.max(0, Math.round(Date.now() / 1000 - t));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return s < 86400 ? `${Math.floor(s / 3600)}h ago` : `${Math.floor(s / 86400)}d ago`;
};

export const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;
