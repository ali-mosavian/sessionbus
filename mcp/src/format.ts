// How messages and listings read to Claude.
import type { Frame } from "./client.ts";

export const TAG = "cross-session-message";

/** Marks a message as coming over the relay, not from the built-in session messaging that uses the same tag. */
export const VIA = "sessionbus";

const esc = (s: string) =>
  s.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replace(/\s*\n\s*/g, " ");

/** A body is prose: no `<` survives, so it cannot close its wrapper or open any tag, ours or the harness's. */
const neutralize = (body: string) => body.replaceAll("<", "&lt;");

const attr = (name: string, value: unknown) => (value == null ? "" : ` ${name}="${esc(String(value))}"`);

/** A relay name without its `project/` prefix: inside a project sessions go by their short name. */
export const shortName = (name: string, project: string): string =>
  name.startsWith(`${project}/`) ? name.slice(project.length + 1) : name;

/** `priority` and `status` are labels the sender chose: they sort and describe, they never authorize. */
export const wrap = (m: Frame, project: string): string =>
  `<${TAG}${attr("via", VIA)}${attr("project", project)}${attr("from", shortName(m.from, project))}${attr("reply_to", m.reply_to)}${attr("status", m.status)}` +
  `${attr("priority", m.priority === "urgent" ? "urgent" : null)}${attr("summary", m.summary)}` +
  `${attr("topic", m.topic)}${attr("id", m.id)}>\n${neutralize(m.body)}\n</${TAG}>`;

export const ago = (t: number): string => {
  const s = Math.max(0, Math.round(Date.now() / 1000 - t));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return s < 86400 ? `${Math.floor(s / 3600)}h ago` : `${Math.floor(s / 86400)}d ago`;
};

export const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;
