// Claude Code settings that run `sessionbus-wait`: it is armed when a session
// starts and again whenever a turn ends, so messages wake the session.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

type Settings = { hooks?: Record<string, Array<{ matcher?: string; hooks: Array<Record<string, unknown>> }>> } & Record<string, unknown>;

/** Events that arm the waiter: the start of a session, and the end of every turn (the wake itself ends in one). */
const EVENTS = ["SessionStart", "Stop"] as const;

/** The command a hook runs: this node, this package's waiter. */
export const waitCommand = (): string => {
  const ts = import.meta.url.endsWith(".ts");
  const script = fileURLToPath(new URL(ts ? "./wait.ts" : "./wait.js", import.meta.url));
  return `${JSON.stringify(process.execPath)} ${JSON.stringify(script)}`;
};

const isOurs = (h: Record<string, unknown>) => typeof h.command === "string" && /[\\/]wait\.(js|ts)"?$/.test(h.command);

/** `settings` with the waiter hook on each event, once; other hooks are left alone. */
export function withHooks(settings: Settings, command = waitCommand()): Settings {
  const hooks = { ...(settings.hooks ?? {}) };
  for (const event of EVENTS) {
    const groups = (hooks[event] ?? []).map((g) => ({ ...g, hooks: g.hooks.filter((h) => !isOurs(h)) })).filter((g) => g.hooks.length);
    groups.push({ hooks: [{ type: "command", command, async: true, asyncRewake: true }] });
    hooks[event] = groups;
  }
  return { ...settings, hooks };
}

export const userSettingsPath = (): string => join(homedir(), ".claude", "settings.json");

/** Prints the settings that would be written; with `write`, merges them into the user's settings file. */
export function installHooks(write: boolean, path = userSettingsPath()): string {
  let current: Settings = {};
  try {
    current = JSON.parse(readFileSync(path, "utf8"));
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw new Error(`${path}: ${(e as Error).message}`);
  }
  if (!write) {
    // only what would be added: the rest of the file is the user's business
    return JSON.stringify({ hooks: withHooks({}).hooks }, null, 2) + "\n";
  }
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(withHooks(current), null, 2) + "\n");
  return `Wrote the sessionbus hooks to ${path}. New Claude Code sessions use them.`;
}
