// One inbox per project a session talks in, opened on first use: the relay
// name there is `project/name`.
import type { Config } from "./client.ts";
import { relayInbox, type Inbox } from "./inbox.ts";

export class Inboxes {
  private cfg: Config;
  private open: (cfg: Config) => Inbox;
  private byIdentity = new Map<string, Inbox>();
  /** The name this session last used: later calls may leave `as` out. */
  private current?: string;
  private watcher?: (inbox: Inbox, project: string) => void;

  constructor(cfg: Config, open: (cfg: Config) => Inbox = relayInbox) {
    this.cfg = cfg;
    this.open = open;
  }

  /**
   * This session's name for a call: `as` if given (and then remembered), else
   * the one used last, else SESSIONBUS_NAME. Each Claude session has its own
   * MCP process, so each remembers its own.
   */
  resolveName(as?: unknown): string {
    if (as !== undefined) {
      if (!validPart(as)) throw new Error("`as` must be a short name without spaces, '/' or '#' (e.g. \"coordinator\")");
      this.current = as;
    }
    const name = this.current ?? this.cfg.name;
    if (!name) throw new Error("missing string argument `as`: your session name (e.g. \"coordinator\"); later calls may leave it out");
    return name;
  }

  /** The config that speaks as `name` in `project`. */
  inProject(project: string, name: string): Config {
    return { ...this.cfg, name: `${project}/${name}` };
  }

  /** The inbox of `name` in `project`, online from the first call. */
  get(project: string, name: string): Inbox {
    const key = `${project}/${name}`;
    let inbox = this.byIdentity.get(key);
    if (!inbox) {
      inbox = this.open(this.inProject(project, name));
      this.byIdentity.set(key, inbox);
      this.watcher?.(inbox, project);
      inbox.start();
    }
    return inbox;
  }

  /** Runs `fn` on every inbox, now and when later ones open. */
  watch(fn: (inbox: Inbox, project: string) => void): void {
    this.watcher = fn;
    for (const [key, inbox] of this.byIdentity) fn(inbox, key.slice(0, key.indexOf("/")));
  }

  closeAll(): void {
    this.byIdentity.forEach((i) => i.close());
  }
}

/** A project or a session name is one path segment: together they are the relay name. */
const validPart = (p: unknown): p is string => typeof p === "string" && /^[^\s/#]{1,64}$/.test(p);
export const validProject = validPart;
