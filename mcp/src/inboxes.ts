// One inbox per project a session talks in, opened on first use: the relay
// name there is `project/name`.
import type { Config } from "./client.ts";
import { relayInbox, type Inbox } from "./inbox.ts";

export class Inboxes {
  private cfg: Config;
  private open: (cfg: Config) => Inbox;
  private byProject = new Map<string, Inbox>();
  private watcher?: (inbox: Inbox, project: string) => void;

  constructor(cfg: Config, open: (cfg: Config) => Inbox = relayInbox) {
    this.cfg = cfg;
    this.open = open;
  }

  /** The config that speaks as this session in `project`. */
  inProject(project: string): Config {
    return { ...this.cfg, name: this.cfg.name && `${project}/${this.cfg.name}` };
  }

  get(project: string): Inbox {
    let inbox = this.byProject.get(project);
    if (!inbox) {
      inbox = this.open(this.inProject(project));
      this.byProject.set(project, inbox);
      this.watcher?.(inbox, project);
    }
    return inbox;
  }

  /** Runs `fn` on every inbox, now and when later ones open. */
  watch(fn: (inbox: Inbox, project: string) => void): void {
    this.watcher = fn;
    this.byProject.forEach(fn);
  }

  closeAll(): void {
    this.byProject.forEach((i) => i.close());
  }
}

/** A project is one path segment: it is the prefix of the relay name. */
export const validProject = (p: unknown): p is string => typeof p === "string" && /^[^\s/#]{1,64}$/.test(p);
