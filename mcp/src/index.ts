#!/usr/bin/env node
// Env: SESSIONBUS_NAME (optional default session name; sessions normally give `as` on their first call), SESSIONBUS_PROJECTS
// (optional, with NAME: projects to be online in from boot), SESSIONBUS_CHANNEL_FROM (comma-separated
// project/name senders to push into the conversation), SESSIONBUS_ADDR (default 127.0.0.1:8787), SESSIONBUS_KEY (default ~/.ssh/id_ed25519).
import { configFromEnv } from "./client.ts";
import { allowedSenders } from "./channel.ts";
import { Inboxes, validProject } from "./inboxes.ts";
import { startControl } from "./control.ts";
import { serve } from "./server.ts";

if (process.argv[2] === "hooks") {
  // sessionbus-mcp hooks [--write]: the Claude Code settings that wake idle sessions
  console.log((await import("./hooks.ts")).installHooks(process.argv.includes("--write")));
  process.exit(0);
}

const cfg = configFromEnv();
const inboxes = new Inboxes(cfg);
const control = startControl(inboxes); // lets the sessionbus-wait hook reach this session
if (cfg.name) // a configured name is online in these projects from boot; otherwise a session joins on its first call
  for (const p of (process.env.SESSIONBUS_PROJECTS ?? "").split(",").map((p) => p.trim()).filter(validProject))
    inboxes.get(p, cfg.name);

await serve({ cfg, inboxes, channel: allowedSenders(process.env.SESSIONBUS_CHANNEL_FROM) }, process.stdin, process.stdout);
control.close(); // the socket would otherwise keep this process alive after Claude is gone
