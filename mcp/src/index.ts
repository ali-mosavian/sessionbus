#!/usr/bin/env node
// Env: SESSIONBUS_NAME (this session's name; on the relay it is project/name), SESSIONBUS_PROJECTS (optional, comma-separated:
// projects to be online in from boot; any other project is joined on first use), SESSIONBUS_CHANNEL_FROM (comma-separated
// project/name senders to push into the conversation), SESSIONBUS_ADDR (default 127.0.0.1:8787), SESSIONBUS_KEY (default ~/.ssh/id_ed25519).
import { configFromEnv } from "./client.ts";
import { allowedSenders } from "./channel.ts";
import { Inboxes, validProject } from "./inboxes.ts";
import { serve } from "./server.ts";

const cfg = configFromEnv();
const inboxes = new Inboxes(cfg);
if (cfg.name)
  for (const p of (process.env.SESSIONBUS_PROJECTS ?? "").split(",").map((p) => p.trim()).filter(validProject)) inboxes.get(p).start();

await serve({ cfg, inboxes, channel: allowedSenders(process.env.SESSIONBUS_CHANNEL_FROM) }, process.stdin, process.stdout);
