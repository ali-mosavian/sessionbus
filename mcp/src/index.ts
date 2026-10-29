#!/usr/bin/env node
// Env: SESSIONBUS_NAME, SESSIONBUS_ADDR (default 127.0.0.1:8787), SESSIONBUS_KEY (default ~/.ssh/id_ed25519).
import { configFromEnv } from "./client.ts";
import { relayInbox } from "./inbox.ts";
import { serve } from "./server.ts";

const cfg = configFromEnv();
const inbox = relayInbox(cfg);
if (cfg.name) inbox.start(); // from boot, so the session shows online

await serve({ cfg, inbox }, process.stdin, process.stdout);
