#!/usr/bin/env node
// Env: SESSIONBUS_NAME, SESSIONBUS_ADDR (default 127.0.0.1:8787), SESSIONBUS_KEY (default ~/.ssh/id_ed25519).
import { configFromEnv } from "./client.ts";
import { serve } from "./server.ts";

await serve({ cfg: configFromEnv() }, process.stdin, process.stdout);
