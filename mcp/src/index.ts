#!/usr/bin/env node
// Env: SESSIONBUS_SOCK (the local agent's socket, default ~/.sessionbus/agent.sock).
import { runtime, serve } from "./server.ts";

if (process.argv[2] === "hooks") {
  // sessionbus-mcp hooks [--write]: the Claude Code settings that register sessions and wake idle ones
  console.log((await import("./hooks.ts")).installHooks(process.argv.includes("--write")));
  process.exit(0);
}

await serve(runtime(), process.stdin, process.stdout);
