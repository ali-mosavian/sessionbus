import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { installHooks, withHooks } from "../src/hooks.ts";

const ours = (s: any, event: string) => s.hooks[event].flatMap((g: any) => g.hooks).filter((h: any) => /wait\.(js|ts)"?$/.test(h.command));

test("the waiter is armed at session start and after every turn, asynchronously and able to wake", () => {
  const s = withHooks({}, '"node" "/x/dist/wait.js"');
  for (const event of ["SessionStart", "Stop"]) {
    const [h] = ours(s, event);
    assert.deepEqual([h.async, h.asyncRewake], [true, true], `${event} would not wake an idle session`);
  }
});

test("installing twice, or from a moved package, leaves one waiter per event and keeps other hooks", () => {
  const other = { hooks: [{ type: "command", command: "echo mine" }] };
  const once = withHooks({ model: "x", hooks: { Stop: [other] } }, '"node" "/old/dist/wait.js"');
  const twice = withHooks(withHooks(once, '"node" "/old/dist/wait.js"'), '"node" "/new/dist/wait.js"');
  assert.equal(twice.model, "x");
  for (const event of ["SessionStart", "Stop"]) {
    assert.deepEqual(ours(twice, event).map((h: any) => h.command), ['"node" "/new/dist/wait.js"'], `${event}: duplicates or a stale path`);
  }
  assert.ok(twice.hooks!.Stop!.some((g) => g.hooks.some((h) => h.command === "echo mine")), "another hook was dropped");
});

test("printing shows only the hooks to add, never the rest of the user's settings", () => {
  const path = join(mkdtempSync(join(tmpdir(), "sessionbus-hooks-")), "settings.json");
  writeFileSync(path, JSON.stringify({ permissions: { allow: ["Bash(secret-tool *)"] } }));
  const out = installHooks(false, path);
  assert.doesNotMatch(out, /secret-tool|permissions/, "the dry run echoed the user's other settings");
  assert.deepEqual(Object.keys(JSON.parse(out).hooks).sort(), ["SessionStart", "Stop"]);
});

test("--write merges into an existing settings file, and printing changes nothing", () => {
  const path = join(mkdtempSync(join(tmpdir(), "sessionbus-hooks-")), "settings.json");
  writeFileSync(path, JSON.stringify({ permissions: { allow: ["Bash(ls)"] } }));
  installHooks(false, path);
  assert.deepEqual(JSON.parse(readFileSync(path, "utf8")), { permissions: { allow: ["Bash(ls)"] } });
  installHooks(true, path);
  const written = JSON.parse(readFileSync(path, "utf8"));
  assert.deepEqual(written.permissions, { allow: ["Bash(ls)"] });
  assert.equal(ours(written, "SessionStart").length, 1);
});
