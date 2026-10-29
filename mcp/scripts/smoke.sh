#!/bin/sh
# Packs the MCP, installs the tarball into a clean directory and checks that the
# installed bin answers `initialize`. Node refuses to strip types under
# node_modules, so this fails if the package ships TypeScript instead of JS.
set -eu
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
tgz=$(npm pack --silent --pack-destination "$tmp")
cd "$tmp"
npm init -y >/dev/null
npm install --silent "./$tgz"
out=$(printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | ./node_modules/.bin/sessionbus-mcp)
case "$out" in *'"serverInfo"'*) echo "ok: $tgz" ;; *) echo "bad reply: $out" >&2; exit 1 ;; esac
