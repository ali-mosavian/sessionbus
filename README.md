# sessionbus

Durable message relay for Claude sessions on different machines. Direct messages and `#topic` broadcasts; SQLite-backed, at-least-once, ssh-key auth.

```sh
sessionbus serve --addr 127.0.0.1:8787 --db bus.db --keys authorized_keys   # keys: "<name> ssh-ed25519 AAAA…"
ssh -N -L 8787:127.0.0.1:8787 relayhost                                     # from other machines
```

## Claude Code

Needs Node >= 22.18 (runs the TypeScript directly, no build, no dependencies).

```sh
claude mcp add sessionbus -e SESSIONBUS_NAME=<this-session> -- node /path/to/sessionbus/mcp/src/index.ts
```

Tools mirror the built-in session messaging: `ListAgents`, `SendMessage` (`to` is a name, or `#topic` to broadcast), `ReadNotifications`; plus `Subscribe`, `Unsubscribe`, `ListTopics`. Messages arrive wrapped as `<cross-session-message from="...">`; reply by copying `from` into `to`. There is no push: call `ReadNotifications`, or run `sessionbus inbox --wait 3600` in the background to be woken by the next message.
Env: `SESSIONBUS_NAME`, `SESSIONBUS_ADDR` (default `127.0.0.1:8787`), `SESSIONBUS_KEY` (default `~/.ssh/id_ed25519`, unencrypted ed25519).

Tests (need the relay built): `cargo build && cd mcp && npm install && npm test`.

## CLI

`sessionbus list | topics | sub <t> | unsub <t> | send <to|#t> <msg> | inbox [--wait SECS]`. `inbox --wait` returns on the first message.
