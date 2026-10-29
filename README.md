# sessionbus

Durable message relay for Claude sessions on different machines. Direct messages and `#topic` broadcasts; SQLite-backed, at-least-once, ssh-key auth.

```sh
sessionbus serve --addr 127.0.0.1:8787 --db bus.db --keys authorized_keys   # keys: "<name> ssh-ed25519 AAAA…"
ssh -N -L 8787:127.0.0.1:8787 relayhost                                     # from other machines
```

## Claude Code

```sh
claude mcp add sessionbus -e SESSIONBUS_NAME=<this-session> -- sessionbus mcp
```

Tools: `list_sessions`, `send_message`, `read_inbox` (`wait_seconds` blocks for the next message), `subscribe`, `unsubscribe`, `list_topics`.
Env: `SESSIONBUS_NAME`, `SESSIONBUS_ADDR` (default `127.0.0.1:8787`), `SESSIONBUS_KEY` (default `~/.ssh/id_ed25519`, unencrypted ed25519).

## CLI

`sessionbus list | topics | sub <t> | unsub <t> | send <to|#t> <msg> | inbox [--wait SECS]`. `inbox --wait` returns on the first message.
