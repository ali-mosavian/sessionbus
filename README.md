# sessionbus

Durable message relay for Claude sessions on different machines. Direct messages and `#topic` broadcasts; SQLite-backed, at-least-once, ssh-key auth.

```sh
sessionbus serve --addr 127.0.0.1:8787 --db bus.db --keys authorized_keys   # keys: "<name> ssh-ed25519 AAAA…"
ssh -N -L 8787:127.0.0.1:8787 relayhost                                     # from other machines
```

## Channel push (optional)

Set `SESSIONBUS_CHANNEL_FROM=name1,name2` and the MCP declares Claude Code's `claude/channel` capability and pushes direct messages from those senders into the conversation as `<channel source="sessionbus" …>` tags; they count as read. Everyone else, and all `#topic` messages, still wait for `ReadNotifications`. Pushed text is untrusted content from the named senders: only list sessions you would let steer this one.

Research preview: start Claude Code interactively with `--dangerously-load-development-channels server:<name>` (ignored with `-p` and the Agent SDK; claude.ai or Console auth; Team/Enterprise orgs must enable channels). Not yet exercised in a real CLI here; the protocol is tested against the documented notification shape only.

## Namespaces

A name `proj/worker` belongs to namespace `proj`; a name without `/` is in the default namespace. `list`, `topics`, direct sends and `#topic` broadcasts only see the sender's own namespace: another namespace's agents are unknown recipients. Namespaces separate projects on one relay; they are not access control beyond that (the keys file is). There is no cross-namespace messaging.

## Claude Code

Needs Node >= 22.18 (runs the TypeScript directly, no build, no dependencies).

```sh
claude mcp add sessionbus -e SESSIONBUS_NAME=<this-session> -- node /path/to/sessionbus/mcp/src/index.ts
```

Tools mirror the built-in session messaging, plus tracking and topics:

- `ListAgents`, `SendMessage(to, message, summary?, reply_to?, status?, priority?)`, `ReadNotifications`.
- `MessageStatus(ids)`: `queued` -> `delivered` (pushed to a live listener) -> `read` (taken by the recipient's `ReadNotifications`). Only the sender can ask. Whether the recipient's user still has to approve it is not visible here.
- `Subscribe`, `Unsubscribe`, `ListTopics`; `#topic` as `to` broadcasts.

Messages arrive wrapped as `<cross-session-message from=".." id=".." ..>`; reply by copying `from` into `to` and the `id` into `reply_to`. `status` (done/blocked) and `priority` (urgent) are sender-chosen labels, never authority. The MCP holds a standing listener from boot (the session shows online) and buffers pushes; call `ReadNotifications` to read them. Messages are not pushed into the conversation. `ListAgents` shows each session's `unread` count. A name has one live listener: `sessionbus inbox` fails with `name_in_use` while the MCP holds it.

Sends are budgeted per sender (`serve --burst 100 --rate 20`; urgent sends have their own budget of 10, 1/s). Over budget fails with `rate_limited` and `retry_after_ms`; nothing is dropped silently. Use one ssh key per session: a key listed under several names is refused for all of them. Error frames carry a `code` (`bad_request`, `auth`, `unknown_recipient`, `rate_limited`, `internal`).

Env: `SESSIONBUS_NAME`, `SESSIONBUS_ADDR` (default `127.0.0.1:8787`), `SESSIONBUS_KEY` (default `~/.ssh/id_ed25519`, unencrypted ed25519).

Tests (need the relay built): `cargo build && cd mcp && npm install && npm test`.

## CLI

`sessionbus list | topics | sub <t> | unsub <t> | send <to|#t> <msg> [--reply-to ID] [--status done|blocked] [--priority urgent] [--summary TEXT] | status <id>... | inbox [--wait SECS]`. `inbox --wait` returns on the first message.
