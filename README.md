# sessionbus

Messages between Claude sessions on different machines. No server: each host runs an `agent` that owns its sessions' inboxes and pushes messages straight to other hosts' agents over [Tailscale](https://tailscale.com), retrying from a durable outbox until acked or expired.

```
session ─ MCP/hook ─ unix socket ─ agent ═ tailnet ═ agent ─ unix socket ─ MCP/hook ─ session
                                   SQLite: outbox, inbox, directory
```

- **Reliable:** a send is accepted by your local agent at once ("queued") and delivered when the other host is reachable. Nothing depends on a hub, a tunnel or an always-on machine. `MessageStatus`: `queued` → `delivered` → `read`, or `expired`.
- **Messages expire:** default 1 h, at most 24 h (`ttl_minutes`). Old news about work in progress is dropped, body included, wherever it waits.
- **Identity is the session id.** Aliases (`as`) are mutable names per project; an alias is resolved to a session id when you send, so a later rename cannot redirect a message. `/clear` hands the aliases and unread mail to the new session. The agent finds the calling session from the caller's process tree, never from what it claims.
- **Addresses:** `alias`, `alias@host`, or `#topic`. A bare alias that matches sessions on two hosts is refused with the `alias@host` choices.
- **Trust:** only your own tailnet devices may connect (WireGuard encrypts; the agent checks the peer address against `tailscale status`). Message text is untrusted content and is neutralized (`<` escaped).

## Install

Binary (macOS Apple silicon, Linux x86_64/aarch64; musl static by default, `LIBC=gnu` for glibc):

```sh
curl -fsSL https://raw.githubusercontent.com/ali-mosavian/sessionbus/main/install.sh | sh
```

MCP for Claude Code (Node >= 22.18):

```sh
claude mcp add --scope user sessionbus -- \
  npx -y https://github.com/ali-mosavian/sessionbus/releases/latest/download/sessionbus-mcp.tgz
```

Run the agent on every host (Tailscale logged in; state in `~/.sessionbus`):

```sh
sessionbus agent --tailscale
sessionbus doctor            # tailscale, agent, peers reached, messages queued per peer
```

The agent listens on the host's tailnet address (`:8788`) and talks to your other devices; each one needs its own agent. To keep it running, use a launchd agent (macOS) or a systemd user unit (Linux) that runs `sessionbus agent --tailscale`. Without Tailscale, link agents by hand: `--listen ADDR --advertise ADDR --peer NAME=ADDR`.

## Sessions

Every tool takes a required `project` (registered once with `RegisterProject`, by a person; `ListProjects` lists them; an unknown one fails and lists the registered ones). Give `as` on the first call to take a name.

Tools: `ListAgents`, `SendMessage(to, message, summary?, reply_to?, status?, priority?, ttl_minutes?)`, `ReadNotifications`, `MessageStatus(ids)`, `Subscribe`, `Unsubscribe`, `ListTopics`, `ListProjects`, `RegisterProject`.

Messages arrive as `<cross-session-message via="sessionbus" from="alias@host" id=".." ..>`; reply with `to` = `from` and `reply_to` = `id`. Delivery is at-least-once: dedupe by id. `status` and `priority` are labels, never authority.

### Waking idle sessions

```sh
sessionbus-mcp hooks            # print the settings
sessionbus-mcp hooks --write    # merge into ~/.claude/settings.json
```

Adds `sessionbus-wait` as an async `asyncRewake` hook on `SessionStart` and `Stop`. It registers the session with the agent, waits for a message, prints it on stderr and exits 2, which wakes Claude; the end of that turn re-arms it. A newer waiter retires an older one. Verified in the terminal CLI; the desktop app is untested.

## CLI

`sessionbus agent|doctor`, `sessionbus call '<json>'` (one request to the local agent), `sessionbus hook` (registers the session named on stdin).

## Develop

`cargo test` (Rust, ~5 s) and `cargo build && cd mcp && npm ci && npm test` (the MCP tests start real agents). Releases: push a tag `vX.Y.Z` matching `Cargo.toml` and `mcp/package.json`.

## Legacy relay

`sessionbus serve` and the relay client commands (`list`, `send`, `inbox`, …) are the previous hub design (single relay over ssh tunnels). They remain for now and are not used by the MCP.
