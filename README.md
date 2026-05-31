# Dust

A reliable Roblox Studio MCP (Model Context Protocol) server, connecting Claude
to Roblox Studio.

## Architecture

Three processes share one wire protocol ([`src/protocol.rs`](src/protocol.rs)):

```
Claude Desktop / Code ──stdio──► dust adapter ──loopback HTTP──► dust broker ◄──HTTP poll── Studio plugin
```

- **broker** (`dust broker`) — the daemon that owns a loopback port (1801→1803),
  the session registry, and routing. Single source of truth.
- **adapter** (`dust adapter`) — the stdio MCP server Claude launches. Forks or
  attaches to the broker, exposes MCP tools.
- **plugin** ([`plugin/DustPlugin.rbxm`](plugin/DustPlugin.rbxm)) —
  runs in Studio's edit context, discovers the broker over HTTP, and long-polls
  for commands.

## Setup

```sh
cargo build --release
./target/release/dust setup   # prints MCP client config + plugin install steps
```

Then install the plugin and enable **Game Settings → Security → Allow HTTP
Requests**. Ask Claude to run `list_sessions` to confirm the connection.

## Build status (per the design's §10 build order)

- [x] **Step 1** — broker daemon: port bind/fallback, `/health`, registry,
      long-poll + idempotent queue, `broker.json`, idle-shutdown.
- [x] **Step 2** — stdio MCP adapter: spawn-or-attach, `list_sessions`,
      session disambiguation, round-trip probes (`ping_session`,
      `get_place_info`); Studio plugin handshake + long-poll loop.
- [x] **Step 3** — search subsystem: `grep_scripts`, `search_instances`
      (depth-bounded, paginated cursor, class/name/tag/property filters),
      `search_by_property`, `get_script_source`; non-invasive `GetDebugId`
      handles via a session registry. *(Luau handlers pending live Studio test.)*
- [~] **Step 4** — F8 control (`start_simulation`/`stop_simulation`), `read_output`
      (LogService capture), `run_luau` done; **host-side screenshots pending**
      (Linux capture approach TBD).
- [~] **Step 5** — injection (idempotent, tagged, via `ScriptEditorService`) +
      the three helpers (server runner / input poller / client handler, self-gating)
      + `start_playtest`/`stop_playtest`/`run_server_code`/`get_server_state`.
      **Spikes to confirm live**: S4 F5 start (manual for now) & end (`EndTest`).
- [ ] Step 6 — input / movement / camera via the client handler.
- [ ] Step 7 — report assembly + verdict.

## Development

```sh
cargo test      # broker + adapter integration tests
cargo clippy --all-targets
```
