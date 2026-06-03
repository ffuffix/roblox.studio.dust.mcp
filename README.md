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
- **plugin** ([`plugin/Dust.rbxm`](plugin/Dust.rbxm)) — runs in Studio's edit
  context, discovers the broker over HTTP, and long-polls for commands.

## Install

### From a release (recommended)

Download the archive for your platform from the
[Releases page](https://github.com/ffuffix/roblox.studio.dust.mcp/releases),
extract the `dust` binary somewhere on your `PATH`, then register it:

```sh
claude mcp add dust -- dust adapter
```

### From source

```sh
cargo build --release
claude mcp add dust -- /path/to/target/release/dust adapter
```

For other clients (e.g. Claude Desktop), run `dust setup` to print the
copy-paste config and the plugin install steps.

### Studio plugin

Download `Dust.rbxm` from the release (or use [`plugin/Dust.rbxm`](plugin/Dust.rbxm)),
copy it into your Roblox Studio plugins folder, and enable **Game Settings →
Security → Allow HTTP Requests**. The adapter forks the broker on first use —
you do not start it yourself.

Open a place in Studio, then ask Claude to run `list_sessions` to confirm the
connection.

## Development

```sh
cargo test      # broker + adapter integration tests
cargo clippy --all-targets
```
