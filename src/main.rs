//! `dust` — the single binary for every process in the topology. Subcommands
//! select the role; the stdio adapter forks `dust broker` to spawn the daemon.

use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use dust_robloxstudio_mcp::broker::{self, BrokerConfig};
use dust_robloxstudio_mcp::adapter;

#[derive(Parser)]
#[command(name = "dust", version, about = "Reliable Roblox Studio MCP")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the broker daemon (port bind/fallback, registry, long-poll).
    Broker(BrokerArgs),
    /// Run the stdio MCP adapter (Claude Desktop / Code).
    Adapter,
    /// Write client config and install the Studio plugin.
    Setup,
}

#[derive(clap::Args)]
struct BrokerArgs {
    /// Idle seconds before auto-shutdown (0 disables it).
    #[arg(long, default_value_t = 600)]
    idle_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    match Cli::parse().command {
        Command::Broker(args) => {
            let idle_timeout = (args.idle_timeout_secs > 0)
                .then(|| Duration::from_secs(args.idle_timeout_secs));
            broker::run(BrokerConfig { idle_timeout }).await
        }
        Command::Adapter => adapter::run().await,
        Command::Setup => print_setup(),
    }
}

/// Print copy-paste MCP client config and plugin install guidance. Writes to
/// stdout (this subcommand is run by a human, not over the MCP transport).
fn print_setup() -> Result<()> {
    let exe = std::env::current_exe()?;
    let exe = exe.display();
    println!(
        r#"Dust setup
==========

1. Register the MCP server with your client.

   Claude Code (per-user):
     claude mcp add dust -- "{exe}" adapter

   Claude Desktop — add to claude_desktop_config.json under "mcpServers":
     "dust": {{
       "command": "{exe}",
       "args": ["adapter"]
     }}

   The adapter forks the broker on first use; you do not start it yourself.

2. Install the Studio plugin.

   Copy plugin/Dust.rbxm into your Roblox Studio plugins folder
   (Studio: right-click in Explorer's plugin area, or save it as a Local Plugin),
   then enable "Allow HTTP Requests" in Game Settings -> Security.

3. Open a place in Studio, then ask Claude to run list_sessions to confirm the
   connection."#
    );
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Logs to stderr so stdout stays clean for the stdio adapter's MCP traffic.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
