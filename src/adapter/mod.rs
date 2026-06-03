//! The stdio MCP adapter: a thin, stateless bridge that Claude Desktop / Claude
//! Code launch. It attaches to (or forks) the broker, then serves MCP over
//! stdio, translating tool calls into broker commands.

pub mod broker_client;
mod server;
mod spawn;

pub use broker_client::{BrokerClient, resolve_session};

use std::sync::Arc;

use anyhow::Result;
use rmcp::ServiceExt;
use rmcp::transport::stdio;

use server::DustServer;

pub async fn run() -> Result<()> {
    let info = spawn::ensure_broker().await?;
    let broker = Arc::new(BrokerClient::new(&info));

    let service = DustServer::new(broker).serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
