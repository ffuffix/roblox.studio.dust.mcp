//! Spawn-or-attach: the adapter discovers a running broker via `broker.json`
//! and health-checks it, or forks a fresh `dust broker` and waits for it to
//! come up (§2). The forked broker is detached so it outlives the adapter —
//! restarting Claude must not drop live Studio sessions.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use super::broker_client::BrokerClient;
use crate::discovery;
use crate::protocol::{BrokerInfo, PROTOCOL_VERSION};

/// How long to wait for a freshly forked broker to publish `broker.json` and
/// answer `/health`.
const SPAWN_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Return a healthy broker, attaching to an existing one or forking a new one.
pub async fn ensure_broker() -> Result<BrokerInfo> {
    if let Some(info) = discovery::read_broker_info() {
        if health_ok(&info).await {
            tracing::info!(port = info.port, "attached to existing broker");
            return Ok(info);
        }
        tracing::warn!("broker.json present but broker not responding; forking a new one");
    }

    fork_broker().context("forking broker daemon")?;
    wait_for_broker().await
}

/// `true` if the broker at `info` answers `/health` and its identity matches —
/// guards against attaching to a stale entry or a foreign listener (§2, §8).
async fn health_ok(info: &BrokerInfo) -> bool {
    let client = BrokerClient::new(info);
    match client.health().await {
        Ok(health) => health.broker_uuid == info.broker_uuid && health.protocol == PROTOCOL_VERSION,
        Err(_) => false,
    }
}

/// Fork `dust broker` as a detached child with stdio kept off the adapter's
/// stdout (which carries MCP JSON-RPC). Broker logs are appended to
/// `~/.rbxmcp/broker.log` when possible.
fn fork_broker() -> Result<()> {
    let exe = std::env::current_exe().context("locating current executable")?;
    let mut cmd = Command::new(exe);
    cmd.arg("broker").stdin(Stdio::null());

    match log_file().and_then(|f| f.try_clone().ok().map(|c| (f, c))) {
        Some((out, err)) => {
            cmd.stdout(Stdio::from(out)).stderr(Stdio::from(err));
        }
        None => {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }
    }

    // Detach into its own process group so a signal to the adapter's group
    // (e.g. Ctrl-C in a terminal) does not also kill the broker.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    cmd.spawn().context("spawning `dust broker`")?;
    Ok(())
}

fn log_file() -> Option<std::fs::File> {
    let dir = discovery::rbxmcp_dir().ok()?;
    std::fs::create_dir_all(&dir).ok()?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("broker.log"))
        .ok()
}

async fn wait_for_broker() -> Result<BrokerInfo> {
    let deadline = Instant::now() + SPAWN_TIMEOUT;
    loop {
        if let Some(info) = discovery::read_broker_info()
            && health_ok(&info).await
        {
            tracing::info!(port = info.port, "forked broker is up");
            return Ok(info);
        }
        if Instant::now() >= deadline {
            bail!("forked broker did not become healthy within {SPAWN_TIMEOUT:?}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
