//! The broker daemon: the single process that owns the port, the session
//! registry, and routing (§1). Adapters and plugins are clients of it.

mod registry;
mod routes;

pub use registry::Registry;

use std::ops::RangeInclusive;
use std::sync::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::discovery;
use crate::protocol::{BrokerInfo, Health, PROTOCOL_VERSION};

/// Loopback port range the broker binds within, in order (§2, §8).
pub const PORT_RANGE: RangeInclusive<u16> = 1801..=1803;
/// Shut down after this long with zero adapter activity *and* zero live
/// sessions (§2). Tunable via [`BrokerConfig`].
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);
/// How often the background task checks for idle-shutdown and reaps dead
/// sessions.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(15);

/// Shared state handed to every request handler.
pub struct AppState {
    pub registry: Registry,
    /// Pre-rendered `/health` body. `port` is filled in after binding.
    pub health: Health,
    /// Secret authenticating adapters (recorded in `broker.json`).
    pub token: String,
    /// Last time an authenticated adapter request arrived (idle tracking).
    pub last_activity: Mutex<Instant>,
    /// Notified to trigger graceful shutdown (idle task, `/shutdown`, signal).
    pub shutdown: Notify,
}

impl AppState {
    fn new(token: String, broker_uuid: String, port: u16) -> Self {
        Self {
            registry: Registry::new(),
            health: Health { protocol: PROTOCOL_VERSION, broker_uuid, port },
            token,
            last_activity: Mutex::new(Instant::now()),
            shutdown: Notify::new(),
        }
    }
}

/// Tunables for a broker run.
pub struct BrokerConfig {
    pub idle_timeout: Option<Duration>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self { idle_timeout: Some(DEFAULT_IDLE_TIMEOUT) }
    }
}

/// Generate a 64-hex-char adapter token (two v4 UUIDs, no extra deps).
fn generate_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

/// Build the shared state and router for a given bound port. Exposed so tests
/// can drive the broker on an ephemeral port without touching `broker.json` or
/// the maintenance task.
pub fn build_app(token: impl Into<String>, port: u16) -> (axum::Router, Arc<AppState>) {
    let state = Arc::new(AppState::new(token.into(), Uuid::new_v4().to_string(), port));
    (routes::router(state.clone()), state)
}

/// Bind the first free port in [`PORT_RANGE`] on loopback (§2, §8). All busy is
/// a hard error with guidance.
async fn bind_in_range() -> Result<TcpListener> {
    for port in PORT_RANGE {
        match TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => return Ok(listener),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(e) => return Err(e).with_context(|| format!("binding 127.0.0.1:{port}")),
        }
    }
    Err(anyhow!(
        "no free port in {}..={}; another broker or process owns them all",
        PORT_RANGE.start(),
        PORT_RANGE.end()
    ))
}

/// Background loop: reap dead sessions and shut down when idle.
async fn maintenance(state: Arc<AppState>, idle_timeout: Option<Duration>) {
    loop {
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
        let reaped = state.registry.reap();
        if reaped > 0 {
            tracing::debug!(reaped, "reaped dead sessions");
        }
        if let Some(idle_timeout) = idle_timeout {
            let idle_for = state.last_activity.lock().unwrap().elapsed();
            if idle_for >= idle_timeout && state.registry.live_session_count() == 0 {
                tracing::info!(?idle_for, "idle with no live sessions, shutting down");
                state.shutdown.notify_waiters();
                return;
            }
        }
    }
}

/// Run the broker to completion: bind, publish `broker.json`, serve until
/// shutdown (idle, `/shutdown`, or Ctrl-C), then clean up.
pub async fn run(config: BrokerConfig) -> Result<()> {
    let listener = bind_in_range().await?;
    let port = listener.local_addr()?.port();
    let token = generate_token();

    let (app, state) = build_app(token.clone(), port);

    let info = BrokerInfo {
        port,
        pid: std::process::id(),
        token,
        protocol: PROTOCOL_VERSION,
        broker_uuid: state.health.broker_uuid.clone(),
    };
    discovery::write_broker_info(&info).context("publishing broker.json")?;
    tracing::info!(port, pid = info.pid, broker_uuid = %info.broker_uuid, "broker listening");

    tokio::spawn(maintenance(state.clone(), config.idle_timeout));

    let shutdown_signal = {
        let state = state.clone();
        async move {
            tokio::select! {
                _ = state.shutdown.notified() => {}
                _ = tokio::signal::ctrl_c() => tracing::info!("received Ctrl-C"),
            }
        }
    };

    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .context("serving");

    discovery::remove_broker_info_if_owned(&state.health.broker_uuid);
    tracing::info!("broker stopped");
    result
}
