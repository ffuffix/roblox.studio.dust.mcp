//! Axum router and handlers for the broker (§4).
//!
//! Endpoint groups:
//! - Public discovery:   `GET  /health`
//! - Participant (plugin/helpers, loopback only, no token — plugins can't hold
//!   secrets): `POST /poll`, `POST /result`
//! - Adapter (loopback + bearer token): `GET /sessions`, `POST /command`,
//!   `POST /shutdown`

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, response::Json as JsonResponse};
use serde::Deserialize;
use serde_json::Value;

use crate::protocol::{
    Command, CommandResult, Handshake, Health, PollResponse, Role, SessionInfo,
};

use super::AppState;

/// How long a poll is held open before returning `204` (§4: under HttpService's
/// timeout, ~30s).
const POLL_HOLD: Duration = Duration::from_secs(25);
/// Default ceiling for how long an adapter's `command` call waits for a result.
const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/poll", post(poll))
        .route("/result", post(result))
        .route("/sessions", get(sessions))
        .route("/command", post(command))
        .route("/shutdown", post(shutdown))
        .with_state(state)
}

// ---- Public discovery -----------------------------------------------------

async fn health(State(st): State<Arc<AppState>>) -> JsonResponse<Health> {
    Json(st.health.clone())
}

// ---- Participant endpoints ------------------------------------------------

/// Long-poll. Registers/refreshes the role, then returns queued commands or
/// holds up to [`POLL_HOLD`] before a `204`.
async fn poll(State(st): State<Arc<AppState>>, Json(hs): Json<Handshake>) -> Response {
    let session = st.registry.upsert(&hs);
    // Clone the Arc out so we don't hold any DashMap ref across the await below.
    let queue = session.role_queue(hs.role);
    let deadline = Instant::now() + POLL_HOLD;

    loop {
        // Arm the notification *before* checking the queue so an enqueue that
        // races with us can't be missed.
        let notified = queue.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let batch = queue.take_for_poll();
        if !batch.is_empty() {
            return JsonResponse(PollResponse { commands: batch }).into_response();
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return StatusCode::NO_CONTENT.into_response();
        }
        match tokio::time::timeout(remaining, notified.as_mut()).await {
            Ok(()) => continue,
            Err(_) => return StatusCode::NO_CONTENT.into_response(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultRequest {
    session_id: String,
    role: Role,
    #[serde(flatten)]
    result: CommandResult,
}

/// A participant posts a command result. We ack the queue (idempotent) and hand
/// the result to any waiting adapter.
async fn result(State(st): State<Arc<AppState>>, Json(req): Json<ResultRequest>) -> StatusCode {
    let id = req.result.id;
    if let Some(session) = st.registry.get(&req.session_id) {
        session.role_queue(req.role).ack(id);
    }
    st.registry.complete(&req.session_id, req.result);
    StatusCode::OK
}

// ---- Adapter endpoints ----------------------------------------------------

/// Validate the adapter's bearer token and record activity (resets idle timer).
fn authorize(st: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-broker-token").and_then(|v| v.to_str().ok()));

    match provided {
        Some(t) if t == st.token => {
            *st.last_activity.lock().unwrap() = Instant::now();
            Ok(())
        }
        _ => Err((StatusCode::UNAUTHORIZED, "invalid or missing broker token".into())),
    }
}

async fn sessions(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<JsonResponse<Vec<SessionInfo>>, (StatusCode, String)> {
    authorize(&st, &headers)?;
    Ok(Json(st.registry.list()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandRequest {
    session_id: String,
    tool: String,
    #[serde(default)]
    args: Value,
    target_role: Role,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// Enqueue a command for a session and wait for its result. This is the shape
/// an MCP tool call maps onto: enqueue → plugin executes → result returns.
async fn command(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CommandRequest>,
) -> Result<JsonResponse<CommandResult>, (StatusCode, String)> {
    authorize(&st, &headers)?;

    let session = st
        .registry
        .get(&req.session_id)
        .ok_or((StatusCode::NOT_FOUND, format!("no session {}", req.session_id)))?;

    let id = session.next_id();
    // Register interest before enqueueing so we cannot miss a fast result.
    let rx = st.registry.register_pending(&req.session_id, id);
    session.enqueue(Command {
        id,
        tool: req.tool,
        args: req.args,
        target_role: req.target_role,
    });

    let timeout = req
        .timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_COMMAND_TIMEOUT);

    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(result)) => Ok(Json(result)),
        Ok(Err(_)) => Err((StatusCode::INTERNAL_SERVER_ERROR, "result channel dropped".into())),
        Err(_) => {
            st.registry.cancel_pending(&req.session_id, id);
            Err((StatusCode::GATEWAY_TIMEOUT, "timed out waiting for plugin result".into()))
        }
    }
}

async fn shutdown(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize(&st, &headers)?;
    st.shutdown.notify_waiters();
    Ok(StatusCode::ACCEPTED)
}
