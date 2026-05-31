//! Wire protocol shared by the broker, the stdio adapter, and (mirrored in Luau)
//! the Studio plugin / helpers.
//!

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Bumped on any breaking change to the wire schema. The plugin pins to a
/// broker whose `/health` reports a compatible protocol.
pub const PROTOCOL_VERSION: u32 = 1;

/// The three participants that can attach to a session. They share one
/// `sessionId` (a single place launch) but poll independently and receive
/// commands tagged for their role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Plugin,
    Server,
    Client,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LiveState {
    Live,
    Stale,
    Dead,
}

/// Sent by a participant on every long-poll. Doubles as the heartbeat — the
/// poll *is* the heartbeat.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Handshake {
    pub session_id: String,
    pub role: Role,
    /// `0` for unpublished places — disambiguate via `label`/GUID instead.
    #[serde(default)]
    pub place_id: u64,
    #[serde(default)]
    pub game_id: u64,
    #[serde(default)]
    pub place_name: String,
    #[serde(default)]
    pub creator_id: u64,
    /// Optional user/model-assignable friendly name.
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub protocol: u32,
    /// Client clock, informational only.
    #[serde(default)]
    pub ts: u64,
}

/// A unit of work routed to a single role's queue.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    /// Monotonic per session. The plugin dedups by this and echoes it back.
    pub id: u64,
    pub tool: String,
    #[serde(default)]
    pub args: Value,
    pub target_role: Role,
}

/// The plugin's reply to a [`Command`], echoing `id` for idempotent matching.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult {
    pub id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl CommandResult {
    pub fn ok(id: u64, result: Value) -> Self {
        Self { id, ok: true, result: Some(result), error: None }
    }
    pub fn err(id: u64, error: impl Into<String>) -> Self {
        Self { id, ok: false, result: None, error: Some(error.into()) }
    }
}

/// Long-poll body returned to a participant when commands are queued. An empty
/// queue returns `204 No Content` instead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PollResponse {
    pub commands: Vec<Command>,
}

/// Public, unauthenticated discovery endpoint. The plugin scans `/health`
/// across the port range and pins to one broker via `brokerUuid`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Health {
    pub protocol: u32,
    pub broker_uuid: String,
    pub port: u16,
}

/// Persisted to `~/.rbxmcp/broker.json` for adapters (which, unlike plugins,
/// can read files) to discover and authenticate to the broker.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    pub port: u16,
    pub pid: u32,
    pub token: String,
    pub protocol: u32,
    pub broker_uuid: String,
}

/// One role's liveness within a session, for `list_sessions`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleInfo {
    pub role: Role,
    pub state: LiveState,
    pub last_seen_ms: u64,
}

/// A session as reported by `list_sessions`, grouping its connected roles.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub session_id: String,
    pub place_id: u64,
    pub game_id: u64,
    pub place_name: String,
    pub creator_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub roles: Vec<RoleInfo>,
}
