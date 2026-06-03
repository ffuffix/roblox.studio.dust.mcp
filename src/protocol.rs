//! Wire protocol shared by the broker, the stdio adapter, and (mirrored in Luau)
//! the Studio plugin / helpers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 1;

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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Handshake {
    pub session_id: String,
    pub role: Role,
    #[serde(default)]
    pub place_id: u64,
    #[serde(default)]
    pub game_id: u64,
    #[serde(default)]
    pub place_name: String,
    #[serde(default)]
    pub creator_id: u64,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub protocol: u32,
    #[serde(default)]
    pub ts: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub id: u64,
    pub tool: String,
    #[serde(default)]
    pub args: Value,
    pub target_role: Role,
}

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PollResponse {
    pub commands: Vec<Command>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Health {
    pub protocol: u32,
    pub broker_uuid: String,
    pub port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    pub port: u16,
    pub pid: u32,
    pub token: String,
    pub protocol: u32,
    pub broker_uuid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleInfo {
    pub role: Role,
    pub state: LiveState,
    pub last_seen_ms: u64,
}

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
