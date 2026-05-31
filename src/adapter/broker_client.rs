//! The adapter's HTTP client to the broker, plus the session disambiguation
//! rule (§3). The client speaks the authenticated adapter surface
//! (`/sessions`, `/command`, `/shutdown`) over loopback.

use std::time::Duration;

use anyhow::{Result, bail};
use serde_json::{Value, json};

use crate::protocol::{BrokerInfo, CommandResult, Health, LiveState, Role, SessionInfo};

/// Extra slack added on top of a command's broker-side timeout before the
/// client-side request gives up, so the broker's own `504` wins the race and we
/// surface its message rather than a generic transport timeout.
const CLIENT_TIMEOUT_SLACK: Duration = Duration::from_secs(5);

pub struct BrokerClient {
    base: String,
    token: String,
    http: reqwest::Client,
}

impl BrokerClient {
    pub fn new(info: &BrokerInfo) -> Self {
        Self {
            base: format!("http://127.0.0.1:{}", info.port),
            token: info.token.clone(),
            http: reqwest::Client::new(),
        }
    }

    /// Unauthenticated discovery probe.
    pub async fn health(&self) -> Result<Health> {
        let resp = self
            .http
            .get(format!("{}/health", self.base))
            .timeout(Duration::from_secs(2))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        let resp = self
            .http
            .get(format!("{}/sessions", self.base))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    /// Enqueue a command for a session's role and wait for the plugin's result.
    pub async fn command(
        &self,
        session_id: &str,
        tool: &str,
        args: Value,
        target_role: Role,
        timeout_ms: u64,
    ) -> Result<CommandResult> {
        let resp = self
            .http
            .post(format!("{}/command", self.base))
            .bearer_auth(&self.token)
            .timeout(Duration::from_millis(timeout_ms) + CLIENT_TIMEOUT_SLACK)
            .json(&json!({
                "sessionId": session_id,
                "tool": tool,
                "args": args,
                "targetRole": target_role,
                "timeoutMs": timeout_ms,
            }))
            .send()
            .await?;

        let status = resp.status();
        if status.is_success() {
            Ok(resp.json().await?)
        } else {
            let body = resp.text().await.unwrap_or_default();
            bail!("broker /command returned {status}: {body}");
        }
    }

    /// Ask the broker to shut down (used by tooling; not on the normal path).
    pub async fn shutdown(&self) -> Result<()> {
        self.http
            .post(format!("{}/shutdown", self.base))
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(2))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

/// A session has a live connection if any of its roles is `Live`.
fn is_live(s: &SessionInfo) -> bool {
    s.roles.iter().any(|r| r.state == LiveState::Live)
}

/// Short human description of a session for ambiguity errors.
fn describe(s: &SessionInfo) -> String {
    let name = if s.place_name.is_empty() { "<unnamed>" } else { &s.place_name };
    match &s.label {
        Some(label) => format!("'{label}' ({name}, id {})", s.session_id),
        None => format!("{name} (id {})", s.session_id),
    }
}

/// Resolve a session selector to a concrete `sessionId`, enforcing the §3
/// disambiguation rule: default to the sole live session, but **never guess**
/// when several are live — force a `list_sessions` call instead.
///
/// `Err` carries a model-facing message explaining how to proceed.
pub fn resolve_session(sessions: &[SessionInfo], selector: Option<&str>) -> Result<String, String> {
    if let Some(sel) = selector {
        return sessions
            .iter()
            .find(|s| s.session_id == sel || s.label.as_deref() == Some(sel))
            .map(|s| s.session_id.clone())
            .ok_or_else(|| {
                format!(
                    "no connected session matches '{sel}'. Call list_sessions to see \
                     the open places and their ids/labels."
                )
            });
    }

    let live: Vec<&SessionInfo> = sessions.iter().filter(|s| is_live(s)).collect();
    match live.as_slice() {
        [] => Err("no live Studio sessions are connected. Open the place in Studio with \
                   the Dust plugin enabled (and 'Allow HTTP Requests' on), then retry."
            .to_string()),
        [only] => Ok(only.session_id.clone()),
        many => {
            let candidates = many.iter().map(|s| describe(s)).collect::<Vec<_>>().join("; ");
            Err(format!(
                "{} live Studio sessions are connected; refusing to guess which place to \
                 target. Call list_sessions, then pass session=<id or label>. Candidates: {candidates}",
                many.len()
            ))
        }
    }
}
