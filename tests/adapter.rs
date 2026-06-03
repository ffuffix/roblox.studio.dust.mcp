//! Tests for the adapter layer and the
//! broker HTTP client against a live broker.

use dust_robloxstudio_mcp::adapter::{BrokerClient, resolve_session};
use dust_robloxstudio_mcp::broker::build_app;
use dust_robloxstudio_mcp::protocol::{
    BrokerInfo, LiveState, PROTOCOL_VERSION, Role, RoleInfo, SessionInfo,
};
use tokio::net::TcpListener;

fn session(id: &str, label: Option<&str>, state: LiveState) -> SessionInfo {
    SessionInfo {
        session_id: id.to_string(),
        place_id: 0,
        game_id: 0,
        place_name: "Place".to_string(),
        creator_id: 1,
        label: label.map(str::to_string),
        roles: vec![RoleInfo { role: Role::Plugin, state, last_seen_ms: 0 }],
    }
}

#[test]
fn single_live_session_is_the_default() {
    let sessions = vec![session("only", None, LiveState::Live)];
    assert_eq!(resolve_session(&sessions, None).unwrap(), "only");
}

#[test]
fn no_live_sessions_errors() {
    let sessions = vec![session("dead", None, LiveState::Dead)];
    let err = resolve_session(&sessions, None).unwrap_err();
    assert!(err.contains("no live"), "got: {err}");
}

#[test]
fn multiple_live_sessions_force_disambiguation() {
    let sessions = vec![
        session("a", None, LiveState::Live),
        session("b", Some("staging"), LiveState::Live),
    ];
    let err = resolve_session(&sessions, None).unwrap_err();
    assert!(err.contains("list_sessions"), "got: {err}");
    assert!(err.contains("staging"), "got: {err}");
}

#[test]
fn selector_matches_by_id_or_label() {
    let sessions = vec![
        session("a", None, LiveState::Live),
        session("b", Some("staging"), LiveState::Stale),
    ];
    assert_eq!(resolve_session(&sessions, Some("a")).unwrap(), "a");
    assert_eq!(resolve_session(&sessions, Some("staging")).unwrap(), "b");
}

#[test]
fn unknown_selector_errors() {
    let sessions = vec![session("a", None, LiveState::Live)];
    let err = resolve_session(&sessions, Some("nope")).unwrap_err();
    assert!(err.contains("list_sessions"), "got: {err}");
}

async fn spawn_broker_and_client() -> (String, BrokerClient) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (app, _state) = build_app("tok", port);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let info = BrokerInfo {
        port,
        pid: 0,
        token: "tok".to_string(),
        protocol: PROTOCOL_VERSION,
        broker_uuid: "ignored".to_string(),
    };
    (format!("http://127.0.0.1:{port}"), BrokerClient::new(&info))
}

#[tokio::test]
async fn client_lists_empty_then_errors_on_unknown_session() {
    let (_base, client) = spawn_broker_and_client().await;

    let sessions = client.list_sessions().await.unwrap();
    assert!(sessions.is_empty());

    let err = client
        .command("ghost", "ping", serde_json::json!({}), Role::Plugin, 500)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("404"), "got: {err}");
}
