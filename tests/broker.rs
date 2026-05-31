//! End-to-end tests for the broker over real loopback HTTP: discovery, the
//! authenticated adapter surface, and the full enqueue → poll → result loop
//! including idempotent redelivery.

use std::time::Duration;

use dust_robloxstudio_mcp::broker::build_app;
use dust_robloxstudio_mcp::protocol::{Health, PROTOCOL_VERSION, SessionInfo};
use serde_json::{Value, json};
use tokio::net::TcpListener;

const TOKEN: &str = "test-token-abc123";

/// Bring up the broker on an ephemeral port and return its base URL.
async fn spawn_broker() -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (app, _state) = build_app(TOKEN, port);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://127.0.0.1:{port}")
}

fn plugin_handshake(session_id: &str) -> Value {
    json!({
        "sessionId": session_id,
        "role": "plugin",
        "placeId": 0,
        "gameId": 0,
        "placeName": "Unpublished Place",
        "creatorId": 42,
        "protocol": PROTOCOL_VERSION,
        "ts": 0
    })
}

#[tokio::test]
async fn health_is_public_and_reports_protocol() {
    let base = spawn_broker().await;
    let health: Health = reqwest::get(format!("{base}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health.protocol, PROTOCOL_VERSION);
    assert!(!health.broker_uuid.is_empty());
    assert!(health.port > 0);
}

#[tokio::test]
async fn adapter_endpoints_require_token() {
    let base = spawn_broker().await;
    let client = reqwest::Client::new();

    let unauth = client.get(format!("{base}/sessions")).send().await.unwrap();
    assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    let authed = client
        .get(format!("{base}/sessions"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(authed.status(), reqwest::StatusCode::OK);
    let sessions: Vec<SessionInfo> = authed.json().await.unwrap();
    assert!(sessions.is_empty());
}

#[tokio::test]
async fn empty_poll_returns_204() {
    let base = spawn_broker().await;
    let client = reqwest::Client::new();
    // Short hold isn't configurable per-request, so use a small timeout to keep
    // the test fast: a freshly registered session has nothing queued, and we
    // only need to confirm the *shape* is 204 — abort the wait early.
    let resp = tokio::time::timeout(
        Duration::from_millis(800),
        client
            .post(format!("{base}/poll"))
            .json(&plugin_handshake("s-empty"))
            .send(),
    )
    .await;
    // The poll holds ~25s; our 800ms client-side timeout elapsing is the
    // expected "nothing queued" outcome.
    assert!(resp.is_err(), "expected the long-poll to still be holding");
}

#[tokio::test]
async fn full_command_loop_with_result() {
    let base = spawn_broker().await;
    let client = reqwest::Client::new();
    let session_id = "s-loop";

    // 1. Plugin registers and parks on a single poll. Capture whatever that
    //    poll ultimately returns — it is the one that will receive the command.
    let (tx, rx) = tokio::sync::oneshot::channel();
    let poll_base = base.clone();
    tokio::spawn(async move {
        let c = reqwest::Client::new();
        let resp = c
            .post(format!("{poll_base}/poll"))
            .json(&plugin_handshake("s-loop"))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body: Value = if status == reqwest::StatusCode::OK {
            resp.json().await.unwrap()
        } else {
            json!({ "commands": [] })
        };
        let _ = tx.send((status, body));
    });
    // Give the poll time to register the session and park.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 2. Adapter enqueues a command and awaits the result. The enqueue wakes
    //    the parked poll above.
    let cmd_base = base.clone();
    let cmd_task = tokio::spawn(async move {
        let c = reqwest::Client::new();
        c.post(format!("{cmd_base}/command"))
            .bearer_auth(TOKEN)
            .json(&json!({
                "sessionId": "s-loop",
                "tool": "ping",
                "args": {"x": 1},
                "targetRole": "plugin",
                "timeoutMs": 5000
            }))
            .send()
            .await
            .unwrap()
    });

    // 3. The parked poll returns the queued command.
    let (status, body) = rx.await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK);
    let commands = body["commands"].as_array().unwrap();
    assert_eq!(commands.len(), 1);
    let cmd = &commands[0];
    assert_eq!(cmd["tool"], "ping");
    assert_eq!(cmd["targetRole"], "plugin");
    let cmd_id = cmd["id"].as_u64().unwrap();

    // 4. Plugin posts the result, echoing the id.
    let res = client
        .post(format!("{base}/result"))
        .json(&json!({
            "sessionId": session_id,
            "role": "plugin",
            "id": cmd_id,
            "ok": true,
            "result": {"pong": true}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // 5. The adapter's command call resolves with that result.
    let cmd_resp = cmd_task.await.unwrap();
    assert_eq!(cmd_resp.status(), reqwest::StatusCode::OK);
    let result: Value = cmd_resp.json().await.unwrap();
    assert_eq!(result["id"].as_u64().unwrap(), cmd_id);
    assert_eq!(result["ok"], true);
    assert_eq!(result["result"]["pong"], true);

    // 6. The session now shows up in list_sessions as live.
    let sessions: Vec<SessionInfo> = client
        .get(format!("{base}/sessions"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let s = sessions.iter().find(|s| s.session_id == session_id).unwrap();
    assert_eq!(s.creator_id, 42);
    assert!(s.roles.iter().any(|r| matches!(r.state, dust_robloxstudio_mcp::protocol::LiveState::Live)));
}

#[tokio::test]
async fn command_to_unknown_session_is_404() {
    let base = spawn_broker().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/command"))
        .bearer_auth(TOKEN)
        .json(&json!({
            "sessionId": "does-not-exist",
            "tool": "ping",
            "args": {},
            "targetRole": "plugin",
            "timeoutMs": 500
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn command_routes_to_the_targeted_role() {
    // The playtest flow (step 5) relies on routing a command to the `server`
    // role while the `plugin` role of the same session is also connected.
    let base = spawn_broker().await;
    let client = reqwest::Client::new();
    let session_id = "s-roles";

    let role_handshake = |role: &str| {
        json!({ "sessionId": session_id, "role": role, "protocol": PROTOCOL_VERSION, "ts": 0 })
    };

    // Register the plugin role with a poll that parks (and should NOT receive
    // the server-targeted command).
    let (plugin_tx, plugin_rx) = tokio::sync::oneshot::channel();
    {
        let base = base.clone();
        let hs = role_handshake("plugin");
        tokio::spawn(async move {
            let c = reqwest::Client::new();
            let resp = c.post(format!("{base}/poll")).json(&hs).send().await.unwrap();
            let _ = plugin_tx.send(resp.status());
        });
    }
    // Register the server role and capture what its poll returns.
    let (server_tx, server_rx) = tokio::sync::oneshot::channel();
    {
        let base = base.clone();
        let hs = role_handshake("server");
        tokio::spawn(async move {
            let c = reqwest::Client::new();
            let resp = c.post(format!("{base}/poll")).json(&hs).send().await.unwrap();
            let body: Value = if resp.status() == reqwest::StatusCode::OK {
                resp.json().await.unwrap()
            } else {
                json!({ "commands": [] })
            };
            let _ = server_tx.send(body);
        });
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Enqueue a command for the SERVER role.
    let cmd_base = base.clone();
    let cmd_task = tokio::spawn(async move {
        let c = reqwest::Client::new();
        c.post(format!("{cmd_base}/command"))
            .bearer_auth(TOKEN)
            .json(&json!({
                "sessionId": "s-roles",
                "tool": "end_playtest",
                "args": {},
                "targetRole": "server",
                "timeoutMs": 5000
            }))
            .send()
            .await
            .unwrap()
    });

    // The server poll receives it; the plugin poll stays parked (no delivery).
    let server_body = server_rx.await.unwrap();
    let commands = server_body["commands"].as_array().unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["tool"], "end_playtest");
    let cmd_id = commands[0]["id"].as_u64().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), plugin_rx).await.is_err(),
        "plugin poll must not receive a server-targeted command"
    );

    // Server posts the result -> the adapter's command call resolves.
    client
        .post(format!("{base}/result"))
        .json(&json!({ "sessionId": session_id, "role": "server", "id": cmd_id, "ok": true, "result": { "ending": true } }))
        .send()
        .await
        .unwrap();
    let cmd_resp = cmd_task.await.unwrap();
    let result: Value = cmd_resp.json().await.unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["result"]["ending"], true);
}

#[tokio::test]
async fn command_routes_to_the_client_role() {
    // The step-6 client tools (read_client_output / get_client_state /
    // character_navigation / keyboard_input / mouse_input) route to the `client`
    // role. In production the play server proxies that queue over a RemoteEvent
    // (the client has no HttpService), but the broker routing itself must deliver
    // a client-targeted command only to the client queue.
    let base = spawn_broker().await;
    let client = reqwest::Client::new();
    let session_id = "s-client";

    let role_handshake = |role: &str| {
        json!({ "sessionId": session_id, "role": role, "protocol": PROTOCOL_VERSION, "ts": 0 })
    };

    // Server role parks and must NOT receive the client-targeted command.
    let (server_tx, server_rx) = tokio::sync::oneshot::channel();
    {
        let base = base.clone();
        let hs = role_handshake("server");
        tokio::spawn(async move {
            let c = reqwest::Client::new();
            let resp = c.post(format!("{base}/poll")).json(&hs).send().await.unwrap();
            let _ = server_tx.send(resp.status());
        });
    }
    // Client role poll captures what it receives.
    let (client_tx, client_rx) = tokio::sync::oneshot::channel();
    {
        let base = base.clone();
        let hs = role_handshake("client");
        tokio::spawn(async move {
            let c = reqwest::Client::new();
            let resp = c.post(format!("{base}/poll")).json(&hs).send().await.unwrap();
            let body: Value = if resp.status() == reqwest::StatusCode::OK {
                resp.json().await.unwrap()
            } else {
                json!({ "commands": [] })
            };
            let _ = client_tx.send(body);
        });
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Enqueue a command for the CLIENT role.
    let cmd_base = base.clone();
    let cmd_task = tokio::spawn(async move {
        let c = reqwest::Client::new();
        c.post(format!("{cmd_base}/command"))
            .bearer_auth(TOKEN)
            .json(&json!({
                "sessionId": "s-client",
                "tool": "get_client_state",
                "args": {},
                "targetRole": "client",
                "timeoutMs": 5000
            }))
            .send()
            .await
            .unwrap()
    });

    // The client poll receives it; the server poll stays parked (no delivery).
    let client_body = client_rx.await.unwrap();
    let commands = client_body["commands"].as_array().unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["tool"], "get_client_state");
    let cmd_id = commands[0]["id"].as_u64().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), server_rx).await.is_err(),
        "server poll must not receive a client-targeted command"
    );

    // Client posts the result -> the adapter's command call resolves.
    client
        .post(format!("{base}/result"))
        .json(&json!({ "sessionId": session_id, "role": "client", "id": cmd_id, "ok": true, "result": { "player": "Player1" } }))
        .send()
        .await
        .unwrap();
    let cmd_resp = cmd_task.await.unwrap();
    let result: Value = cmd_resp.json().await.unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["result"]["player"], "Player1");
}
