use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::Query;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use runtime_core::{RuntimeEventCriticality, RuntimeEventScope};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;

use super::*;

#[tokio::test]
async fn client_adds_bearer_token_and_decodes_runtime_records() {
    let addr = spawn_client_mock("runtime-token", Arc::new(Mutex::new(Vec::new()))).await;
    let client = test_client(addr, Some("runtime-token".to_string()));

    let sessions = client
        .list_sessions()
        .await
        .expect("sessions response decodes");

    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].id, "session_1");
    assert_eq!(sessions[0].provider, "codex");
}

#[tokio::test]
async fn client_paginates_global_replay_with_after_seq() {
    let seen_after = Arc::new(Mutex::new(Vec::new()));
    let addr = spawn_client_mock("runtime-token", seen_after.clone()).await;
    let client = test_client(addr, Some("runtime-token".to_string()));

    let mut cursor = None;
    let mut rows = Vec::new();
    loop {
        let page = client
            .replay_global_events(cursor, Some(2))
            .await
            .expect("replay page");
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|event| event.row_id);
        rows.extend(page);
        if rows.len() >= 3 {
            break;
        }
    }

    assert_eq!(
        rows.iter().map(|event| event.row_id).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(*seen_after.lock().unwrap(), vec![None, Some(2)]);
}

fn test_client(addr: SocketAddr, token: Option<String>) -> GooselakeRuntimeClient {
    GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
        "local",
        format!("http://{addr}"),
        token,
    ))
    .expect("test client")
}

async fn spawn_client_mock(
    expected_token: &'static str,
    seen_after: Arc<Mutex<Vec<Option<i64>>>>,
) -> SocketAddr {
    #[derive(Debug, Deserialize)]
    struct ReplayQuery {
        after_seq: Option<i64>,
        limit: Option<usize>,
    }

    let sessions_route = move |headers: HeaderMap| async move {
        if !authorized(&headers, expected_token) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        Json(vec![session_record("session_1")]).into_response()
    };

    let replay_seen_after = seen_after.clone();
    let replay_route = move |headers: HeaderMap, Query(query): Query<ReplayQuery>| {
        let replay_seen_after = replay_seen_after.clone();
        async move {
            if !authorized(&headers, expected_token) {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            replay_seen_after.lock().unwrap().push(query.after_seq);
            let after = query.after_seq.unwrap_or(0);
            let limit = query.limit.unwrap_or(500);
            let events = (1..=3)
                .filter(|row_id| *row_id > after)
                .take(limit)
                .map(runtime_event)
                .collect::<Vec<_>>();
            Json(events).into_response()
        }
    };

    let app = Router::new()
        .route("/v1/sessions", get(sessions_route))
        .route("/v1/events", get(replay_route));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock server");
    });
    addr
}

fn authorized(headers: &HeaderMap, expected_token: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(format!("Bearer {expected_token}").as_str())
}

fn session_record(id: &str) -> SessionRecord {
    SessionRecord {
        id: id.to_string(),
        provider: "codex".to_string(),
        status: "running".to_string(),
        cwd: Some("/repo".to_string()),
        model: Some("gpt-5".to_string()),
        permission_mode: None,
        system_prompt: None,
        metadata: json!({}),
        provider_session_ref: None,
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 1,
        updated_at: 1,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

fn runtime_event(row_id: i64) -> RuntimeEventRecord {
    RuntimeEventRecord {
        row_id,
        event_id: format!("event_{row_id}"),
        scope: RuntimeEventScope::Session,
        scope_id: "session_1".to_string(),
        session_id: Some("session_1".to_string()),
        team_id: None,
        turn_id: None,
        seq: row_id + 10,
        kind: "session.updated".to_string(),
        criticality: RuntimeEventCriticality::Droppable,
        payload: json!({ "row": row_id }),
        provider: Some("codex".to_string()),
        provider_seq: Some(row_id),
        created_at: row_id,
    }
}
