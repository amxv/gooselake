use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::net::TcpListener;

use super::*;

const STALE_AFTER_MS: u64 = 40;

#[tokio::test]
async fn keepalives_preserve_idle_command_admission_without_advancing_cursor() {
    let create_hits = Arc::new(AtomicUsize::new(0));
    let addr = spawn_keepalive_runtime(create_hits.clone()).await;
    let gateway = configured_gateway(addr);
    gateway.bootstrap_enabled_sources().await;
    let initial_health = gateway.materialized.read().await["local"]
        .source_health
        .clone();
    assert_eq!(initial_health.last_source_seq, Some(1));
    let handles = gateway.spawn_runtime_source_tasks().await;

    tokio::time::sleep(Duration::from_millis(STALE_AFTER_MS * 3)).await;
    let refreshed = gateway.materialized.read().await["local"]
        .source_health
        .clone();
    assert_eq!(refreshed.state, SourceHealthState::Live);
    assert_eq!(refreshed.last_source_seq, Some(1));
    assert!(refreshed.updated_at > initial_health.updated_at);

    let mut connection = test_connection(&gateway);
    let response = gateway
        .admit_and_route_command(
            &mut connection,
            create_session_command("cmd_idle_keepalive", "local"),
        )
        .await;
    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));
    assert_eq!(create_hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        gateway.materialized.read().await["local"]
            .source_health
            .last_source_seq,
        Some(1)
    );

    for handle in handles {
        handle.abort();
    }
}

#[tokio::test]
async fn silent_stream_becomes_stale_and_rejects_commands_without_cursor_change() {
    let addr = spawn_silent_runtime().await;
    let gateway = configured_gateway(addr);
    gateway.bootstrap_enabled_sources().await;
    let handles = gateway.spawn_runtime_source_tasks().await;

    tokio::time::sleep(Duration::from_millis(STALE_AFTER_MS * 2)).await;
    let health = gateway.materialized.read().await["local"]
        .source_health
        .clone();
    assert_eq!(health.state, SourceHealthState::Stale);
    assert_eq!(health.last_source_seq, Some(1));

    let mut connection = test_connection(&gateway);
    let response = gateway
        .admit_and_route_command(
            &mut connection,
            create_session_command("cmd_silent_stale", "local"),
        )
        .await;
    let Some(Payload::CommandRejected(rejected)) = response.payload else {
        panic!("silent source command must be rejected");
    };
    assert_eq!(rejected.error.expect("error").code, REASON_SOURCE_STALE);
    assert_eq!(
        gateway.materialized.read().await["local"]
            .source_health
            .last_source_seq,
        Some(1)
    );

    for handle in handles {
        handle.abort();
    }
}

fn configured_gateway(addr: SocketAddr) -> Arc<GatewayState> {
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{addr}");
    config.replay.source_stale_after_ms = STALE_AFTER_MS;
    Arc::new(test_gateway(config))
}

async fn spawn_keepalive_runtime(create_hits: Arc<AtomicUsize>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind runtime");
    let addr = listener.local_addr().expect("runtime addr");
    let create_session = move |Json(input): Json<serde_json::Value>| {
        let create_hits = create_hits.clone();
        async move {
            create_hits.fetch_add(1, Ordering::SeqCst);
            Json(session_response(&input))
        }
    };
    let app = base_runtime_router()
        .route(
            "/v1/events/stream",
            get(|| async {
                Sse::new(tokio_stream::pending::<Result<Event, Infallible>>()).keep_alive(
                    KeepAlive::new()
                        .interval(Duration::from_millis(10))
                        .text("runtime keepalive"),
                )
            }),
        )
        .route("/v1/sessions", post(create_session));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve runtime") });
    addr
}

async fn spawn_silent_runtime() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind runtime");
    let addr = listener.local_addr().expect("runtime addr");
    let app = base_runtime_router().route(
        "/v1/events/stream",
        get(|| async { Sse::new(tokio_stream::pending::<Result<Event, Infallible>>()) }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve runtime") });
    addr
}

fn base_runtime_router() -> Router {
    Router::new()
        .route(
            "/v1/bootstrap",
            get(|| async {
                Json(json!({
                    "source_epoch": "epoch-idle", "high_watermark": 1,
                    "records": { "sessions": [], "approvals": [], "teams": [],
                        "team_members": [], "team_messages": [], "team_deliveries": [],
                        "managed_worktrees": [], "managed_worktree_claims": [], "processes": [] }
                }))
            }),
        )
        .route(
            "/v1/providers",
            get(|| async { Json(json!({ "providers": [] })) }),
        )
        .route(
            "/v1/diagnostics",
            get(|| async {
                Json(json!({
                    "providers": {}, "comms": {}, "processes": {},
                    "worktrees": {}, "recovery": {}
                }))
            }),
        )
}

fn session_response(input: &serde_json::Value) -> serde_json::Value {
    json!({
        "id": "session_idle", "provider": input["provider"], "status": "ready",
        "cwd": input["cwd"], "model": input["model"],
        "permission_mode": input["permission_mode"], "system_prompt": null,
        "metadata": input["metadata"], "provider_session_ref": null,
        "canonical_provider_session_ref": null, "active_turn_id": null,
        "worktree_id": null, "created_at": 1, "updated_at": 1,
        "closed_at": null, "failure_code": null, "failure_message": null
    })
}
