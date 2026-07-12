use std::collections::HashMap;

use axum::extract::Query;
use axum::http::StatusCode;
use tokio::sync::Notify;

use super::*;

#[tokio::test]
async fn forced_gap_freezes_later_events_until_replay_restores_continuity() {
    let replay_started = Arc::new(Notify::new());
    let release_replay = Arc::new(Notify::new());
    let runtime_addr =
        spawn_repair_runtime(replay_started.clone(), release_replay.clone(), false).await;
    let gateway = Arc::new(gateway_at_cursor(runtime_addr, 3).await);
    let mut recoveries = gateway.recoveries.subscribe();

    let repair_gateway = gateway.clone();
    let repair = tokio::spawn(async move {
        repair_gateway.ingest_source_event(runtime_event(11)).await;
    });
    replay_started.notified().await;
    gateway.ingest_source_event(runtime_event(12)).await;

    {
        let states = gateway.materialized.read().await;
        let state = &states["local"];
        assert_eq!(state.source_health.last_source_seq, Some(3));
        assert_eq!(state.source_health.state, SourceHealthState::GapDetected);
        assert!(!state.sessions.contains_key("session_2"));
    }

    release_replay.notify_one();
    repair.await.expect("repair task");
    let states = gateway.materialized.read().await;
    let state = &states["local"];
    assert_eq!(state.source_health.last_source_seq, Some(12));
    assert_eq!(state.source_health.state, SourceHealthState::Live);
    assert_eq!(state.sessions["session_1"].status, "ready");
    assert!(state.sessions.contains_key("session_2"));
    assert!(matches!(
        recoveries.try_recv(),
        Ok(SourceRecoverySignal::Filled(SourceCursor {
            source_seq: 12,
            ..
        }))
    ));
}

#[tokio::test]
async fn replay_failure_uses_atomic_high_watermark_fallback() {
    let replay_started = Arc::new(Notify::new());
    let release_replay = Arc::new(Notify::new());
    let runtime_addr =
        spawn_repair_runtime(replay_started.clone(), release_replay.clone(), true).await;
    let gateway = Arc::new(gateway_at_cursor(runtime_addr, 3).await);
    let mut recoveries = gateway.recoveries.subscribe();

    let repair_gateway = gateway.clone();
    let repair = tokio::spawn(async move {
        repair_gateway.ingest_source_event(runtime_event(11)).await;
    });
    replay_started.notified().await;
    release_replay.notify_one();
    repair.await.expect("repair task");

    let states = gateway.materialized.read().await;
    let state = &states["local"];
    assert_eq!(state.source_health.last_source_seq, Some(11));
    assert_eq!(state.source_health.state, SourceHealthState::Live);
    assert_eq!(state.sessions["session_1"].status, "ready");
    assert_eq!(
        gateway
            .metrics
            .snapshot_resync_count
            .load(Ordering::Relaxed),
        1
    );
    assert!(matches!(
        recoveries.try_recv(),
        Ok(SourceRecoverySignal::Resync { source_id, .. }) if source_id == "local"
    ));
}

async fn gateway_at_cursor(runtime_addr: SocketAddr, cursor: i64) -> GatewayState {
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    let mut materialized = gateway.materialized.write().await;
    let state = materialized.get_mut("local").unwrap();
    let session = state.sessions.get_mut("session_1").unwrap();
    session.status = "turn_running".into();
    session.active_turn_id = Some("turn_1".into());
    state
        .source_health
        .transition(SourceHealthState::Live, Some(cursor), None);
    drop(materialized);
    gateway
}

async fn spawn_repair_runtime(
    replay_started: Arc<Notify>,
    release_replay: Arc<Notify>,
    fail_replay: bool,
) -> SocketAddr {
    let replay = move |Query(query): Query<HashMap<String, String>>| {
        let (started, release) = (replay_started.clone(), release_replay.clone());
        async move {
            started.notify_one();
            release.notified().await;
            if fail_replay {
                return Err((StatusCode::SERVICE_UNAVAILABLE, "forced replay failure"));
            }
            let after = query
                .get("after_seq")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default();
            let rows = (4..=11)
                .filter(|seq| *seq > after)
                .map(runtime_record)
                .collect::<Vec<_>>();
            Ok(Json(rows))
        }
    };
    let app = Router::new()
        .route("/v1/events", get(replay))
        .route(
            "/v1/bootstrap",
            get(|| async {
                Json(json!({
                    "source_epoch": "static-0",
                    "high_watermark": 11,
                    "records": {
                        "sessions": [{
                            "id": "session_1", "provider": "codex", "status": "ready",
                            "cwd": null, "model": null, "permission_mode": null,
                            "system_prompt": null, "metadata": {}, "provider_session_ref": null,
                            "canonical_provider_session_ref": null, "active_turn_id": null,
                            "worktree_id": null, "created_at": 1, "updated_at": 11,
                            "closed_at": null, "failure_code": null, "failure_message": null
                        }],
                        "approvals": [], "teams": [], "team_members": [],
                        "team_messages": [], "team_deliveries": [], "managed_worktrees": [],
                        "managed_worktree_claims": [], "processes": []
                    }
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
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

fn runtime_event(seq: i64) -> SourceEvent {
    SourceEvent::from_runtime_event("local", "static-0", runtime_record(seq))
}

fn runtime_record(seq: i64) -> RuntimeEventRecord {
    let (scope, scope_id, session_id, turn_id, kind, payload) = match seq {
        11 => (
            RuntimeEventScope::Session,
            "session_1",
            Some("session_1".to_string()),
            Some("turn_1".to_string()),
            "turn.completed",
            json!({}),
        ),
        12 => (
            RuntimeEventScope::Session,
            "session_2",
            Some("session_2".to_string()),
            None,
            "session.created",
            json!({ "provider": "codex" }),
        ),
        _ => (
            RuntimeEventScope::System,
            "runtime",
            None,
            None,
            "runtime.progress",
            json!({}),
        ),
    };
    RuntimeEventRecord {
        row_id: seq,
        event_id: format!("evt_{seq}"),
        scope,
        scope_id: scope_id.into(),
        session_id,
        team_id: None,
        turn_id,
        seq,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: seq,
    }
}
