use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use axum::extract::Query;
use axum::http::StatusCode;
use tokio::sync::Notify;

use super::*;

#[tokio::test]
async fn forced_gap_freezes_later_events_until_replay_restores_continuity() {
    let replay_started = Arc::new(Notify::new());
    let release_replay = Arc::new(Notify::new());
    let runtime_addr = spawn_repair_runtime(
        replay_started.clone(),
        release_replay.clone(),
        ReplayResponse::Complete,
    )
    .await;
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
        Ok(SourceRecoverySignal::Resync {
            cursor: SourceCursor { source_seq: 12, .. },
            ..
        })
    ));
}

#[tokio::test]
async fn replay_failure_uses_atomic_high_watermark_fallback() {
    let replay_started = Arc::new(Notify::new());
    let release_replay = Arc::new(Notify::new());
    let runtime_addr = spawn_repair_runtime(
        replay_started.clone(),
        release_replay.clone(),
        ReplayResponse::Fail,
    )
    .await;
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
        Ok(SourceRecoverySignal::Resync { cursor, .. }) if cursor.source_id == "local"
    ));
}

#[tokio::test]
async fn missing_replay_row_falls_back_without_publishing_partial_authority() {
    let replay_started = Arc::new(Notify::new());
    let release_replay = Arc::new(Notify::new());
    let runtime_addr = spawn_repair_runtime(
        replay_started.clone(),
        release_replay.clone(),
        ReplayResponse::MissingFirst,
    )
    .await;
    let gateway = Arc::new(gateway_at_cursor(runtime_addr, 3).await);
    let mut recoveries = gateway.recoveries.subscribe();
    let mut patches = gateway.verification_patch_receiver();

    let repair_gateway = gateway.clone();
    let repair = tokio::spawn(async move {
        repair_gateway.ingest_source_event(runtime_event(11)).await;
    });
    replay_started.notified().await;
    assert_eq!(
        gateway.materialized.read().await["local"]
            .source_health
            .last_source_seq,
        Some(3)
    );
    release_replay.notify_one();
    repair.await.expect("missing-row fallback");

    let states = gateway.materialized.read().await;
    let state = &states["local"];
    assert_eq!(state.source_health.last_source_seq, Some(11));
    assert_eq!(state.source_health.state, SourceHealthState::Live);
    assert_eq!(state.sessions["session_1"].status, "ready");
    while let Ok(patch) = patches.try_recv() {
        assert_eq!(patch.kind, MaterializedPatchKind::SourceHealthTransition);
        assert!(patch
            .source_cursor
            .is_none_or(|cursor| cursor.source_seq <= 3));
    }
    assert!(matches!(
        recoveries.try_recv(),
        Ok(SourceRecoverySignal::Resync { cursor, .. }) if cursor.source_seq == 11
    ));
    assert!(recoveries.try_recv().is_err());
}

#[tokio::test]
async fn failed_repairs_keep_pending_events_bounded_without_false_recovery() {
    let runtime_addr = spawn_failed_repair_runtime().await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    config.materializer.event_buffer_size = 4;
    let gateway = live_gateway_with_session_version(config, 1).await;
    {
        let mut states = gateway.materialized.write().await;
        states.get_mut("local").unwrap().source_health.transition(
            SourceHealthState::Live,
            Some(3),
            None,
        );
    }
    let mut recoveries = gateway.recoveries.subscribe();
    for seq in 11..=24 {
        gateway.ingest_source_event(runtime_event(seq)).await;
    }

    assert_eq!(
        gateway.verification_gap_queue("local").await,
        (4, true, false)
    );
    let states = gateway.materialized.read().await;
    assert_eq!(states["local"].source_health.last_source_seq, Some(3));
    assert_eq!(
        states["local"].source_health.state,
        SourceHealthState::GapDetected
    );
    assert!(recoveries.try_recv().is_err());
}

#[tokio::test]
async fn overflow_forces_snapshot_resync_covering_every_observed_row() {
    let replay_started = Arc::new(Notify::new());
    let release_replay = Arc::new(Notify::new());
    let runtime_addr =
        spawn_overflow_repair_runtime(replay_started.clone(), release_replay.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    config.materializer.event_buffer_size = 4;
    let gateway = Arc::new(live_gateway_with_session_version(config, 1).await);
    {
        let mut states = gateway.materialized.write().await;
        states.get_mut("local").unwrap().source_health.transition(
            SourceHealthState::Live,
            Some(3),
            None,
        );
    }
    let mut recoveries = gateway.recoveries.subscribe();
    let repair_gateway = gateway.clone();
    let repair = tokio::spawn(async move {
        repair_gateway.ingest_source_event(runtime_event(11)).await;
    });
    replay_started.notified().await;
    for seq in 12..=15 {
        gateway.ingest_source_event(runtime_event(seq)).await;
    }
    assert_eq!(
        gateway.verification_gap_queue("local").await,
        (4, true, true)
    );
    release_replay.notify_one();
    repair.await.expect("overflow repair");

    let states = gateway.materialized.read().await;
    assert_eq!(states["local"].source_health.last_source_seq, Some(15));
    assert!(states["local"].sessions.contains_key("session_2"));
    assert!(matches!(
        recoveries.try_recv(),
        Ok(SourceRecoverySignal::Resync { cursor, .. }) if cursor.source_id == "local"
    ));
    assert!(recoveries.try_recv().is_err());
    assert_eq!(gateway.verification_gap_queue("local").await.0, 0);
}

#[tokio::test]
async fn epoch_mismatch_skips_replay_and_atomically_rebases_before_resync() {
    let replay_called = Arc::new(AtomicBool::new(false));
    let runtime_addr = spawn_epoch_rebase_runtime(replay_called.clone()).await;
    let gateway = gateway_at_cursor(runtime_addr, 3).await;
    let mut recoveries = gateway.recoveries.subscribe();
    let event = SourceEvent::from_runtime_event("local", "epoch-b", runtime_record(11));
    gateway.ingest_source_event(event).await;

    assert!(!replay_called.load(Ordering::SeqCst));
    let states = gateway.materialized.read().await;
    assert_eq!(states["local"].source_epoch, "epoch-b");
    assert_eq!(states["local"].source_health.last_source_seq, Some(11));
    assert_eq!(states["local"].sessions["session_1"].status, "ready");
    assert!(matches!(
        recoveries.try_recv(),
        Ok(SourceRecoverySignal::Resync { cursor, .. }) if cursor.source_id == "local"
    ));
    assert!(recoveries.try_recv().is_err());
    assert_eq!(gateway.verification_gap_queue("local").await.0, 0);
}

#[tokio::test]
async fn released_failed_repair_rearms_once_on_replaying_and_converges_atomically() {
    let controls = ReleasedRepairControls::default();
    let runtime_addr = spawn_released_repair_runtime(controls.clone()).await;
    let gateway = Arc::new(gateway_at_cursor(runtime_addr, 4).await);
    let mut recoveries = gateway.recoveries.subscribe();
    let mut patches = gateway.verification_patch_receiver();

    let repair_gateway = gateway.clone();
    let repair = tokio::spawn(async move {
        repair_gateway.ingest_source_event(runtime_event(7)).await;
    });
    controls.replay_started.notified().await;

    assert_frozen_at_four(&gateway).await;
    assert!(recoveries.try_recv().is_err());
    let duplicate_edge = SourceHealth {
        source_id: "local".into(),
        source_epoch: "static-0".into(),
        state: SourceHealthState::Replaying,
        last_source_seq: Some(7),
        last_error: None,
        updated_at: now_ms(),
    };
    gateway
        .verification_update_source_health(duplicate_edge.clone())
        .await;
    assert_eq!(controls.replay_calls.load(Ordering::SeqCst), 1);
    assert_frozen_at_four(&gateway).await;

    controls.release_replay.notify_one();
    controls.bootstrap_started.notified().await;
    assert_frozen_at_four(&gateway).await;
    assert!(recoveries.try_recv().is_err());

    controls.release_bootstrap.notify_one();
    repair.await.expect("initial released repair");
    assert_eq!(
        gateway.verification_gap_queue("local").await,
        (1, false, false)
    );
    assert_frozen_at_four(&gateway).await;
    assert!(recoveries.try_recv().is_err());

    controls.ready.store(true, Ordering::SeqCst);
    gateway
        .verification_update_source_health(duplicate_edge)
        .await;

    assert_eq!(controls.replay_calls.load(Ordering::SeqCst), 2);
    assert_eq!(controls.bootstrap_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        gateway.verification_gap_queue("local").await,
        (0, false, false)
    );
    let state = gateway.materialized.read().await["local"].clone();
    assert_eq!(state.source_health.last_source_seq, Some(7));
    assert_eq!(state.source_health.state, SourceHealthState::Live);
    assert_eq!(state.sessions["session_1"].status, "turn_running");

    while let Ok(patch) = patches.try_recv() {
        assert_eq!(patch.kind, MaterializedPatchKind::SourceHealthTransition);
        assert!(patch
            .source_cursor
            .is_none_or(|cursor| cursor.source_seq <= 4));
    }

    let SourceRecoverySignal::Resync { cursor, reason } =
        recoveries.try_recv().expect("one recovery replacement");
    assert_eq!(cursor.source_seq, 7);
    assert!(recoveries.try_recv().is_err());
    let (filled, replacement) = gateway
        .source_recovery_frames(cursor.clone(), &state, &reason)
        .expect("installed recovery authority");
    assert!(matches!(filled.payload, Some(Payload::SourceGapFilled(_))));
    let Some(Payload::SourceSnapshotResync(replacement)) = replacement.payload else {
        panic!("authoritative replacement must follow gap_filled");
    };
    assert_eq!(
        replacement.cursor.expect("replacement cursor").sources[0].source_seq,
        7
    );
    assert!(
        replacement
            .coverage
            .expect("replacement coverage")
            .authoritative
    );

    let mut advanced = state.clone();
    assert!(!advanced.reduce_source_event(runtime_event(8)).duplicate);
    let (filled, replacement) = gateway
        .source_recovery_frames(cursor.clone(), &advanced, &reason)
        .expect("contiguous live authority may advance before recovery delivery");
    let Some(Payload::SourceGapFilled(filled)) = filled.payload else {
        panic!("advanced recovery must remain gap_filled first");
    };
    assert_eq!(filled.cursor.expect("advanced fill cursor").source_seq, 8);
    let Some(Payload::SourceSnapshotResync(replacement)) = replacement.payload else {
        panic!("advanced recovery must retain its authoritative replacement");
    };
    assert_eq!(
        replacement.cursor.expect("advanced resync cursor").sources[0].source_seq,
        8
    );

    advanced.transition_source_health(
        SourceHealthState::GapDetected,
        Some("second gap".to_string()),
    );
    assert!(gateway
        .source_recovery_frames(cursor, &advanced, &reason)
        .is_err());
}

async fn assert_frozen_at_four(gateway: &GatewayState) {
    let states = gateway.materialized.read().await;
    let state = &states["local"];
    assert_eq!(state.source_health.last_source_seq, Some(4));
    assert_eq!(state.source_health.state, SourceHealthState::GapDetected);
    assert_eq!(state.sessions["session_1"].status, "turn_running");
    assert!(!state.sessions.contains_key("session_2"));
}

#[derive(Clone, Default)]
struct ReleasedRepairControls {
    ready: Arc<AtomicBool>,
    replay_started: Arc<Notify>,
    bootstrap_started: Arc<Notify>,
    release_replay: Arc<Notify>,
    release_bootstrap: Arc<Notify>,
    replay_calls: Arc<AtomicUsize>,
    bootstrap_calls: Arc<AtomicUsize>,
}

#[derive(Clone, Copy)]
enum ReplayResponse {
    Complete,
    Fail,
    MissingFirst,
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
    response: ReplayResponse,
) -> SocketAddr {
    let replay = move |Query(query): Query<HashMap<String, String>>| {
        let (started, release) = (replay_started.clone(), release_replay.clone());
        async move {
            started.notify_one();
            release.notified().await;
            if matches!(response, ReplayResponse::Fail) {
                return Err((StatusCode::SERVICE_UNAVAILABLE, "forced replay failure"));
            }
            let after = query
                .get("after_seq")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default();
            let first = match response {
                ReplayResponse::Complete => 4,
                ReplayResponse::MissingFirst => 5,
                ReplayResponse::Fail => unreachable!(),
            };
            let rows = (first..=11)
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

async fn spawn_failed_repair_runtime() -> SocketAddr {
    let app = Router::new()
        .route(
            "/v1/events",
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "replay unavailable") }),
        )
        .route(
            "/v1/bootstrap",
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "bootstrap unavailable") }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn spawn_released_repair_runtime(controls: ReleasedRepairControls) -> SocketAddr {
    let replay_controls = controls.clone();
    let replay = move |Query(query): Query<HashMap<String, String>>| {
        let controls = replay_controls.clone();
        async move {
            controls.replay_calls.fetch_add(1, Ordering::SeqCst);
            if !controls.ready.load(Ordering::SeqCst) {
                controls.replay_started.notify_one();
                controls.release_replay.notified().await;
                return Err((StatusCode::GATEWAY_TIMEOUT, "released replay expired"));
            }
            let after = query
                .get("after_seq")
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or_default();
            Ok(Json(
                (5..=7)
                    .filter(|seq| *seq > after)
                    .map(runtime_record)
                    .collect::<Vec<_>>(),
            ))
        }
    };
    let bootstrap_controls = controls.clone();
    let bootstrap = move || {
        let controls = bootstrap_controls.clone();
        async move {
            controls.bootstrap_calls.fetch_add(1, Ordering::SeqCst);
            if !controls.ready.load(Ordering::SeqCst) {
                controls.bootstrap_started.notify_one();
                controls.release_bootstrap.notified().await;
                return Err((StatusCode::GATEWAY_TIMEOUT, "released bootstrap expired"));
            }
            Ok(Json(json!({
                "source_epoch": "static-0",
                "high_watermark": 7,
                "records": {
                    "sessions": [], "approvals": [], "teams": [], "team_members": [],
                    "team_messages": [], "team_deliveries": [], "managed_worktrees": [],
                    "managed_worktree_claims": [], "processes": []
                }
            })))
        }
    };
    let app = Router::new()
        .route("/v1/events", get(replay))
        .route("/v1/bootstrap", get(bootstrap))
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

async fn spawn_epoch_rebase_runtime(replay_called: Arc<AtomicBool>) -> SocketAddr {
    let replay = move || {
        let replay_called = replay_called.clone();
        async move {
            replay_called.store(true, Ordering::SeqCst);
            Json((4..=11).map(runtime_record).collect::<Vec<_>>())
        }
    };
    let app = Router::new()
        .route("/v1/events", get(replay))
        .route(
            "/v1/bootstrap",
            get(|| async {
                Json(json!({
                    "source_epoch": "epoch-b", "high_watermark": 11,
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

async fn spawn_overflow_repair_runtime(
    replay_started: Arc<Notify>,
    release_replay: Arc<Notify>,
) -> SocketAddr {
    let replay = move || {
        let (started, release) = (replay_started.clone(), release_replay.clone());
        async move {
            started.notify_one();
            release.notified().await;
            Json((4..=10).map(runtime_record).collect::<Vec<_>>())
        }
    };
    let app = Router::new()
        .route("/v1/events", get(replay))
        .route(
            "/v1/bootstrap",
            get(|| async {
                Json(json!({
                    "source_epoch": "static-0", "high_watermark": 15,
                    "records": {
                        "sessions": [
                            {
                                "id": "session_1", "provider": "codex", "status": "ready",
                                "cwd": null, "model": null, "permission_mode": null,
                                "system_prompt": null, "metadata": {}, "provider_session_ref": null,
                                "canonical_provider_session_ref": null, "active_turn_id": null,
                                "worktree_id": null, "created_at": 1, "updated_at": 15,
                                "closed_at": null, "failure_code": null, "failure_message": null
                            },
                            {
                                "id": "session_2", "provider": "codex", "status": "ready",
                                "cwd": null, "model": null, "permission_mode": null,
                                "system_prompt": null, "metadata": {}, "provider_session_ref": null,
                                "canonical_provider_session_ref": null, "active_turn_id": null,
                                "worktree_id": null, "created_at": 15, "updated_at": 15,
                                "closed_at": null, "failure_code": null, "failure_message": null
                            }
                        ],
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
