use super::*;
use base64::Engine as _;

#[test]
fn patch_frames_use_versioned_operations_coverage_and_nested_cursor() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let envelope = gateway.patch_envelope(ledger_patch(7));
    assert!(envelope.source_id.is_empty());
    assert_eq!(envelope.source_seq, 0);
    let Some(Payload::Patch(patch)) = envelope.payload else {
        panic!("expected patch payload");
    };
    assert_eq!(patch.schema_version, DETAIL_SCHEMA_VERSION);
    assert_eq!(patch.operation, ViewOperation::Upsert as i32);
    let cursor = patch.cursor.expect("canonical nested cursor");
    assert_eq!(cursor.sources[0].source_seq, 7);
    let coverage = patch.coverage.expect("declared coverage");
    assert!(coverage.authoritative);
    assert_eq!(coverage.domains, vec!["ledger_events"]);
}

#[test]
fn rust_decodes_shared_typescript_detail_frame_corpus() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../../../verification/gooseweb/fixtures/p08-detail-frame-corpus.json"
    ))
    .expect("P08 frame corpus JSON");
    let encoded = corpus["frames"][0]["base64"]
        .as_str()
        .expect("base64 frame");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("base64 decode");
    let envelope = RealtimeEnvelope::decode(bytes.as_slice()).expect("Rust frame decode");
    let Some(Payload::Snapshot(snapshot)) = envelope.payload else {
        panic!("expected snapshot corpus frame");
    };
    assert_eq!(snapshot.view_kind, "session_detail");
    assert_eq!(snapshot.operation, ViewOperation::Replace as i32);
    assert_eq!(
        snapshot.cursor.expect("nested cursor").sources[0].source_seq,
        17
    );
    assert_eq!(
        snapshot.coverage.expect("coverage").entity_ids,
        vec!["session-1"]
    );
}

#[tokio::test]
async fn legacy_detail_subscriptions_publish_canonical_replacement_snapshots() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = MaterializedState::new("local", "static-0");
    state.upsert_session(session_record());
    state.reduce_source_event(runtime_source_event("local", "static-0", "session_1", 1));
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;

    let envelope = gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "selected-session".to_string(),
            view_kind: "session".to_string(),
            filters: HashMap::from([
                ("source_id".to_string(), "local".to_string()),
                ("session_id".to_string(), "session_1".to_string()),
            ]),
        })
        .await;
    let Some(Payload::Snapshot(snapshot)) = envelope.payload else {
        panic!("expected snapshot payload");
    };
    assert_eq!(snapshot.view_kind, "session_detail");
    assert_eq!(snapshot.schema_version, DETAIL_SCHEMA_VERSION);
    assert_eq!(snapshot.operation, ViewOperation::Replace as i32);
    assert!(snapshot.cursor.is_some());
    assert!(envelope.source_id.is_empty());
    let coverage = snapshot.coverage.expect("declared coverage");
    assert_eq!(coverage.domains, vec!["session_details"]);
    assert_eq!(coverage.entity_ids, vec!["session_1"]);
    let detail: crate::materializer::SessionDetailView =
        serde_json::from_slice(&snapshot.body).expect("typed session detail body");
    assert_eq!(detail.session.id, "session_1");
}

#[tokio::test]
async fn resume_clean_reconnect_uses_gateway_replay_without_duplicates() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut conn = test_connection(&gateway);
    let patch = ledger_patch(1);
    let envelope = gateway.patch_envelope(patch);
    gateway.record_replayable(envelope).await;

    gateway
        .handle_resume(
            &mut conn,
            resume_request(0, 1, "static-0", vec![ledger_sub()]),
        )
        .await
        .expect("resume");

    let replayed = drain_payloads(&mut conn);
    assert_eq!(payload_count(&replayed, MessageKind::Patch), 1);
    assert_eq!(payload_count(&replayed, MessageKind::SourceGapFilled), 1);
    assert_eq!(gateway.metrics.resume_success.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn resume_source_replay_fills_missing_events_and_dedupes_overlap() {
    let runtime_addr = spawn_resume_runtime(ResumeRuntimeMode::ReplayOverlap).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    config.replay.max_events_per_request = 10;
    let gateway = test_gateway(config);
    let mut conn = test_connection(&gateway);

    gateway
        .handle_resume(
            &mut conn,
            resume_request(10, 1, "static-0", vec![ledger_sub()]),
        )
        .await
        .expect("resume fallback");

    let replayed = drain_payloads(&mut conn);
    assert_eq!(payload_count(&replayed, MessageKind::Patch), 2);
    assert_eq!(payload_count(&replayed, MessageKind::SourceGapFilled), 1);
    assert_eq!(gateway.metrics.resume_partial.load(Ordering::Relaxed), 1);
    assert_eq!(gateway.metrics.replay_events.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn resume_gap_detection_triggers_snapshot_resync() {
    let runtime_addr = spawn_resume_runtime(ResumeRuntimeMode::ReplayGap).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    config.replay.max_events_per_request = 10;
    let gateway = test_gateway(config);
    let mut conn = test_connection(&gateway);

    gateway
        .handle_resume(
            &mut conn,
            resume_request(10, 1, "static-0", vec![ledger_sub()]),
        )
        .await
        .expect("snapshot resync");

    let replayed = drain_payloads(&mut conn);
    assert_eq!(payload_count(&replayed, MessageKind::SourceGapDetected), 1);
    assert_eq!(
        payload_count(&replayed, MessageKind::SourceSnapshotResync),
        1
    );
    assert_eq!(gateway.metrics.gap_count.load(Ordering::Relaxed), 1);
    assert_eq!(
        gateway
            .metrics
            .snapshot_resync_count
            .load(Ordering::Relaxed),
        1
    );
    let materialized = gateway.materialized.read().await;
    let state = materialized.get("local").expect("local state");
    assert_eq!(state.discontinuities.len(), 1);
    assert!(
        state
            .snapshot_ledger(&Default::default())
            .discontinuities
            .len()
            == 1
    );
}

#[tokio::test]
async fn resume_epoch_change_is_gap_detected_and_resynced() {
    let runtime_addr = spawn_resume_runtime(ResumeRuntimeMode::ReplayOverlap).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = test_gateway(config);
    let mut conn = test_connection(&gateway);

    gateway
        .handle_resume(
            &mut conn,
            resume_request(10, 1, "old-epoch", vec![ledger_sub()]),
        )
        .await
        .expect("epoch gap resync");

    let replayed = drain_payloads(&mut conn);
    assert_eq!(payload_count(&replayed, MessageKind::SourceGapDetected), 1);
    assert_eq!(
        payload_count(&replayed, MessageKind::SourceSnapshotResync),
        1
    );
}

#[tokio::test]
async fn resume_stale_source_disables_destructive_commands() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = MaterializedState::new("local", "static-0");
    state.mark_live();
    state.upsert_session(session_record());
    state.transition_source_health(SourceHealthState::Stale, Some("test stale".to_string()));
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(&mut conn, send_turn_command("cmd_stale"))
        .await;

    let Some(Payload::CommandRejected(rejected)) = response.payload else {
        panic!("expected command rejection");
    };
    assert_eq!(rejected.error.expect("error").code, "source_stale");
}
