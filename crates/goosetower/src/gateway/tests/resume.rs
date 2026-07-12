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

#[tokio::test]
async fn replay_publication_reserves_one_matching_nested_gateway_sequence() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let first = gateway.patch_envelope(ledger_patch(7));
    let second = gateway.patch_envelope(ledger_patch(8));
    let (first, second) = tokio::join!(
        gateway.record_replayable(first),
        gateway.record_replayable(second)
    );
    let mut entries = [first, second];
    entries.sort_by_key(|entry| entry.gateway_seq);
    assert_eq!(entries[1].gateway_seq, entries[0].gateway_seq + 1);
    for entry in entries {
        assert_eq!(entry.envelope.gateway_seq, entry.gateway_seq);
        let Some(Payload::Patch(patch)) = entry.envelope.payload else {
            panic!("expected recorded patch");
        };
        assert_eq!(
            patch.cursor.expect("canonical patch cursor").gateway_seq,
            entry.gateway_seq
        );
    }
}

#[tokio::test]
async fn source_resync_records_complete_source_replacement_authority() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = MaterializedState::new("local", "epoch-restarted");
    state.upsert_session(session_record());
    state.reduce_source_event(runtime_source_event(
        "local",
        "epoch-restarted",
        "session_1",
        1,
    ));
    let envelope = gateway
        .source_snapshot_resync(&state, "tower restarted")
        .expect("source replacement envelope");
    let entry = gateway.record_replayable(envelope).await;
    assert_eq!(entry.gateway_seq, entry.envelope.gateway_seq);
    let Some(Payload::SourceSnapshotResync(resync)) = entry.envelope.payload else {
        panic!("expected source resync payload");
    };
    assert_eq!(
        resync
            .cursor
            .expect("source replacement cursor")
            .gateway_seq,
        entry.gateway_seq
    );
    assert_eq!(
        resync
            .coverage
            .expect("source replacement coverage")
            .domains,
        vec![
            "fleet_rows",
            "sessions",
            "session_details",
            "teams",
            "team_workspaces",
            "approvals",
            "processes",
            "worktrees",
            "sources",
        ]
    );
    let replacement: crate::materializer::SourceReplacementView =
        serde_json::from_slice(&resync.body).expect("typed source replacement body");
    assert_eq!(replacement.source_id, "local");
    assert_eq!(replacement.session_details.len(), 1);
}

#[test]
fn selected_team_message_limits_are_clamped_for_all_filter_inputs() {
    for (raw, expected) in [
        ("0", 1usize),
        ("not-a-number", 100usize),
        (
            "18446744073709551615",
            crate::materializer::MAX_TEAM_MESSAGE_LIMIT,
        ),
    ] {
        let subscription = Subscription::from_proto(&Subscribe {
            subscription_id: format!("team-{raw}"),
            view_kind: "team_workspace".to_string(),
            filters: HashMap::from([
                ("team_id".to_string(), "team_1".to_string()),
                ("message_limit".to_string(), raw.to_string()),
            ]),
        })
        .expect("bounded team subscription");
        let Subscription::Team(team) = subscription else {
            panic!("expected team subscription");
        };
        assert_eq!(team.message_limit, expected);
    }
}

#[test]
fn full_detail_patch_frames_are_scoped_replacements() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = MaterializedState::new("local", "epoch-1");
    state.upsert_session(session_record());
    let patch = state
        .session_patches("session_1", None)
        .into_iter()
        .find(|patch| patch.view_kind == "session_detail")
        .expect("session detail patch");
    let envelope = gateway.patch_envelope(patch);
    let Some(Payload::Patch(patch)) = envelope.payload else {
        panic!("expected patch frame");
    };
    assert_eq!(patch.operation, ViewOperation::Replace as i32);
    assert_eq!(
        patch.coverage.expect("coverage").entity_ids,
        vec!["session_1"]
    );
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

    let coverage = |domain: &str, entity_id: &str| ViewCoverage {
        domains: vec![domain.to_string()],
        entity_ids: vec![entity_id.to_string()],
        authoritative: true,
    };
    let cursor = |source_seq| CursorVector {
        gateway_seq: source_seq,
        sources: vec![SourceCursor {
            source_id: "source-1".to_string(),
            source_epoch: "epoch-1".to_string(),
            source_seq,
        }],
    };
    let rust_frames = [
        RealtimeEnvelope {
            protocol_version: 1,
            message_id: "rust-team-replace".to_string(),
            message_kind: MessageKind::Patch as i32,
            lane: Lane::State as i32,
            payload: Some(Payload::Patch(Patch {
                view_kind: "team_workspace".to_string(),
                entity: Some(EntityRef {
                    entity_id: "team-1".to_string(),
                    ..EntityRef::default()
                }),
                cursor: Some(cursor(18)),
                body: br#"{"source_id":"source-1","team":{"id":"team-1","name":"Team","lead_agent_id":"session-1"},"members":[],"messages":[],"deliveries":[]}"#.to_vec().into(),
                schema_version: 1,
                operation: ViewOperation::Replace as i32,
                coverage: Some(coverage("team_workspaces", "team-1")),
            })),
            ..RealtimeEnvelope::default()
        },
        RealtimeEnvelope {
            protocol_version: 1,
            message_id: "rust-session-upsert".to_string(),
            message_kind: MessageKind::Patch as i32,
            lane: Lane::State as i32,
            payload: Some(Payload::Patch(Patch {
                view_kind: "session_detail".to_string(),
                entity: Some(EntityRef {
                    entity_id: "session-1".to_string(),
                    ..EntityRef::default()
                }),
                cursor: Some(cursor(20)),
                body: br#"{"source_id":"source-1","session":{"id":"session-1","provider":"codex","status":"ready"},"transcript":[{"role":"assistant","text":"reloaded answer"}],"appended_text":"","latest_activity_unix_ms":200}"#.to_vec().into(),
                schema_version: 1,
                operation: ViewOperation::Upsert as i32,
                coverage: Some(coverage("session_details", "session-1")),
            })),
            ..RealtimeEnvelope::default()
        },
    ];
    for frame in rust_frames {
        let name = if frame.message_id == "rust-team-replace" {
            "team_replace"
        } else {
            "session_upsert"
        };
        let fixture = corpus["frames"]
            .as_array()
            .expect("corpus frames")
            .iter()
            .find(|entry| entry["name"] == name)
            .expect("Rust-produced fixture");
        assert_eq!(fixture["producer"], "rust");
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(frame.encode_to_vec()),
            fixture["base64"].as_str().expect("fixture base64"),
            "Rust encoder drift for {name}"
        );
    }
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

    let subscribe = || Subscribe {
        subscription_id: "selected-session".to_string(),
        view_kind: "session".to_string(),
        filters: HashMap::from([
            ("source_id".to_string(), "local".to_string()),
            ("session_id".to_string(), "session_1".to_string()),
        ]),
    };
    let envelope = gateway.snapshot_for_subscription(subscribe()).await;
    let second = gateway.snapshot_for_subscription(subscribe()).await;
    assert_ne!(envelope.message_id, second.message_id);
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
