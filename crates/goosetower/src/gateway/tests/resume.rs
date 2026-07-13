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
async fn snapshots_use_last_reserved_authority_without_stealing_first_patch() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = materialized_session_state("local", "static-0", "session_1");
    state.reduce_source_event(runtime_source_event("local", "static-0", "session_1", 1));
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;
    let subscribe = || Subscribe {
        subscription_id: "board".to_string(),
        request_id: format!("request-{}", rand::random::<u64>()),
        view_kind: "board".to_string(),
        filters: HashMap::new(),
    };
    let (first, second) = tokio::join!(
        gateway.snapshot_for_subscription(subscribe()),
        gateway.snapshot_for_subscription(subscribe())
    );
    for snapshot in [first.unwrap(), second.unwrap()] {
        let Some(Payload::Snapshot(snapshot)) = snapshot.payload else {
            panic!("expected snapshot");
        };
        assert_eq!(snapshot.cursor.expect("snapshot cursor").gateway_seq, 0);
    }
    let patch = gateway
        .record_replayable(gateway.patch_envelope(ledger_patch(1)))
        .await;
    assert_eq!(patch.gateway_seq, 1);
    let mut conn = test_connection(&gateway);
    gateway
        .handle_resume(
            &mut conn,
            resume_request(&gateway, 0, 1, "static-0", vec![ledger_sub()]),
        )
        .await
        .expect("resume after non-advancing snapshots");
    assert_eq!(
        payload_count(&drain_payloads(&mut conn), MessageKind::Patch),
        1
    );
}

#[tokio::test]
async fn selected_snapshots_bind_body_cursor_and_coverage_to_requested_source() {
    let gateway = test_gateway(GoosetowerConfig::default());
    for (source_id, epoch) in [("source-a", "epoch-a"), ("source-b", "epoch-b")] {
        let mut state = materialized_session_state(source_id, epoch, "session_1");
        state.reduce_source_event(runtime_source_event(source_id, epoch, "session_1", 1));
        gateway
            .replace_materialized_state(source_id.to_string(), state)
            .await;
    }
    let envelope = gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "selected-b".to_string(),
            request_id: "request-b".to_string(),
            view_kind: "session_detail".to_string(),
            filters: HashMap::from([
                ("source_id".to_string(), "source-b".to_string()),
                ("session_id".to_string(), "session_1".to_string()),
            ]),
        })
        .await
        .expect("source-scoped selected snapshot");
    let Some(Payload::Snapshot(snapshot)) = envelope.payload else {
        panic!("expected selected snapshot");
    };
    let cursor = snapshot.cursor.expect("selected cursor");
    assert_eq!(cursor.sources.len(), 1);
    assert_eq!(cursor.sources[0].source_id, "source-b");
    assert_eq!(
        snapshot.coverage.expect("selected coverage").entity_ids,
        vec!["session_1"]
    );
    let body: crate::materializer::SessionDetailView =
        serde_json::from_slice(&snapshot.body).unwrap();
    assert_eq!(body.source_id, "source-b");
    assert!(gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "ambiguous".to_string(),
            request_id: "ambiguous-request".to_string(),
            view_kind: "session_detail".to_string(),
            filters: HashMap::from([("session_id".to_string(), "session_1".to_string())]),
        })
        .await
        .is_err());
}

#[tokio::test]
async fn teams_snapshot_is_bounded_cross_source_summary_only() {
    let gateway = test_gateway(GoosetowerConfig::default());
    for (source_id, epoch) in [("source-a", "epoch-a"), ("source-b", "epoch-b")] {
        let mut state = materialized_session_state(source_id, epoch, "session_1");
        for index in 0..150 {
            let mut team = team_record(&format!("team-{index:03}-{source_id}"));
            team.name = format!("{source_id} team {index}");
            state.upsert_team(team);
        }
        state.reduce_source_event(runtime_source_event(source_id, epoch, "session_1", 1));
        gateway
            .replace_materialized_state(source_id.to_string(), state)
            .await;
    }
    let envelope = gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "teams-page".to_string(),
            request_id: "teams-request".to_string(),
            view_kind: "teams".to_string(),
            filters: HashMap::from([
                ("offset".to_string(), "0".to_string()),
                ("limit".to_string(), "10000".to_string()),
            ]),
        })
        .await
        .expect("bounded team summaries");
    assert!(envelope.encoded_len() <= gateway.config.websocket.max_message_bytes);
    let Some(Payload::Snapshot(snapshot)) = envelope.payload else {
        panic!("expected teams snapshot");
    };
    assert_eq!(snapshot.cursor.as_ref().unwrap().sources.len(), 2);
    let body: crate::materializer::TeamSummaryListView =
        serde_json::from_slice(&snapshot.body).unwrap();
    assert_eq!(body.total_rows, 300);
    assert_eq!(
        body.teams.len(),
        crate::materializer::MAX_TEAM_SUMMARY_LIMIT
    );
    let json = serde_json::to_value(&body).unwrap();
    assert!(json.to_string().find("messages").is_none());
    assert!(json.to_string().find("deliveries").is_none());
    assert!(body.teams.iter().any(|team| team.source_id == "source-a"));
    assert!(body.teams.iter().any(|team| team.source_id == "source-b"));
}

#[tokio::test]
async fn process_tail_coverage_and_snapshot_overflow_fail_closed() {
    let mut config = GoosetowerConfig::default();
    let gateway = test_gateway(config.clone());
    let mut state = materialized_session_state("local", "epoch-1", "session_1");
    state.reduce_source_event(runtime_source_event("local", "epoch-1", "session_1", 1));
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;
    let envelope = gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "missing-process".to_string(),
            request_id: "process-request".to_string(),
            view_kind: "process_tail".to_string(),
            filters: HashMap::from([
                ("source_id".to_string(), "local".to_string()),
                ("process_id".to_string(), "process-missing".to_string()),
            ]),
        })
        .await
        .expect("explicit process absence");
    let Some(Payload::Snapshot(snapshot)) = envelope.payload else {
        panic!("expected process snapshot");
    };
    assert!(snapshot.not_found);
    assert_eq!(
        snapshot.coverage.unwrap().entity_ids,
        vec!["process-missing"]
    );

    config.websocket.max_message_bytes = 64;
    let tiny_gateway = test_gateway(config);
    let mut tiny_state = materialized_session_state("local", "epoch-1", "session_1");
    tiny_state.reduce_source_event(runtime_source_event("local", "epoch-1", "session_1", 1));
    tiny_gateway
        .replace_materialized_state("local".to_string(), tiny_state)
        .await;
    let before = tiny_gateway.next_gateway_seq.load(Ordering::Relaxed);
    let error = tiny_gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "oversized".to_string(),
            request_id: "oversized-request".to_string(),
            view_kind: "board".to_string(),
            filters: HashMap::new(),
        })
        .await
        .expect_err("oversized snapshot must not publish");
    assert!(error.to_string().contains("max_message_bytes"));
    assert_eq!(
        tiny_gateway.next_gateway_seq.load(Ordering::Relaxed),
        before
    );
}

#[tokio::test]
async fn source_resync_records_bounded_source_ownership_reset() {
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
    let replacement: Value =
        serde_json::from_slice(&resync.body).expect("typed source replacement body");
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../verification/gooseweb/fixtures/p08-source-replacement-rust.json"
    ))
    .expect("Rust source replacement fixture");
    assert_eq!(
        replacement, fixture,
        "Rust source replacement fixture drift"
    );
    assert_eq!(replacement["source_id"], "local");
    assert_eq!(replacement.as_object().unwrap().len(), 1);
    assert!(entry.encoded_len <= gateway.config.websocket.max_message_bytes);
}

#[test]
fn prebuilt_multi_source_resets_have_collision_resistant_ids() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut first = MaterializedState::new("source-a", "epoch-a");
    first.reduce_source_event(runtime_source_event("source-a", "epoch-a", "a", 1));
    let mut second = MaterializedState::new("source-b", "epoch-b");
    second.reduce_source_event(runtime_source_event("source-b", "epoch-b", "b", 1));

    let first = gateway.source_snapshot_resync(&first, "restart").unwrap();
    let second = gateway.source_snapshot_resync(&second, "restart").unwrap();
    assert_ne!(first.message_id, second.message_id);
    assert!(first
        .message_id
        .starts_with(&format!("view_{}_", gateway.gateway_epoch)));
    assert!(second
        .message_id
        .starts_with(&format!("view_{}_", gateway.gateway_epoch)));
}

#[tokio::test]
async fn resume_from_prior_gateway_generation_emits_same_source_epoch_reset() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = MaterializedState::new("local", "epoch-runtime");
    state.upsert_session(session_record());
    state.reduce_source_event(runtime_source_event(
        "local",
        "epoch-runtime",
        "session_1",
        1,
    ));
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;
    let mut resume = resume_request(&gateway, 100, 1, "epoch-runtime", vec![ledger_sub()]);
    let cursor = resume.cursor.as_mut().expect("resume cursor");
    cursor.gateway_epoch = "prior-gateway-generation".to_string();
    cursor.gateway_started_at_unix_ns = gateway.gateway_started_at_unix_ns.saturating_sub(1);
    let mut conn = test_connection(&gateway);
    gateway
        .handle_resume(&mut conn, resume)
        .await
        .expect("gateway generation reset");
    let payloads = drain_payloads(&mut conn);
    let reset = payloads
        .iter()
        .find_map(|payload| match payload {
            Payload::SourceSnapshotResync(reset) => Some(reset),
            _ => None,
        })
        .expect("explicit source reset");
    let authority = reset.cursor.as_ref().expect("reset cursor");
    assert_eq!(authority.gateway_epoch, gateway.gateway_epoch);
    assert_eq!(authority.sources[0].source_epoch, "epoch-runtime");
    assert_eq!(authority.sources[0].source_seq, 1);
    assert_eq!(gateway.next_gateway_seq.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn source_over_prior_byte_budget_resets_and_refills_bounded_page() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = MaterializedState::new("local", "epoch-1");
    for index in 0..10_000 {
        let mut session = session_record();
        session.id = format!("session-{index:05}");
        state.sessions.insert(session.id.clone(), session);
    }
    state.reduce_source_event(runtime_source_event("local", "epoch-1", "session_1", 1));
    let envelope = gateway
        .source_snapshot_resync(&state, "bounded source replacement")
        .expect("large source uses bounded ownership reset");
    assert!(envelope.encode_to_vec().len() <= gateway.config.websocket.max_message_bytes);
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;
    let refill = gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "bounded-board".to_string(),
            request_id: "request-board".to_string(),
            view_kind: "board".to_string(),
            filters: HashMap::from([
                ("source_id".to_string(), "local".to_string()),
                ("limit".to_string(), "100".to_string()),
            ]),
        })
        .await
        .expect("bounded board snapshot");
    assert!(refill.encode_to_vec().len() <= gateway.config.websocket.max_message_bytes);
    let Some(Payload::Snapshot(snapshot)) = refill.payload else {
        panic!("expected bounded board refill");
    };
    let body: crate::materializer::FleetBoardView = serde_json::from_slice(&snapshot.body).unwrap();
    assert_eq!(body.rows.len(), 100);
}

#[tokio::test]
async fn source_over_prior_entity_budget_resets_and_refills_exact_selected_detail() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut state = MaterializedState::new("local", "epoch-1");
    for index in 0..1_025 {
        let mut session = session_record();
        session.id = format!("session-{index}");
        state.sessions.insert(session.id.clone(), session);
    }
    state.reduce_source_event(runtime_source_event("local", "epoch-1", "session_1", 1));
    let envelope = gateway
        .source_snapshot_resync(&state, "bounded source replacement")
        .expect("large source uses bounded ownership reset");
    assert!(envelope.encode_to_vec().len() <= gateway.config.websocket.max_message_bytes);
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;
    let refill = gateway
        .snapshot_for_subscription(Subscribe {
            subscription_id: "selected-session".to_string(),
            request_id: "request-session".to_string(),
            view_kind: "session_detail".to_string(),
            filters: HashMap::from([
                ("source_id".to_string(), "local".to_string()),
                ("session_id".to_string(), "session-1024".to_string()),
            ]),
        })
        .await
        .expect("selected detail snapshot");
    assert!(refill.encode_to_vec().len() <= gateway.config.websocket.max_message_bytes);
    let Some(Payload::Snapshot(snapshot)) = refill.payload else {
        panic!("expected selected detail refill");
    };
    let body: crate::materializer::SessionDetailView =
        serde_json::from_slice(&snapshot.body).unwrap();
    assert_eq!(body.session.id, "session-1024");
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
            request_id: format!("request-team-{raw}"),
            view_kind: "team_workspace".to_string(),
            filters: HashMap::from([
                ("source_id".to_string(), "local".to_string()),
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
        gateway_epoch: "fixture-gateway".to_string(),
        gateway_started_at_unix_ns: 1,
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
        request_id: "request-selected-session".to_string(),
        view_kind: "session".to_string(),
        filters: HashMap::from([
            ("source_id".to_string(), "local".to_string()),
            ("session_id".to_string(), "session_1".to_string()),
        ]),
    };
    let envelope = gateway
        .snapshot_for_subscription(subscribe())
        .await
        .unwrap();
    let second = gateway
        .snapshot_for_subscription(subscribe())
        .await
        .unwrap();
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
            resume_request(&gateway, 0, 1, "static-0", vec![ledger_sub()]),
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
            resume_request(&gateway, 10, 1, "static-0", vec![ledger_sub()]),
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
            resume_request(&gateway, 10, 1, "static-0", vec![ledger_sub()]),
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
            resume_request(&gateway, 10, 1, "old-epoch", vec![ledger_sub()]),
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
