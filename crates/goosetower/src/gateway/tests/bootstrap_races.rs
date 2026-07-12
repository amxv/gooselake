use super::*;
use runtime_core::{TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord};
use tokio::sync::Notify;

use crate::materializer::EntityVersion;

#[tokio::test]
async fn equal_revision_targeted_snapshots_cannot_regress_newer_truth() {
    let gateway = live_gateway_with_session_version(GoosetowerConfig::default(), 1).await;
    let mut team = team_record("team_equal");
    team.updated_at = 10;
    let member_a = team_member_record("team_equal", "session_2", 10);
    let member_b = team_member_record("team_equal", "session_3", 10);
    {
        let mut states = gateway.materialized.write().await;
        let state = states.get_mut("local").unwrap();
        for session_id in ["session_2", "session_3"] {
            let mut session = session_record();
            session.id = session_id.into();
            state.upsert_session(session);
        }
    }
    gateway
        .merge_authoritative_team(
            "local",
            runtime_core::TeamWithMembers {
                team: team.clone(),
                members: vec![member_a.clone(), member_b.clone()],
            },
        )
        .await;
    gateway
        .merge_authoritative_team(
            "local",
            runtime_core::TeamWithMembers {
                team: team.clone(),
                members: vec![member_a],
            },
        )
        .await;

    let mut terminal = session_record();
    terminal.status = "ready".into();
    terminal.active_turn_id = None;
    terminal.updated_at = 20;
    {
        let mut states = gateway.materialized.write().await;
        let state = states.get_mut("local").unwrap();
        state.upsert_session(terminal.clone());
        state.upsert_delivery(delivery_record("del_equal", "injected", 20));
    }
    let mut stale_session = terminal;
    stale_session.status = "turn_running".into();
    stale_session.active_turn_id = Some("turn_stale".into());
    gateway
        .merge_authoritative_session("local", stale_session)
        .await;
    gateway
        .merge_authoritative_team_message(
            "local",
            runtime_core::TeamMessageAck {
                message: message_record("msg_equal", 21),
                deliveries: vec![delivery_record("del_equal", "pending", 20)],
                disposition: "accepted".into(),
            },
        )
        .await;

    let states = gateway.materialized.read().await;
    let state = &states["local"];
    let members = &state.members_by_team["team_equal"];
    assert!(members.contains_key("session_2"));
    assert!(members.contains_key("session_3"));
    assert_eq!(state.version("team", "team_equal"), EntityVersion(10));
    assert_eq!(state.sessions["session_1"].status, "ready");
    let delivery = state.deliveries_by_team["team_1"]
        .iter()
        .find(|delivery| delivery.id == "del_equal")
        .unwrap();
    assert_eq!(delivery.status, "injected");
    assert_eq!(
        state.version("team_delivery", "del_equal"),
        EntityVersion(20)
    );
    drop(states);

    {
        let mut states = gateway.materialized.write().await;
        states
            .get_mut("local")
            .unwrap()
            .remove_team_member("team_equal", "session_3");
    }
    let mut patches = gateway.verification_patch_receiver();
    gateway
        .merge_authoritative_team(
            "local",
            runtime_core::TeamWithMembers {
                team,
                members: vec![team_member_record("team_equal", "session_2", 10), member_b],
            },
        )
        .await;
    let states = gateway.materialized.read().await;
    assert!(!states["local"].members_by_team["team_equal"].contains_key("session_3"));
    assert_eq!(
        states["local"].agent_row("session_3").unwrap().team_id,
        None
    );
    assert!(patches.try_recv().is_err());
}

#[tokio::test]
async fn targeted_command_merge_cannot_overwrite_newer_sse_record() {
    let fetched = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runtime_addr = spawn_blocked_broadcast_runtime(fetched.clone(), release.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = Arc::new(live_gateway_with_session_version(config, 1).await);
    {
        let mut materialized = gateway.materialized.write().await;
        let state = materialized.get_mut("local").unwrap();
        state.upsert_team(team_record("team_1"));
        state
            .source_health
            .transition(SourceHealthState::Live, Some(10), None);
    }

    let command_gateway = gateway.clone();
    let command = tokio::spawn(async move {
        let mut conn = test_connection(&command_gateway);
        command_gateway
            .admit_and_route_command(
                &mut conn,
                broadcast_team_message_command("cmd_race", "team_1", "stale response"),
            )
            .await
    });
    fetched.notified().await;

    gateway
        .ingest_source_event(SourceEvent::from_runtime_event(
            "local",
            "static-0",
            RuntimeEventRecord {
                row_id: 11,
                event_id: "evt_message_11".into(),
                scope: RuntimeEventScope::Team,
                scope_id: "team_1".into(),
                session_id: None,
                team_id: Some("team_1".into()),
                turn_id: None,
                seq: 1,
                kind: "team_message.created".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: json!({ "message": team_message_value("newer event", 11) }),
                provider: None,
                provider_seq: None,
                created_at: 11,
            },
        ))
        .await;
    release.notify_one();
    let response = command.await.expect("command task");
    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));

    let materialized = gateway.materialized.read().await;
    let message = &materialized["local"].messages_by_team["team_1"][0];
    assert_eq!(
        message.input,
        json!([{ "type": "text", "text": "newer event" }])
    );
    assert_eq!(
        materialized["local"].source_health.last_source_seq,
        Some(11)
    );
}

#[tokio::test]
async fn inflight_targeted_response_is_discarded_while_source_gap_is_frozen() {
    let fetched = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runtime_addr = spawn_blocked_broadcast_runtime(fetched.clone(), release.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = Arc::new(live_gateway_with_session_version(config, 1).await);
    {
        let mut materialized = gateway.materialized.write().await;
        let state = materialized.get_mut("local").unwrap();
        state.upsert_team(team_record("team_1"));
        state
            .source_health
            .transition(SourceHealthState::Live, Some(3), None);
    }
    let command_gateway = gateway.clone();
    let command = tokio::spawn(async move {
        let mut conn = test_connection(&command_gateway);
        command_gateway
            .admit_and_route_command(
                &mut conn,
                broadcast_team_message_command("cmd_gap_race", "team_1", "deferred response"),
            )
            .await
    });
    fetched.notified().await;
    {
        let mut materialized = gateway.materialized.write().await;
        materialized
            .get_mut("local")
            .unwrap()
            .transition_source_health(
                SourceHealthState::GapDetected,
                Some("forced frozen gap".into()),
            );
    }
    release.notify_one();
    let response = command.await.expect("command task");
    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));
    assert!(gateway.materialized.read().await["local"]
        .messages_by_team
        .get("team_1")
        .is_none_or(Vec::is_empty));

    {
        let mut materialized = gateway.materialized.write().await;
        let state = materialized.get_mut("local").unwrap();
        state
            .source_health
            .transition(SourceHealthState::Live, Some(10), None);
    }
    gateway
        .ingest_source_event(SourceEvent::from_runtime_event(
            "local",
            "static-0",
            RuntimeEventRecord {
                row_id: 11,
                event_id: "evt_gap_message".into(),
                scope: RuntimeEventScope::Team,
                scope_id: "team_1".into(),
                session_id: None,
                team_id: Some("team_1".into()),
                turn_id: None,
                seq: 1,
                kind: "team_message.created".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: json!({ "message": team_message_value("authoritative replay", 11) }),
                provider: None,
                provider_seq: None,
                created_at: 11,
            },
        ))
        .await;
    let states = gateway.materialized.read().await;
    assert_eq!(states["local"].messages_by_team["team_1"].len(), 1);
    assert_eq!(states["local"].source_health.last_source_seq, Some(11));
}

async fn spawn_blocked_broadcast_runtime(fetched: Arc<Notify>, release: Arc<Notify>) -> SocketAddr {
    let handler = move || {
        let (fetched, release) = (fetched.clone(), release.clone());
        async move {
            fetched.notify_one();
            release.notified().await;
            Json(json!({
                "message": team_message_value("stale response", 10),
                "deliveries": [],
                "disposition": "accepted"
            }))
        }
    };
    let app = Router::new().route(
        "/v1/teams/{team_id}/broadcasts",
        axum::routing::post(handler),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

fn team_message_value(text: &str, created_at: i64) -> Value {
    json!({
        "id": "msg_race",
        "team_id": "team_1",
        "scope": "broadcast",
        "sender_agent_id": "session_1",
        "recipient_agent_ids": [],
        "input": [{ "type": "text", "text": text }],
        "image_paths": [],
        "priority": "normal",
        "policy": "non_interrupting",
        "correlation_id": "cmd_race",
        "reply_to_message_id": null,
        "idempotency_key": "cmd_race",
        "created_at": created_at
    })
}

fn team_member_record(team_id: &str, agent_id: &str, joined_at: i64) -> TeamMemberRecord {
    TeamMemberRecord {
        team_id: team_id.into(),
        agent_id: agent_id.into(),
        title: None,
        joined_at,
        added_by: "session_1".into(),
        creator_agent_id: Some("session_1".into()),
        creator_compaction_subscription: "auto".into(),
        worktree_id: None,
    }
}

fn message_record(id: &str, created_at: i64) -> TeamMessageRecord {
    TeamMessageRecord {
        id: id.into(),
        team_id: "team_1".into(),
        scope: "broadcast".into(),
        sender_agent_id: "session_1".into(),
        recipient_agent_ids: json!([]),
        input: json!([]),
        image_paths: json!([]),
        priority: "normal".into(),
        policy: "non_interrupting".into(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: None,
        created_at,
    }
}

fn delivery_record(id: &str, status: &str, updated_at: i64) -> TeamDeliveryRecord {
    TeamDeliveryRecord {
        id: id.into(),
        message_id: "msg_equal".into(),
        team_id: "team_1".into(),
        recipient_agent_id: "session_2".into(),
        provider: "codex".into(),
        status: status.into(),
        effective_policy: None,
        injection_strategy: None,
        injected_turn_id: (status == "injected").then(|| "turn_1".into()),
        last_error_code: None,
        last_error_message: None,
        created_at: 10,
        updated_at,
    }
}
