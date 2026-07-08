use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};

use axum::extract::Query;
use axum::routing::get;
use axum::{Json, Router};
use runtime_core::{
    RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, SessionRecord, TeamRecord,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpListener;

use super::*;
use crate::materializer::state::SourceCursorView;
use crate::protocol::generated::goosetower::v1::command::Payload as CommandPayload;
use crate::protocol::generated::goosetower::v1::{
    Command, CommandCreateSession, CommandCreateTeam, CommandInputItem, CommandJoinTeamMember,
    CommandSendTurn, EntityRef,
};

mod team_commands;

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

#[tokio::test]
async fn command_without_scope_is_rejected_as_unauthorized_without_leaving_pending() {
    let gateway = test_gateway(GoosetowerConfig::default());
    let mut conn = test_connection(&gateway);
    conn.auth.scopes = vec!["gateway:connect".to_string()];

    let response = gateway
        .admit_and_route_command(&mut conn, send_turn_command("cmd_unauthorized"))
        .await;

    let Some(Payload::CommandRejected(rejected)) = response.payload else {
        panic!("expected command rejection");
    };
    assert_eq!(rejected.error.expect("error").code, REASON_UNAUTHORIZED);
    assert!(gateway
        .command_store
        .lock()
        .await
        .get("cmd_unauthorized")
        .is_none());
}

#[tokio::test]
async fn command_scope_mismatch_is_rejected_before_runtime_route() {
    let gateway = live_gateway_with_session_version(GoosetowerConfig::default(), 1).await;
    let mut conn = test_connection(&gateway);
    let mut command = send_turn_command("cmd_invalid_scope");
    command.target.as_mut().expect("target").scope = Scope::Team as i32;

    let response = gateway.admit_and_route_command(&mut conn, command).await;

    let Some(Payload::CommandRejected(rejected)) = response.payload else {
        panic!("expected command rejection");
    };
    assert_eq!(rejected.error.expect("error").code, REASON_INVALID_SCOPE);
}

#[tokio::test]
async fn stale_entity_version_is_rejected_with_refreshable_reason() {
    let gateway = live_gateway_with_session_version(GoosetowerConfig::default(), 2).await;
    let mut conn = test_connection(&gateway);
    let response = gateway
        .admit_and_route_command(&mut conn, send_turn_command("cmd_stale_version"))
        .await;

    let Some(Payload::CommandRejected(rejected)) = response.payload else {
        panic!("expected command rejection");
    };
    let error = rejected.error.expect("error");
    assert_eq!(error.code, REASON_STALE_ENTITY_VERSION);
    assert!(error.retryable);
}

#[tokio::test]
async fn upstream_runtime_http_error_is_rejected_as_upstream_rejected() {
    let runtime_addr = spawn_rejecting_command_runtime().await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(&mut conn, send_turn_command("cmd_upstream_reject"))
        .await;

    let Some(Payload::CommandRejected(rejected)) = response.payload else {
        panic!("expected command rejection");
    };
    assert_eq!(
        rejected.error.expect("error").code,
        REASON_UPSTREAM_REJECTED
    );
}

#[tokio::test]
async fn duplicate_command_returns_duplicate_disposition_reason() {
    let runtime_addr = spawn_rejecting_command_runtime().await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    let mut conn = test_connection(&gateway);

    let _ = gateway
        .admit_and_route_command(&mut conn, send_turn_command("cmd_duplicate"))
        .await;
    let response = gateway
        .admit_and_route_command(&mut conn, send_turn_command("cmd_duplicate"))
        .await;

    let Some(Payload::CommandDuplicate(duplicate)) = response.payload else {
        panic!("expected duplicate command response");
    };
    assert_eq!(duplicate.command_id, "cmd_duplicate");
    assert_eq!(duplicate.original_command_id, "cmd_duplicate");
}

#[tokio::test]
async fn command_routes_to_materialized_owner_source() {
    let west_hits = Arc::new(StdMutex::new(Vec::new()));
    let east_hits = Arc::new(StdMutex::new(Vec::new()));
    let west_addr = spawn_accepting_command_runtime("west", west_hits.clone()).await;
    let east_addr = spawn_accepting_command_runtime("east", east_hits.clone()).await;
    let gateway = two_source_gateway(west_addr, east_addr).await;
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(
            &mut conn,
            send_turn_command_for_session("cmd_east", "east_session"),
        )
        .await;

    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));
    assert!(west_hits.lock().unwrap().is_empty());
    assert_eq!(east_hits.lock().unwrap().as_slice(), ["east:east_session"]);
}

#[tokio::test]
async fn send_turn_command_routes_structured_image_input() {
    let captured = Arc::new(StdMutex::new(Vec::new()));
    let runtime_addr = spawn_capturing_turn_runtime(captured.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    let mut conn = test_connection(&gateway);
    let mut command = send_turn_command("cmd_image_input");
    if let Some(CommandPayload::SendTurn(input)) = command.payload.as_mut() {
        input.text = "fallback text".to_string();
        input.input = vec![
            CommandInputItem {
                r#type: "text".to_string(),
                text: "Inspect this image".to_string(),
                ..CommandInputItem::default()
            },
            CommandInputItem {
                r#type: "image".to_string(),
                media_type: "image/png".to_string(),
                data: "iVBORw0KGgo=".to_string(),
                ..CommandInputItem::default()
            },
        ];
    }

    let response = gateway.admit_and_route_command(&mut conn, command).await;

    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));
    let body = captured
        .lock()
        .unwrap()
        .pop()
        .expect("captured runtime turn body");
    assert_eq!(body["input"][0]["type"], "text");
    assert_eq!(body["input"][0]["text"], "Inspect this image");
    assert_eq!(body["input"][1]["type"], "image");
    assert_eq!(body["input"][1]["media_type"], "image/png");
    assert_eq!(body["input"][1]["data"], "iVBORw0KGgo=");
}

#[tokio::test]
async fn source_scoped_create_session_routes_to_explicit_source() {
    let west_hits = Arc::new(StdMutex::new(Vec::new()));
    let east_hits = Arc::new(StdMutex::new(Vec::new()));
    let west_addr = spawn_accepting_create_runtime("west", west_hits.clone()).await;
    let east_addr = spawn_accepting_create_runtime("east", east_hits.clone()).await;
    let gateway = two_source_gateway(west_addr, east_addr).await;
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(&mut conn, create_session_command("cmd_create", "east"))
        .await;

    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));
    assert!(west_hits.lock().unwrap().is_empty());
    assert_eq!(
        east_hits.lock().unwrap().as_slice(),
        ["east:create_session:codex:gpt-5.4"]
    );
}

#[tokio::test]
async fn source_scoped_create_team_routes_to_explicit_source() {
    let hits = Arc::new(StdMutex::new(Vec::new()));
    let runtime_addr = spawn_accepting_create_runtime("local", hits.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    {
        let mut materialized = gateway.materialized.write().await;
        materialized
            .get_mut("local")
            .expect("local source")
            .upsert_team(team_record("team_1"));
    }
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(&mut conn, create_team_command("cmd_team", "local"))
        .await;

    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));
    assert_eq!(
        hits.lock().unwrap().as_slice(),
        ["local:create_team:Live Team:session_1"]
    );
}

#[tokio::test]
async fn source_stale_disables_only_affected_owner_commands() {
    let west_hits = Arc::new(StdMutex::new(Vec::new()));
    let east_hits = Arc::new(StdMutex::new(Vec::new()));
    let west_addr = spawn_accepting_command_runtime("west", west_hits.clone()).await;
    let east_addr = spawn_accepting_command_runtime("east", east_hits.clone()).await;
    let gateway = two_source_gateway(west_addr, east_addr).await;
    {
        let mut materialized = gateway.materialized.write().await;
        materialized
            .get_mut("west")
            .expect("west")
            .transition_source_health(SourceHealthState::Stale, Some("test".to_string()));
    }
    let mut conn = test_connection(&gateway);

    let west = gateway
        .admit_and_route_command(
            &mut conn,
            send_turn_command_for_session("cmd_west", "west_session"),
        )
        .await;
    let east = gateway
        .admit_and_route_command(
            &mut conn,
            send_turn_command_for_session("cmd_east_ok", "east_session"),
        )
        .await;

    let Some(Payload::CommandRejected(rejected)) = west.payload else {
        panic!("expected west rejection");
    };
    assert_eq!(rejected.error.expect("error").code, REASON_SOURCE_STALE);
    assert!(matches!(east.payload, Some(Payload::CommandAccepted(_))));
    assert_eq!(east_hits.lock().unwrap().as_slice(), ["east:east_session"]);
}

#[tokio::test]
async fn ingest_source_events_assigns_gateway_sequence_and_gaps_per_source() {
    let gateway = test_gateway(two_source_config(
        "http://127.0.0.1:1".to_string(),
        "http://127.0.0.1:2".to_string(),
    ));
    gateway
        .replace_materialized_state(
            "west".to_string(),
            materialized_session_state("west", "west-epoch", "west_session"),
        )
        .await;
    gateway
        .replace_materialized_state(
            "east".to_string(),
            materialized_session_state("east", "east-epoch", "east_session"),
        )
        .await;

    gateway
        .ingest_source_event(runtime_source_event(
            "west",
            "west-epoch",
            "west_session",
            1,
        ))
        .await;
    gateway
        .ingest_source_event(runtime_source_event(
            "east",
            "east-epoch",
            "east_session",
            1,
        ))
        .await;
    gateway
        .ingest_source_event(runtime_source_event(
            "east",
            "east-epoch",
            "east_session",
            3,
        ))
        .await;

    let replay = gateway.replay_buffer.lock().await.replay_after(0);
    assert!(replay.entries.len() >= 3);
    assert_eq!(replay.entries[0].gateway_seq, 1);
    assert_eq!(replay.entries[1].gateway_seq, 2);
    let materialized = gateway.materialized.read().await;
    assert_eq!(
        materialized["west"].source_health.state,
        SourceHealthState::Live
    );
    assert_eq!(
        materialized["east"].source_health.state,
        SourceHealthState::GapDetected
    );
}

fn test_gateway(config: GoosetowerConfig) -> GatewayState {
    GatewayState::new(Arc::new(config)).expect("gateway")
}

async fn live_gateway_with_session_version(
    config: GoosetowerConfig,
    session_version: usize,
) -> GatewayState {
    let gateway = test_gateway(config);
    let mut state = MaterializedState::new("local", "static-0");
    state.mark_live();
    for _ in 0..session_version {
        state.upsert_session(session_record());
    }
    gateway
        .replace_materialized_state("local".to_string(), state)
        .await;
    gateway
}

fn test_connection(gateway: &GatewayState) -> ConnectionState {
    ConnectionState::new(
        "conn_test".to_string(),
        AuthContext {
            subject: "session_1".to_string(),
            workspace_id: "default".to_string(),
            scopes: vec!["gateway:connect".to_string(), "gateway:command".to_string()],
            allowed_origins: vec!["http://localhost:3000".to_string()],
            expires_at_unix_ms: now_ms() + 60_000,
            jti: "jti_test".to_string(),
        },
        gateway.config.lanes.clone(),
        gateway.config.websocket.max_message_bytes,
    )
}

fn resume_request(
    gateway_seq: u64,
    source_seq: u64,
    source_epoch: &str,
    active_subscriptions: Vec<Subscribe>,
) -> Resume {
    Resume {
        previous_connection_id: "conn_previous".to_string(),
        cursor: Some(CursorVector {
            gateway_seq,
            sources: vec![SourceCursor {
                source_id: "local".to_string(),
                source_epoch: source_epoch.to_string(),
                source_seq,
            }],
        }),
        active_subscriptions,
    }
}

fn ledger_sub() -> Subscribe {
    Subscribe {
        subscription_id: "sub_ledger".to_string(),
        view_kind: "ledger".to_string(),
        filters: Default::default(),
    }
}

fn ledger_patch(source_seq: i64) -> MaterializedPatch {
    MaterializedPatch {
        kind: MaterializedPatchKind::ListInsert,
        view_kind: "ledger".to_string(),
        entity: Some(crate::materializer::EntityKey::new(
            "local",
            "ledger_event",
            source_seq.to_string(),
        )),
        version: None,
        source_cursor: Some(SourceCursorView {
            source_id: "local".to_string(),
            source_epoch: "static-0".to_string(),
            source_seq,
        }),
        body: json!({ "source_seq": source_seq }),
    }
}

fn drain_payloads(conn: &mut ConnectionState) -> Vec<Payload> {
    let mut payloads = Vec::new();
    while let Some(envelope) = conn.next_outbound() {
        if let Some(payload) = envelope.payload {
            payloads.push(payload);
        }
    }
    payloads
}

fn payload_count(payloads: &[Payload], kind: MessageKind) -> usize {
    payloads
        .iter()
        .filter(|payload| payload_kind(payload) == kind)
        .count()
}

fn payload_kind(payload: &Payload) -> MessageKind {
    match payload {
        Payload::Patch(_) => MessageKind::Patch,
        Payload::SourceGapDetected(_) => MessageKind::SourceGapDetected,
        Payload::SourceGapFilled(_) => MessageKind::SourceGapFilled,
        Payload::SourceSnapshotResync(_) => MessageKind::SourceSnapshotResync,
        Payload::CommandRejected(_) => MessageKind::CommandRejected,
        Payload::ConnectionDegraded(_) => MessageKind::ConnectionDegraded,
        _ => MessageKind::Unspecified,
    }
}

fn send_turn_command(command_id: &str) -> Command {
    send_turn_command_for_session(command_id, "session_1")
}

fn send_turn_command_for_session(command_id: &str, session_id: &str) -> Command {
    Command {
        command_id: command_id.to_string(),
        target: Some(EntityRef {
            scope: Scope::Session as i32,
            scope_id: session_id.to_string(),
            entity_id: String::new(),
            entity_version: 1,
        }),
        base_entity_version: 1,
        created_at_client_unix_ms: 1,
        payload: Some(CommandPayload::SendTurn(CommandSendTurn {
            session_id: session_id.to_string(),
            text: "hello".to_string(),
            input: Vec::new(),
        })),
        ..Command::default()
    }
}

fn create_session_command(command_id: &str, source_id: &str) -> Command {
    Command {
        command_id: command_id.to_string(),
        target: Some(EntityRef {
            scope: Scope::Source as i32,
            scope_id: source_id.to_string(),
            entity_id: format!("source:{source_id}"),
            entity_version: 0,
        }),
        created_at_client_unix_ms: 1,
        payload: Some(CommandPayload::CreateSession(CommandCreateSession {
            provider: "codex".to_string(),
            model: "gpt-5.4".to_string(),
            cwd: "/repo".to_string(),
            title: "Lead".to_string(),
            permission_mode: String::new(),
            metadata: Default::default(),
        })),
        ..Command::default()
    }
}

fn create_team_command(command_id: &str, source_id: &str) -> Command {
    Command {
        command_id: command_id.to_string(),
        target: Some(EntityRef {
            scope: Scope::Source as i32,
            scope_id: source_id.to_string(),
            entity_id: format!("source:{source_id}"),
            entity_version: 0,
        }),
        created_at_client_unix_ms: 1,
        payload: Some(CommandPayload::CreateTeam(CommandCreateTeam {
            name: "Live Team".to_string(),
            lead_agent_id: "session_1".to_string(),
            member_agent_ids: Vec::new(),
            created_by: "session_1".to_string(),
        })),
        ..Command::default()
    }
}

fn join_team_member_command(command_id: &str, team_id: &str) -> Command {
    Command {
        command_id: command_id.to_string(),
        target: Some(EntityRef {
            scope: Scope::Team as i32,
            scope_id: team_id.to_string(),
            entity_id: team_id.to_string(),
            entity_version: 0,
        }),
        created_at_client_unix_ms: 1,
        payload: Some(CommandPayload::JoinTeamMember(CommandJoinTeamMember {
            team_id: team_id.to_string(),
            agent_id: "session_2".to_string(),
            title: "Second".to_string(),
            added_by: "session_1".to_string(),
        })),
        ..Command::default()
    }
}

async fn two_source_gateway(west_addr: SocketAddr, east_addr: SocketAddr) -> GatewayState {
    let gateway = test_gateway(two_source_config(
        format!("http://{west_addr}"),
        format!("http://{east_addr}"),
    ));
    gateway
        .replace_materialized_state(
            "west".to_string(),
            materialized_session_state("west", "west-epoch", "west_session"),
        )
        .await;
    gateway
        .replace_materialized_state(
            "east".to_string(),
            materialized_session_state("east", "east-epoch", "east_session"),
        )
        .await;
    gateway
}

fn two_source_config(west_url: String, east_url: String) -> GoosetowerConfig {
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources = vec![
        crate::config::RuntimeSourceConfig {
            source_id: "west".to_string(),
            source_epoch: "west-epoch".to_string(),
            base_url: west_url,
            display_name: "West".to_string(),
            ..crate::config::RuntimeSourceConfig::default()
        },
        crate::config::RuntimeSourceConfig {
            source_id: "east".to_string(),
            source_epoch: "east-epoch".to_string(),
            base_url: east_url,
            display_name: "East".to_string(),
            ..crate::config::RuntimeSourceConfig::default()
        },
    ];
    config
}

fn materialized_session_state(
    source_id: &str,
    source_epoch: &str,
    session_id: &str,
) -> MaterializedState {
    let mut state = MaterializedState::new(source_id, source_epoch);
    state.mark_live();
    let mut session = session_record();
    session.id = session_id.to_string();
    state.upsert_session(session);
    state
}

fn runtime_source_event(
    source_id: &str,
    source_epoch: &str,
    session_id: &str,
    row_id: i64,
) -> SourceEvent {
    SourceEvent::from_runtime_event(
        source_id,
        source_epoch,
        RuntimeEventRecord {
            session_id: Some(session_id.to_string()),
            scope_id: session_id.to_string(),
            row_id,
            event_id: format!("{source_id}_{row_id}"),
            scope: RuntimeEventScope::Session,
            turn_id: Some("turn_1".to_string()),
            team_id: None,
            seq: row_id,
            kind: "turn.completed".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({ "assistant_text": format!("{source_id} {row_id}") }),
            provider: Some("codex".to_string()),
            provider_seq: Some(row_id),
            created_at: row_id,
        },
    )
}

async fn spawn_accepting_command_runtime(
    label: &'static str,
    hits: Arc<StdMutex<Vec<String>>>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind accepting runtime");
    let addr = listener.local_addr().expect("runtime addr");
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/v1/sessions/{session_id}/turns",
            axum::routing::post(
                move |axum::extract::Path(session_id): axum::extract::Path<String>| {
                    let hits = hits.clone();
                    async move {
                        hits.lock().unwrap().push(format!("{label}:{session_id}"));
                        axum::Json(json!({
                            "session_id": session_id,
                            "turn_id": "turn_accepted",
                            "status": "accepted"
                        }))
                    }
                },
            ),
        );
        axum::serve(listener, app)
            .await
            .expect("serve accepting runtime");
    });
    addr
}

async fn spawn_capturing_turn_runtime(captured: Arc<StdMutex<Vec<Value>>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind capturing runtime");
    let addr = listener.local_addr().expect("runtime addr");
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/v1/sessions/{session_id}/turns",
            axum::routing::post(
                move |axum::extract::Path(session_id): axum::extract::Path<String>,
                      Json(input): Json<Value>| {
                    let captured = captured.clone();
                    async move {
                        captured.lock().unwrap().push(input);
                        Json(json!({
                            "session_id": session_id,
                            "turn_id": "turn_accepted",
                            "status": "accepted"
                        }))
                    }
                },
            ),
        );
        axum::serve(listener, app)
            .await
            .expect("serve capturing runtime");
    });
    addr
}

async fn spawn_accepting_create_runtime(
    label: &'static str,
    hits: Arc<StdMutex<Vec<String>>>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind accepting create runtime");
    let addr = listener.local_addr().expect("runtime addr");
    tokio::spawn(async move {
        let create_session_hits = hits.clone();
        let create_session = move |Json(input): Json<Value>| {
            let hits = create_session_hits.clone();
            async move {
                hits.lock().unwrap().push(format!(
                    "{label}:create_session:{}:{}",
                    input["provider"].as_str().unwrap_or_default(),
                    input["model"].as_str().unwrap_or_default()
                ));
                Json(json!({
                    "id": format!("{label}_session"),
                    "provider": input["provider"],
                    "status": "ready",
                    "cwd": input["cwd"],
                    "model": input["model"],
                    "permission_mode": input["permission_mode"],
                    "system_prompt": null,
                    "metadata": input["metadata"],
                    "provider_session_ref": null,
                    "canonical_provider_session_ref": null,
                    "active_turn_id": null,
                    "worktree_id": null,
                    "created_at": 1,
                    "updated_at": 1,
                    "closed_at": null,
                    "failure_code": null,
                    "failure_message": null
                }))
            }
        };
        let create_team_hits = hits.clone();
        let create_team = move |Json(input): Json<Value>| {
            let hits = create_team_hits.clone();
            async move {
                hits.lock().unwrap().push(format!(
                    "{label}:create_team:{}:{}",
                    input["name"].as_str().unwrap_or_default(),
                    input["lead_agent_id"].as_str().unwrap_or_default()
                ));
                Json(json!({
                    "team": {
                        "id": format!("{label}_team"),
                        "name": input["name"],
                        "lead_agent_id": input["lead_agent_id"],
                        "created_by": input["created_by"],
                        "created_at": 1,
                        "updated_at": 1,
                        "deleted_at": null
                    },
                    "members": []
                }))
            }
        };
        let join_team_hits = hits.clone();
        let join_team = move |axum::extract::Path(team_id): axum::extract::Path<String>,
                              Json(input): Json<Value>| {
            let hits = join_team_hits.clone();
            async move {
                hits.lock().unwrap().push(format!(
                    "{label}:join_team_member:{}:{}",
                    team_id,
                    input["agent_id"].as_str().unwrap_or_default()
                ));
                Json(json!({
                    "team": {
                        "id": team_id,
                        "name": "Live Team",
                        "lead_agent_id": "session_1",
                        "created_by": "session_1",
                        "created_at": 1,
                        "updated_at": 2,
                        "deleted_at": null
                    },
                    "members": []
                }))
            }
        };
        let app = axum::Router::new()
            .route("/v1/sessions", axum::routing::post(create_session))
            .route("/v1/teams", axum::routing::post(create_team))
            .route(
                "/v1/teams/{team_id}/members",
                axum::routing::post(join_team),
            );
        axum::serve(listener, app)
            .await
            .expect("serve accepting create runtime");
    });
    addr
}

async fn spawn_rejecting_command_runtime() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind rejecting runtime");
    let addr = listener.local_addr().expect("runtime addr");
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/v1/sessions/{session_id}/turns",
            axum::routing::post(|| async {
                (
                    axum::http::StatusCode::CONFLICT,
                    axum::Json(json!({
                        "error": "session already has an active turn"
                    })),
                )
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("serve rejecting runtime");
    });
    addr
}

#[derive(Debug, Clone, Copy)]
enum ResumeRuntimeMode {
    ReplayOverlap,
    ReplayGap,
}

async fn spawn_resume_runtime(mode: ResumeRuntimeMode) -> SocketAddr {
    #[derive(Debug, Deserialize)]
    struct ReplayQuery {
        after_seq: Option<i64>,
    }

    let replay = move |Query(query): Query<ReplayQuery>| async move {
        let events = match (mode, query.after_seq) {
            (ResumeRuntimeMode::ReplayOverlap, Some(1)) => vec![
                runtime_event(2, "turn.completed"),
                runtime_event(2, "turn.completed"),
                runtime_event(3, "turn.completed"),
            ],
            (ResumeRuntimeMode::ReplayGap, Some(1)) => vec![runtime_event(3, "turn.completed")],
            _ => vec![runtime_event(3, "session.created")],
        };
        Json(events)
    };

    let app = Router::new()
        .route("/v1/events", get(replay))
        .route(
            "/v1/sessions",
            get(|| async { Json(vec![session_record()]) }),
        )
        .route("/v1/teams", get(|| async { Json(Vec::<Value>::new()) }))
        .route("/v1/processes", get(|| async { Json(Vec::<Value>::new()) }))
        .route("/v1/worktrees", get(|| async { Json(Vec::<Value>::new()) }))
        .route(
            "/v1/providers",
            get(|| async { Json(json!({ "providers": [] })) }),
        )
        .route(
            "/v1/diagnostics",
            get(|| async {
                Json(json!({
                    "providers": {},
                    "comms": {},
                    "processes": {},
                    "worktrees": {},
                    "recovery": {},
                }))
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind runtime");
    let addr = listener.local_addr().expect("runtime addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("runtime server");
    });
    addr
}

fn runtime_event(row_id: i64, kind: &str) -> RuntimeEventRecord {
    RuntimeEventRecord {
        row_id,
        event_id: format!("evt_{row_id}"),
        scope: RuntimeEventScope::Session,
        scope_id: "session_1".to_string(),
        session_id: Some("session_1".to_string()),
        team_id: None,
        turn_id: Some("turn_1".to_string()),
        seq: row_id,
        kind: kind.to_string(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({ "assistant_text": format!("event {row_id}") }),
        provider: Some("codex".to_string()),
        provider_seq: Some(row_id),
        created_at: row_id,
    }
}

fn session_record() -> SessionRecord {
    SessionRecord {
        id: "session_1".to_string(),
        provider: "codex".to_string(),
        status: "ready".to_string(),
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

fn team_record(team_id: &str) -> TeamRecord {
    TeamRecord {
        id: team_id.to_string(),
        name: "Live Team".to_string(),
        lead_agent_id: "session_1".to_string(),
        created_by: "session_1".to_string(),
        created_at: 1,
        updated_at: 1,
        deleted_at: None,
    }
}
