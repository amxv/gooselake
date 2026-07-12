use base64::Engine;
use prost::Message;
use std::collections::HashSet;
use std::process::Command;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use super::*;
use crate::config::{GoosetowerConfig, RuntimeSourceConfig};
use crate::gateway::GatewayState;
use crate::materializer::{BootstrapOptions, SourceBootstrap};
use crate::runtime::{
    GooselakeRuntimeClient, GooselakeRuntimeClientConfig, RuntimeSseFanIn, RuntimeSseFanInConfig,
    SourceHealthState,
};

async fn spawn() -> (FakeGooselakeSource, String) {
    spawn_source(FakeGooselakeSource::default()).await
}

async fn spawn_source(source: FakeGooselakeSource) -> (FakeGooselakeSource, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = source.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (source, format!("http://{address}"))
}

fn client(base: String) -> GooselakeRuntimeClient {
    GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
        SOURCE_ID,
        INITIAL_EPOCH,
        base,
        Some(RUNTIME_BEARER.into()),
    ))
    .unwrap()
}

async fn apply_control(base: &str, control: FaultControl) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/__verification/v1/control"))
        .header(CONTROL_HEADER, CONTROL_SECRET)
        .json(&control)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn fake_source_public_contract_bootstraps_real_materializer() {
    let (_source, base) = spawn().await;
    let client = client(base);
    assert_eq!(client.version().await.unwrap().version, SEED_VERSION);
    let bootstrap = SourceBootstrap::from_runtime_client(
        &client,
        BootstrapOptions {
            replay_cursor_limit: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(bootstrap.state.sessions.len(), 1);
    assert_eq!(bootstrap.state.teams.len(), 1);
    assert_eq!(bootstrap.state.messages_by_team["p02-team-001"].len(), 1);
    assert_eq!(bootstrap.state.processes.len(), 1);
    assert_eq!(bootstrap.state.source_health.last_source_seq, Some(3));
}

#[tokio::test]
async fn exact_runtime_client_contract_and_scoped_cursors_are_implemented() {
    let (_source, base) = spawn().await;
    let runtime = client(base.clone());
    let models = runtime
        .provider_models(runtime_core::ProviderKind::Codex)
        .await
        .unwrap();
    assert_eq!(models.models[0].id, "gpt-5");
    assert!(
        runtime
            .provider_auth_status(runtime_core::ProviderKind::Codex)
            .await
            .unwrap()
            .authenticated
    );
    let direct = runtime
        .send_team_direct(
            "p02-team-001",
            &crate::runtime::TeamDirectInput {
                sender_agent_id: "p02-session-001".into(),
                recipient_agent_id: "p02-session-001".into(),
                input: json!([{"type":"text","text":"P02 deterministic direct"}]),
                image_paths: None,
                priority: Some("normal".into()),
                policy: Some("non_interrupting".into()),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: Some("p02-direct-key".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(direct.message.id, "p02-message-command-001");
    let broadcast = runtime
        .send_team_broadcast(
            "p02-team-001",
            &crate::runtime::TeamBroadcastInput {
                sender_agent_id: "p02-session-001".into(),
                input: json!([{"type":"text","text":"P02 deterministic broadcast"}]),
                image_paths: None,
                priority: Some("normal".into()),
                policy: Some("non_interrupting".into()),
                include_sender: Some(true),
                correlation_id: None,
                idempotency_key: Some("p02-broadcast-key".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(broadcast.disposition, "created");
    assert_eq!(
        runtime
            .list_team_messages("p02-team-001", None, Some(10))
            .await
            .unwrap()
            .messages
            .len(),
        3
    );
    let team_events = runtime
        .replay_team_events("p02-team-001", Some(1), Some(10))
        .await
        .unwrap();
    assert_eq!(
        team_events
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(team_events
        .iter()
        .all(|event| event.scope == RuntimeEventScope::Team));
    let json_replay_ignores_header = reqwest::Client::new()
        .get(format!("{base}/v1/events?after_seq=0"))
        .header("authorization", format!("Bearer {RUNTIME_BEARER}"))
        .header("last-event-id", "3")
        .send()
        .await
        .unwrap()
        .json::<Vec<RuntimeEventRecord>>()
        .await
        .unwrap();
    assert_eq!(json_replay_ignores_header.len(), 5);
    let precedence = reqwest::Client::new()
        .get(format!("{base}/v1/events/stream?after_seq=4&limit=10000"))
        .header("authorization", format!("Bearer {RUNTIME_BEARER}"))
        .header("last-event-id", "3")
        .send()
        .await
        .unwrap();
    let mut precedence_body = precedence.bytes_stream();
    let first = tokio::time::timeout(Duration::from_millis(100), precedence_body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("id: 5"));
    let invalid = reqwest::Client::new()
        .get(format!("{base}/v1/events/stream?after_seq=4"))
        .header("authorization", format!("Bearer {RUNTIME_BEARER}"))
        .header("last-event-id", "not-an-integer")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.json::<Value>().await.unwrap(),
        json!({"error":"invalid last-event-id header; expected integer"})
    );
    let negative = reqwest::Client::new()
        .get(format!("{base}/v1/events/stream"))
        .header("authorization", format!("Bearer {RUNTIME_BEARER}"))
        .header("last-event-id", "-1")
        .send()
        .await
        .unwrap();
    assert_eq!(negative.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        negative.json::<Value>().await.unwrap(),
        json!({"error":"invalid last-event-id header; expected non-negative integer"})
    );
    let scoped = reqwest::Client::new()
        .get(format!(
            "{base}/v1/teams/p02-team-001/events/stream?after_seq=0"
        ))
        .header("authorization", format!("Bearer {RUNTIME_BEARER}"))
        .header("last-event-id", "2")
        .send()
        .await
        .unwrap();
    assert_eq!(scoped.status(), StatusCode::OK);
    let mut scoped_body = scoped.bytes_stream();
    let first = tokio::time::timeout(Duration::from_millis(100), scoped_body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("id: 1"));
    assert_eq!(
        runtime
            .get_process("p02-process-001", None)
            .await
            .unwrap()
            .process
            .process_id,
        "p02-process-001"
    );
    assert_eq!(
        runtime.diagnostics().await.unwrap().recovery["epoch"],
        INITIAL_EPOCH
    );
    assert_eq!(
        runtime.provider_diagnostics().await.unwrap()["seed_version"],
        SEED_VERSION
    );
    let created = runtime
        .create_worktree(&crate::runtime::client::WorktreeCreateInput {
            source_session_id: "p02-session-001".into(),
            repo_root: Some("/p02/repo".into()),
            worktree_name: "p02".into(),
            branch_prefix: None,
            base_ref: None,
            deletion_policy: Some("retain".into()),
            run_init_script: Some(false),
            created_by_session_id: Some("p02-session-001".into()),
            operation_id: Some("p02-operation-001".into()),
            team_id: Some("p02-team-001".into()),
        })
        .await
        .unwrap();
    assert_eq!(created.worktree.id, "p02-worktree-001");
    let lookalike = reqwest::Client::new()
        .post(format!("{base}/v1/teams/p02-team-001/direct"))
        .header("authorization", format!("Bearer {RUNTIME_BEARER}"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(lookalike.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fake_source_bootstraps_real_gateway_materialized_observer() {
    let (_source, base) = spawn().await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources = vec![RuntimeSourceConfig {
        source_id: SOURCE_ID.into(),
        source_epoch: INITIAL_EPOCH.into(),
        base_url: base,
        bearer_token: Some(RUNTIME_BEARER.into()),
        display_name: "P02 deterministic source".into(),
        ..Default::default()
    }];
    let gateway = GatewayState::new(Arc::new(config)).unwrap();
    gateway.bootstrap_enabled_sources().await;
    let observer = gateway.debug_materializer_summary().await;
    assert_eq!(observer.len(), 1);
    assert_eq!(observer[0].source_id, SOURCE_ID);
    assert_eq!(observer[0].source_epoch, INITIAL_EPOCH);
    assert_eq!(observer[0].sessions, 1);
    assert_eq!(observer[0].teams, 1);
    assert_eq!(observer[0].processes, 1);
    assert_eq!(observer[0].source_health.last_source_seq, Some(1));
}

#[tokio::test]
async fn real_source_event_reaches_gateway_frame_and_production_worker_store() {
    let (source, base) = spawn().await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources = vec![RuntimeSourceConfig {
        source_id: SOURCE_ID.into(),
        source_epoch: INITIAL_EPOCH.into(),
        base_url: base.clone(),
        bearer_token: Some(RUNTIME_BEARER.into()),
        display_name: "P02 deterministic source".into(),
        ..Default::default()
    }];
    let gateway = GatewayState::new(Arc::new(config)).unwrap();
    gateway.bootstrap_enabled_sources().await;
    let mut patch_rx = gateway.verification_patch_receiver();
    let runtime_client = client(base);
    runtime_client
        .send_turn(
            "p02-session-001",
            &runtime_core::SendTurnInput {
                input: vec![json!({"type":"text","text":"P02 deterministic turn action"})],
                expected_turn_id: Some("p02-turn-001".into()),
                permission_mode: None,
            },
        )
        .await
        .unwrap();
    let runtime = source.observer().await;
    assert_eq!(runtime.events.last().unwrap().event_id, "p02-event-0004");
    let fan_in = RuntimeSseFanIn::new(
        runtime_client,
        RuntimeSseFanInConfig {
            stale_after: Duration::from_millis(50),
            ..Default::default()
        },
    );
    let (tx, mut rx) = mpsc::channel(16);
    let mut seen = HashSet::new();
    assert_eq!(
        fan_in.consume_once(Some(1), &tx, &mut seen).await.unwrap(),
        Some(4)
    );
    drop(tx);
    while let Some(event) = rx.recv().await {
        gateway.ingest_source_event(event).await;
    }
    let mut terminal_patch = None;
    while let Ok(patch) = patch_rx.try_recv() {
        if patch.view_kind == "session_detail"
            && patch.body.get("text").and_then(Value::as_str) == Some("P02 deterministic terminal")
        {
            terminal_patch = Some(patch);
        }
    }
    let patch = terminal_patch.expect("real materializer must publish terminal session detail");
    assert_eq!(patch.entity.as_ref().unwrap().entity_id, "p02-session-001");
    let frame = gateway.verification_frame_for_patch(patch);
    let encoded = base64::engine::general_purpose::STANDARD.encode(frame.encode_to_vec());
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new("bun")
        .current_dir(repo)
        .args([
            "run",
            "--cwd",
            "apps/gooseweb",
            "scripts/p02-chain-consumer.ts",
            &encoded,
        ])
        .output()
        .expect("bun chain consumer must start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let observed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(observed["source_id"], SOURCE_ID);
    assert_eq!(observed["session_id"], "p02-session-001");
    assert_eq!(observed["visible_text"], "P02 deterministic terminal");
}

#[tokio::test]
async fn supervisor_epoch_restart_pairs_fresh_source_and_tower_configuration() {
    let (_source, base) = spawn_source(FakeGooselakeSource::with_epoch_number(2)).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources = vec![RuntimeSourceConfig {
        source_id: SOURCE_ID.into(),
        source_epoch: "p02-epoch-002".into(),
        base_url: base.clone(),
        bearer_token: Some(RUNTIME_BEARER.into()),
        display_name: "P02 epoch two source".into(),
        ..Default::default()
    }];
    let gateway = GatewayState::new(Arc::new(config)).unwrap();
    gateway.bootstrap_enabled_sources().await;
    let runtime = GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
        SOURCE_ID,
        "p02-epoch-002",
        base,
        Some(RUNTIME_BEARER.into()),
    ))
    .unwrap();
    runtime
        .send_turn(
            "p02-session-001",
            &runtime_core::SendTurnInput {
                input: vec![json!({"type":"text","text":"epoch two"})],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await
        .unwrap();
    let fan_in = RuntimeSseFanIn::new(
        runtime,
        RuntimeSseFanInConfig {
            stale_after: Duration::from_millis(25),
            ..Default::default()
        },
    );
    let (tx, mut rx) = mpsc::channel(4);
    let mut seen = HashSet::new();
    assert_eq!(
        fan_in.consume_once(Some(0), &tx, &mut seen).await.unwrap(),
        Some(1)
    );
    drop(tx);
    while let Some(event) = rx.recv().await {
        assert_eq!(event.source_epoch, "p02-epoch-002");
        gateway.ingest_source_event(event).await;
    }
    let observer = gateway.debug_materializer_summary().await;
    assert_eq!(observer[0].source_epoch, "p02-epoch-002");
    assert_eq!(observer[0].source_health.last_source_seq, Some(1));
}

#[tokio::test]
async fn replay_paginates_and_sse_handoff_dedupes_overlap() {
    let (_source, base) = spawn().await;
    let client = client(base.clone());
    assert_eq!(
        client
            .replay_global_events(None, Some(2))
            .await
            .unwrap()
            .iter()
            .map(|event| event.row_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        client.replay_global_events(Some(2), Some(2)).await.unwrap()[0].row_id,
        3
    );
    apply_control(&base, FaultControl::EmitNext).await;
    let raw = client.replay_global_events(Some(3), Some(8)).await.unwrap();
    assert_eq!(
        raw.iter().map(|event| event.row_id).collect::<Vec<_>>(),
        vec![4]
    );
    apply_control(&base, FaultControl::DuplicateNext).await;
    apply_control(&base, FaultControl::EmitNext).await;
    let fan_in = RuntimeSseFanIn::new(
        client,
        RuntimeSseFanInConfig {
            stale_after: Duration::from_millis(50),
            ..Default::default()
        },
    );
    let (tx, mut rx) = mpsc::channel(16);
    let mut seen = HashSet::new();
    let cursor = fan_in.consume_once(Some(3), &tx, &mut seen).await.unwrap();
    assert_eq!(cursor, Some(5));
    drop(tx);
    let mut ids = Vec::new();
    while let Some(event) = rx.recv().await {
        ids.push(event.source_seq);
    }
    assert_eq!(ids, vec![4, 5]);
    assert_eq!(fan_in.health().state, SourceHealthState::Stale);
}

#[tokio::test]
async fn gap_fault_localizes_existing_live_to_gap_transition_baseline() {
    let (_source, base) = spawn().await;
    apply_control(&base, FaultControl::GapNext).await;
    apply_control(&base, FaultControl::EmitNext).await;
    let fan_in = RuntimeSseFanIn::new(
        client(base),
        RuntimeSseFanInConfig {
            stale_after: Duration::from_millis(50),
            ..Default::default()
        },
    );
    let (tx, _rx) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        let mut seen = HashSet::new();
        fan_in.consume_once(Some(3), &tx, &mut seen).await
    });
    let failure = task
        .await
        .expect_err("P06 baseline must remain detected in P02");
    assert!(failure.is_panic());
    let panic = failure.into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(message.contains("invalid source lifecycle transition Live -> GapDetected"));
}

#[tokio::test]
async fn faults_epoch_offline_rejection_delay_and_redaction_are_bounded() {
    let (source, base) = spawn().await;
    assert_eq!(
        apply_control(&base, FaultControl::EmitBatch { count: 100 })
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(source.observer().await.events.len(), 35);
    apply_control(
        &base,
        FaultControl::RejectNextCommand {
            code: "p02_rejected".into(),
        },
    )
    .await;
    let error = client(base.clone())
        .create_session(&runtime_core::CreateSessionInput {
            provider: runtime_core::ProviderKind::Codex,
            cwd: None,
            model: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("409"));
    apply_control(&base, FaultControl::DelayNext { milliseconds: 2 }).await;
    apply_control(&base, FaultControl::EmitNext).await;
    let started = std::time::Instant::now();
    let response = client(base.clone())
        .http()
        .get(format!("{base}/v1/events/stream?after_seq=35"))
        .bearer_auth(RUNTIME_BEARER)
        .send()
        .await
        .unwrap();
    let mut body = response.bytes_stream();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), body.next())
            .await
            .unwrap()
            .is_some()
    );
    assert!(started.elapsed() >= Duration::from_millis(2));
    apply_control(&base, FaultControl::DelayTerminal { milliseconds: 2 }).await;
    apply_control(&base, FaultControl::EmitNext).await;
    let started = std::time::Instant::now();
    let response = client(base.clone())
        .http()
        .get(format!("{base}/v1/events/stream?after_seq=36"))
        .bearer_auth(RUNTIME_BEARER)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.bytes_stream();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), body.next())
            .await
            .unwrap()
            .is_some()
    );
    assert!(started.elapsed() >= Duration::from_millis(2));
    apply_control(&base, FaultControl::DisconnectNext).await;
    let disconnected = client(base.clone())
        .http()
        .get(format!("{base}/v1/events/stream?after_seq=37"))
        .bearer_auth(RUNTIME_BEARER)
        .send()
        .await
        .unwrap();
    let mut body = disconnected.bytes_stream();
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(100), body.next()).await,
        Ok(None)
    ));
    apply_control(&base, FaultControl::ChangeEpoch).await;
    assert_eq!(source.observer().await.source_epoch, "p02-epoch-002");
    apply_control(&base, FaultControl::Offline(true)).await;
    let response = reqwest::Client::new()
        .get(format!("{base}/v1/events/stream"))
        .bearer_auth(RUNTIME_BEARER)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    apply_control(&base, FaultControl::Reset).await;
    assert_eq!(source.observer().await.source_epoch, INITIAL_EPOCH);
    assert!(!source.observer().await.offline);
    for _ in 0..100 {
        apply_control(&base, FaultControl::DelayNext { milliseconds: 1 }).await;
    }
    assert_eq!(source.observer().await.pending_faults.len(), 64);
    apply_control(&base, FaultControl::Reset).await;
    for _ in 0..10 {
        apply_control(&base, FaultControl::EmitBatch { count: 32 }).await;
    }
    assert_eq!(source.observer().await.events.len(), MAX_RETAINED_EVENTS);
    let secret = json!({"authorization":"Bearer should-not-leak","nested":{"ticket_secret":"bad"},"safe":"kept"});
    assert_eq!(
        redact_json(&secret),
        json!({"authorization":"[redacted]","nested":{"ticket_secret":"[redacted]"},"safe":"kept"})
    );
}

#[tokio::test]
async fn controls_are_not_reachable_without_verification_secret() {
    let (_source, base) = spawn().await;
    let response = reqwest::Client::new()
        .post(format!("{base}/__verification/v1/control"))
        .json(&FaultControl::EmitNext)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let unauthorized = reqwest::Client::new()
        .get(format!("{base}/v1/sessions"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        unauthorized.json::<Value>().await.unwrap(),
        json!({"error":"missing or invalid bearer token"})
    );
    let public_health = reqwest::Client::new()
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(public_health.status(), StatusCode::OK);
    assert_eq!(
        public_health.json::<Value>().await.unwrap(),
        json!({"status":"ok","providers":1,"public_base_url":"http://127.0.0.1:18102"})
    );
    let protected_health = client(base).protected_health().await.unwrap();
    assert_eq!(protected_health.status, "ok");
    assert_eq!(protected_health.providers, Some(1));
    assert_eq!(
        protected_health.public_base_url.as_deref(),
        Some("http://127.0.0.1:18102")
    );
}
