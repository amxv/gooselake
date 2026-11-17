use super::super::native_review_refusal::assert_refusal;
use super::super::workspace_worktree_routes::call;
use super::*;
use runtime_core::{OperationDetails, RuntimeApp, RuntimeEventRecord, RuntimeEventScope};
use std::sync::atomic::Ordering;

struct Fixture {
    router: Router,
    token: String,
    temp: tempfile::TempDir,
    provider: Arc<TestProvider>,
    app: Arc<RuntimeApp>,
    workspace_id: String,
    agent_id: String,
    turn_id: String,
}

impl Fixture {
    async fn new() -> Self {
        let (router, token, temp, provider, runtime, app) =
            build_test_router_with_services(TeamMcpPolicy::default()).await;
        let root = temp.path().join("interrupt-target-events");
        fs::create_dir_all(&root).unwrap();
        let workspace =
            register_control_workspace(&router, &token, &root, "Interrupt event HTTP").await;
        let agent = create_control_agent_with_permission(
            &router,
            &token,
            &workspace.workspace_id,
            &root,
            "Target",
            "workspace_write",
        )
        .await;
        finish_test_onboarding(&runtime, &workspace.workspace_id, &agent.agent_id).await;
        let (status, turn) = call(
            &router,
            &token,
            "POST",
            &format!("/v2/agents/{}/turns", agent.agent_id),
            Some(json!({"input":[{"type":"text","text":"model_assign_wait_for_interrupt"}]})),
            Some("original-interrupt-native-target"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{turn}");
        let turn_id = turn["turn_id"].as_str().unwrap().to_owned();
        let session = runtime.get_session(&agent.agent_id).await.unwrap();
        assert_eq!(session.active_turn_id.as_deref(), Some(turn_id.as_str()));
        assert_eq!(session.status, "turn_running");
        assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 0);
        Self {
            router,
            token,
            temp,
            provider,
            app,
            workspace_id: workspace.workspace_id,
            agent_id: agent.agent_id,
            turn_id,
        }
    }

    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.temp.path().join("runtime.sqlite3")).unwrap()
    }

    fn fail_event(&self, phase: &str) {
        assert!(matches!(phase, "started" | "finalized" | "uncertain"));
        self.db()
            .execute_batch(&format!(
                "CREATE TRIGGER reject_interrupt_http_event BEFORE INSERT ON runtime_events
             WHEN NEW.kind='workspace.interrupt_target_{phase}'
             BEGIN SELECT RAISE(ABORT,'fixture HTTP interrupt event failure'); END;"
            ))
            .unwrap();
    }

    fn allow_event(&self) {
        self.db()
            .execute_batch("DROP TRIGGER reject_interrupt_http_event;")
            .unwrap();
    }

    async fn interrupt(&self, router: &Router) -> (StatusCode, serde_json::Value) {
        call(
            router,
            &self.token,
            "POST",
            &format!("/v2/workspaces/{}/interrupt", self.workspace_id),
            None,
            Some("interrupt-event-original"),
        )
        .await
    }

    fn details(&self, operation_id: &str) -> OperationDetails {
        self.app
            .services
            .store
            .get_operation(operation_id)
            .unwrap()
            .unwrap()
    }

    fn target_events(&self) -> Vec<RuntimeEventRecord> {
        self.app
            .services
            .store
            .list_runtime_events(
                Some((RuntimeEventScope::Workspace, &self.workspace_id)),
                None,
                100,
            )
            .unwrap()
            .into_iter()
            .filter(|event| event.kind.starts_with("workspace.interrupt_target_"))
            .collect()
    }

    fn acknowledgements(&self, operation_id: &str) -> Vec<RuntimeEventRecord> {
        self.app
            .services
            .store
            .list_runtime_events(
                Some((RuntimeEventScope::Session, &self.agent_id)),
                None,
                100,
            )
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.kind == "turn.interrupt_requested"
                    && event.turn_id.as_deref() == Some(self.turn_id.as_str())
                    && event.payload["operation_id"].as_str() == Some(operation_id)
            })
            .collect()
    }

    async fn reopen(&self) -> Router {
        // Rebuild the HTTP/runtime caches over the same disposable SQLite
        // history and fake native provider. This does not run startup resume.
        let (router, _, _) = reopen_test_router_with_services(
            &self.app,
            self.temp.path(),
            &self.token,
            TeamMcpPolicy::default(),
        )
        .await;
        router
    }
}

#[tokio::test]
async fn interrupt_started_event_fault_prevents_native_call_and_original_intent_can_continue_once()
{
    let fixture = Fixture::new().await;
    fixture.fail_event("started");
    let (status, body) = fixture.interrupt(&fixture.router).await;
    let operation_id = body["error"]["details"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_refusal(status, &body, &operation_id);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("fixture HTTP interrupt event failure"));
    let original = fixture.details(&operation_id);
    assert_eq!(original.operation.kind, "workspace_interrupt");
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::Requested
    );
    assert_eq!(original.effects.len(), 1);
    assert_eq!(original.effects[0].phase, "intended");
    assert_eq!(original.effects[0].attempt_count, 0);
    assert_eq!(original.claims.len(), 1);
    assert!(fixture.target_events().is_empty());
    assert!(fixture.acknowledgements(&operation_id).is_empty());
    assert_eq!(
        fixture
            .provider
            .interrupt_invocations
            .load(Ordering::SeqCst),
        0
    );

    fixture.allow_event();
    let (status, completed) = fixture.interrupt(&fixture.router).await;
    assert_eq!(status, StatusCode::OK, "{completed}");
    assert_eq!(completed["operation_id"], operation_id);
    assert_eq!(
        fixture
            .provider
            .interrupt_invocations
            .load(Ordering::SeqCst),
        1
    );
    let saved = fixture.details(&operation_id);
    let events = fixture.target_events();
    assert_eq!(events.len(), 2);
    assert_eq!(saved.effects[0].phase, "finalized");
    assert_eq!(saved.effects[0].attempt_count, 1);
    assert!(saved.claims.is_empty());
    let (status, replay) = fixture.interrupt(&fixture.router).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["operation_id"], completed["operation_id"]);
    assert_eq!(
        replay["interrupted_agent_ids"],
        completed["interrupted_agent_ids"]
    );
    assert_eq!(fixture.details(&operation_id), saved);
    assert_eq!(fixture.target_events(), events);
    assert_eq!(
        fixture
            .provider
            .interrupt_invocations
            .load(Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn interrupt_final_event_fault_after_native_ack_recovers_receipt_without_another_native_call()
{
    let fixture = Fixture::new().await;
    fixture.fail_event("finalized");
    let (status, body) = fixture.interrupt(&fixture.router).await;
    let operation_id = body["error"]["details"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_refusal(status, &body, &operation_id);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("fixture HTTP interrupt event failure"));
    let original = fixture.details(&operation_id);
    assert_eq!(original.effects[0].phase, "started");
    assert_eq!(original.effects[0].attempt_count, 1);
    assert_eq!(original.claims.len(), 1);
    assert_eq!(
        fixture
            .provider
            .interrupt_invocations
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(fixture.acknowledgements(&operation_id).len(), 1);
    assert_eq!(fixture.target_events().len(), 1);

    fixture.allow_event();
    let reopened = fixture.reopen().await;
    let (status, completed) = fixture.interrupt(&reopened).await;
    assert_eq!(status, StatusCode::OK, "{completed}");
    assert_eq!(completed["operation_id"], operation_id);
    assert_eq!(
        completed["interrupted_agent_ids"],
        json!([fixture.agent_id])
    );
    let saved = fixture.details(&operation_id);
    let events = fixture.target_events();
    assert_eq!(
        saved.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(saved.effects[0].phase, "finalized");
    assert!(saved.claims.is_empty());
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].kind, "workspace.interrupt_target_finalized");
    assert_eq!(
        events[1].payload["evidence"]["reason"],
        "recovered_interrupt_request_event"
    );
    assert_eq!(
        fixture
            .provider
            .interrupt_invocations
            .load(Ordering::SeqCst),
        1
    );
    let (status, replay) = fixture.interrupt(&reopened).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["operation_id"], completed["operation_id"]);
    assert_eq!(
        replay["interrupted_agent_ids"],
        completed["interrupted_agent_ids"]
    );
    assert_eq!(fixture.details(&operation_id), saved);
    assert_eq!(fixture.target_events(), events);
    assert_eq!(fixture.acknowledgements(&operation_id).len(), 1);
    assert_eq!(
        fixture
            .provider
            .interrupt_invocations
            .load(Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn interrupt_lost_native_ack_with_or_without_uncertain_event_fault_never_reinterrupts() {
    for receipt_fault in [false, true] {
        let fixture = Fixture::new().await;
        fixture
            .provider
            .interrupt_failure_mode
            .store(1, Ordering::SeqCst);
        if receipt_fault {
            fixture.fail_event("uncertain");
        }
        let (status, body) = fixture.interrupt(&fixture.router).await;
        let operation_id = body["error"]["details"]["operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_refusal(status, &body, &operation_id);
        let message = body["error"]["message"].as_str().unwrap();
        assert!(
            message.contains(if receipt_fault {
                "fixture HTTP interrupt event failure"
            } else {
                "native interrupt acknowledgement or durable receipt is unavailable"
            }),
            "{message}"
        );
        let original = fixture.details(&operation_id);
        assert_eq!(
            original.operation.phase,
            runtime_core::OperationPhase::Requested
        );
        assert_eq!(
            original.effects[0].phase,
            if receipt_fault {
                "started"
            } else {
                "uncertain"
            }
        );
        assert_eq!(original.effects[0].attempt_count, 1);
        assert_eq!(original.claims.len(), 1);
        assert!(original.operation.exact_terminal_result.is_none());
        assert_eq!(
            fixture
                .provider
                .interrupt_invocations
                .load(Ordering::SeqCst),
            1
        );
        assert!(fixture.acknowledgements(&operation_id).is_empty());
        let events = fixture.target_events();
        assert_eq!(events.len(), if receipt_fault { 1 } else { 2 });
        if receipt_fault {
            fixture.allow_event();
        }
        let reopened = fixture.reopen().await;
        for router in [&fixture.router, &reopened] {
            let (status, retry) = fixture.interrupt(router).await;
            assert_refusal(status, &retry, &operation_id);
            assert!(retry["error"]["message"]
                .as_str()
                .unwrap()
                .contains("refusing duplicate provider interrupt"));
            assert_eq!(fixture.details(&operation_id), original);
            assert_eq!(fixture.target_events(), events);
            assert!(fixture.acknowledgements(&operation_id).is_empty());
            assert_eq!(
                fixture
                    .provider
                    .interrupt_invocations
                    .load(Ordering::SeqCst),
                1
            );
        }
        let (status, inspect) = call(
            &reopened,
            &fixture.token,
            "GET",
            &format!("/v2/operations/{operation_id}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{inspect}");
        assert_eq!(inspect["operation"]["operation_id"], operation_id);
        assert_eq!(inspect["effects"][0]["phase"], original.effects[0].phase);
    }
}

#[tokio::test]
async fn interrupt_ignored_or_damaged_started_receipt_never_grants_native_dispatch() {
    for damage in [None, Some("critical=0"), Some("payload_json='{}'")] {
        let fixture = Fixture::new().await;
        let (timing, body) = match damage {
            None => ("BEFORE", "SELECT RAISE(IGNORE);".to_string()),
            Some(change) => (
                "AFTER",
                format!("UPDATE runtime_events SET {change} WHERE event_id=NEW.event_id;"),
            ),
        };
        fixture
            .db()
            .execute_batch(&format!(
                "CREATE TRIGGER reject_interrupt_http_event {timing} INSERT ON runtime_events
             WHEN NEW.kind='workspace.interrupt_target_started'
             BEGIN {body} END;"
            ))
            .unwrap();
        let (status, body) = fixture.interrupt(&fixture.router).await;
        let operation_id = body["error"]["details"]["operation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_refusal(status, &body, &operation_id);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("workspace interrupt target event"),
            "{body}"
        );
        let original = fixture.details(&operation_id);
        assert_eq!(
            original.operation.phase,
            runtime_core::OperationPhase::Requested
        );
        assert_eq!(original.effects.len(), 1);
        assert_eq!(original.effects[0].phase, "intended");
        assert_eq!(original.effects[0].attempt_count, 0);
        assert_eq!(original.claims.len(), 1);
        assert!(fixture.target_events().is_empty());
        assert!(fixture.acknowledgements(&operation_id).is_empty());
        assert_eq!(
            fixture
                .provider
                .interrupt_invocations
                .load(Ordering::SeqCst),
            0
        );

        let reopened = fixture.reopen().await;
        let (status, retry) = fixture.interrupt(&reopened).await;
        assert_refusal(status, &retry, &operation_id);
        assert_eq!(retry["error"], body["error"]);
        assert_eq!(fixture.details(&operation_id), original);
        assert!(fixture.target_events().is_empty());
        assert!(fixture.acknowledgements(&operation_id).is_empty());
        assert_eq!(
            fixture
                .provider
                .interrupt_invocations
                .load(Ordering::SeqCst),
            0
        );

        fixture.allow_event();
        let (status, completed) = fixture.interrupt(&reopened).await;
        assert_eq!(status, StatusCode::OK, "{completed}");
        assert_eq!(completed["operation_id"], operation_id);
        assert_eq!(
            fixture
                .provider
                .interrupt_invocations
                .load(Ordering::SeqCst),
            1
        );
        let saved = fixture.details(&operation_id);
        let events = fixture.target_events();
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|event| event.criticality == runtime_core::RuntimeEventCriticality::Critical));
        assert_eq!(saved.effects[0].phase, "finalized");
        assert_eq!(saved.effects[0].attempt_count, 1);
        assert!(saved.claims.is_empty());
        let (status, replay) = fixture.interrupt(&reopened).await;
        assert_eq!(status, StatusCode::OK, "{replay}");
        assert_eq!(replay["operation_id"], completed["operation_id"]);
        assert_eq!(
            replay["interrupted_agent_ids"],
            completed["interrupted_agent_ids"]
        );
        assert_eq!(replay["skipped_agent_ids"], completed["skipped_agent_ids"]);
        assert_eq!(fixture.details(&operation_id), saved);
        assert_eq!(fixture.target_events(), events);
        assert_eq!(fixture.acknowledgements(&operation_id).len(), 1);
        assert_eq!(
            fixture
                .provider
                .interrupt_invocations
                .load(Ordering::SeqCst),
            1
        );
    }
}

#[tokio::test]
async fn interrupt_carrier_fault_preserves_original_native_boundary_and_known_ack() {
    for phase in ["started", "finalized"] {
        for after_event in [false, true] {
            let fixture = Fixture::new().await;
            let reset_phase = if phase == "started" {
                "intended"
            } else {
                "started"
            };
            let (on, condition, effect_id, damage) = if after_event {
                (
                    "INSERT ON runtime_events",
                    format!("NEW.kind='workspace.interrupt_target_{phase}'"),
                    "json_extract(NEW.payload_json,'$.effect_id')",
                    format!("phase='{reset_phase}'"),
                )
            } else {
                (
                    "UPDATE ON runtime_operation_effects",
                    format!("NEW.effect_kind='workspace_turn_interrupt' AND NEW.phase='{phase}'"),
                    "NEW.effect_id",
                    "evidence_json=json_set(evidence_json,'$.outcome','tampered')".to_string(),
                )
            };
            fixture
                .db()
                .execute_batch(&format!(
                    "CREATE TRIGGER reject_interrupt_http_event AFTER {on}
                 WHEN {condition}
                 BEGIN UPDATE runtime_operation_effects SET {damage}
                   WHERE effect_id={effect_id}; END;"
                ))
                .unwrap();
            let (status, body) = fixture.interrupt(&fixture.router).await;
            let operation_id = body["error"]["details"]["operation_id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert_refusal(status, &body, &operation_id);
            assert!(
                body["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("carrier"),
                "{body}"
            );
            let original = fixture.details(&operation_id);
            let expected_calls = usize::from(phase == "finalized");
            assert_eq!(
                original.operation.phase,
                runtime_core::OperationPhase::Requested
            );
            assert_eq!(original.effects.len(), 1);
            assert_eq!(original.effects[0].phase, reset_phase);
            assert_eq!(original.effects[0].attempt_count, expected_calls as u64);
            assert_eq!(original.claims.len(), 1);
            assert!(original.operation.exact_terminal_result.is_none());
            assert_eq!(
                fixture
                    .provider
                    .interrupt_invocations
                    .load(Ordering::SeqCst),
                expected_calls
            );
            assert_eq!(
                fixture.acknowledgements(&operation_id).len(),
                expected_calls
            );
            let events = fixture.target_events();
            assert_eq!(events.len(), expected_calls);

            let reopened = fixture.reopen().await;
            let (status, retry) = fixture.interrupt(&reopened).await;
            assert_refusal(status, &retry, &operation_id);
            assert_eq!(fixture.details(&operation_id), original);
            assert_eq!(fixture.target_events(), events);
            assert_eq!(
                fixture
                    .provider
                    .interrupt_invocations
                    .load(Ordering::SeqCst),
                expected_calls
            );
            assert_eq!(
                fixture.acknowledgements(&operation_id).len(),
                expected_calls
            );

            fixture.allow_event();
            let (status, completed) = fixture.interrupt(&reopened).await;
            assert_eq!(status, StatusCode::OK, "{completed}");
            assert_eq!(completed["operation_id"], operation_id);
            assert_eq!(
                fixture
                    .provider
                    .interrupt_invocations
                    .load(Ordering::SeqCst),
                1
            );
            assert_eq!(fixture.acknowledgements(&operation_id).len(), 1);
            let saved = fixture.details(&operation_id);
            let events = fixture.target_events();
            assert_eq!(
                saved.operation.phase,
                runtime_core::OperationPhase::Completed
            );
            assert_eq!(saved.effects[0].phase, "finalized");
            assert_eq!(saved.effects[0].attempt_count, 1);
            assert!(saved.claims.is_empty());
            assert_eq!(events.len(), 2);
            let (status, replay) = fixture.interrupt(&reopened).await;
            assert_eq!(status, StatusCode::OK, "{replay}");
            assert_eq!(replay["operation_id"], completed["operation_id"]);
            assert_eq!(
                replay["interrupted_agent_ids"],
                completed["interrupted_agent_ids"]
            );
            assert_eq!(fixture.details(&operation_id), saved);
            assert_eq!(fixture.target_events(), events);
            assert_eq!(
                fixture
                    .provider
                    .interrupt_invocations
                    .load(Ordering::SeqCst),
                1
            );
        }
    }
}
