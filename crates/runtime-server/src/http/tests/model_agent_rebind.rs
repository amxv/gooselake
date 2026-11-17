use super::operator_rebind_receipt_replay::fixture;
use super::workspace_worktree_routes::call;
use super::*;
use serde_json::{json, Value};

mod fences;

struct Fixture {
    state: AppState,
    temp: tempfile::TempDir,
    provider: Arc<TestProvider>,
    workspace: runtime_core::WorkspaceRecord,
    caller: runtime_core::WorkspaceAgentRecord,
    target: runtime_core::WorkspaceAgentRecord,
}

impl Fixture {
    async fn new() -> Self {
        let (state, temp, provider, workspace, caller) = fixture().await;
        let (status, result) = call(
            &build_router(state.clone()),
            &state.bearer_token,
            "POST",
            &format!("/v2/workspaces/{}/agents", workspace.workspace_id),
            Some(json!({"provider":"codex","model":"test-model","title":"Idle rebind peer"})),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        let target: runtime_core::WorkspaceAgentRecord = serde_json::from_value(result).unwrap();
        finish_test_onboarding(&state.runtime, &workspace.workspace_id, &target.agent_id).await;
        Self {
            state,
            temp,
            provider,
            workspace,
            caller,
            target,
        }
    }

    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.temp.path().join("runtime.sqlite3")).unwrap()
    }

    fn plan(&self, key: &str) -> runtime_core::MemberAgentRebindCommand {
        let request = json!({"tool_name":"gg_team","args":{"mode":"assign_worktree",
            "assign_worktree":{"agent_id":self.target.agent_id,"worktree_name":"linked"}}});
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let parent_id = format!("op_fixture_assign_{key}");
        let command = runtime_core::AgentToolMutationCommand {
            operation_id: parent_id.clone(),
            workspace_id: self.workspace.workspace_id.clone(),
            caller_agent_id: self.caller.agent_id.clone(),
            idempotency_key: key.into(),
            tool_name: "gg_team".into(),
            normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
            normalized_request: request,
            admitted_at: now,
        };
        assert!(matches!(
            self.state
                .app
                .services
                .store
                .begin_agent_tool_mutation(&command)
                .unwrap(),
            runtime_core::AgentToolMutationAdmission::Execute { .. }
        ));
        let identity =
            runtime_core::resolve_repository_identity(Path::new(&self.workspace.canonical_root))
                .unwrap();
        self.state
            .app
            .services
            .store
            .plan_member_agent_rebind(
                &parent_id,
                &self.caller.agent_id,
                runtime_core::WorkspaceMembershipPolicy::default(),
                "wt_receipt",
                &identity,
                now,
            )
            .unwrap()
    }

    async fn assign(&self, key: &str) -> Value {
        self.assign_as(&self.caller.agent_id, key).await
    }

    async fn assign_as(&self, caller: &str, key: &str) -> Value {
        self.assign_as_to(caller, &self.target.agent_id, key).await
    }

    async fn assign_as_to(&self, caller: &str, target: &str, key: &str) -> Value {
        let scoped = runtime_core::mint_scoped_mcp_token(&self.state.bearer_token, caller);
        let response = build_router(self.state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/mcp/invoke")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {scoped}"))
                    .header("x-gg-tool-invocation-id", key)
                    .body(Body::from(
                        json!({"tool_name":"gg_team","args":{
                            "mode":"assign_worktree","assign_worktree":{
                                "agent_id":target,"worktree_name":"linked"}
                        }})
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
    }
}

#[tokio::test]
async fn model_assign_child_keeps_real_agent_and_original_parent_receipt() {
    let fixture = Fixture::new().await;
    let result = fixture.assign("model-assign-original").await;
    assert_eq!(result["ok"], true, "{result}");
    let child_id = result["result"]["rebind"]["operation"]["operation_id"]
        .as_str()
        .unwrap();
    let child = fixture
        .state
        .app
        .services
        .store
        .get_operation(child_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        child.operation.actor.kind,
        runtime_core::OperationActorKind::Agent
    );
    assert_eq!(child.operation.actor.identifier, fixture.caller.agent_id);
    assert!(child.operation.idempotency_key.as_ref().unwrap().len() > 128);
    assert_ne!(
        child.operation.idempotency_key.as_deref(),
        Some("model-assign-original")
    );
    let parent_id: String = fixture.db().query_row(
        "SELECT operation_id FROM runtime_operations WHERE actor_kind='agent' AND actor_id=?1 AND idempotency_key=?2",
        [&fixture.caller.agent_id, "model-assign-original"], |row| row.get(0)).unwrap();
    let parent = fixture
        .state
        .app
        .services
        .store
        .get_operation(&parent_id)
        .unwrap()
        .unwrap();
    assert_eq!(parent.effects.len(), 1);
    assert_eq!(parent.effects[0].phase, "finalized");
    assert_eq!(
        child.operation.exact_terminal_result.as_ref().unwrap(),
        &result["result"]["rebind"]
    );
    fixture.db().execute("UPDATE managed_worktrees SET worktree_name='later-name',revision=revision+1 WHERE id='wt_receipt'", []).unwrap();
    fixture
        .db()
        .execute(
            "UPDATE workspace_agents SET revision=revision+1 WHERE session_id=?1",
            [&fixture.target.agent_id],
        )
        .unwrap();
    assert_eq!(fixture.assign("model-assign-original").await, result);
    assert_eq!(fixture.provider.rebind_calls().await, 1);
}

#[tokio::test]
async fn model_assign_uncertain_binding_retains_child_details_and_blocks_startup_resume() {
    for mode in ["bad_evidence", "applied_unknown"] {
        uncertain_binding(mode).await;
    }
}

async fn uncertain_binding(mode: &str) {
    let fixture = Fixture::new().await;
    let original = fixture
        .state
        .runtime
        .get_session(&fixture.target.agent_id)
        .await
        .unwrap();
    fixture.provider.set_rebind_mode(Some(mode)).await;
    let result = fixture.assign("model-assign-uncertain").await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
    let target = &result["error"]["details"]["targets"][0];
    assert_eq!(target["agent_id"], fixture.target.agent_id, "{result}");
    assert_eq!(target["status"], "outcome_unresolved");
    let child_id = target["operation_id"].as_str().unwrap();
    let child = fixture
        .state
        .app
        .services
        .store
        .get_operation(child_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        child.operation.actor.kind,
        runtime_core::OperationActorKind::Agent
    );
    assert_eq!(child.operation.actor.identifier, fixture.caller.agent_id);
    assert_eq!(
        child.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(child.claims.len(), 1);
    assert_ne!(
        fixture.provider.native_cwd(&fixture.target.agent_id).await,
        original.cwd
    );
    assert_eq!(
        fixture
            .state
            .runtime
            .get_session(&fixture.target.agent_id)
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        fixture.assign("model-assign-uncertain").await["error"]["details"],
        result["error"]["details"]
    );
    assert_eq!(fixture.assign("model-assign-new-key").await["ok"], false);
    assert_eq!(fixture.provider.rebind_calls().await, 1);
    fixture.provider.set_rebind_mode(None).await;
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(fixture.provider.clone()).unwrap();
    let reopened = Arc::new(
        RuntimeSessionManager::new(
            fixture.state.app.services.store.clone(),
            Arc::new(providers),
            128,
        )
        .unwrap(),
    );
    let before = fixture
        .provider
        .resumed_requests()
        .await
        .into_iter()
        .filter(|request| request.runtime_session_id == fixture.target.agent_id)
        .count();
    reopened.recover_startup().await.unwrap();
    assert_eq!(
        fixture
            .provider
            .resumed_requests()
            .await
            .into_iter()
            .filter(|request| request.runtime_session_id == fixture.target.agent_id)
            .count(),
        before
    );
    assert_eq!(fixture.provider.rebind_calls().await, 1);
    assert_eq!(
        reopened
            .get_session(&fixture.target.agent_id)
            .await
            .unwrap(),
        original
    );
}

#[tokio::test]
async fn model_assign_independent_admissions_grant_one_executor_and_startup_retains_both_claims() {
    use runtime_core::MemberAgentRebindAdmission;
    let fixture = Fixture::new().await;
    let command = fixture.plan("independent-admission");
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let workers = (0..2)
        .map(|_| {
            let path = fixture.temp.path().join("runtime.sqlite3");
            let command = command.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let store = SqliteRuntimeStore::new(SqliteStoreConfig {
                    database_path: path,
                });
                barrier.wait();
                store.admit_member_agent_rebind(&command).unwrap()
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, MemberAgentRebindAdmission::Execute))
            .count(),
        1
    );
    assert_eq!(results.iter().filter(|result| matches!(result,MemberAgentRebindAdmission::Unresolved { operation_id } if operation_id == &command.operation.operation_id)).count(),1);
    assert_eq!(fixture.provider.rebind_calls().await, 0);
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(fixture.provider.clone()).unwrap();
    let reopened = Arc::new(
        RuntimeSessionManager::new(
            fixture.state.app.services.store.clone(),
            Arc::new(providers),
            128,
        )
        .unwrap(),
    );
    reopened.recover_startup().await.unwrap();
    let child = fixture
        .state
        .app
        .services
        .store
        .get_operation(&command.operation.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        child.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(child.claims.len(), 1);
    let reservation: String = fixture.db().query_row("SELECT claim_role FROM managed_worktree_claims WHERE worktree_id='wt_receipt' AND session_id=?1 AND released_at IS NULL",
        [&fixture.target.agent_id],|row|row.get(0)).unwrap();
    assert_eq!(reservation, "rebind_reservation");
    let parent = fixture
        .state
        .app
        .services
        .store
        .get_operation(&command.parent_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(parent.effects[0].phase, "uncertain");
    assert_eq!(
        parent.effects[0].evidence.as_ref().unwrap()["outcome"]["status"],
        "outcome_unresolved"
    );
    assert_eq!(
        fixture
            .provider
            .resumed_requests()
            .await
            .into_iter()
            .filter(|row| row.runtime_session_id == fixture.target.agent_id)
            .count(),
        0
    );
    assert_eq!(fixture.provider.rebind_calls().await, 0);
}

#[tokio::test]
async fn model_assign_frozen_snapshot_and_independent_destination_changes_refuse_native_admission()
{
    for mutation in [
        "target_revision",
        "native_reference",
        "destination_revision",
        "destination_path",
    ] {
        let fixture = Fixture::new().await;
        let command = fixture.plan(mutation);
        let mut substituted = command.clone();
        substituted.actor.identifier = fixture.target.agent_id.clone();
        assert!(fixture
            .state
            .app
            .services
            .store
            .admit_member_agent_rebind(&substituted)
            .is_err());
        let mut substituted = command.clone();
        substituted.operation.destination_cwd = "/substituted".into();
        assert!(fixture
            .state
            .app
            .services
            .store
            .admit_member_agent_rebind(&substituted)
            .is_err());
        let db = fixture.db();
        match mutation {
            "target_revision" => {
                db.execute(
                    "UPDATE workspace_agents SET revision=revision+1 WHERE session_id=?1",
                    [&fixture.target.agent_id],
                )
                .unwrap();
            }
            "native_reference" => {
                db.execute(
                    "UPDATE sessions SET provider_session_ref='changed-native-target' WHERE id=?1",
                    [&fixture.target.agent_id],
                )
                .unwrap();
            }
            "destination_revision" => {
                db.execute(
                    "UPDATE managed_worktrees SET revision=revision+1 WHERE id='wt_receipt'",
                    [],
                )
                .unwrap();
            }
            _ => {
                db.execute("UPDATE managed_worktrees SET worktree_cwd=worktree_cwd || '-moved',revision=revision+1 WHERE id='wt_receipt'",[]).unwrap();
            }
        }
        assert!(
            fixture
                .state
                .runtime
                .execute_member_agent_rebind(command.clone())
                .await
                .is_err(),
            "{mutation}"
        );
        assert!(fixture
            .state
            .app
            .services
            .store
            .get_operation(&command.operation.operation_id)
            .unwrap()
            .is_none());
        assert_eq!(fixture.provider.rebind_calls().await, 0, "{mutation}");
    }
}

#[tokio::test]
async fn model_assign_receipt_survives_caller_revocation_and_later_roster_changes() {
    let fixture = Fixture::new().await;
    let command = fixture.plan("settlement-after-revocation");
    assert!(matches!(
        fixture
            .state
            .app
            .services
            .store
            .admit_member_agent_rebind(&command)
            .unwrap(),
        runtime_core::MemberAgentRebindAdmission::Execute
    ));
    let evidence = fixture
        .provider
        .rebind_workspace(runtime_core::ProviderWorkspaceRebindRequest {
            runtime_session_id: fixture.target.agent_id.clone(),
            cwd: command.destination.worktree_cwd.clone(),
        })
        .await
        .unwrap();
    fixture.db().execute("UPDATE workspace_agents SET lifecycle_state='archived',archived_at=?2,revision=revision+1 WHERE session_id=?1",
        rusqlite::params![fixture.caller.agent_id,command.operation.created_at+1]).unwrap();
    let mut session = command.original_session.clone();
    session.cwd = Some(command.destination.worktree_cwd.clone());
    session.worktree_id = Some(command.destination.id.clone());
    session.provider_session_ref = evidence.provider_session_ref.clone();
    session.canonical_provider_session_ref = evidence.canonical_provider_session_ref.clone();
    session.updated_at = command.operation.created_at + 2;
    let mut policy = command.original_agent.recreation_policy.clone();
    policy.authoritative_cwd = command.destination.worktree_cwd.clone();
    fixture
        .state
        .app
        .services
        .store
        .finalize_workspace_agent_rebind(
            &command.operation.operation_id,
            &session,
            &policy,
            &evidence,
            session.updated_at,
        )
        .unwrap();
    let response = fixture
        .state
        .runtime
        .execute_member_agent_rebind(command.clone())
        .await
        .unwrap();
    assert_eq!(
        response.agent.as_ref().unwrap().revision,
        fixture.target.revision + 1
    );
    fixture.db().execute("UPDATE workspace_agents SET lifecycle_state='archived',archived_at=?2,revision=revision+1 WHERE session_id=?1",
        rusqlite::params![fixture.target.agent_id,session.updated_at+1]).unwrap();
    fixture.db().execute("UPDATE managed_worktrees SET worktree_name='renamed-after-receipt',revision=revision+1 WHERE id='wt_receipt'",[]).unwrap();
    let replay = fixture
        .state
        .runtime
        .execute_member_agent_rebind(command.clone())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&replay).unwrap(),
        serde_json::to_value(&response).unwrap()
    );
    let outer = json!({"ok":true,"result":{"operation":"assign_worktree","agent_id":fixture.target.agent_id,"rebind":response}});
    fixture
        .state
        .app
        .services
        .store
        .finish_agent_tool_mutation(&command.parent_operation_id, &outer, session.updated_at + 2)
        .unwrap();
    assert_eq!(
        fixture
            .state
            .app
            .services
            .store
            .get_operation(&command.parent_operation_id)
            .unwrap()
            .unwrap()
            .operation
            .exact_terminal_result,
        Some(outer)
    );
    assert_eq!(fixture.provider.rebind_calls().await, 1);
}

#[tokio::test]
async fn model_assign_revoked_caller_and_fabricated_success_cannot_admit_or_complete() {
    let fixture = Fixture::new().await;
    let command = fixture.plan("revoked-before-admission");
    let fabricated = json!({"ok":true,"result":{"operation":"assign_worktree","agent_id":fixture.target.agent_id,"rebind":{}}});
    assert!(fixture
        .state
        .app
        .services
        .store
        .finish_agent_tool_mutation(
            &command.parent_operation_id,
            &fabricated,
            command.operation.created_at + 1
        )
        .is_err());
    fixture.db().execute("UPDATE workspace_agents SET lifecycle_state='archived',archived_at=?2,revision=revision+1 WHERE session_id=?1",
        rusqlite::params![fixture.caller.agent_id,command.operation.created_at+2]).unwrap();
    assert!(fixture
        .state
        .runtime
        .execute_member_agent_rebind(command.clone())
        .await
        .is_err());
    assert!(fixture
        .state
        .app
        .services
        .store
        .get_operation(&command.operation.operation_id)
        .unwrap()
        .is_none());
    assert_eq!(fixture.provider.rebind_calls().await, 0);
}

#[tokio::test]
async fn model_assign_late_child_and_parent_receipt_faults_never_redispatch_native_work() {
    for parent_only in [false, true] {
        let fixture = Fixture::new().await;
        let original = fixture
            .state
            .runtime
            .get_session(&fixture.target.agent_id)
            .await
            .unwrap();
        let destination = fixture
            .state
            .app
            .services
            .store
            .get_managed_worktree("wt_receipt")
            .unwrap()
            .unwrap();
        let sql = if parent_only {
            "CREATE TRIGGER fail_member_receipt BEFORE UPDATE OF phase ON runtime_operations WHEN NEW.kind='agent_model_tool_mutation' AND NEW.phase='completed'
             BEGIN SELECT RAISE(ABORT,'lost parent receipt'); END;"
        } else {
            "CREATE TRIGGER fail_member_receipt BEFORE INSERT ON runtime_events WHEN NEW.kind='model_tool.rebind_target_updated' AND json_extract(NEW.payload_json,'$.status')='native_rebind_confirmed'
             BEGIN SELECT RAISE(ABORT,'lost native target receipt'); END;"
        };
        fixture.db().execute_batch(sql).unwrap();
        let result = fixture.assign("receipt-fault").await;
        assert_eq!(result["ok"], false, "{parent_only} {result}");
        assert_eq!(result["error"]["code"], "outcome_unresolved");
        let outcome = &result["error"]["details"]["targets"][0];
        let child_id = outcome["operation_id"].as_str().unwrap();
        let child = fixture
            .state
            .app
            .services
            .store
            .get_operation(child_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            child.operation.phase,
            if parent_only {
                runtime_core::OperationPhase::Completed
            } else {
                runtime_core::OperationPhase::ManualReview
            }
        );
        assert_eq!(child.claims.len(), usize::from(!parent_only));
        assert_eq!(
            outcome["status"],
            if parent_only {
                "native_rebind_confirmed"
            } else {
                "outcome_unresolved"
            }
        );
        let agent = fixture
            .state
            .app
            .services
            .store
            .get_workspace_agent_by_id(&fixture.target.agent_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            agent.revision,
            fixture.target.revision + u64::from(parent_only)
        );
        assert_eq!(
            fixture.provider.native_cwd(&fixture.target.agent_id).await,
            Some(destination.worktree_cwd.clone())
        );
        let persisted = fixture
            .state
            .app
            .services
            .store
            .get_persisted_session(&fixture.target.agent_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.cwd,
            if parent_only {
                Some(destination.worktree_cwd.clone())
            } else {
                original.cwd.clone()
            }
        );
        assert_eq!(
            persisted.worktree_id,
            if parent_only {
                Some(destination.id.clone())
            } else {
                original.worktree_id.clone()
            }
        );
        assert_eq!(
            agent.recreation_policy.authoritative_cwd,
            if parent_only {
                destination.worktree_cwd.clone()
            } else {
                fixture.target.recreation_policy.authoritative_cwd.clone()
            }
        );
        let claim:String=fixture.db().query_row("SELECT claim_role FROM managed_worktree_claims WHERE worktree_id='wt_receipt' AND session_id=?1 AND released_at IS NULL",
            [&fixture.target.agent_id],|row|row.get(0)).unwrap();
        assert_eq!(
            claim,
            if parent_only {
                "owner"
            } else {
                "rebind_reservation"
            }
        );
        fixture
            .db()
            .execute_batch("DROP TRIGGER fail_member_receipt")
            .unwrap();
        assert_eq!(
            fixture.assign("receipt-fault").await["error"]["details"],
            result["error"]["details"]
        );
        assert_eq!(fixture.provider.rebind_calls().await, 1);
    }
}
