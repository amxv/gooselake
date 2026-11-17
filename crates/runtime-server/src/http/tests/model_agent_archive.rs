use super::operator_native_policy::operator_policy_fixture;
use super::workspace_worktree_routes::call;
use super::*;
use serde_json::{json, Value};

struct Fixture {
    router: Router,
    token: String,
    temp: tempfile::TempDir,
    provider: Arc<TestProvider>,
    runtime: Arc<RuntimeSessionManager>,
    workspace: String,
    caller: String,
    targets: Vec<runtime_core::WorkspaceAgentRecord>,
}

impl Fixture {
    async fn new(target_count: usize) -> Self {
        let (router, token, temp, provider, runtime, caller, _) = operator_policy_fixture().await;
        let workspace = runtime
            .get_active_workspace_agent_by_id(&caller)
            .unwrap()
            .workspace_id;
        let mut targets = Vec::new();
        for index in 0..target_count {
            let (status, body) = call(
                &router,
                &token,
                "POST",
                &format!("/v2/workspaces/{workspace}/agents"),
                Some(json!({"provider":"codex","model":"test-model","title":format!("Target {index}")})),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let target: runtime_core::WorkspaceAgentRecord = serde_json::from_value(body).unwrap();
            finish_test_onboarding(&runtime, &workspace, &target.agent_id).await;
            targets.push(target);
        }
        Self {
            router,
            token,
            temp,
            provider,
            runtime,
            workspace,
            caller,
            targets,
        }
    }

    fn store(&self) -> SqliteRuntimeStore {
        SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: self.temp.path().join("runtime.sqlite3"),
        })
    }

    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.temp.path().join("runtime.sqlite3")).unwrap()
    }

    fn plan_as(
        &self,
        caller: &str,
        key: &str,
        targets: &[String],
        policy: runtime_core::WorkspaceMembershipPolicy,
    ) -> (String, Vec<runtime_core::OperatorAgentArchiveCommand>) {
        let request = json!({"tool_name":"gg_team","args":{
            "mode":"remove","remove_agent_ids":targets}});
        let parent_id = format!("op_test_member_{key}");
        let command = runtime_core::AgentToolMutationCommand {
            operation_id: parent_id.clone(),
            workspace_id: self.workspace.clone(),
            caller_agent_id: caller.into(),
            idempotency_key: key.into(),
            tool_name: "gg_team".into(),
            normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
            normalized_request: request,
            admitted_at: 100,
        };
        assert!(matches!(
            self.store().begin_agent_tool_mutation(&command).unwrap(),
            runtime_core::AgentToolMutationAdmission::Execute { .. }
        ));
        let plans = self
            .store()
            .plan_member_agent_archives(&parent_id, caller, policy, 101)
            .unwrap();
        (parent_id, plans)
    }

    async fn remove(&self, key: &str, targets: &[String]) -> Value {
        let scoped = runtime_core::mint_scoped_mcp_token(&self.token, &self.caller);
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/mcp/invoke")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {scoped}"))
                    .header("x-gg-tool-invocation-id", key)
                    .body(Body::from(
                        json!({"tool_name":"gg_team","args":{
                            "mode":"remove","remove_agent_ids":targets
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
async fn model_remove_lost_close_ack_preserves_active_target_and_original_child_fence() {
    let fixture = Fixture::new(1).await;
    let target = &fixture.targets[0];
    let original = fixture.runtime.get_session(&target.agent_id).await.unwrap();
    fixture.provider.set_archive_close_mode(2);
    let before = fixture.provider.archive_close_count();
    let result = fixture
        .remove("model-remove-lost-ack", &[target.agent_id.clone()])
        .await;
    assert_eq!(
        result["ok"], false,
        "unproven native closure became success: {result}"
    );
    assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
    let parent_id = result["error"]["details"]["operation_id"].as_str().unwrap();
    let outcome = &result["error"]["details"]["targets"][0];
    assert_eq!(outcome["agent_id"], target.agent_id);
    assert_eq!(outcome["status"], "outcome_unresolved");
    let child_id = outcome["operation_id"].as_str().unwrap();
    let store = fixture.store();
    let parent = store.get_operation(parent_id).unwrap().unwrap();
    let child = store.get_operation(child_id).unwrap().unwrap();
    assert_eq!(
        parent.operation.actor.kind,
        runtime_core::OperationActorKind::Agent
    );
    assert_eq!(child.operation.actor, parent.operation.actor);
    assert_eq!(child.operation.actor.identifier, fixture.caller);
    assert!(child.operation.idempotency_key.as_ref().unwrap().len() > 128);
    assert_eq!(
        child.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(child.claims.len(), 1);
    assert_eq!(parent.effects[0].phase, "uncertain");
    let current = store
        .get_workspace_agent_by_id(&target.agent_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        current.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Active
    );
    assert_eq!(current.revision, target.revision);
    assert_eq!(
        fixture.runtime.get_session(&target.agent_id).await.unwrap(),
        original
    );
    let replay = fixture
        .remove("model-remove-lost-ack", &[target.agent_id.clone()])
        .await;
    assert_eq!(replay["error"]["details"], result["error"]["details"]);
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
}

#[tokio::test]
async fn model_remove_strong_close_replays_exact_parent_without_another_native_call() {
    let fixture = Fixture::new(1).await;
    let target = &fixture.targets[0];
    let before = fixture.provider.archive_close_count();
    let result = fixture
        .remove("model-remove-proven", &[target.agent_id.clone()])
        .await;
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
    assert_eq!(result["result"]["removed"][0]["native_close"], "confirmed");
    let parent_id = result["result"]["operation_id"].as_str().unwrap();
    let parent = fixture.store().get_operation(parent_id).unwrap().unwrap();
    assert_eq!(
        parent.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(parent.operation.exact_terminal_result, Some(result.clone()));
    assert_eq!(parent.effects[0].phase, "finalized");
    let replay = fixture
        .remove("model-remove-proven", &[target.agent_id.clone()])
        .await;
    assert_eq!(replay, result);
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
    let changed = fixture
        .remove("model-remove-proven", &[fixture.caller.clone()])
        .await;
    assert_eq!(changed["error"]["code"], "conflict");
}

#[tokio::test]
async fn model_remove_partial_batch_stops_at_uncertain_child_and_preserves_every_target() {
    let fixture = Fixture::new(3).await;
    let ids = fixture
        .targets
        .iter()
        .map(|target| target.agent_id.clone())
        .collect::<Vec<_>>();
    let db = fixture.db();
    db.execute_batch("CREATE TABLE test_member_receipt_failure(target_id TEXT PRIMARY KEY);
        CREATE TRIGGER deny_second_member_archive BEFORE UPDATE OF lifecycle_state ON workspace_agents
        WHEN OLD.session_id IN (SELECT target_id FROM test_member_receipt_failure)
        BEGIN SELECT RAISE(ABORT, 'injected second member receipt failure'); END;").unwrap();
    db.execute(
        "INSERT INTO test_member_receipt_failure VALUES(?1)",
        [&ids[1]],
    )
    .unwrap();
    let before = fixture.provider.archive_close_count();
    let result = fixture.remove("partial-member-batch", &ids).await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(result["error"]["code"], "outcome_unresolved");
    assert_eq!(fixture.provider.archive_close_count(), before + 2);
    let outcomes = result["error"]["details"]["targets"].as_array().unwrap();
    assert_eq!(outcomes.len(), 3);
    assert_eq!(outcomes[0]["status"], "native_close_confirmed");
    assert_eq!(outcomes[1]["status"], "outcome_unresolved");
    assert_eq!(outcomes[2]["status"], "not_admitted");
    let store = fixture.store();
    for (index, target) in fixture.targets.iter().enumerate() {
        let current = store
            .get_workspace_agent_by_id(&target.agent_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            current.lifecycle_state,
            if index == 0 {
                runtime_core::WorkspaceAgentLifecycleState::Archived
            } else {
                runtime_core::WorkspaceAgentLifecycleState::Active
            }
        );
        assert_eq!(current.revision, target.revision + u64::from(index == 0));
        let child = store
            .get_operation(outcomes[index]["operation_id"].as_str().unwrap())
            .unwrap();
        if index == 2 {
            assert!(child.is_none());
        } else {
            assert_eq!(child.unwrap().claims.len(), usize::from(index == 1));
        }
    }
    let parent_id = result["error"]["details"]["operation_id"].as_str().unwrap();
    let parent = store.get_operation(parent_id).unwrap().unwrap();
    assert_eq!(
        parent.operation.phase,
        runtime_core::OperationPhase::Requested
    );
    assert!(parent.operation.exact_terminal_result.is_none());
    let replay = fixture.remove("partial-member-batch", &ids).await;
    assert_eq!(replay["error"]["details"], result["error"]["details"]);
    assert_eq!(fixture.provider.archive_close_count(), before + 2);
}

#[tokio::test]
async fn model_remove_parent_receipt_failure_never_repeats_confirmed_native_close() {
    let fixture = Fixture::new(1).await;
    fixture
        .db()
        .execute_batch(
            "CREATE TRIGGER deny_model_parent_receipt
        BEFORE UPDATE OF exact_terminal_result_json ON runtime_operations
        WHEN OLD.kind='agent_model_tool_mutation'
        BEGIN SELECT RAISE(ABORT, 'injected model parent receipt failure'); END;",
        )
        .unwrap();
    let target = &fixture.targets[0];
    let before = fixture.provider.archive_close_count();
    let result = fixture
        .remove("parent-receipt-failure", &[target.agent_id.clone()])
        .await;
    assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
    assert_eq!(
        result["error"]["details"]["targets"][0]["status"],
        "native_close_confirmed"
    );
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
    assert_eq!(
        fixture
            .runtime
            .get_session(&target.agent_id)
            .await
            .unwrap()
            .status,
        "closed"
    );
    let replay = fixture
        .remove("parent-receipt-failure", &[target.agent_id.clone()])
        .await;
    assert_eq!(replay, result);
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
}

#[tokio::test]
async fn model_remove_revoked_caller_cannot_admit_a_frozen_target() {
    let fixture = Fixture::new(1).await;
    let target = &fixture.targets[0];
    let (parent, plans) = fixture.plan_as(
        &fixture.caller,
        "revoked-caller",
        &[target.agent_id.clone()],
        Default::default(),
    );
    fixture
        .db()
        .execute(
            "UPDATE workspace_agents SET lifecycle_state='archived',archived_at=1000 WHERE session_id=?1",
            [&fixture.caller],
        )
        .unwrap();
    let before = fixture.provider.archive_close_count();
    let error = fixture
        .runtime
        .execute_member_agent_archive(plans[0].clone())
        .await
        .unwrap_err();
    assert!(
        matches!(error, runtime_core::RuntimeError::InvalidState(_)),
        "{error}"
    );
    assert_eq!(fixture.provider.archive_close_count(), before);
    assert!(fixture
        .store()
        .get_operation(&plans[0].operation_id)
        .unwrap()
        .is_none());
    let parent = fixture.store().get_operation(&parent).unwrap().unwrap();
    assert_eq!(parent.effects[0].phase, "intended");
    assert_eq!(
        parent.effects[0].evidence.as_ref().unwrap()["outcome"]["status"],
        "not_admitted"
    );
}

#[tokio::test]
async fn model_remove_frozen_snapshot_rejects_substitution_and_target_revision_race() {
    let fixture = Fixture::new(1).await;
    let target = &fixture.targets[0];
    let (_, plans) = fixture.plan_as(
        &fixture.caller,
        "frozen-target",
        &[target.agent_id.clone()],
        Default::default(),
    );
    let store = fixture.store();
    let mut substituted = plans[0].clone();
    substituted.provider_session_ref = "another-native-attachment".into();
    assert!(matches!(
        store.admit_member_agent_archive(&substituted),
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    substituted = plans[0].clone();
    substituted.actor = runtime_core::OperationActor::operator("runtime_operator");
    assert!(store.admit_member_agent_archive(&substituted).is_err());
    assert!(store.admit_operator_agent_archive(&substituted).is_err());
    fixture
        .db()
        .execute(
            "UPDATE workspace_agents SET revision=revision+1 WHERE session_id=?1",
            [&target.agent_id],
        )
        .unwrap();
    let before = fixture.provider.archive_close_count();
    assert!(matches!(
        fixture
            .runtime
            .execute_member_agent_archive(plans[0].clone())
            .await,
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    assert_eq!(fixture.provider.archive_close_count(), before);
    assert!(store
        .get_operation(&plans[0].operation_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn model_remove_two_callers_cannot_share_a_child_or_borrow_its_native_outcome() {
    let fixture = Fixture::new(2).await;
    let target = &fixture.targets[0];
    let other_caller = &fixture.targets[1].agent_id;
    let policy = runtime_core::WorkspaceMembershipPolicy {
        non_lead_can_remove_members: true,
        ..Default::default()
    };
    let (_, first) = fixture.plan_as(
        &fixture.caller,
        "first-caller",
        &[target.agent_id.clone()],
        policy,
    );
    let (other_parent, second) = fixture.plan_as(
        other_caller,
        "second-caller",
        &[target.agent_id.clone()],
        policy,
    );
    assert_ne!(first[0].operation_id, second[0].operation_id);
    assert_ne!(first[0].idempotency_key, second[0].idempotency_key);
    fixture.provider.set_archive_close_mode(2);
    let before = fixture.provider.archive_close_count();
    assert!(fixture
        .runtime
        .execute_member_agent_archive(first[0].clone())
        .await
        .is_err());
    assert!(fixture
        .runtime
        .execute_member_agent_archive(second[0].clone())
        .await
        .is_err());
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
    let store = fixture.store();
    let first_child = store
        .get_operation(&first[0].operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(first_child.claims.len(), 1);
    assert!(store
        .get_operation(&second[0].operation_id)
        .unwrap()
        .is_none());
    let parent = store.get_operation(&other_parent).unwrap().unwrap();
    assert_eq!(
        parent.effects[0].evidence.as_ref().unwrap()["outcome"]["status"],
        "not_admitted"
    );
}

#[tokio::test]
async fn model_remove_startup_quarantines_admitted_child_without_native_replay() {
    let fixture = Fixture::new(1).await;
    let target = &fixture.targets[0];
    let (parent, plans) = fixture.plan_as(
        &fixture.caller,
        "admitted-before-crash",
        &[target.agent_id.clone()],
        Default::default(),
    );
    let store = fixture.store();
    assert!(matches!(
        store.admit_member_agent_archive(&plans[0]).unwrap(),
        runtime_core::OperatorAgentArchiveAdmission::Execute { .. }
    ));
    let before = fixture.provider.archive_close_count();
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(fixture.provider.clone()).unwrap();
    let reopened = Arc::new(
        RuntimeSessionManager::new(Arc::new(fixture.store()), Arc::new(providers), 128).unwrap(),
    );
    let before_resume = fixture.provider.restore_call_count().await;
    let recovery = reopened.recover_startup().await.unwrap();
    assert!(
        recovery
            .notes
            .iter()
            .any(|note| note.contains(&target.agent_id)
                && note.contains("native provider resume withheld")),
        "{recovery:?}"
    );
    // Only the unrelated caller can resume; the admitted target stays fenced.
    assert_eq!(
        fixture.provider.restore_call_count().await,
        before_resume + 1
    );
    assert!(reopened
        .execute_member_agent_archive(plans[0].clone())
        .await
        .is_err());
    assert_eq!(fixture.provider.archive_close_count(), before);
    let child = store
        .get_operation(&plans[0].operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        child.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(child.claims.len(), 1);
    assert_eq!(
        store.get_operation(&parent).unwrap().unwrap().effects[0].phase,
        "uncertain"
    );
    assert_eq!(
        store
            .get_workspace_agent_by_id(&target.agent_id)
            .unwrap()
            .unwrap()
            .revision,
        target.revision
    );
}

fn review_input(original: &runtime_core::SessionRecord, revision: u64, resolution: &str) -> Value {
    let mut body = json!({"resolution":resolution,
        "evidence":"Fixture independently verified exact original provider attachment and close outcome",
        "provider":original.provider,"original_provider_session_ref":original.provider_session_ref,
        "original_canonical_provider_session_ref":original.canonical_provider_session_ref,
        "expected_revision":revision,"session_updated_at":original.updated_at});
    if resolution == "not_closed" {
        body["observed_provider_session_ref"] = json!(original.provider_session_ref);
        body["observed_canonical_provider_session_ref"] =
            json!(original.canonical_provider_session_ref);
    }
    body
}

#[tokio::test]
async fn model_remove_operator_evidence_settles_original_agent_after_caller_revocation() {
    for (mode, resolution, outcome) in [
        (2, "closed", "native_close_confirmed"),
        (4, "not_closed", "verified_not_closed"),
    ] {
        let fixture = Fixture::new(1).await;
        let target = &fixture.targets[0];
        let original = fixture.runtime.get_session(&target.agent_id).await.unwrap();
        fixture.provider.set_archive_close_mode(mode);
        let before = fixture.provider.archive_close_count();
        let result = fixture
            .remove("review-original-agent", &[target.agent_id.clone()])
            .await;
        assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
        let parent_id = result["error"]["details"]["operation_id"].as_str().unwrap();
        let child_id = result["error"]["details"]["targets"][0]["operation_id"]
            .as_str()
            .unwrap();
        fixture
            .db()
            .execute(
                "UPDATE workspace_agents SET lifecycle_state='archived',archived_at=1000 WHERE session_id=?1",
                [&fixture.caller],
            )
            .unwrap();
        let uri = format!(
            "/v2/workspaces/{}/agents/{}/archives/{child_id}/resolve",
            fixture.workspace, target.agent_id
        );
        let input = review_input(&original, target.revision, resolution);
        let (status, review) = call(
            &fixture.router,
            &fixture.token,
            "POST",
            &uri,
            Some(input.clone()),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{review}");
        let (status, repeated) = call(
            &fixture.router,
            &fixture.token,
            "POST",
            &uri,
            Some(input),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{repeated}");
        assert_eq!(repeated, review);
        assert_eq!(fixture.provider.archive_close_count(), before + 1);
        let store = fixture.store();
        let child = store.get_operation(child_id).unwrap().unwrap();
        assert_eq!(
            child.operation.actor.kind,
            runtime_core::OperationActorKind::Agent
        );
        assert_eq!(child.operation.actor.identifier, fixture.caller);
        assert!(child.claims.is_empty());
        assert_eq!(
            child.effects[0].evidence.as_ref().unwrap()["operator_review"]["actor"]["kind"],
            "operator"
        );
        let parent = store.get_operation(parent_id).unwrap().unwrap();
        assert_eq!(
            parent.operation.phase,
            runtime_core::OperationPhase::Requested
        );
        assert_eq!(
            parent.effects[0].evidence.as_ref().unwrap()["outcome"]["status"],
            outcome
        );
        assert_eq!(
            fixture
                .runtime
                .get_session(&target.agent_id)
                .await
                .unwrap()
                .status,
            if resolution == "closed" {
                "closed"
            } else {
                "archive_recovery_required"
            }
        );
    }
}

#[tokio::test]
async fn model_remove_unproven_aggregate_and_canonical_duplicate_never_claim_success() {
    let fixture = Fixture::new(1).await;
    let target = &fixture.targets[0];
    let (parent, _) = fixture.plan_as(
        &fixture.caller,
        "unproven-aggregate",
        &[target.agent_id.clone()],
        Default::default(),
    );
    assert!(fixture.store().finish_agent_tool_mutation(&parent,
        &json!({"ok":true,"result":{"operation":"remove","removed":[{"agent_id":target.agent_id}]}}), 200).is_err());
    assert_eq!(
        fixture
            .store()
            .get_operation(&parent)
            .unwrap()
            .unwrap()
            .operation
            .phase,
        runtime_core::OperationPhase::Requested
    );
    let before = fixture.provider.archive_close_count();
    let result = fixture
        .remove(
            "canonical-duplicate",
            &[target.agent_id.clone(), format!(" {} ", target.agent_id)],
        )
        .await;
    assert_eq!(result["ok"], false, "{result}");
    assert_eq!(fixture.provider.archive_close_count(), before);
    let count: i64 = fixture
        .db()
        .query_row(
            "SELECT COUNT(*) FROM runtime_operations
        WHERE idempotency_key='canonical-duplicate'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn model_remove_lead_change_after_plan_rejects_child_before_native_dispatch() {
    let fixture = Fixture::new(2).await;
    let target = &fixture.targets[0];
    let (_, plans) = fixture.plan_as(
        &fixture.caller,
        "lead-changed-after-plan",
        &[target.agent_id.clone()],
        Default::default(),
    );
    fixture
        .db()
        .execute(
            "UPDATE workspaces SET lead_agent_id=?1,revision=revision+1 WHERE workspace_id=?2",
            rusqlite::params![fixture.targets[1].agent_id, fixture.workspace],
        )
        .unwrap();
    let before = fixture.provider.archive_close_count();
    let error = fixture
        .runtime
        .execute_member_agent_archive(plans[0].clone())
        .await
        .unwrap_err();
    assert!(
        matches!(error, runtime_core::RuntimeError::InvalidState(_)),
        "{error}"
    );
    assert_eq!(fixture.provider.archive_close_count(), before);
    assert!(fixture
        .store()
        .get_operation(&plans[0].operation_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn model_remove_admitted_native_receipt_survives_later_caller_revocation() {
    let fixture = Fixture::new(1).await;
    let target = &fixture.targets[0];
    let (parent, plans) = fixture.plan_as(
        &fixture.caller,
        "receipt-after-revocation",
        &[target.agent_id.clone()],
        Default::default(),
    );
    let command = &plans[0];
    let store = fixture.store();
    assert!(matches!(
        store.admit_member_agent_archive(command).unwrap(),
        runtime_core::OperatorAgentArchiveAdmission::Execute { .. }
    ));
    let before = fixture.provider.archive_close_count();
    let proof = fixture
        .provider
        .close_session_for_retirement(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: target.agent_id.clone(),
            reason: Some("synthetic admitted close".into()),
        })
        .await
        .unwrap();
    fixture.db().execute("UPDATE workspace_agents SET lifecycle_state='archived',archived_at=1000 WHERE session_id=?1",
        [&fixture.caller]).unwrap();
    let mut closed = fixture.runtime.get_session(&target.agent_id).await.unwrap();
    let at = closed.updated_at + 1;
    closed.status = "closed".into();
    closed.updated_at = at;
    closed.closed_at = Some(at);
    closed.failure_code = None;
    closed.failure_message = None;
    let receipt = store
        .complete_operator_agent_archive(command, &closed, &proof, at)
        .unwrap();
    assert_eq!(
        receipt.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );
    assert_eq!(receipt.revision, target.revision + 1);
    assert_eq!(fixture.provider.archive_close_count(), before + 1);
    let child = store.get_operation(&command.operation_id).unwrap().unwrap();
    assert_eq!(child.operation.actor.identifier, fixture.caller);
    assert!(child.claims.is_empty());
    assert_eq!(
        store.get_operation(&parent).unwrap().unwrap().effects[0].phase,
        "finalized"
    );
}
