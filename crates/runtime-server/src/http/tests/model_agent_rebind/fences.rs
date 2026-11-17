use super::*;

fn policy_command(
    command: &runtime_core::MemberAgentRebindCommand,
) -> runtime_core::OperatorNativePolicyCommand {
    let session = &command.original_session;
    let request = json!({"agent_id":session.id,"kind":"preferences",
        "expected_revision":command.original_agent.revision,
        "current_preferences":{"thinking_effort":"high"}});
    runtime_core::OperatorNativePolicyCommand {
        operation_id: format!("policy_before_{}", command.operation.operation_id),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: format!("policy:{}", command.operation.operation_id),
        workspace_id: command.operation.workspace_id.clone(),
        agent_id: session.id.clone(),
        kind: runtime_core::OperatorNativePolicyKind::Preferences,
        expected_revision: command.original_agent.revision,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: command.operation.created_at + 1,
    }
}

#[tokio::test]
async fn model_assign_independent_native_policy_and_rebind_admissions_fence_both_orders() {
    for policy_first in [true, false] {
        let fixture = Fixture::new().await;
        let command = fixture.plan(if policy_first {
            "policy-first"
        } else {
            "rebind-first"
        });
        let policy = policy_command(&command);
        let independent = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: fixture.temp.path().join("runtime.sqlite3"),
        });
        if policy_first {
            assert!(matches!(
                independent.admit_operator_native_policy(&policy).unwrap(),
                runtime_core::OperatorNativePolicyAdmission::Execute { .. }
            ));
            let result = fixture
                .state
                .app
                .services
                .store
                .admit_member_agent_rebind(&command);
            assert!(
                matches!(
                    result,
                    Err(runtime_core::RuntimeError::OperationUnresolved { .. })
                ),
                "rebind must observe independently committed native policy claim: {result:?}"
            );
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
        } else {
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
            assert!(independent.admit_operator_native_policy(&policy).is_err());
            assert!(independent
                .get_operation(&policy.operation_id)
                .unwrap()
                .is_none());
        }
        assert_eq!(fixture.provider.rebind_calls().await, 0);
        assert_eq!(fixture.provider.preferences_mutation_calls(), 0);
    }
}

#[tokio::test]
async fn model_assign_two_callers_with_same_host_key_cannot_share_definite_rejection_or_success() {
    let fixture = Fixture::new().await;
    let original = fixture
        .state
        .runtime
        .get_session(&fixture.target.agent_id)
        .await
        .unwrap();
    fixture.provider.set_rebind_mode(Some("unsupported")).await;
    let first = fixture.assign("same-host-key").await;
    assert_eq!(first["ok"], false, "{first}");
    let first_outcome = &first["error"]["details"]["targets"][0];
    assert_eq!(first_outcome["status"], "verified_not_rebound");
    let first_child = fixture
        .state
        .app
        .services
        .store
        .get_operation(first_outcome["operation_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        first_child.operation.phase,
        runtime_core::OperationPhase::Failed
    );
    assert_eq!(
        first_child.operation.actor.identifier,
        fixture.caller.agent_id
    );
    assert!(first_child.claims.is_empty());
    let reserved:i64=fixture.db().query_row("SELECT COUNT(*) FROM managed_worktree_claims WHERE worktree_id='wt_receipt' AND session_id=?1 AND released_at IS NULL",
        [&fixture.target.agent_id],|row|row.get(0)).unwrap();
    assert_eq!(reserved, 0);
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
        fixture.provider.native_cwd(&fixture.target.agent_id).await,
        original.cwd
    );
    assert_eq!(fixture.provider.rebind_calls().await, 1);

    // Both credentials are real scoped callers. Changing the lead authorizes
    // the second caller; it cannot borrow the first caller's failed child.
    fixture
        .db()
        .execute(
            "UPDATE workspaces SET lead_agent_id=?2 WHERE workspace_id=?1",
            [&fixture.workspace.workspace_id, &fixture.target.agent_id],
        )
        .unwrap();
    fixture.provider.set_rebind_mode(None).await;
    let second = fixture
        .assign_as(&fixture.target.agent_id, "same-host-key")
        .await;
    assert_eq!(second["ok"], true, "{second}");
    let second_id = second["result"]["rebind"]["operation"]["operation_id"]
        .as_str()
        .unwrap();
    assert_ne!(Some(second_id), first_outcome["operation_id"].as_str());
    let second_child = fixture
        .state
        .app
        .services
        .store
        .get_operation(second_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        second_child.operation.actor.kind,
        runtime_core::OperationActorKind::Agent
    );
    assert_eq!(
        second_child.operation.actor.identifier,
        fixture.target.agent_id
    );
    assert_ne!(
        second_child.operation.idempotency_key,
        first_child.operation.idempotency_key
    );
    let parents:i64=fixture.db().query_row("SELECT COUNT(*) FROM runtime_operations WHERE kind='agent_model_tool_mutation' AND idempotency_key='same-host-key'",[],|row|row.get(0)).unwrap();
    assert_eq!(parents, 2);
    assert_eq!(
        fixture.assign("same-host-key").await["error"]["details"],
        first["error"]["details"]
    );
    assert_eq!(
        fixture
            .assign_as(&fixture.target.agent_id, "same-host-key")
            .await,
        second
    );
    assert_eq!(fixture.provider.rebind_calls().await, 2);
}

#[tokio::test]
async fn model_assign_failed_definite_rejection_receipt_retains_both_claims() {
    let fixture = Fixture::new().await;
    let original = fixture
        .state
        .runtime
        .get_session(&fixture.target.agent_id)
        .await
        .unwrap();
    fixture.provider.set_rebind_mode(Some("unsupported")).await;
    fixture.db().execute_batch("CREATE TRIGGER fail_rebind_rejection BEFORE INSERT ON runtime_events
        WHEN NEW.kind='model_tool.rebind_target_updated' AND json_extract(NEW.payload_json,'$.status')='verified_not_rebound'
        BEGIN SELECT RAISE(ABORT,'lost definite rejection receipt'); END;").unwrap();
    let result = fixture.assign("rejection-receipt-fault").await;
    assert_eq!(result["error"]["code"], "outcome_unresolved", "{result}");
    let outcome = &result["error"]["details"]["targets"][0];
    assert_eq!(outcome["status"], "native_admitted");
    let child = fixture
        .state
        .app
        .services
        .store
        .get_operation(outcome["operation_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        child.operation.phase,
        runtime_core::OperationPhase::Requested
    );
    assert_eq!(child.claims.len(), 1);
    let reserved:i64=fixture.db().query_row("SELECT COUNT(*) FROM managed_worktree_claims WHERE worktree_id='wt_receipt' AND session_id=?1 AND claim_role='rebind_reservation' AND released_at IS NULL",
        [&fixture.target.agent_id],|row|row.get(0)).unwrap();
    assert_eq!(reserved, 1);
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
        fixture.provider.native_cwd(&fixture.target.agent_id).await,
        original.cwd
    );
    fixture
        .db()
        .execute_batch("DROP TRIGGER fail_rebind_rejection")
        .unwrap();
    assert_eq!(
        fixture.assign("rejection-receipt-fault").await["error"]["details"],
        result["error"]["details"]
    );
    assert_eq!(fixture.provider.rebind_calls().await, 1);
}

async fn reopened(fixture: &Fixture) -> Arc<RuntimeSessionManager> {
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(fixture.provider.clone()).unwrap();
    Arc::new(
        RuntimeSessionManager::new(
            Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
                database_path: fixture.temp.path().join("runtime.sqlite3"),
            })),
            Arc::new(providers),
            128,
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn model_assign_startup_critical_event_failure_rolls_back_before_any_native_resume() {
    let fixture = Fixture::new().await;
    let command = fixture.plan("startup-receipt-fault");
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
    fixture.db().execute_batch("CREATE TRIGGER fail_startup_rebind BEFORE INSERT ON runtime_events
        WHEN NEW.kind='model_tool.rebind_target_updated' AND json_extract(NEW.payload_json,'$.status')='outcome_unresolved'
        BEGIN SELECT RAISE(ABORT,'lost startup quarantine receipt'); END;").unwrap();
    let runtime = reopened(&fixture).await;
    assert!(runtime.recover_startup().await.is_err());
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
        runtime_core::OperationPhase::Requested
    );
    assert_eq!(child.claims.len(), 1);
    let parent = fixture
        .state
        .app
        .services
        .store
        .get_operation(&command.parent_operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(parent.effects[0].phase, "started");
    assert_eq!(
        parent.effects[0].evidence.as_ref().unwrap()["outcome"]["status"],
        "native_admitted"
    );
    assert!(fixture.provider.resumed_requests().await.is_empty());
    fixture
        .db()
        .execute_batch("DROP TRIGGER fail_startup_rebind")
        .unwrap();
    runtime.recover_startup().await.unwrap();
    assert_eq!(
        fixture
            .state
            .app
            .services
            .store
            .get_operation(&command.operation.operation_id)
            .unwrap()
            .unwrap()
            .operation
            .phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert!(fixture
        .provider
        .resumed_requests()
        .await
        .into_iter()
        .all(|request| request.runtime_session_id != fixture.target.agent_id));
    assert_eq!(fixture.provider.rebind_calls().await, 0);
}

#[tokio::test]
async fn model_assign_shared_operator_startup_guard_preserves_original_unreceipted_rebind() {
    let fixture = Fixture::new().await;
    let command = fixture.plan("operator-startup-template");
    let mut operation = command.operation.clone();
    operation.operation_id = "op_operator_unreceipted_rebind".into();
    operation.idempotency_key = Some("operator-unreceipted-rebind".into());
    fixture
        .state
        .app
        .services
        .store
        .begin_workspace_agent_rebind(&operation)
        .unwrap();
    let runtime = reopened(&fixture).await;
    runtime.recover_startup().await.unwrap();
    let child = fixture
        .state
        .app
        .services
        .store
        .get_operation(&operation.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        child.operation.actor.kind,
        runtime_core::OperationActorKind::Operator
    );
    assert_eq!(
        child.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(child.claims.len(), 1);
    assert!(fixture
        .provider
        .resumed_requests()
        .await
        .into_iter()
        .all(|request| request.runtime_session_id != fixture.target.agent_id));
    assert_eq!(fixture.provider.rebind_calls().await, 0);
}

#[tokio::test]
async fn model_assign_running_caller_can_move_idle_peer_but_not_its_own_busy_target() {
    let fixture = Fixture::new().await;
    let (status, started) = call(
        &build_router(fixture.state.clone()),
        &fixture.state.bearer_token,
        "POST",
        &format!("/v2/agents/{}/turns", fixture.caller.agent_id),
        Some(json!({"input":[{"type":"text","text":"model_assign_wait_for_interrupt"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let original = fixture
        .state
        .runtime
        .get_session(&fixture.caller.agent_id)
        .await
        .unwrap();
    assert_eq!(original.status, "turn_running");
    assert_eq!(
        original.active_turn_id.as_deref(),
        started["turn_id"].as_str()
    );
    let denied = fixture
        .assign_as_to(
            &fixture.caller.agent_id,
            &fixture.caller.agent_id,
            "running-self-assign",
        )
        .await;
    assert_eq!(denied["ok"], false, "{denied}");
    assert_eq!(fixture.provider.rebind_calls().await, 0);
    let moved = fixture.assign("running-peer-assign").await;
    assert_eq!(moved["ok"], true, "{moved}");
    assert_eq!(fixture.provider.rebind_calls().await, 1);
    assert_eq!(
        fixture
            .state
            .runtime
            .get_session(&fixture.caller.agent_id)
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        fixture.provider.native_cwd(&fixture.caller.agent_id).await,
        original.cwd
    );
    assert_eq!(
        fixture
            .state
            .app
            .services
            .store
            .get_operation(
                moved["result"]["rebind"]["operation"]["operation_id"]
                    .as_str()
                    .unwrap()
            )
            .unwrap()
            .unwrap()
            .operation
            .actor
            .identifier,
        fixture.caller.agent_id
    );
}
