use super::*;
use runtime_core::{
    prepare_workspace_retirement, ApprovalResponseInput, ProviderPermissionIntent,
    ProviderRetirementCloseEvidence, ProviderRetirementCloseProofKind, TurnDispatchState,
    WorkspaceRetirementAdmission, WorkspaceRetirementRequest, WorkspaceRetirementTurnStep,
    PROCESS_COMPLETION_PENDING,
};

fn completion_admissions(
    store: &SqliteRuntimeStore,
    correlation: &str,
) -> Vec<runtime_core::TurnAdmissionRecord> {
    store
        .list_turn_admissions()
        .unwrap()
        .into_iter()
        .filter(|admission| admission.correlation.correlation_id.as_deref() == Some(correlation))
        .collect()
}

#[tokio::test]
async fn completion_waiting_for_approval_survives_retirement_until_explicit_restore_and_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let workspace = register_workspace(&store, &root, "Completion retirement");
    let (runtime, _comms) = build_runtime_and_team_comms(store.clone()).await;
    let mut request = workspace_agent_request(&root, "Completion owner");
    request.permission_intent = ProviderPermissionIntent::Explicit {
        mode: "require_approval".into(),
    };
    let agent = runtime
        .create_workspace_agent(&workspace.workspace_id, request, "operator")
        .await
        .unwrap();
    // Complete the mandatory onboarding prerequisite through its real worker
    // and approval path before testing a separate process completion input.
    runtime.dispatch_workspace_system_notices().await.unwrap();
    let onboarding = runtime
        .get_workspace_onboarding_notice(&workspace.workspace_id, &agent.agent_id)
        .unwrap()
        .unwrap();
    let onboarding_turn = runtime
        .latest_turn_admission_for_correlation(&agent.agent_id, &onboarding.correlation_id)
        .await
        .unwrap();
    let onboarding_approval = store
        .hydrate_runtime_state()
        .unwrap()
        .approvals
        .into_iter()
        .find(|approval| approval.turn_id == onboarding_turn.turn_id)
        .unwrap();
    runtime
        .respond_approval(
            &agent.agent_id,
            &onboarding_approval.id,
            ApprovalResponseInput {
                decision: "accept".into(),
                payload: None,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime
            .get_session(&agent.agent_id)
            .await
            .unwrap()
            .active_turn_id
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    runtime.dispatch_workspace_system_notices().await.unwrap();
    assert_eq!(
        runtime
            .get_workspace_onboarding_notice(&workspace.workspace_id, &agent.agent_id)
            .unwrap()
            .unwrap()
            .state,
        "delivered"
    );
    let manager = RuntimeProcessManager::new_with_runtime(
        store.clone(),
        runtime.clone(),
        process_config(&temp, 1, 1024),
    )
    .await
    .unwrap();
    let run = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some(agent.agent_id.clone()),
            tool_call_id: None,
            command: "printf retained-completion".into(),
            cwd: Some(root.display().to_string()),
            timeout_ms: None,
        })
        .await
        .unwrap();
    let terminal = wait_for_terminal(&manager, &run.process.process_id).await;
    let terminal_record = store
        .get_managed_process(&terminal.process.process_id)
        .unwrap()
        .unwrap();
    let correlation = format!("process_completion:{}", run.process.process_id);
    let waiting = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(admission) = runtime
                .latest_turn_admission_for_correlation(&agent.agent_id, &correlation)
                .await
            {
                break admission;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "completion worker did not admit its approval-gated input: {:?}",
            store.get_managed_process(&run.process.process_id).unwrap()
        )
    });
    let pending = store
        .get_managed_process(&run.process.process_id)
        .unwrap()
        .unwrap();
    assert_eq!(pending.completion_state, PROCESS_COMPLETION_INJECTING);
    assert_eq!(pending.completion_turn_id, None);
    assert_eq!(waiting.dispatch_state, TurnDispatchState::Pending);
    assert!(
        store
            .update_managed_process_completion(
                &run.process.process_id,
                &ProcessCompletionUpdate {
                    state: PROCESS_COMPLETION_DELIVERED.into(),
                    turn_id: Some(waiting.turn_id.clone()),
                    attempt_count: pending.completion_attempt_count,
                    last_error: None,
                    updated_at: crate::now_ms(),
                }
            )
            .unwrap()
            .is_none(),
        "a caller cannot mark an undispatched admission delivered"
    );

    let current = store
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = prepare_workspace_retirement(
        &workspace.workspace_id,
        WorkspaceRetirementRequest {
            expected_revision: current.revision,
        },
        OperationActor::operator("completion-retirement"),
        Some("completion-retirement".into()),
    )
    .unwrap();
    let plan = match store.admit_workspace_retirement(&command).unwrap() {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected admission {other:?}"),
    };
    assert!(plan.managed_process_ids.contains(&run.process.process_id));
    store
        .settle_passive_workspace_retirement(&command.operation_id, crate::now_ms())
        .unwrap();
    assert_eq!(
        store
            .progress_workspace_retirement_turn(
                &command.operation_id,
                &waiting.turn_id,
                crate::now_ms()
            )
            .unwrap(),
        WorkspaceRetirementTurnStep::Finalized
    );
    let attempt = store
        .claim_workspace_retirement_native_close(
            &command.operation_id,
            &agent.agent_id,
            crate::now_ms(),
        )
        .unwrap();
    store
        .finalize_workspace_retirement_native_close(
            &attempt,
            &ProviderRetirementCloseEvidence {
                runtime_session_id: agent.agent_id.clone(),
                provider_session_ref: agent.provider_session_ref.clone().unwrap(),
                proof_kind: ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
            },
            crate::now_ms(),
        )
        .unwrap();
    store
        .finalize_settled_workspace_retirement(&command.operation_id, crate::now_ms())
        .unwrap()
        .unwrap();
    let deferred = store
        .get_managed_process(&run.process.process_id)
        .unwrap()
        .unwrap();
    assert_eq!(deferred.completion_state, PROCESS_COMPLETION_PENDING);
    assert_eq!(
        deferred.completion_last_error.as_deref(),
        Some("workspace_retirement_deferred_completion")
    );

    let reactivated = register_workspace(&store, &root, "Completion retirement");
    assert_eq!(reactivated.workspace_id, workspace.workspace_id);
    let (restored_runtime, _restored_comms) = build_runtime_and_team_comms(store.clone()).await;
    manager
        .try_deliver_completion(&restored_runtime, &deferred)
        .await
        .unwrap();
    assert_eq!(
        completion_admissions(&store, &correlation).len(),
        1,
        "workspace reactivation must not reopen an archived member for completion input"
    );
    restored_runtime
        .restore_workspace_agent(&workspace.workspace_id, &agent.agent_id)
        .await
        .unwrap();
    manager
        .try_deliver_completion(&restored_runtime, &deferred)
        .await
        .unwrap();
    let restored_waiting = restored_runtime
        .latest_turn_admission_for_correlation(&agent.agent_id, &correlation)
        .await
        .unwrap();
    assert_ne!(restored_waiting.turn_id, waiting.turn_id);
    assert_eq!(restored_waiting.dispatch_state, TurnDispatchState::Pending);
    let approval = store
        .hydrate_runtime_state()
        .unwrap()
        .approvals
        .into_iter()
        .find(|approval| approval.turn_id == restored_waiting.turn_id)
        .unwrap();
    restored_runtime
        .respond_approval(
            &agent.agent_id,
            &approval.id,
            ApprovalResponseInput {
                decision: "accept".into(),
                payload: None,
            },
        )
        .await
        .unwrap();
    let claimed = store
        .get_managed_process(&run.process.process_id)
        .unwrap()
        .unwrap();
    manager
        .try_deliver_completion(&restored_runtime, &claimed)
        .await
        .unwrap();
    manager
        .try_deliver_completion(&restored_runtime, &claimed)
        .await
        .unwrap();
    let delivered = store
        .get_managed_process(&run.process.process_id)
        .unwrap()
        .unwrap();
    assert_eq!(delivered.completion_state, PROCESS_COMPLETION_DELIVERED);
    assert_eq!(
        delivered.completion_turn_id.as_deref(),
        Some(restored_waiting.turn_id.as_str())
    );
    let admissions = completion_admissions(&store, &correlation);
    assert_eq!(admissions.len(), 2);
    assert_eq!(
        admissions
            .iter()
            .filter(|admission| admission.dispatch_state == TurnDispatchState::Dispatched)
            .count(),
        1
    );
    assert_eq!(
        admissions
            .iter()
            .filter(|admission| admission.dispatch_state == TurnDispatchState::NotDispatched)
            .count(),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while restored_runtime
            .get_session(&agent.agent_id)
            .await
            .unwrap()
            .active_turn_id
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Resume the send step from a worker whose claim predates retirement.
    // The recipient is restored and idle, so only durable correlation authority
    // can prevent a second completion input after the new worker succeeded.
    let stale_error = restored_runtime
        .send_turn(
            &agent.agent_id,
            runtime_core::SendTurnInput {
                input: crate::process_helpers::build_completion_input(&terminal_record),
                expected_turn_id: None,
                permission_mode: None,
                projection_source: Some(runtime_core::TurnInputProjectionSource::AutomationContext),
                user_input_snapshot: None,
                correlation_id: Some(correlation.clone()),
                turn_settings: Default::default(),
            },
        )
        .await
        .unwrap_err();
    assert!(stale_error.to_string().contains("completion already"));
    assert_eq!(completion_admissions(&store, &correlation).len(), 2);
}
