use super::*;
use runtime_core::{
    OperationActor, OperatorNativePolicyCommand, OperatorNativePolicyKind,
    OperatorNativePolicyReviewCommand, OperatorNativePolicyReviewInput,
    OperatorNativePolicyReviewResolution, RuntimeError, WorkspaceAgentLifecycleState,
};

#[test]
fn direct_lifecycle_writes_cannot_bypass_native_policy_intent_or_review_reattachment() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = super::workspace_agent::registered_workspace(&repository, &root);
    let (mut session, agent) = super::workspace_agent::workspace_agent_fixture(
        &workspace,
        "sess_native_lifecycle_fence",
        "quiet-heron-native-fence",
    );
    repository.create_workspace_agent(&session, &agent).unwrap();

    let actor = OperationActor::operator("runtime_operator");
    let request = serde_json::json!({
        "agent_id":agent.agent_id, "kind":"preferences", "expected_revision":0,
        "current_preferences":{"thinking_effort":"high"},
    });
    let command = OperatorNativePolicyCommand {
        operation_id: "op_policy_lifecycle_fence".into(),
        actor: actor.clone(),
        idempotency_key: "lifecycle-fence".into(),
        workspace_id: workspace.workspace_id.clone(),
        agent_id: agent.agent_id.clone(),
        kind: OperatorNativePolicyKind::Preferences,
        expected_revision: 0,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: 200,
    };
    repository.admit_operator_native_policy(&command).unwrap();

    session.status = "closed".into();
    session.closed_at = Some(201);
    session.updated_at = 201;
    let try_archive = || {
        repository.set_workspace_agent_lifecycle(
            &session,
            &agent.agent_id,
            WorkspaceAgentLifecycleState::Archived,
            Some("archive during native mutation"),
            201,
        )
    };
    for _ in 0..2 {
        assert!(matches!(
            try_archive(),
            Err(RuntimeError::OperationUnresolved { operation_id, .. })
                if operation_id == command.operation_id
        ));
        repository
            .mark_operator_native_policy_uncertain(
                &command.operation_id,
                "native outcome unknown",
                202,
            )
            .unwrap();
    }
    let unchanged = repository
        .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        unchanged.lifecycle_state,
        WorkspaceAgentLifecycleState::Active
    );
    assert_eq!(unchanged.revision, 0);

    repository
        .resolve_operator_native_policy_review(&OperatorNativePolicyReviewCommand {
            operation_id: command.operation_id,
            agent_id: agent.agent_id.clone(),
            actor,
            input: OperatorNativePolicyReviewInput {
                resolution: OperatorNativePolicyReviewResolution::NotApplied,
                evidence: "Native session independently verified to have old policy".into(),
                provider: "codex".into(),
                provider_session_ref: session.provider_session_ref.clone().unwrap(),
                canonical_provider_session_ref: None,
                expected_revision: 0,
                session_updated_at: 100,
            },
            reviewed_at: 203,
        })
        .unwrap();
    assert!(
        matches!(try_archive(), Err(RuntimeError::Conflict(message)) if message.contains("reattached"))
    );

    // Only a new verified provider attachment can clear this state in the
    // running product; the test simulates that successful startup receipt.
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET status='ready' WHERE id=?1",
        [&agent.agent_id],
    )
    .unwrap();
    drop(db);
    assert_eq!(
        try_archive().unwrap().lifecycle_state,
        WorkspaceAgentLifecycleState::Archived
    );

    // The same transactional guard applies to restoration, including a
    // stale direct persistence caller that bypasses manager validation.
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET status='policy_recovery_required' WHERE id=?1",
        [&agent.agent_id],
    )
    .unwrap();
    drop(db);
    session.status = "ready".into();
    session.closed_at = None;
    session.updated_at = 300;
    assert!(
        matches!(repository.set_workspace_agent_lifecycle(&session, &agent.agent_id, WorkspaceAgentLifecycleState::Active, None, 300), Err(RuntimeError::Conflict(message)) if message.contains("reattached"))
    );
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET status='closed' WHERE id=?1",
        [&agent.agent_id],
    )
    .unwrap();
    assert_eq!(
        repository
            .set_workspace_agent_lifecycle(
                &session,
                &agent.agent_id,
                WorkspaceAgentLifecycleState::Active,
                None,
                300
            )
            .unwrap()
            .lifecycle_state,
        WorkspaceAgentLifecycleState::Active
    );
}

#[test]
fn native_archive_claim_fences_stale_persistence_policy_lifecycle_and_compaction() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = super::workspace_agent::registered_workspace(&repository, &root);
    let (session, agent) = super::workspace_agent::workspace_agent_fixture(
        &workspace,
        "sess_archive_claim",
        "quiet-heron-archive-claim",
    );
    repository.create_workspace_agent(&session, &agent).unwrap();
    let actor = OperationActor::operator("runtime_operator");
    let request = serde_json::json!({
        "workspace_id":workspace.workspace_id,
        "agent_id":agent.agent_id,
        "action":"archive",
        "reason":"suspend",
    });
    let command = runtime_core::OperatorAgentArchiveCommand {
        operation_id: "op_archive_cross_runtime_claim".into(),
        actor: actor.clone(),
        idempotency_key: "archive-cross-runtime".into(),
        workspace_id: workspace.workspace_id.clone(),
        agent_id: agent.agent_id.clone(),
        reason: Some("suspend".into()),
        expected_revision: 0,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: None,
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: 200,
    };
    assert!(matches!(
        repository.admit_operator_agent_archive(&command).unwrap(),
        runtime_core::OperatorAgentArchiveAdmission::Execute { .. }
    ));
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let mut closed = session.clone();
    closed.status = "closed".into();
    closed.closed_at = Some(201);
    closed.updated_at = 201;
    assert!(matches!(reopened.set_workspace_agent_lifecycle(
        &closed, &agent.agent_id, WorkspaceAgentLifecycleState::Archived,
        Some("unsafe direct close"), 201,
    ), Err(RuntimeError::OperationUnresolved { operation_id, .. })
        if operation_id == command.operation_id));

    assert!(
        matches!(reopened.compare_and_set_workspace_agent_recreation_policy(
        &session, &agent.agent_id, 0, &agent.recreation_policy, 201,
    ), Err(RuntimeError::OperationUnresolved { operation_id, .. })
        if operation_id == command.operation_id)
    );

    let policy_request = serde_json::json!({
        "agent_id":agent.agent_id,"kind":"preferences","expected_revision":0,
        "current_preferences":{"thinking_effort":"low"},
    });
    let preference = runtime_core::OperatorNativePolicyCommand {
        operation_id: "op_policy_competing_archive".into(),
        actor: actor.clone(),
        idempotency_key: "competing-policy-archive".into(),
        workspace_id: workspace.workspace_id.clone(),
        agent_id: agent.agent_id.clone(),
        kind: OperatorNativePolicyKind::Preferences,
        expected_revision: 0,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: None,
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&policy_request).unwrap(),
        normalized_request: policy_request,
        requested_at: 201,
    };
    assert!(matches!(
        reopened.admit_operator_native_policy(&preference),
        Err(RuntimeError::Conflict(_))
    ));

    let compaction = runtime_core::SessionCompactionRun {
        run_id: "cmp_blocked_by_archive".into(),
        agent_id: agent.agent_id.clone(),
        provider: runtime_core::ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: None,
        agent_revision: 0,
        expected_session_updated_at: session.updated_at,
        idempotency_key: Some("compaction-while-archive".into()),
        phase: "intended".into(),
        outcome: None,
        error_code: None,
        review_evidence: None,
        source: "manual".into(),
        confidence: None,
        observed_turn_id: None,
        trigger: Some("manual".into()),
        pre_tokens: None,
        post_tokens: None,
        context_window_size: None,
        created_at_ms: 201,
        updated_at_ms: 201,
    };
    assert!(matches!(reopened.begin_session_compaction(&compaction),
        Err(RuntimeError::OperationUnresolved { operation_id, .. })
            if operation_id == command.operation_id));

    reopened
        .mark_operator_agent_archive_uncertain(&command.operation_id, "unknown provider close", 202)
        .unwrap();
    // Claim survives transition requested -> manual_review; no native caller
    // can mistake a newly reopened repository for a clean provider attachment.
    assert_eq!(
        reopened
            .unresolved_operator_agent_archive_for_agent(&agent.agent_id)
            .unwrap()
            .as_deref(),
        Some(command.operation_id.as_str())
    );
    let unchanged = reopened
        .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.revision, 0);
    assert_eq!(
        unchanged.lifecycle_state,
        WorkspaceAgentLifecycleState::Active
    );

    let review = runtime_core::OperatorAgentArchiveReviewCommand {
        operation_id: command.operation_id.clone(),
        agent_id: agent.agent_id.clone(),
        workspace_id: workspace.workspace_id.clone(),
        actor,
        input: runtime_core::OperatorAgentArchiveReviewInput {
            resolution: runtime_core::OperatorAgentArchiveResolution::NotClosed,
            evidence: "Original native session independently observed to remain open".into(),
            provider: session.provider.clone(),
            original_provider_session_ref: session.provider_session_ref.clone().unwrap(),
            original_canonical_provider_session_ref: None,
            expected_revision: 0,
            session_updated_at: session.updated_at,
            observed_provider_session_ref: session.provider_session_ref.clone(),
            observed_canonical_provider_session_ref: None,
        },
        reviewed_at: 203,
    };
    let reviewed = reopened
        .resolve_operator_agent_archive_review(&review)
        .unwrap();
    assert!(reviewed.restart_required);
    assert_eq!(reviewed.agent_revision, 0);
    assert!(reopened
        .unresolved_operator_agent_archive_for_agent(&agent.agent_id)
        .unwrap()
        .is_none());
    assert!(reopened
        .operator_agent_archive_reattachment_pending(&agent.agent_id)
        .unwrap());
    assert!(matches!(
        reopened.begin_session_compaction(&compaction),
        Err(RuntimeError::Conflict(_))
    ));
}
