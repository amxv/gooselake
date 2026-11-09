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
