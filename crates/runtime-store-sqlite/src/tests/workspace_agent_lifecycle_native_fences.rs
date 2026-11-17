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

    let review = OperatorNativePolicyReviewCommand {
        operation_id: command.operation_id.clone(),
        agent_id: agent.agent_id.clone(),
        actor,
        input: OperatorNativePolicyReviewInput {
            resolution: OperatorNativePolicyReviewResolution::NotApplied,
            evidence: "Native session independently claimed to have old policy".into(),
            provider: "codex".into(),
            provider_session_ref: session.provider_session_ref.clone().unwrap(),
            canonical_provider_session_ref: None,
            expected_revision: 0,
            session_updated_at: 100,
        },
        reviewed_at: 203,
    };
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let before = super::legacy_native_turn_guards::raw_state(&db);
    for _ in 0..2 {
        assert!(
            matches!(repository.resolve_operator_native_policy_review(&review),Err(RuntimeError::OperationUnresolved {operation_id,..}) if operation_id==command.operation_id)
        );
        assert!(
            matches!(try_archive(),Err(RuntimeError::OperationUnresolved {operation_id,..}) if operation_id==command.operation_id)
        );
        assert_eq!(super::legacy_native_turn_guards::raw_state(&db), before);
    }
    // Separate retained historical status; original uncertain owner stays untouched.
    let (mut historical_session, historical_agent) =
        super::workspace_agent::workspace_agent_fixture(
            &workspace,
            "sess_historical_policy_fence",
            "historical-policy-fence",
        );
    repository
        .create_workspace_agent(&historical_session, &historical_agent)
        .unwrap();
    db.execute(
        "UPDATE sessions SET status='policy_recovery_required' WHERE id=?1",
        [&historical_agent.agent_id],
    )
    .unwrap();
    assert!(repository
        .unresolved_operator_native_policy_for_agent(&historical_agent.agent_id)
        .unwrap()
        .is_none());
    historical_session.status = "closed".into();
    historical_session.closed_at = Some(250);
    historical_session.updated_at = 250;
    assert!(
        matches!(repository.set_workspace_agent_lifecycle(&historical_session,&historical_agent.agent_id,WorkspaceAgentLifecycleState::Archived,Some("historical status cannot authorize archive"),250),Err(RuntimeError::Conflict(message)) if message.contains("reattached"))
    );
    db.execute("UPDATE workspace_agents SET lifecycle_state='archived',archived_at=250 WHERE session_id=?1",[&historical_agent.agent_id]).unwrap();
    historical_session.status = "ready".into();
    historical_session.closed_at = None;
    historical_session.updated_at = 300;
    assert!(
        matches!(repository.set_workspace_agent_lifecycle(&historical_session,&historical_agent.agent_id,WorkspaceAgentLifecycleState::Active,None,300),Err(RuntimeError::Conflict(message)) if message.contains("reattached"))
    );
    assert!(repository
        .operator_native_policy_reattachment_pending(&historical_agent.agent_id)
        .unwrap());
    // Independent clean owner retains the ordinary persistence positive control.
    let (mut clean_session, clean_agent) = super::workspace_agent::workspace_agent_fixture(
        &workspace,
        "sess_clean_lifecycle",
        "clean-lifecycle",
    );
    repository
        .create_workspace_agent(&clean_session, &clean_agent)
        .unwrap();
    clean_session.status = "closed".into();
    clean_session.closed_at = Some(301);
    clean_session.updated_at = 301;
    assert_eq!(
        repository
            .set_workspace_agent_lifecycle(
                &clean_session,
                &clean_agent.agent_id,
                WorkspaceAgentLifecycleState::Archived,
                Some("ordinary lifecycle fixture"),
                301
            )
            .unwrap()
            .lifecycle_state,
        WorkspaceAgentLifecycleState::Archived
    );
    clean_session.status = "ready".into();
    clean_session.closed_at = None;
    clean_session.updated_at = 302;
    assert_eq!(
        repository
            .set_workspace_agent_lifecycle(
                &clean_session,
                &clean_agent.agent_id,
                WorkspaceAgentLifecycleState::Active,
                None,
                302
            )
            .unwrap()
            .lifecycle_state,
        WorkspaceAgentLifecycleState::Active
    );
    assert_eq!(
        repository
            .unresolved_operator_native_policy_for_agent(&agent.agent_id)
            .unwrap()
            .as_deref(),
        Some(command.operation_id.as_str())
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
        legacy_native_recovery_evidence: None,
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
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let before = super::legacy_native_turn_guards::raw_state(&db);
    for _ in 0..2 {
        assert!(
            matches!(reopened.resolve_operator_agent_archive_review(&review),Err(RuntimeError::OperationUnresolved {operation_id,..}) if operation_id==command.operation_id)
        );
        assert!(
            matches!(reopened.begin_session_compaction(&compaction),Err(RuntimeError::OperationUnresolved {operation_id,..}) if operation_id==command.operation_id)
        );
        assert_eq!(super::legacy_native_turn_guards::raw_state(&db), before);
    }
    assert_eq!(
        reopened
            .unresolved_operator_agent_archive_for_agent(&agent.agent_id)
            .unwrap()
            .as_deref(),
        Some(command.operation_id.as_str())
    );
    let (historical_session, historical_agent) = super::workspace_agent::workspace_agent_fixture(
        &workspace,
        "sess_historical_archive_fence",
        "historical-archive-fence",
    );
    reopened
        .create_workspace_agent(&historical_session, &historical_agent)
        .unwrap();
    db.execute(
        "UPDATE sessions SET status='archive_recovery_required' WHERE id=?1",
        [&historical_agent.agent_id],
    )
    .unwrap();
    assert!(reopened
        .unresolved_operator_agent_archive_for_agent(&historical_agent.agent_id)
        .unwrap()
        .is_none());
    assert!(reopened
        .operator_agent_archive_reattachment_pending(&historical_agent.agent_id)
        .unwrap());
    let mut historical_compaction = compaction.clone();
    historical_compaction.run_id = "cmp_historical_archive_fence".into();
    historical_compaction.agent_id = historical_agent.agent_id;
    historical_compaction.provider_session_ref = historical_session.provider_session_ref.unwrap();
    historical_compaction.canonical_provider_session_ref =
        historical_session.canonical_provider_session_ref;
    historical_compaction.agent_revision = historical_agent.revision;
    historical_compaction.expected_session_updated_at = historical_session.updated_at;
    historical_compaction.idempotency_key = Some("historical-archive-compaction".into());
    let historical_before = super::legacy_native_turn_guards::raw_state(&db);
    assert!(matches!(
        reopened.begin_session_compaction(&historical_compaction),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        super::legacy_native_turn_guards::raw_state(&db),
        historical_before
    );
}
