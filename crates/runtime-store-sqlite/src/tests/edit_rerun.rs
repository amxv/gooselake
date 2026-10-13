use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::{ProviderKind, RuntimeError};

#[test]
fn historical_edit_fork_admission_and_replacement_are_atomic_and_replay_safe() {
    use runtime_core::{
        PersistedUserInputSnapshot, ProviderSession, SessionEditRerunOperation,
        TurnAdmissionRecord, TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState,
        TurnInputProjectionSource, TurnRecord,
    };

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("edit-workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repo = repo(&temp);
    repo.initialize_schema().unwrap();
    let workspace = registered_workspace(&repo, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "sess_edit", "quiet-edit-fox");
    repo.create_workspace_agent(&session, &agent).unwrap();

    let original = TurnRecord {
        id: "turn_before_edit".into(),
        session_id: session.id.clone(),
        provider_turn_ref: Some("native-old".into()),
        status: "completed".into(),
        input: serde_json::json!([{"type":"text","text":"before"}]),
        source: Some("user".into()),
        started_at: Some(101),
        completed_at: Some(103),
        usage: None,
        error: None,
    };
    repo.upsert_turn(&original).unwrap();
    let original_admission = TurnAdmissionRecord {
        turn_id: original.id.clone(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot {
            prompt_text: "before".into(),
            ..Default::default()
        },
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Dispatched,
        provider_native_turn_id: Some("native-old".into()),
        dispatch_error: None,
        admitted_at: 101,
        updated_at: 103,
    };
    repo.upsert_turn_admission(&original_admission).unwrap();

    let operation = SessionEditRerunOperation {
        operation_id: "edit_1".into(),
        agent_id: session.id.clone(),
        target_turn_id: original.id.clone(),
        provider: ProviderKind::Codex,
        source_provider_session_ref: session.provider_session_ref.clone().unwrap(),
        source_canonical_provider_session_ref: None,
        agent_revision: 0,
        expected_session_updated_at: session.updated_at,
        request_hash: "a".repeat(64),
        idempotency_key: Some("edit-key".into()),
        edited_snapshot: PersistedUserInputSnapshot {
            prompt_text: "after".into(),
            ..Default::default()
        },
        phase: "intended".into(),
        child_provider_session_ref: None,
        child_canonical_provider_session_ref: None,
        replacement_turn_id: None,
        error_code: None,
        review_evidence: None,
        created_at_ms: 104,
        updated_at_ms: 104,
    };
    assert_eq!(
        repo.begin_session_edit_rerun(&operation).unwrap(),
        operation
    );
    assert_eq!(
        repo.begin_session_edit_rerun(&operation).unwrap(),
        operation
    );
    let mut different = operation.clone();
    different.request_hash = "b".repeat(64);
    assert!(matches!(
        repo.begin_session_edit_rerun(&different),
        Err(RuntimeError::Conflict(_))
    ));

    let mut submitted = session.clone();
    submitted.status = "turn_admitted".into();
    submitted.active_turn_id = Some("turn_edit_race".into());
    submitted.updated_at = 106;
    let pending = TurnRecord {
        id: "turn_edit_race".into(),
        session_id: session.id.clone(),
        provider_turn_ref: None,
        status: "admitted".into(),
        input: serde_json::json!([{"type":"text","text":"race"}]),
        source: Some("user".into()),
        started_at: Some(106),
        completed_at: None,
        usage: None,
        error: None,
    };
    let mut pending_admission = original_admission.clone();
    pending_admission.turn_id = pending.id.clone();
    pending_admission.provider_native_turn_id = None;
    pending_admission.dispatch_state = TurnDispatchState::Pending;
    pending_admission.admitted_at = 106;
    pending_admission.updated_at = 106;
    assert!(matches!(
        repo.admit_turn(&pending_admission, &pending, &submitted, None),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(repo.unresolved_session_edit_rerun(&session.id).unwrap());
    let child = ProviderSession {
        runtime_session_id: session.id.clone(),
        provider_session_ref: "native-child".into(),
        canonical_provider_session_ref: Some("native-child".into()),
    };
    repo.advance_session_edit_rerun("edit_1", "native_started", None, None, None, 105)
        .unwrap();
    let committed = repo
        .advance_session_edit_rerun("edit_1", "fork_observed", Some(&child), None, None, 106)
        .unwrap();
    assert_eq!(
        committed.child_provider_session_ref.as_deref(),
        Some("native-child")
    );
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let superseded: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM session_edit_rerun_superseded_turns WHERE operation_id='edit_1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(superseded, 1);

    // Only an admitted replacement correlated to the exact operation may
    // pass the SQLite edit fence, not a competing uncorrelated turn.
    assert!(matches!(
        repo.admit_turn(&pending_admission, &pending, &submitted, None),
        Err(RuntimeError::Conflict(_))
    ));
    let loaded = repo.hydrate_runtime_state().unwrap();
    let mut updated = loaded
        .sessions
        .into_iter()
        .find(|s| s.id == session.id)
        .unwrap();
    assert_eq!(
        updated.provider_session_ref.as_deref(),
        Some("native-child")
    );
    updated.active_turn_id = Some("turn_edit_race".into());
    updated.status = "turn_admitted".into();
    updated.updated_at = 107;
    pending_admission.correlation.correlation_id = Some("edit_1".into());
    repo.admit_turn(&pending_admission, &pending, &updated, None)
        .unwrap();
    let terminal = repo
        .advance_session_edit_rerun(
            "edit_1",
            "completed",
            None,
            Some("turn_edit_race"),
            None,
            108,
        )
        .unwrap();
    assert_eq!(
        terminal.replacement_turn_id.as_deref(),
        Some("turn_edit_race")
    );
    assert!(!repo.unresolved_session_edit_rerun(&session.id).unwrap());
    assert_eq!(
        repo.get_session_edit_rerun_by_key(&session.id, "edit-key")
            .unwrap(),
        Some(terminal)
    );
    assert!(matches!(
        repo.begin_session_edit_rerun(&different),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn incomplete_native_edit_is_quarantined_on_restart_and_cannot_be_asserted_safe() {
    use runtime_core::{
        PersistedUserInputSnapshot, SessionEditRerunOperation, TurnAdmissionRecord,
        TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState,
        TurnInputProjectionSource, TurnRecord,
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("native-uncertain");
    std::fs::create_dir_all(&root).unwrap();
    let repo = repo(&temp);
    repo.initialize_schema().unwrap();
    let workspace = registered_workspace(&repo, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "sess_uncertain", "amber-edit-wolf");
    repo.create_workspace_agent(&session, &agent).unwrap();
    let original = TurnRecord {
        id: "old_native".into(),
        session_id: session.id.clone(),
        provider_turn_ref: Some("native-before".into()),
        status: "completed".into(),
        input: serde_json::json!([{"type":"text","text":"before"}]),
        source: Some("user".into()),
        started_at: Some(101),
        completed_at: Some(102),
        usage: None,
        error: None,
    };
    repo.upsert_turn(&original).unwrap();
    repo.upsert_turn_admission(&TurnAdmissionRecord {
        turn_id: original.id.clone(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot {
            prompt_text: "before".into(),
            ..Default::default()
        },
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Dispatched,
        provider_native_turn_id: Some("native-before".into()),
        dispatch_error: None,
        admitted_at: 101,
        updated_at: 102,
    })
    .unwrap();
    let operation = SessionEditRerunOperation {
        operation_id: "edit_ambiguous".into(),
        agent_id: session.id.clone(),
        target_turn_id: original.id.clone(),
        provider: ProviderKind::Codex,
        source_provider_session_ref: session.provider_session_ref.unwrap(),
        source_canonical_provider_session_ref: None,
        agent_revision: 0,
        expected_session_updated_at: 100,
        request_hash: "f".repeat(64),
        idempotency_key: None,
        edited_snapshot: PersistedUserInputSnapshot {
            prompt_text: "revised".into(),
            ..Default::default()
        },
        phase: "intended".into(),
        child_provider_session_ref: None,
        child_canonical_provider_session_ref: None,
        replacement_turn_id: None,
        error_code: None,
        review_evidence: None,
        created_at_ms: 104,
        updated_at_ms: 104,
    };
    repo.begin_session_edit_rerun(&operation).unwrap();
    repo.advance_session_edit_rerun(
        &operation.operation_id,
        "native_started",
        None,
        None,
        None,
        105,
    )
    .unwrap();
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let recovered = restarted.recover_session_edit_reruns(106).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].phase, "manual_review");
    assert_eq!(restarted.recover_session_edit_reruns(107).unwrap().len(), 0);
    assert!(restarted
        .unresolved_session_edit_rerun(&operation.agent_id)
        .unwrap());
    assert!(matches!(
        restarted.resolve_session_edit_rerun_review(
            &operation.operation_id,
            "I verified the provider",
            108
        ),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn same_millisecond_historical_turns_use_durable_admission_order_not_lexical_ids() {
    use runtime_core::{
        PersistedUserInputSnapshot, ProviderSession, SessionEditRerunOperation,
        TurnAdmissionRecord, TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState,
        TurnInputProjectionSource, TurnRecord,
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("ordered-fork");
    std::fs::create_dir_all(&root).unwrap();
    let repo = repo(&temp);
    repo.initialize_schema().unwrap();
    let workspace = registered_workspace(&repo, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "sess_ordered", "ordered-edit-fox");
    repo.create_workspace_agent(&session, &agent).unwrap();

    // All three logical IDs share the same millisecond. Lexical ordering is
    // deliberately the reverse of insertion/admission order.
    for (id, native) in [
        ("turn_codex_100_9", "native-1"),
        ("turn_codex_100_2", "native-2"),
        ("turn_codex_100_10", "native-3"),
    ] {
        repo.upsert_turn(&TurnRecord {
            id: id.into(),
            session_id: session.id.clone(),
            provider_turn_ref: Some(native.into()),
            status: "completed".into(),
            input: serde_json::json!([{"type":"text","text":id}]),
            source: Some("user".into()),
            started_at: Some(100),
            completed_at: Some(100),
            usage: None,
            error: None,
        })
        .unwrap();
        repo.upsert_turn_admission(&TurnAdmissionRecord {
            turn_id: id.into(),
            session_id: session.id.clone(),
            provider: "codex".into(),
            projection_source: TurnInputProjectionSource::UserVisible,
            user_input_snapshot: PersistedUserInputSnapshot {
                prompt_text: id.into(),
                ..Default::default()
            },
            dispatch_policy: TurnDispatchPolicySnapshot {
                permission_mode: None,
                pre_dispatch_approval_required: false,
                turn_settings: Default::default(),
            },
            correlation: TurnCorrelationState::default(),
            dispatch_state: TurnDispatchState::Dispatched,
            provider_native_turn_id: Some(native.into()),
            dispatch_error: None,
            admitted_at: 100,
            updated_at: 100,
        })
        .unwrap();
    }
    let op = SessionEditRerunOperation {
        operation_id: "edit_ordered".into(),
        agent_id: session.id.clone(),
        target_turn_id: "turn_codex_100_2".into(),
        provider: ProviderKind::Codex,
        source_provider_session_ref: session.provider_session_ref.clone().unwrap(),
        source_canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        agent_revision: agent.revision,
        expected_session_updated_at: session.updated_at,
        request_hash: "d".repeat(64),
        idempotency_key: Some("ordered-fork".into()),
        edited_snapshot: PersistedUserInputSnapshot {
            prompt_text: "revised".into(),
            ..Default::default()
        },
        phase: "intended".into(),
        child_provider_session_ref: None,
        child_canonical_provider_session_ref: None,
        replacement_turn_id: None,
        error_code: None,
        review_evidence: None,
        created_at_ms: 104,
        updated_at_ms: 104,
    };
    repo.begin_session_edit_rerun(&op).unwrap();
    repo.advance_session_edit_rerun(&op.operation_id, "native_started", None, None, None, 105)
        .unwrap();
    repo.advance_session_edit_rerun(
        &op.operation_id,
        "fork_observed",
        Some(&ProviderSession {
            runtime_session_id: session.id.clone(),
            provider_session_ref: "verified-child".into(),
            canonical_provider_session_ref: Some("verified-child".into()),
        }),
        None,
        None,
        106,
    )
    .unwrap();
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut statement = connection.prepare(
        "SELECT turn_id FROM session_edit_rerun_superseded_turns WHERE operation_id=?1 ORDER BY turn_id"
    ).unwrap();
    let superseded = statement
        .query_map([&op.operation_id], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(superseded, vec!["turn_codex_100_10", "turn_codex_100_2"]);
    let resumed = repo
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .into_iter()
        .find(|record| record.id == session.id)
        .unwrap();
    let retained = resumed.metadata["session_transcript"].as_array().unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0]["text"], "turn_codex_100_9");
}
