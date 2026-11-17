use super::*;

#[test]
fn manual_compaction_admission_replay_and_native_recovery_are_durable() {
    use runtime_core::{ProviderCompactSessionOutcome, SessionCompactionRun};

    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).expect("root");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "sess_compact", "steady-heron-compaction");
    repository
        .create_workspace_agent(&session, &agent)
        .expect("agent");

    let run = SessionCompactionRun {
        run_id: "cmp_1".into(),
        agent_id: session.id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: None,
        agent_revision: 0,
        expected_session_updated_at: session.updated_at,
        idempotency_key: Some("first".into()),
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
        created_at_ms: 101,
        updated_at_ms: 101,
    };
    repository
        .begin_session_compaction(&run)
        .expect("admit before provider");
    // A second runtime instance using the same database must not be able to
    // admit a turn while a native compact request is unresolved, even when
    // it bypasses the manager's in-memory session mutation lock.
    let pending_turn = runtime_core::TurnRecord {
        id: "turn_racing_compaction".into(),
        session_id: session.id.clone(),
        provider_turn_ref: None,
        status: "admitted".into(),
        input: serde_json::json!([{"type":"text","text":"race"}]),
        source: Some("user".into()),
        started_at: Some(102),
        completed_at: None,
        usage: None,
        error: None,
    };
    let pending_admission = runtime_core::TurnAdmissionRecord {
        turn_id: pending_turn.id.clone(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: runtime_core::TurnInputProjectionSource::UserVisible,
        user_input_snapshot: runtime_core::PersistedUserInputSnapshot {
            prompt_text: "race".into(),
            ..Default::default()
        },
        dispatch_policy: runtime_core::TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: Default::default(),
        dispatch_state: runtime_core::TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 102,
        updated_at: 102,
    };
    let mut admitted_session = session.clone();
    admitted_session.status = "turn_admitted".into();
    admitted_session.active_turn_id = Some(pending_turn.id.clone());
    admitted_session.updated_at = 102;
    assert!(matches!(
        repository.admit_turn(&pending_admission, &pending_turn, &admitted_session, None),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(repository
        .unresolved_session_compaction(&session.id)
        .unwrap());
    assert_eq!(
        repository
            .get_session_compaction_by_key(&session.id, "first")
            .unwrap(),
        Some(run.clone())
    );
    let mut other = run.clone();
    other.run_id = "cmp_2".into();
    other.idempotency_key = Some("different".into());
    assert!(matches!(
        repository.begin_session_compaction(&other),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repository.begin_session_compaction(&run).unwrap(), run);

    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_request_critical BEFORE INSERT ON runtime_events
         WHEN NEW.kind='compaction.request_state_changed'
           AND json_extract(NEW.payload_json,'$.phase')='accepted'
         BEGIN SELECT RAISE(ABORT,'injected request Critical event failure'); END;",
    )
    .unwrap();
    assert!(repository
        .transition_session_compaction(
            "cmp_1",
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            102,
        )
        .is_err());
    assert_eq!(
        repository.get_session_compaction("cmp_1").unwrap(),
        Some(run.clone())
    );
    let request_event_count = || -> i64 {
        db.query_row(
            "SELECT COUNT(*) FROM runtime_events WHERE kind='compaction.request_state_changed'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(request_event_count(), 0, "failed append rolls state back");
    db.execute_batch("DROP TRIGGER fail_request_critical")
        .unwrap();
    let accepted_receipt = repository
        .transition_session_compaction_with_receipt(
            "cmp_1",
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            102,
        )
        .expect("native admission accepted");
    assert!(accepted_receipt.changed);
    assert_eq!(
        accepted_receipt.journal_event.as_ref().unwrap().payload["phase"],
        "accepted"
    );
    assert_eq!(
        accepted_receipt.journal_event.as_ref().unwrap().payload["run_id"],
        "cmp_1"
    );
    assert_eq!(
        accepted_receipt.journal_event.as_ref().unwrap().criticality,
        runtime_core::RuntimeEventCriticality::Critical
    );
    let accepted = accepted_receipt.run;
    assert_eq!(accepted.phase, "accepted");
    let replay = repository
        .transition_session_compaction_with_receipt(
            "cmp_1",
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            103,
        )
        .unwrap();
    assert!(!replay.changed);
    assert!(replay.journal_event.is_none());
    assert_eq!(replay.run, accepted);
    assert_eq!(
        request_event_count(),
        1,
        "exact replay cannot republish Critical event"
    );
    assert!(repository
        .unresolved_session_compaction(&session.id)
        .unwrap());
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        restarted.get_session_compaction("cmp_1").unwrap(),
        Some(accepted)
    );
    assert!(restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
    let completed = restarted
        .transition_session_compaction(
            "cmp_1",
            "completed",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            103,
        )
        .expect("provider completion evidence");
    assert_eq!(completed.phase, "completed");
    assert!(!restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
    assert!(matches!(
        restarted.transition_session_compaction("cmp_1", "accepted", None, None, 104),
        Err(RuntimeError::Conflict(_))
    ));

    restarted
        .begin_session_compaction(&other)
        .expect("next fresh request");
    session.provider_session_ref = Some("different-native-thread".into());
    session.updated_at += 1;
    restarted
        .upsert_session(&session)
        .expect("native session replacement");
    assert!(matches!(
        restarted.transition_session_compaction("cmp_2", "completed", None, None, 105),
        Err(RuntimeError::Conflict(_))
    ));
    let unresolved = restarted
        .transition_session_compaction(
            "cmp_2",
            "manual_review",
            None,
            Some("attachment_changed"),
            106,
        )
        .expect("fail closed");
    assert_eq!(unresolved.phase, "manual_review");
    assert!(restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
    assert!(
        restarted
            .resolve_session_compaction_review(
                "cmp_2",
                "completed",
                "Provider reports compaction completed",
                107
            )
            .is_err(),
        "old attachment cannot be falsely confirmed complete"
    );
    assert!(matches!(
        restarted.resolve_session_compaction_review(
            "cmp_2",
            "failed",
            "Operator confirmed no native compact completion",
            108,
        ),
        Err(RuntimeError::Unsupported(_))
    ));
    assert_eq!(
        restarted.get_session_compaction("cmp_2").unwrap(),
        Some(unresolved)
    );
    assert!(restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
}

#[test]
fn restart_classifies_pre_dispatch_and_accepted_compaction_without_replaying_provider() {
    use runtime_core::{ProviderCompactSessionOutcome, SessionCompactionRun};

    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) =
        workspace_agent_fixture(&workspace, "sess_compact_restart", "nimble-heron-restart");
    repository
        .create_workspace_agent(&session, &agent)
        .expect("create agent");

    let run = SessionCompactionRun {
        run_id: "cmp_pending_restart".into(),
        agent_id: session.id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: session.canonical_provider_session_ref,
        agent_revision: agent.revision,
        expected_session_updated_at: session.updated_at,
        idempotency_key: Some("retry-after-restart".into()),
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
        created_at_ms: 101,
        updated_at_ms: 101,
    };
    repository
        .begin_session_compaction(&run)
        .expect("admit run");
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let recovered = restarted
        .recover_session_compactions(200)
        .expect("restart recovery");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].phase, "manual_review");
    assert_eq!(
        recovered[0].error_code.as_deref(),
        Some("restart_native_compaction_outcome_unverified")
    );
    assert_eq!(restarted.recover_session_compactions(201).unwrap().len(), 0);
    assert_eq!(
        restarted.begin_session_compaction(&run).unwrap(),
        recovered[0]
    );
    assert!(restarted
        .unresolved_session_compaction(&run.agent_id)
        .unwrap());
    assert!(matches!(
        restarted.resolve_session_compaction_review(
            &run.run_id,
            "failed",
            "Operator confirmed no provider work was dispatched",
            202,
        ),
        Err(RuntimeError::Unsupported(_))
    ));
    assert_eq!(
        restarted.get_session_compaction(&run.run_id).unwrap(),
        Some(recovered[0].clone())
    );
    assert!(restarted
        .unresolved_session_compaction(&run.agent_id)
        .unwrap());
    let mut next_run = run.clone();
    next_run.run_id = "cmp_accepted_restart".into();
    next_run.idempotency_key = Some("second-after-restart".into());
    assert!(
        restarted.begin_session_compaction(&next_run).is_err(),
        "unknown first run still fences its agent"
    );
    // Exercise accepted-result restart on a separate disposable agent, without
    // settling or clearing the first agent's unknown native outcome.
    let (other_session, other_agent) = workspace_agent_fixture(
        &workspace,
        "sess_compact_accepted_restart",
        "accepted-restart",
    );
    restarted
        .create_workspace_agent(&other_session, &other_agent)
        .unwrap();
    next_run.agent_id = other_session.id;
    next_run.provider_session_ref = other_session.provider_session_ref.unwrap();
    next_run.canonical_provider_session_ref = other_session.canonical_provider_session_ref;
    next_run.agent_revision = other_agent.revision;
    next_run.expected_session_updated_at = other_session.updated_at;
    restarted
        .begin_session_compaction(&next_run)
        .expect("second run");
    restarted
        .transition_session_compaction(
            &next_run.run_id,
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            205,
        )
        .expect("native admitted");
    let recovered = restarted
        .recover_session_compactions(210)
        .expect("accepted after restart");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].phase, "manual_review");
    assert_eq!(
        recovered[0].outcome,
        Some(ProviderCompactSessionOutcome::Accepted)
    );
    assert_eq!(
        restarted.begin_session_compaction(&next_run).unwrap(),
        recovered[0]
    );
}
