use super::*;
use runtime_core::{
    PersistedUserInputSnapshot, ProviderDispatchStartedCommit, RuntimeError, TurnAdmissionRecord,
    TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
};

fn seeded_start(
    repository: &SqliteRuntimeRepository,
    approval: bool,
) -> (
    TurnAdmissionRecord,
    TurnAdmissionRecord,
    TurnRecord,
    TurnRecord,
    SessionRecord,
    SessionRecord,
    NewRuntimeEvent,
) {
    let session = SessionRecord {
        id: "session_native_start".into(),
        provider: "codex".into(),
        status: "ready".into(),
        cwd: None,
        model: None,
        permission_mode: None,
        system_prompt: None,
        metadata: serde_json::json!({}),
        provider_session_ref: Some("native_session".into()),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 10,
        updated_at: 10,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    };
    repository.upsert_session(&session).unwrap();
    let turn = TurnRecord {
        id: "turn_native_start".into(),
        session_id: session.id.clone(),
        provider_turn_ref: None,
        status: if approval {
            "waiting_for_approval"
        } else {
            "admitted"
        }
        .into(),
        input: serde_json::json!([{"type":"text","text":"test"}]),
        source: Some("user".into()),
        started_at: Some(11),
        completed_at: None,
        usage: None,
        error: None,
    };
    let mut session = session;
    session.status = if approval {
        "waiting_for_approval"
    } else {
        "turn_admitted"
    }
    .into();
    session.active_turn_id = Some(turn.id.clone());
    session.updated_at = 11;
    let mut admission = TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot::from_input(&[]),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: approval,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 11,
        updated_at: 11,
    };
    let pending = approval.then(|| ApprovalRecord {
        id: "approval_native_start".into(),
        session_id: session.id.clone(),
        turn_id: turn.id.clone(),
        origin: "runtime_pre_dispatch_policy".into(),
        tool_call_id: None,
        provider_approval_ref: None,
        status: "pending".into(),
        request: serde_json::json!({"reason":"manual approval required before provider execution"}),
        response: None,
        created_at: 11,
        resolved_at: None,
    });
    repository
        .admit_turn(&admission, &turn, &session, pending.as_ref())
        .unwrap();
    if let Some(mut accepted) = pending {
        accepted.status = "accept".into();
        accepted.resolved_at = Some(12);
        repository.upsert_approval(&accepted).unwrap();
    }
    admission.dispatch_state = TurnDispatchState::Dispatching;
    admission.updated_at = 12;
    repository.upsert_turn_admission(&admission).unwrap();
    let mut after_admission = admission.clone();
    after_admission.dispatch_state = TurnDispatchState::Dispatched;
    after_admission.provider_native_turn_id = Some("native_turn_123".into());
    after_admission.updated_at = 13;
    let mut after_turn = turn.clone();
    after_turn.status = "in_progress".into();
    let mut after_session = session.clone();
    after_session.status = "turn_running".into();
    after_session.updated_at = 13;
    let event = NewRuntimeEvent {
        event_id: format!("evt_turn_{}_started", turn.id),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: "turn.started".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: if approval {
            serde_json::json!({"source":"pre_dispatch_approval.accepted"})
        } else {
            serde_json::json!({})
        },
        provider: None,
        provider_seq: None,
        created_at: 13,
    };
    (
        admission,
        after_admission,
        turn,
        after_turn,
        session,
        after_session,
        event,
    )
}

type Bundle = (
    TurnAdmissionRecord,
    TurnAdmissionRecord,
    TurnRecord,
    TurnRecord,
    SessionRecord,
    SessionRecord,
    NewRuntimeEvent,
);

fn commit(
    repo: &SqliteRuntimeRepository,
    bundle: &Bundle,
) -> Result<ProviderDispatchStartedCommit, RuntimeError> {
    repo.commit_provider_turn_started(
        &bundle.0, &bundle.1, &bundle.2, &bundle.3, &bundle.4, &bundle.5, &bundle.6,
    )
}

fn assert_state(repo: &SqliteRuntimeRepository, bundle: &Bundle, changed: bool) {
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(
        repo.list_turn_admissions()
            .unwrap()
            .iter()
            .find(|r| r.turn_id == bundle.0.turn_id),
        Some(if changed { &bundle.1 } else { &bundle.0 }),
    );
    assert_eq!(
        hydrated.turns.iter().find(|r| r.id == bundle.2.id),
        Some(if changed { &bundle.3 } else { &bundle.2 }),
    );
    assert_eq!(
        hydrated.sessions.iter().find(|r| r.id == bundle.4.id),
        Some(if changed { &bundle.5 } else { &bundle.4 }),
    );
    let events = repo
        .list_runtime_events(Some((RuntimeEventScope::Session, &bundle.4.id)), None, 100)
        .unwrap();
    assert_eq!(
        events.iter().filter(|e| e.kind == "turn.started").count(),
        usize::from(changed)
    );
}

#[test]
fn positive_ack_journal_failure_rolls_all_projections_back_then_restart_replays_once() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let bundle = seeded_start(&repository, false);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: repository.database_path.clone(),
    });
    let mut notifications = store.subscribe_event_commits().unwrap();
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_positive_ack BEFORE INSERT ON runtime_events
         WHEN NEW.kind='turn.started'
         BEGIN SELECT RAISE(ABORT, 'no ack event'); END;",
        )
        .unwrap();
    assert!(commit(&repository, &bundle).is_err());
    assert_state(&repository, &bundle, false);
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    connection
        .execute_batch("DROP TRIGGER fail_positive_ack")
        .unwrap();
    drop(connection);
    let restarted = SqliteRuntimeRepository::new(repository.database_path.clone());
    let first = commit(&restarted, &bundle).unwrap();
    assert_state(&restarted, &bundle, true);
    notifications
        .try_recv()
        .expect("committed journal should wake one subscriber");
    let second = commit(&repository, &bundle).unwrap();
    assert_eq!(first, second);
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn approval_ack_has_one_atomic_critical_event_with_exact_origin() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let bundle = seeded_start(&repository, true);
    let committed = commit(&repository, &bundle).unwrap();
    assert_eq!(
        committed.event.payload,
        serde_json::json!({"source":"pre_dispatch_approval.accepted"})
    );
    assert_state(&repository, &bundle, true);
}

#[test]
fn forged_start_receipt_and_changed_dispatch_owner_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let bundle = seeded_start(&repository, false);
    let mut forged = bundle.6.clone();
    forged.payload = serde_json::json!({"source":"forged"});
    repository.append_runtime_event(&forged).unwrap();
    assert!(matches!(
        commit(&repository, &bundle),
        Err(RuntimeError::Conflict(_))
    ));
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(
        repository
            .list_turn_admissions()
            .unwrap()
            .iter()
            .find(|r| r.turn_id == bundle.0.turn_id),
        Some(&bundle.0)
    );
    assert_eq!(
        hydrated.turns.iter().find(|r| r.id == bundle.2.id),
        Some(&bundle.2)
    );
    assert_eq!(
        hydrated.sessions.iter().find(|r| r.id == bundle.4.id),
        Some(&bundle.4)
    );
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute(
            "DELETE FROM runtime_events WHERE event_id=?1",
            [&forged.event_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE turn_admissions SET dispatch_state='unknown' WHERE turn_id=?1",
            [&bundle.0.turn_id],
        )
        .unwrap();
    assert!(matches!(
        commit(&repository, &bundle),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository
            .list_runtime_events(Some((RuntimeEventScope::Session, &bundle.4.id)), None, 100)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "turn.started")
            .count(),
        0
    );
}

#[test]
fn independent_competing_native_acknowledgements_never_overwrite_the_winner() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let bundle = seeded_start(&repository, false);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for identity in ["native_one", "native_two"] {
        let path = repository.database_path.clone();
        let barrier = barrier.clone();
        let mut bundle = bundle.clone();
        bundle.1.provider_native_turn_id = Some(identity.into());
        handles.push(std::thread::spawn(move || {
            let repo = SqliteRuntimeRepository::new(path);
            barrier.wait();
            commit(&repo, &bundle)
        }));
    }
    let results: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    let restarted = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        restarted
            .list_runtime_events(Some((RuntimeEventScope::Session, &bundle.4.id)), None, 100)
            .unwrap()
            .iter()
            .filter(|e| e.kind == "turn.started")
            .count(),
        1
    );
    assert_eq!(
        restarted
            .list_turn_admissions()
            .unwrap()
            .into_iter()
            .find(|r| r.turn_id == bundle.0.turn_id)
            .unwrap()
            .provider_native_turn_id,
        winner.admission.provider_native_turn_id
    );
}
