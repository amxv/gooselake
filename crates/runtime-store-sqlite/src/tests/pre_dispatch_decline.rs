use super::*;
use runtime_core::{
    PersistedUserInputSnapshot, RuntimeError, TurnAdmissionRecord, TurnCorrelationState,
    TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
};
use std::sync::{Arc, Barrier};

#[derive(Clone)]
struct Bundle {
    admission: TurnAdmissionRecord,
    denied_admission: TurnAdmissionRecord,
    approval: ApprovalRecord,
    denied_approval: ApprovalRecord,
    turn: TurnRecord,
    denied_turn: TurnRecord,
    session: SessionRecord,
    released_session: SessionRecord,
    events: Vec<NewRuntimeEvent>,
}

fn seed(repo: &SqliteRuntimeRepository) -> Bundle {
    repo.initialize_schema().unwrap();
    let mut base = sample_session();
    base.active_turn_id = None;
    base.status = "ready".into();
    repo.upsert_session(&base).unwrap();
    let mut turn = sample_turn();
    turn.provider_turn_ref = None;
    turn.status = "waiting_for_approval".into();
    turn.completed_at = None;
    turn.started_at = Some(111);
    let mut session = base.clone();
    session.active_turn_id = Some(turn.id.clone());
    session.status = "waiting_for_approval".into();
    session.updated_at = 111;
    let admission = TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: session.id.clone(),
        provider: session.provider.clone(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot::from_input(&[]),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: Some("require_approval".into()),
            pre_dispatch_approval_required: true,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 111,
        updated_at: 111,
    };
    let approval = ApprovalRecord {
        id: "approval_local_decline".into(),
        session_id: session.id.clone(),
        turn_id: turn.id.clone(),
        origin: "runtime_pre_dispatch_policy".into(),
        tool_call_id: None,
        provider_approval_ref: None,
        status: "pending".into(),
        request: serde_json::json!({"reason":"policy"}),
        response: None,
        created_at: 111,
        resolved_at: None,
    };
    repo.admit_turn(&admission, &turn, &session, Some(&approval))
        .unwrap();
    let mut denied_admission = admission.clone();
    denied_admission.dispatch_state = TurnDispatchState::NotDispatched;
    denied_admission.updated_at = 120;
    denied_admission.dispatch_error = Some(serde_json::json!({
        "code":"pre_dispatch_approval_declined",
        "message":"runtime pre-dispatch approval declined",
    }));
    let mut denied_approval = approval.clone();
    denied_approval.status = "decline".into();
    denied_approval.resolved_at = Some(120);
    let mut denied_turn = turn.clone();
    denied_turn.status = "interrupted".into();
    denied_turn.completed_at = Some(120);
    denied_turn.error = Some(serde_json::json!({
        "message":"approval declined before provider dispatch",
    }));
    let mut released_session = session.clone();
    released_session.status = "ready".into();
    released_session.active_turn_id = None;
    released_session.updated_at = 120;
    let mut event = NewRuntimeEvent {
        event_id: format!("evt_approval_{}_resolved", approval.id),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: "approval.resolved".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "approval_id": approval.id,
            "origin":"runtime_pre_dispatch_policy",
            "decision":"decline",
        }),
        provider: None,
        provider_seq: None,
        created_at: 120,
    };
    let first = event.clone();
    event.event_id = format!("evt_turn_{}_pre_dispatch_declined", turn.id);
    event.kind = "turn.interrupted".into();
    event.payload = serde_json::json!({"source":"pre_dispatch_approval.declined"});
    Bundle {
        admission,
        denied_admission,
        approval,
        denied_approval,
        turn,
        denied_turn,
        session,
        released_session,
        events: vec![first, event],
    }
}

fn commit(
    repo: &SqliteRuntimeRepository,
    b: &Bundle,
) -> Result<runtime_core::PreDispatchApprovalDeclineCommit, RuntimeError> {
    repo.decline_pre_dispatch_approval(
        &b.admission,
        &b.denied_admission,
        &b.approval,
        &b.denied_approval,
        &b.turn,
        &b.denied_turn,
        &b.session,
        &b.released_session,
        &b.events,
    )
}

fn verify_uncommitted(repo: &SqliteRuntimeRepository, b: &Bundle) {
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let rows = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(
        rows.turns.iter().find(|row| row.id == b.turn.id),
        Some(&b.turn)
    );
    assert_eq!(
        rows.sessions.iter().find(|row| row.id == b.session.id),
        Some(&b.session)
    );
    assert_eq!(
        rows.approvals.iter().find(|row| row.id == b.approval.id),
        Some(&b.approval)
    );
    assert_eq!(
        reopened.list_turn_admissions().unwrap(),
        vec![b.admission.clone()]
    );
    let events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, &b.session.id)), None, 100)
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, "turn.admitted");
    assert_eq!(events[1].kind, "approval.requested");
}

#[test]
fn local_decline_commits_all_authority_and_exact_two_critical_events_across_restart() {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    let bundle = seed(&repo);
    let committed = commit(&repo, &bundle).unwrap();
    assert_eq!(committed.admission, bundle.denied_admission);
    assert_eq!(committed.approval, bundle.denied_approval);
    assert_eq!(committed.turn, bundle.denied_turn);
    assert_eq!(committed.session, bundle.released_session);
    assert_eq!(committed.events.len(), 2);
    assert!(committed
        .events
        .iter()
        .all(|row| row.criticality == RuntimeEventCriticality::Critical));
    assert_eq!(committed.events[1].seq, committed.events[0].seq + 1);
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(commit(&reopened, &bundle).unwrap(), committed);
    let rows = reopened.hydrate_runtime_state().unwrap();
    assert!(rows.approvals.contains(&bundle.denied_approval));
    assert!(rows.turns.contains(&bundle.denied_turn));
    assert!(rows.sessions.contains(&bundle.released_session));
    assert_eq!(
        reopened.list_turn_admissions().unwrap(),
        vec![bundle.denied_admission]
    );
    assert_eq!(
        reopened
            .list_runtime_events(
                Some((RuntimeEventScope::Session, &bundle.session.id)),
                None,
                100
            )
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn failing_either_critical_insert_rolls_back_every_local_decline_projection() {
    for kind in ["approval.resolved", "turn.interrupted"] {
        let temp = tempfile::tempdir().unwrap();
        let repo = repo(&temp);
        let bundle = seed(&repo);
        let db = open_connection(&repo.database_path).unwrap();
        db.execute_batch(&format!(
            "CREATE TRIGGER fail_decline BEFORE INSERT ON runtime_events
             WHEN NEW.kind='{kind}' BEGIN SELECT RAISE(ABORT,'late critical receipt fault'); END;"
        ))
        .unwrap();
        assert!(commit(&repo, &bundle).is_err());
        verify_uncommitted(&repo, &bundle);
        db.execute_batch("DROP TRIGGER fail_decline").unwrap();
        drop(db);
        assert_eq!(commit(&repo, &bundle).unwrap().events.len(), 2);
        assert!(
            commit(&repo, &bundle).is_ok(),
            "exact retry is inspection-only"
        );
    }
}

#[test]
fn independent_writers_cannot_publish_duplicate_local_denial() {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    let bundle = seed(&repo);
    let path = Arc::new(repo.database_path.clone());
    let barrier = Arc::new(Barrier::new(3));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let path = path.clone();
            let barrier = barrier.clone();
            let b = bundle.clone();
            std::thread::spawn(move || {
                let independent = SqliteRuntimeRepository::new((*path).clone());
                barrier.wait();
                commit(&independent, &b)
            })
        })
        .collect();
    barrier.wait();
    let first = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert!(
        first.iter().all(Result::is_ok),
        "exact receipt replay must converge: {first:?}"
    );
    assert_eq!(first[0].as_ref().unwrap(), first[1].as_ref().unwrap());
    let events = repo
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &bundle.session.id)),
            None,
            100,
        )
        .unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "turn.interrupted")
            .count(),
        1
    );
}

#[test]
fn forged_event_stale_writer_and_legacy_missing_request_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    let b = seed(&repository);
    let db = open_connection(&repository.database_path).unwrap();
    db.execute(
        "DELETE FROM runtime_events WHERE event_id=?1",
        [format!("evt_approval_{}_requested", b.approval.id)],
    )
    .unwrap();
    assert!(matches!(
        commit(&repository, &b),
        Err(RuntimeError::Conflict(_))
    ));
    verify_partial_missing_request(&repository, &b);
    drop(db);

    let temp2 = tempfile::tempdir().unwrap();
    let repo2 = repo(&temp2);
    let b2 = seed(&repo2);
    let db2 = open_connection(&repo2.database_path).unwrap();
    db2.execute(
        "UPDATE approvals SET status='accept' WHERE id=?1",
        [&b2.approval.id],
    )
    .unwrap();
    assert!(matches!(
        commit(&repo2, &b2),
        Err(RuntimeError::Conflict(_))
    ));
    let still = SqliteRuntimeRepository::new(repo2.database_path.clone());
    assert!(still
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &b2.session.id)),
            None,
            100
        )
        .unwrap()
        .iter()
        .all(|row| row.kind != "turn.interrupted"));

    let temp3 = tempfile::tempdir().unwrap();
    let repo3 = repo(&temp3);
    let b3 = seed(&repo3);
    let db3 = open_connection(&repo3.database_path).unwrap();
    db3.execute("INSERT INTO runtime_events(event_id,scope,scope_id,session_id,turn_id,seq,kind,critical,payload_json,created_at) VALUES (?1,'session',?2,?2,?3,99,'forged',1,'{}',120)",
        params![b3.events[0].event_id, b3.session.id, b3.turn.id]).unwrap();
    assert!(matches!(
        commit(&repo3, &b3),
        Err(RuntimeError::Conflict(_))
    ));
}

fn verify_partial_missing_request(repo: &SqliteRuntimeRepository, b: &Bundle) {
    let rows = repo.hydrate_runtime_state().unwrap();
    assert!(rows.approvals.contains(&b.approval));
    assert!(rows.turns.contains(&b.turn));
    assert_eq!(
        repo.list_turn_admissions().unwrap(),
        vec![b.admission.clone()]
    );
}
