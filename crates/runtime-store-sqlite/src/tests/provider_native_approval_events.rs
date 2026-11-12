use super::*;
use runtime_core::{
    PersistedUserInputSnapshot, RuntimeError, TurnAdmissionRecord, TurnCorrelationState,
    TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
};
use std::sync::{Arc, Barrier};

fn fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    SessionRecord,
    TurnRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut session = sample_session();
    session.status = "turn_running".into();
    let mut turn = sample_turn();
    turn.status = "in_progress".into();
    repository.upsert_session(&session).unwrap();
    repository.upsert_turn(&turn).unwrap();
    repository
        .upsert_turn_admission(&TurnAdmissionRecord {
            turn_id: turn.id.clone(),
            session_id: session.id.clone(),
            provider: "codex".into(),
            projection_source: TurnInputProjectionSource::UserVisible,
            user_input_snapshot: PersistedUserInputSnapshot {
                prompt_text: "hello".into(),
                image_refs: Vec::new(),
                invocation: None,
            },
            dispatch_policy: TurnDispatchPolicySnapshot {
                permission_mode: None,
                pre_dispatch_approval_required: false,
                turn_settings: Default::default(),
            },
            correlation: TurnCorrelationState::default(),
            dispatch_state: TurnDispatchState::Dispatched,
            provider_native_turn_id: Some("native_turn_1".into()),
            dispatch_error: None,
            admitted_at: 101,
            updated_at: 102,
        })
        .unwrap();
    (temp, repository, session, turn)
}

fn native_request(
    session: &SessionRecord,
    turn: &TurnRecord,
    id: &str,
) -> (SessionRecord, TurnRecord, ApprovalRecord, NewRuntimeEvent) {
    let mut waiting_session = session.clone();
    waiting_session.status = "waiting_for_approval".into();
    waiting_session.updated_at = 200;
    let mut waiting_turn = turn.clone();
    waiting_turn.status = "waiting_for_approval".into();
    let approval = ApprovalRecord {
        id: id.into(),
        session_id: session.id.clone(),
        turn_id: turn.id.clone(),
        origin: "provider".into(),
        tool_call_id: Some("provider_tool_1".into()),
        provider_approval_ref: Some("provider_request_1".into()),
        status: "pending".into(),
        request: serde_json::json!({"tool":"shell","command":"true"}),
        response: None,
        created_at: 200,
        resolved_at: None,
    };
    let event = NewRuntimeEvent {
        event_id: format!("evt_approval_{id}_requested"),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: "approval.requested".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "approval_id":approval.id,
            "provider_approval_ref":"provider_request_1",
            "origin":"provider"
        }),
        provider: None,
        provider_seq: None,
        created_at: 200,
    };
    (waiting_session, waiting_turn, approval, event)
}

fn submit(
    repository: &SqliteRuntimeRepository,
    session: &SessionRecord,
    turn: &TurnRecord,
    id: &str,
) -> Result<runtime_core::ProviderApprovalAdmission, RuntimeError> {
    let (waiting_session, waiting_turn, approval, event) = native_request(session, turn, id);
    repository.admit_provider_approval(
        turn,
        &waiting_turn,
        session,
        &waiting_session,
        &approval,
        &event,
    )
}

#[test]
fn native_approval_event_fault_rolls_back_all_state_and_recovery_reuses_identity() {
    let (_temp, repository, session, turn) = fixture();
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_provider_approval_event
         BEFORE INSERT ON runtime_events WHEN NEW.kind='approval.requested'
         BEGIN SELECT RAISE(ABORT, 'injected native approval event failure'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(submit(&repository, &session, &turn, "apr_native_1").is_err());
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.turns, vec![turn.clone()]);
    assert_eq!(hydrated.sessions, vec![session.clone()]);
    assert!(hydrated.approvals.is_empty());
    assert!(repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 20)
        .unwrap()
        .is_empty());

    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_provider_approval_event")
        .unwrap();
    drop(connection);
    let committed = submit(&repository, &session, &turn, "apr_native_1").unwrap();
    assert_eq!(committed.approval.id, "apr_native_1");
    assert_eq!(committed.turn.status, "waiting_for_approval");
    assert_eq!(committed.session.status, "waiting_for_approval");
    assert_eq!(
        committed.event.as_ref().unwrap().event_id,
        "evt_approval_apr_native_1_requested"
    );

    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    let replayed = submit(&reopened, &session, &turn, "apr_other_after_lost_reply").unwrap();
    assert_eq!(replayed.approval, committed.approval);
    assert_eq!(replayed.turn, committed.turn);
    assert_eq!(replayed.session, committed.session);
    assert!(
        replayed.event.is_none(),
        "native duplicate never produces second critical event"
    );
    let rows = reopened
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 20)
        .unwrap();
    assert_eq!(rows, vec![committed.event.unwrap()]);
}

#[test]
fn native_approval_stale_owner_and_forged_event_cannot_mutate_partial_state() {
    let (_temp, repository, session, turn) = fixture();
    let mut replacement = session.clone();
    replacement.active_turn_id = Some("new_turn".into());
    replacement.updated_at = 240;
    repository.upsert_session(&replacement).unwrap();
    assert!(matches!(
        submit(&repository, &session, &turn, "apr_native_1"),
        Err(RuntimeError::Conflict(_))
    ));
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.sessions[0], replacement);
    assert_eq!(hydrated.turns[0], turn);
    assert!(hydrated.approvals.is_empty());

    let (_temp, repository, session, turn) = fixture();
    let mut uncertain_session = session.clone();
    uncertain_session.status = "turn_recovery_required".into();
    repository.upsert_session(&uncertain_session).unwrap();
    let mut uncertain_turn = turn.clone();
    uncertain_turn.status = "approval_decision_unknown".into();
    repository.upsert_turn(&uncertain_turn).unwrap();
    assert!(matches!(
        submit(
            &repository,
            &uncertain_session,
            &uncertain_turn,
            "apr_quarantined"
        ),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert!(repository
        .hydrate_runtime_state()
        .unwrap()
        .approvals
        .is_empty());

    let (_temp, repository, session, turn) = fixture();
    let (_new_session, _new_turn, _approval, mut event) =
        native_request(&session, &turn, "apr_native_1");
    event.payload = serde_json::json!({"forged":true});
    repository.append_runtime_event(&event).unwrap();
    assert!(matches!(
        submit(&repository, &session, &turn, "apr_native_1"),
        Err(RuntimeError::Conflict(_))
    ));
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.sessions[0], session);
    assert_eq!(hydrated.turns[0], turn);
    assert!(hydrated.approvals.is_empty());
    assert_eq!(
        repository
            .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 20)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn racing_independent_native_writers_converge_on_one_approval_and_one_event() {
    let (_temp, repository, session, turn) = fixture();
    let barrier = Arc::new(Barrier::new(2));
    let workers = ["apr_a", "apr_b"]
        .into_iter()
        .map(|id| {
            let repo = repository.clone();
            let barrier = Arc::clone(&barrier);
            let session = session.clone();
            let turn = turn.clone();
            std::thread::spawn(move || {
                barrier.wait();
                submit(&repo, &session, &turn, id)
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results[0].approval, results[1].approval);
    assert_eq!(
        results
            .iter()
            .filter(|result| result.event.is_some())
            .count(),
        1
    );
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.approvals, vec![results[0].approval.clone()]);
    assert_eq!(hydrated.turns, vec![results[0].turn.clone()]);
    assert_eq!(hydrated.sessions, vec![results[0].session.clone()]);
    assert_eq!(
        repository
            .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 20)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn legacy_provider_request_without_event_is_reused_without_history_fabrication() {
    let (_temp, repository, session, turn) = fixture();
    let (_waiting_session, _waiting_turn, approval, _event) =
        native_request(&session, &turn, "apr_legacy");
    repository.upsert_approval(&approval).unwrap();
    let replayed = submit(&repository, &session, &turn, "apr_new_id").unwrap();
    assert_eq!(replayed.approval, approval);
    assert_eq!(replayed.turn, turn);
    assert_eq!(replayed.session, session);
    assert!(replayed.event.is_none());
    assert!(repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 20)
        .unwrap()
        .is_empty());
}
