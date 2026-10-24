use super::*;
use runtime_core::ProcessCompletionUpdate;
use runtime_core::{
    PersistedUserInputSnapshot, TurnAdmissionRecord, TurnCorrelationState,
    TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
};

#[test]
fn process_completion_delivery_requires_exact_dispatched_owner_and_correlation() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut session = sample_session();
    session.active_turn_id = None;
    session.worktree_id = None;
    session.status = "ready".into();
    repository.upsert_session(&session).unwrap();
    let mut peer = session.clone();
    peer.id = "other-completion-owner".into();
    repository.upsert_session(&peer).unwrap();
    let turn = sample_turn();
    repository.upsert_turn(&turn).unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    db.execute("INSERT INTO managed_processes(process_id,owner_session_id,command_json,status,
        admission_order,queue_order,capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at,
        completion_state,completion_attempt_count,terminal_recorded_at,ended_at)
        VALUES ('proof-process',?1,'{}','completed',1,1,1024,'out','err',1,2,'injecting',1,2,2)",
        [&session.id]).unwrap();
    db.execute("INSERT INTO turn_admissions(turn_id,session_id,provider,projection_source,
        user_input_snapshot_json,dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at)
        VALUES(?1,?2,'codex','automation_context','{}','{}',
        '{\"correlation_id\":\"process_completion:proof-process\"}','pending',3,3)",
        params![turn.id,session.id]).unwrap();
    let delivered = ProcessCompletionUpdate {
        state: "delivered".into(),
        turn_id: Some(turn.id.clone()),
        attempt_count: 1,
        last_error: None,
        updated_at: 4,
    };
    for state in ["pending", "dispatching", "unknown", "not_dispatched"] {
        db.execute(
            "UPDATE turn_admissions SET dispatch_state=?1 WHERE turn_id=?2",
            params![state, turn.id],
        )
        .unwrap();
        assert!(
            repository
                .update_managed_process_completion("proof-process", &delivered)
                .unwrap()
                .is_none(),
            "{state} is not native delivery proof"
        );
    }
    db.execute(
        "UPDATE turn_admissions SET dispatch_state='dispatched',
        correlation_json='{\"correlation_id\":\"process_completion:other-process\"}'
        WHERE turn_id=?1",
        [&turn.id],
    )
    .unwrap();
    assert!(repository
        .update_managed_process_completion("proof-process", &delivered)
        .unwrap()
        .is_none());
    db.execute(
        "UPDATE turn_admissions SET session_id=?1,
        correlation_json='{\"correlation_id\":\"process_completion:proof-process\"}'
        WHERE turn_id=?2",
        params![peer.id, turn.id],
    )
    .unwrap();
    assert!(repository
        .update_managed_process_completion("proof-process", &delivered)
        .unwrap()
        .is_none());
    db.execute(
        "UPDATE turn_admissions SET session_id=?1,projection_source='user_visible'
        WHERE turn_id=?2",
        params![session.id, turn.id],
    )
    .unwrap();
    assert!(repository
        .update_managed_process_completion("proof-process", &delivered)
        .unwrap()
        .is_none());
    db.execute(
        "UPDATE turn_admissions SET projection_source='automation_context'
        WHERE turn_id=?1",
        [&turn.id],
    )
    .unwrap();
    let receipt = repository
        .update_managed_process_completion("proof-process", &delivered)
        .unwrap()
        .unwrap();
    assert_eq!(receipt.completion_state, "delivered");
    assert_eq!(
        receipt.completion_turn_id.as_deref(),
        Some(turn.id.as_str())
    );
    assert!(repository
        .update_managed_process_completion("proof-process", &delivered)
        .unwrap()
        .is_none());
}

#[test]
fn process_completion_stale_admission_cannot_repeat_model_input_after_the_session_is_idle() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut session = sample_session();
    session.status = "ready".into();
    session.active_turn_id = None;
    session.worktree_id = None;
    repository.upsert_session(&session).unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    db.execute("INSERT INTO managed_processes(process_id,owner_session_id,command_json,status,
        admission_order,queue_order,capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at,
        completion_state,completion_attempt_count,terminal_recorded_at,ended_at)
        VALUES ('claimed-output',?1,'{}','completed',1,1,1024,'out','err',1,2,'injecting',2,2,2)",
        [&session.id]).unwrap();
    let mut turn = sample_turn();
    turn.status = "admitted".into();
    turn.source = Some("automation_context".into());
    turn.provider_turn_ref = None;
    let mut admitted_session = session.clone();
    admitted_session.status = "turn_admitted".into();
    admitted_session.active_turn_id = Some(turn.id.clone());
    let mut admission = TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: TurnInputProjectionSource::AutomationContext,
        user_input_snapshot: PersistedUserInputSnapshot::from_input(turn.input.as_array().unwrap()),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState {
            expected_turn_id: None,
            correlation_id: Some("process_completion:claimed-output".into()),
        },
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 10,
        updated_at: 10,
    };
    repository
        .admit_turn(&admission, &turn, &admitted_session, None)
        .unwrap();
    let mut stale_turn = turn.clone();
    stale_turn.id = "stale-completion-turn".into();
    let mut stale_admission = admission.clone();
    stale_admission.turn_id = stale_turn.id.clone();
    let mut stale_session = admitted_session.clone();
    stale_session.active_turn_id = Some(stale_turn.id.clone());
    // Let the first input finish so the ordinary active-turn guard cannot
    // accidentally hide a missing durable correlation fence.
    repository.upsert_session(&session).unwrap();
    for state in [
        TurnDispatchState::Pending,
        TurnDispatchState::Dispatching,
        TurnDispatchState::Unknown,
        TurnDispatchState::Dispatched,
    ] {
        admission.dispatch_state = state;
        repository.upsert_turn_admission(&admission).unwrap();
        let error = repository
            .admit_turn(&stale_admission, &stale_turn, &stale_session, None)
            .unwrap_err();
        assert!(error.to_string().contains("completion already"));
    }
    admission.dispatch_state = TurnDispatchState::NotDispatched;
    repository.upsert_turn_admission(&admission).unwrap();
    repository
        .admit_turn(&stale_admission, &stale_turn, &stale_session, None)
        .unwrap();
    assert_eq!(repository.list_turn_admissions().unwrap().len(), 2);
    for projection in [
        TurnInputProjectionSource::UserVisible,
        TurnInputProjectionSource::AgentMessageDeliveryTransport,
    ] {
        repository.upsert_session(&session).unwrap();
        stale_turn.id = format!("ordinary-{}", projection.as_str());
        stale_admission.turn_id = stale_turn.id.clone();
        stale_admission.projection_source = projection;
        stale_session.active_turn_id = Some(stale_turn.id.clone());
        repository
            .admit_turn(&stale_admission, &stale_turn, &stale_session, None)
            .unwrap();
    }
    assert_eq!(
        repository.list_turn_admissions().unwrap().len(),
        4,
        "ordinary user and agent-message correlations retain their existing semantics"
    );
}
