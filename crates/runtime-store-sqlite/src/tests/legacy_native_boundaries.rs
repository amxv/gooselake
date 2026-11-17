//! First native boundary tests use disposable rows and inert observed-history
//! fixtures. They do not perform provider actions or certify native quiescence.
use super::legacy_native_turn_guards::{legacy, raw_state, LEGACY_ID};
use super::provider_native_approval_events::{fixture as approval_fixture, submit};
use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::{retirement_command, setup};
use super::*;
use runtime_core::{
    prepare_workspace_interrupt, OperationActor, PersistedUserInputSnapshot, TurnAdmissionRecord,
    TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
    WorkspaceInterruptAdmission, WorkspaceInterruptCommand, WorkspaceInterruptPlan,
    WorkspaceRecord, WorkspaceRetirementTurnStep,
};

fn block(db: &Connection, child: &str) {
    legacy(
        db,
        "rolled_back",
        &serde_json::json!({"spawned_session_id":child}).to_string(),
        false,
    );
}

fn approval_decision(pending: &ApprovalRecord) -> ApprovalRecord {
    let mut decision = pending.clone();
    decision.status = "decision_pending".into();
    decision.response = Some(serde_json::json!({
        "decision":"accept", "payload":{}, "decision_attempt_id":"legacy-boundary-attempt",
    }));
    decision
}

fn approval_event(pending: &ApprovalRecord, released: bool) -> NewRuntimeEvent {
    let suffix = if released {
        "not_dispatched"
    } else {
        "reserved"
    };
    NewRuntimeEvent {
        event_id: format!(
            "evt_approval_{}_decision_{suffix}_legacy-boundary-attempt",
            pending.id
        ),
        scope: RuntimeEventScope::Session,
        scope_id: pending.session_id.clone(),
        session_id: Some(pending.session_id.clone()),
        team_id: None,
        turn_id: Some(pending.turn_id.clone()),
        kind: format!("approval.decision_{suffix}"),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "approval_id":pending.id, "decision":"accept", "origin":"provider",
            "decision_attempt_id":"legacy-boundary-attempt",
        }),
        provider: None,
        provider_seq: None,
        created_at: 300,
    }
}

fn owned(
    active_turn: bool,
) -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    WorkspaceRecord,
    SessionRecord,
) {
    let (temp, repository, workspace) = setup();
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "legacy-boundary-agent", "boundary-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    if active_turn {
        let mut turn = sample_turn();
        turn.id = "legacy-boundary-turn".into();
        turn.session_id = session.id.clone();
        turn.status = "in_progress".into();
        turn.provider_turn_ref = Some("observed-boundary-native-turn".into());
        repository.upsert_turn(&turn).unwrap();
        repository
            .upsert_turn_admission(&TurnAdmissionRecord {
                turn_id: turn.id.clone(),
                session_id: session.id.clone(),
                provider: session.provider.clone(),
                projection_source: TurnInputProjectionSource::UserVisible,
                user_input_snapshot: PersistedUserInputSnapshot::from_input(
                    turn.input.as_array().unwrap(),
                ),
                dispatch_policy: TurnDispatchPolicySnapshot {
                    permission_mode: None,
                    pre_dispatch_approval_required: false,
                    turn_settings: Default::default(),
                },
                correlation: TurnCorrelationState::default(),
                dispatch_state: TurnDispatchState::Dispatched,
                provider_native_turn_id: turn.provider_turn_ref.clone(),
                dispatch_error: None,
                admitted_at: 101,
                updated_at: 102,
            })
            .unwrap();
        session.status = "turn_running".into();
        session.active_turn_id = Some(turn.id);
        session.updated_at = 103;
        repository.upsert_session(&session).unwrap();
    }
    (temp, repository, workspace, session)
}

fn interrupt(workspace: &WorkspaceRecord) -> WorkspaceInterruptCommand {
    prepare_workspace_interrupt(
        &workspace.workspace_id,
        OperationActor::operator("boundary-test"),
        Some("legacy-boundary-interrupt".into()),
    )
    .unwrap()
}

fn admit_interrupt(
    repository: &SqliteRuntimeRepository,
    workspace: &WorkspaceRecord,
) -> WorkspaceInterruptPlan {
    match repository
        .begin_workspace_interrupt(&interrupt(workspace))
        .unwrap()
    {
        WorkspaceInterruptAdmission::Execute(plan) => {
            assert_eq!(plan.targets.len(), 1);
            plan
        }
        other => panic!("unexpected interrupt fixture admission: {other:?}"),
    }
}

#[test]
fn provider_approval_first_decision_observes_late_legacy_obligation() {
    let (_temp, repository, session, turn) = approval_fixture();
    let admitted = submit(&repository, &session, &turn, "legacy-boundary-approval").unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    block(&db, &session.id);
    let before = raw_state(&db);
    let error = repository
        .claim_provider_approval_decision(
            &admitted.approval,
            &approval_decision(&admitted.approval),
            &admitted.turn,
            &admitted.session,
            &approval_event(&admitted.approval, false),
        )
        .unwrap_err();
    assert!(error.to_string().contains(LEGACY_ID), "{error}");
    assert_eq!(raw_state(&db), before);
}

#[test]
fn known_no_dispatch_approval_release_is_not_blocked_by_later_history() {
    let (_temp, repository, session, turn) = approval_fixture();
    let admitted = submit(&repository, &session, &turn, "legacy-boundary-release").unwrap();
    let decision = approval_decision(&admitted.approval);
    repository
        .claim_provider_approval_decision(
            &admitted.approval,
            &decision,
            &admitted.turn,
            &admitted.session,
            &approval_event(&admitted.approval, false),
        )
        .unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    block(&db, &session.id);
    let receipt = repository
        .release_unexecuted_provider_approval_decision(
            &decision,
            &admitted.approval,
            &approval_event(&admitted.approval, true),
        )
        .unwrap();
    assert_eq!(receipt.approval, admitted.approval);
    assert_eq!(
        receipt.event.unwrap().kind,
        "approval.decision_not_dispatched"
    );
}

#[test]
fn interrupt_roster_admission_batches_native_targets_before_committing_authority() {
    let (_temp, repository, workspace, session) = owned(true);
    let db = open_connection(&repository.database_path).unwrap();
    block(&db, &session.id);
    let before = raw_state(&db);
    let error = repository
        .begin_workspace_interrupt(&interrupt(&workspace))
        .unwrap_err();
    assert!(error.to_string().contains(LEGACY_ID), "{error}");
    assert_eq!(raw_state(&db), before);
}

#[test]
fn admitted_interrupt_still_guards_its_first_native_transition_after_reopen() {
    let (_temp, repository, workspace, session) = owned(true);
    let plan = admit_interrupt(&repository, &workspace);
    let db = open_connection(&repository.database_path).unwrap();
    block(&db, &session.id);
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    let error = reopened
        .mark_workspace_interrupt_started(
            &plan.operation_id,
            &session.id,
            session.active_turn_id.as_deref().unwrap(),
            200,
        )
        .unwrap_err();
    assert!(error.to_string().contains(LEGACY_ID), "{error}");
    assert_eq!(raw_state(&db), before);
}

#[test]
fn known_started_interrupt_can_record_its_result_despite_later_history() {
    let (_temp, repository, workspace, session) = owned(true);
    let plan = admit_interrupt(&repository, &workspace);
    let turn = session.active_turn_id.as_deref().unwrap();
    repository
        .mark_workspace_interrupt_started(&plan.operation_id, &session.id, turn, 200)
        .unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    block(&db, &session.id);
    repository
        .finalize_workspace_interrupt_effect(
            &plan.operation_id,
            &session.id,
            turn,
            true,
            "exact observed interrupt receipt fixture",
            201,
        )
        .unwrap();
    let operation = repository
        .get_operation(&plan.operation_id)
        .unwrap()
        .unwrap();
    let effect = operation
        .effects
        .iter()
        .find(|e| e.effect_kind == "workspace_turn_interrupt")
        .unwrap();
    assert_eq!(effect.phase, "finalized");
    assert_eq!(effect.attempt_count, 1);
}

#[test]
fn retirement_first_close_keeps_intent_and_native_identity_when_legacy_is_unresolved() {
    let (_temp, repository, workspace, session) = owned(false);
    let command = retirement_command(&workspace, "legacy-boundary-retire-close");
    repository.admit_workspace_retirement(&command).unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    block(&db, &session.id);
    let before = raw_state(&db);
    let error = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &session.id, 200)
        .unwrap_err();
    assert!(error.to_string().contains(LEGACY_ID), "{error}");
    assert_eq!(raw_state(&db), before);
}

#[test]
fn retirement_first_interrupt_is_fenced_but_known_terminal_turn_can_settle() {
    let (_temp, repository, workspace, mut session) = owned(true);
    let command = retirement_command(&workspace, "legacy-boundary-retire-turn");
    repository.admit_workspace_retirement(&command).unwrap();
    let turn_id = session.active_turn_id.clone().unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    block(&db, &session.id);
    let before = raw_state(&db);
    let error = repository
        .progress_workspace_retirement_turn(&command.operation_id, &turn_id, 200)
        .unwrap_err();
    assert!(error.to_string().contains(LEGACY_ID), "{error}");
    assert_eq!(raw_state(&db), before);
    // A separately observed terminal fixture is settlement, not a new interrupt.
    let mut turn = sample_turn();
    turn.id = turn_id.clone();
    turn.session_id = session.id.clone();
    turn.status = "completed".into();
    turn.provider_turn_ref = Some("observed-boundary-native-turn".into());
    turn.completed_at = Some(201);
    repository.upsert_turn(&turn).unwrap();
    session.active_turn_id = None;
    session.status = "ready".into();
    session.updated_at = 201;
    repository.upsert_session(&session).unwrap();
    assert!(matches!(
        repository
            .progress_workspace_retirement_turn(&command.operation_id, &turn_id, 202)
            .unwrap(),
        WorkspaceRetirementTurnStep::Finalized
    ));
    assert!(
        repository
            .claim_workspace_retirement_native_close(&command.operation_id, &session.id, 203)
            .is_err(),
        "terminal turn does not clear the independent legacy spawn obligation"
    );
}
