use super::parent::prepare_operator_parent;
use super::support::*;
use crate::tests::provider_native_approval_events::native_request;
use runtime_core::{
    ApprovalRecord, NewRuntimeEvent, OperationActor, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, StartupReattachAdmission, TurnDispatchState, WorkspaceInterruptAdmission,
};
use serde_json::json;
use std::sync::{Arc, Barrier};

fn provider_approval(fixture: &Fixture) -> (runtime_core::ProviderApprovalAdmission, TurnFixture) {
    let running = running(fixture);
    let (mut session, turn, mut approval, mut event) =
        native_request(&running.session, &running.turn, "startup_native_approval");
    session.updated_at = 230;
    approval.created_at = 230;
    event.created_at = 230;
    let receipt = fixture
        .repository
        .admit_provider_approval(
            &running.turn,
            &turn,
            &running.session,
            &session,
            &approval,
            &event,
        )
        .unwrap();
    (receipt, running)
}

fn decision(pending: &ApprovalRecord) -> ApprovalRecord {
    let mut reserved = pending.clone();
    reserved.status = "decision_pending".into();
    reserved.response = Some(json!({
        "decision":"accept","payload":{"operator":"approved"},
        "decision_attempt_id":"startup_decision_attempt",
    }));
    reserved
}

fn approval_event(pending: &ApprovalRecord, outcome: &str) -> NewRuntimeEvent {
    let mut event = NewRuntimeEvent {
        event_id: String::new(),
        scope: RuntimeEventScope::Session,
        scope_id: pending.session_id.clone(),
        session_id: Some(pending.session_id.clone()),
        team_id: None,
        turn_id: Some(pending.turn_id.clone()),
        kind: String::new(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({"approval_id":pending.id,"decision":"accept","origin":"provider",
            "decision_attempt_id":"startup_decision_attempt"}),
        provider: None,
        provider_seq: None,
        created_at: 400,
    };
    if outcome == "resolved" {
        event.event_id = format!("evt_approval_{}_resolved", pending.id);
        event.kind = "approval.resolved".into();
    } else {
        event.event_id = format!(
            "evt_approval_{}_decision_{outcome}_startup_decision_attempt",
            pending.id
        );
        event.kind = format!("approval.decision_{outcome}");
    }
    event
}

#[test]
fn live_startup_claim_atomically_refuses_new_ordinary_and_operator_turn_admission() {
    for operator in [false, true] {
        let fixture = Fixture::new(true);
        let grant = fixture.grant("startup_new_turn_guard");
        let parent = prepare_operator_parent(&fixture, false, false);
        let before = snapshot(&fixture.repository);
        let result = if operator {
            fixture.repository.admit_operator_user_turn(
                &parent.command,
                &parent.pending.admission,
                &parent.pending.turn,
                &parent.pending.session,
                None,
            )
        } else {
            fixture.repository.admit_turn(
                &parent.pending.admission,
                &parent.pending.turn,
                &parent.pending.session,
                None,
            )
        };
        assert_unresolved(result, &grant.intent.command.operation_id);
        assert_eq!(snapshot(&fixture.repository), before);
        assert!(fixture
            .repository
            .list_turn_admissions()
            .unwrap()
            .is_empty());
        assert_attempt(&fixture.repository, &grant, "requested", "started", true);
    }
}

#[test]
fn pending_dispatch_and_startup_have_one_transactional_winner() {
    let fixture = Fixture::new(true);
    let pending = pending_turn(&fixture.session());
    fixture
        .repository
        .admit_turn(&pending.admission, &pending.turn, &pending.session, None)
        .unwrap();
    let command = fixture.command("startup_pending_race", "pending_race_instance");
    let mut requested = pending.admission.clone();
    requested.dispatch_state = TurnDispatchState::Dispatching;
    requested.updated_at = 310;
    let gate = Arc::new(Barrier::new(2));
    let startup_repo = fixture.repository.clone();
    let startup_command = command.clone();
    let startup_gate = Arc::clone(&gate);
    let startup = std::thread::spawn(move || {
        startup_gate.wait();
        startup_repo.admit_startup_reattach(&startup_command, 300)
    });
    let sender_repo = fixture.repository.clone();
    let sender = std::thread::spawn(move || {
        gate.wait();
        sender_repo.claim_pending_turn_dispatch(&requested)
    });
    let startup = startup.join().unwrap();
    let sender = sender.join().unwrap();
    let saved = fixture.repository.list_turn_admissions().unwrap();
    match startup {
        Ok(StartupReattachAdmission::Execute(grant)) => {
            assert!(
                matches!(sender,
                    Err(RuntimeError::TurnDispatchNotClaimed { blocking_operation_id: Some(ref owner), .. })
                    if owner == &command.operation_id
                ),
                "{sender:?}"
            );
            assert_eq!(saved, vec![pending.admission]);
            assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        }
        Err(RuntimeError::Conflict(_)) => {
            sender.unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].dispatch_state, TurnDispatchState::Dispatching);
            assert_eq!(saved[0].provider_native_turn_id, None);
            assert!(fixture
                .repository
                .get_operation(&command.operation_id)
                .unwrap()
                .is_none());
        }
        other => panic!("unexpected race disposition: {other:?}; sender={sender:?}"),
    }
}

#[test]
fn startup_claim_refuses_native_approval_reservation_without_changing_pending_decision() {
    let fixture = Fixture::new(true);
    let (waiting, _) = provider_approval(&fixture);
    let grant = fixture.grant("startup_approval_first");
    let reserved = decision(&waiting.approval);
    let before = snapshot(&fixture.repository);
    assert_unresolved(
        fixture.repository.claim_provider_approval_decision(
            &waiting.approval,
            &reserved,
            &waiting.turn,
            &waiting.session,
            &approval_event(&waiting.approval, "reserved"),
        ),
        &grant.intent.command.operation_id,
    );
    assert_eq!(snapshot(&fixture.repository), before);
    fixture
        .repository
        .complete_startup_reattach(&grant, &ack(&grant, false), 350)
        .unwrap();
    fixture
        .repository
        .claim_provider_approval_decision(
            &waiting.approval,
            &reserved,
            &waiting.turn,
            &waiting.session,
            &approval_event(&waiting.approval, "reserved"),
        )
        .unwrap();
    let before = snapshot(&fixture.repository);
    assert!(fixture
        .repository
        .admit_startup_reattach(
            &fixture.command("startup_after_reserved", "after_reserved"),
            450,
        )
        .is_err());
    assert_eq!(snapshot(&fixture.repository), before);
}

#[test]
fn existing_approval_decision_refuses_resume_but_exact_release_and_ack_replay_remain_available() {
    for acknowledged in [false, true] {
        let fixture = Fixture::new(true);
        let (waiting, _) = provider_approval(&fixture);
        let reserved = decision(&waiting.approval);
        fixture
            .repository
            .claim_provider_approval_decision(
                &waiting.approval,
                &reserved,
                &waiting.turn,
                &waiting.session,
                &approval_event(&waiting.approval, "reserved"),
            )
            .unwrap();
        let before = snapshot(&fixture.repository);
        assert!(fixture
            .repository
            .admit_startup_reattach(
                &fixture.command("startup_reserved_blocked", "reserved_blocked"),
                450,
            )
            .is_err());
        assert_eq!(snapshot(&fixture.repository), before);
        if acknowledged {
            let mut resolved = reserved.clone();
            resolved.status = "accept".into();
            resolved.response = Some(json!({"operator":"approved"}));
            resolved.resolved_at = Some(400);
            let mut turn = waiting.turn.clone();
            turn.status = "in_progress".into();
            let mut session = waiting.session.clone();
            session.status = "turn_running".into();
            session.updated_at = 401;
            let event = approval_event(&waiting.approval, "resolved");
            let committed = fixture
                .repository
                .finalize_provider_approval_decision(
                    &reserved,
                    &resolved,
                    &waiting.turn,
                    &turn,
                    &waiting.session,
                    &session,
                    &event,
                )
                .unwrap();
            let grant = execute(
                &fixture.repository,
                &fixture.command("startup_known_approval", "known_approval"),
                500,
            );
            let before = snapshot(&fixture.repository);
            let replay = fixture
                .repository
                .finalize_provider_approval_decision(
                    &reserved,
                    &resolved,
                    &waiting.turn,
                    &turn,
                    &waiting.session,
                    &session,
                    &event,
                )
                .unwrap();
            assert_eq!(replay.approval, committed.approval);
            assert_eq!(replay.turn, committed.turn);
            assert_eq!(replay.session, committed.session);
            assert!(committed.event.is_some());
            assert!(
                replay.event.is_none(),
                "known ACK replay must not republish its event"
            );
            assert_eq!(snapshot(&fixture.repository), before);
            assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        } else {
            let event = approval_event(&waiting.approval, "not_dispatched");
            let committed = fixture
                .repository
                .release_unexecuted_provider_approval_decision(&reserved, &waiting.approval, &event)
                .unwrap();
            let grant = execute(
                &fixture.repository,
                &fixture.command("startup_released_approval", "released_approval"),
                500,
            );
            let before = snapshot(&fixture.repository);
            let replay = fixture
                .repository
                .release_unexecuted_provider_approval_decision(&reserved, &waiting.approval, &event)
                .unwrap();
            assert_eq!(replay.approval, committed.approval);
            assert!(committed.event.is_some());
            assert!(
                replay.event.is_none(),
                "known no-effect replay must not republish its event"
            );
            assert_eq!(snapshot(&fixture.repository), before);
            assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        }
    }
}

#[test]
fn interrupt_admission_and_first_dispatch_obey_resume_claim_but_known_ack_can_settle() {
    for intended_first in [false, true] {
        let fixture = Fixture::new(true);
        let running = running(&fixture);
        let mut interrupt = runtime_core::prepare_workspace_interrupt(
            &fixture.workspace.workspace_id,
            OperationActor::operator("startup-interrupt-test"),
            Some("startup-interrupt-key".into()),
        )
        .unwrap();
        interrupt.requested_at = 240;
        let plan = if intended_first {
            let WorkspaceInterruptAdmission::Execute(plan) = fixture
                .repository
                .begin_workspace_interrupt(&interrupt)
                .unwrap()
            else {
                panic!("fresh interrupt did not produce an intended plan");
            };
            assert_eq!(plan.targets.len(), 1);
            assert_eq!(plan.targets[0].agent_id, SESSION);
            assert_eq!(plan.targets[0].turn_id, running.turn.id);
            Some(plan)
        } else {
            None
        };
        let grant = fixture.grant("startup_interrupt_guard");
        let before = snapshot(&fixture.repository);
        if let Some(plan) = plan {
            assert_unresolved(
                fixture.repository.mark_workspace_interrupt_started(
                    &plan.operation_id,
                    SESSION,
                    &running.turn.id,
                    310,
                ),
                &grant.intent.command.operation_id,
            );
            assert_eq!(snapshot(&fixture.repository), before);
            fixture
                .repository
                .complete_startup_reattach(&grant, &ack(&grant, false), 400)
                .unwrap();
            fixture
                .repository
                .mark_workspace_interrupt_started(
                    &plan.operation_id,
                    SESSION,
                    &running.turn.id,
                    410,
                )
                .unwrap();
            let before = snapshot(&fixture.repository);
            assert_unresolved(
                fixture.repository.admit_startup_reattach(
                    &fixture.command("startup_after_interrupt", "after_interrupt"),
                    420,
                ),
                &plan.operation_id,
            );
            assert_eq!(snapshot(&fixture.repository), before);
            fixture
                .repository
                .finalize_workspace_interrupt_effect(
                    &plan.operation_id,
                    SESSION,
                    &running.turn.id,
                    true,
                    "exact observed native interrupt ACK",
                    430,
                )
                .unwrap();
            let next = execute(
                &fixture.repository,
                &fixture.command("startup_known_interrupt", "known_interrupt"),
                440,
            );
            assert_attempt(&fixture.repository, &next, "requested", "started", true);
        } else {
            assert_unresolved(
                fixture.repository.begin_workspace_interrupt(&interrupt),
                &grant.intent.command.operation_id,
            );
            assert_eq!(snapshot(&fixture.repository), before);
            assert!(fixture
                .repository
                .get_operation(&interrupt.operation_id)
                .unwrap()
                .is_none());
        }
    }
}

fn terminal_approval_fixture(
    fixture: &Fixture,
) -> (
    runtime_core::ProviderApprovalAdmission,
    ApprovalRecord,
    runtime_core::SessionRecord,
    runtime_core::TurnRecord,
) {
    let (waiting, running) = provider_approval(fixture);
    let reserved = decision(&waiting.approval);
    fixture
        .repository
        .claim_provider_approval_decision(
            &waiting.approval,
            &reserved,
            &waiting.turn,
            &waiting.session,
            &approval_event(&waiting.approval, "reserved"),
        )
        .unwrap();
    let waiting_turn = TurnFixture {
        session: waiting.session.clone(),
        turn: waiting.turn.clone(),
        admission: running.admission,
    };
    let (finished_session, finished_turn, _) = terminal(
        &fixture.repository,
        &waiting_turn,
        "completed",
        json!({"source":"startup-sqlite-test","session_transcript":[
            {"role":"user","text":"user prompt"},{"role":"assistant","text":"done"},
        ]}),
    );
    assert_eq!(finished_session.status, "ready");
    assert_eq!(finished_session.active_turn_id, None);
    (waiting, reserved, finished_session, finished_turn)
}

fn unknown_terminal_approval(
    fixture: &Fixture,
    reserved: &ApprovalRecord,
    session: &runtime_core::SessionRecord,
    turn: &runtime_core::TurnRecord,
) {
    let mut unknown = approval_event(reserved, "unknown");
    unknown.payload["error"] = json!("native decision acknowledgement was lost");
    unknown.created_at = 500;
    let receipt = fixture
        .repository
        .quarantine_provider_approval_decision(reserved, turn, turn, session, session, &unknown)
        .unwrap();
    assert_eq!(receipt.approval, *reserved);
    assert_eq!(receipt.approval.status, "decision_pending");
    assert_eq!(receipt.turn, *turn);
    assert_eq!(receipt.session, *session);
    assert_eq!(receipt.event.unwrap().kind, "approval.decision_unknown");
}

fn late_terminal_approval_ack(
    fixture: &Fixture,
    waiting: &runtime_core::ProviderApprovalAdmission,
    reserved: &ApprovalRecord,
) -> Result<runtime_core::ProviderApprovalFinalization, RuntimeError> {
    let mut resolved = reserved.clone();
    resolved.status = "accept".into();
    resolved.response = Some(json!({"operator":"approved"}));
    resolved.resolved_at = Some(570);
    let mut after_turn = waiting.turn.clone();
    after_turn.status = "in_progress".into();
    let mut after_session = waiting.session.clone();
    after_session.status = "turn_running".into();
    after_session.updated_at = 570;
    let mut event = approval_event(reserved, "resolved");
    event.created_at = 570;
    fixture.repository.finalize_provider_approval_decision(
        reserved,
        &resolved,
        &waiting.turn,
        &after_turn,
        &waiting.session,
        &after_session,
        &event,
    )
}

#[test]
fn terminal_turn_does_not_erase_an_unresolved_native_approval_before_startup() {
    for unknown in [false, true] {
        let fixture = Fixture::new(true);
        let (waiting, reserved, session, turn) = terminal_approval_fixture(&fixture);
        if unknown {
            unknown_terminal_approval(&fixture, &reserved, &session, &turn);
        }
        let before = snapshot(&fixture.repository);
        let result = fixture.repository.admit_startup_reattach(
            &fixture.command("startup_terminal_approval", "terminal_approval_instance"),
            550,
        );
        assert!(matches!(result, Err(RuntimeError::Conflict(_))),
            "clearing active_turn_id authorized resume with a native decision unresolved: {result:?}");
        assert_eq!(snapshot(&fixture.repository), before);
        if unknown {
            // No known ACK exists in this branch. Preserve the actual ambiguous
            // decision receipt and reservation; do not fabricate a later result.
            let saved = fixture
                .repository
                .get_persisted_retirement_review_session(SESSION)
                .unwrap()
                .unwrap();
            assert_eq!(saved.approvals, vec![reserved]);
            assert_eq!(saved.session, session);
            assert_eq!(saved.turns, vec![turn]);
            assert_eq!(snapshot(&fixture.repository), before);
        } else {
            let known = late_terminal_approval_ack(&fixture, &waiting, &reserved).unwrap();
            assert_eq!(known.turn, turn);
            assert_eq!(known.session, session);
            assert_eq!(known.approval.status, "accept");
            let grant = execute(
                &fixture.repository,
                &fixture.command(
                    "startup_after_known_terminal_approval",
                    "known_terminal_approval",
                ),
                600,
            );
            assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        }
    }
}

// Explicit historical fixture: the previous startup classifier admitted this
// exact Started operation after a terminal receipt detached decision_pending.
// It is not a currently permitted first-reservation race or a provider bypass.
// Keep the original approval row untouched, and use the same transaction helper
// that publishes the operation, claim, effect, and Critical events atomically.
fn historical_started_startup(fixture: &Fixture) -> runtime_core::StartupReattachGrant {
    use crate::operation_tx::OperationAuthorityTransaction;
    use crate::repository_startup_reattachment::{
        value, STARTUP_ACTOR, STARTUP_EFFECT, STARTUP_EFFECT_KEY, STARTUP_KIND,
    };
    let command = fixture.command(
        "startup_historical_terminal_approval",
        "historical_terminal_approval",
    );
    let mut db = crate::db::open_connection(&fixture.repository.database_path).unwrap();
    let tx = db
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let (_, authority) =
        crate::repository_startup_reattachment_authority::current_authority(&tx, SESSION).unwrap();
    let intent = runtime_core::StartupReattachIntent {
        version: 1,
        command: command.clone(),
        authority,
        pending_operator_turn: None,
        terminal_metadata_witness: None,
    };
    let normalized = value(&intent).unwrap();
    let hash = runtime_core::normalized_json_hash(&normalized).unwrap();
    let authority_tx = OperationAuthorityTransaction::new(&tx);
    authority_tx.insert_operation(&runtime_core::OperationRecord {
        operation_id: command.operation_id.clone(),
        workspace_id: intent.authority.scope.workspace_id().map(str::to_owned),
        kind: STARTUP_KIND.into(),
        actor: OperationActor {
            kind: runtime_core::OperationActorKind::System, identifier: STARTUP_ACTOR.into(),
        },
        idempotency_key: Some(runtime_core::normalized_json_hash(&json!({
            "startup_instance_id":command.startup_instance_id,"session_id":command.session_id,
        })).unwrap()),
        normalized_request_hash: hash.clone(), normalized_request: normalized,
        phase: runtime_core::OperationPhase::Requested, exact_terminal_result: None,
        error_code: None, error_message: None, created_at: 450, updated_at: 450,
    }).unwrap();
    let claim = authority_tx
        .acquire_claim(&command.operation_id, RESUME_CLAIM, SESSION, 450)
        .unwrap();
    let grant = runtime_core::StartupReattachGrant {
        intent,
        normalized_request_hash: hash,
        claim,
    };
    authority_tx
        .insert_effect(&runtime_core::OperationEffectRecord {
            effect_id: format!("{}:resume", command.operation_id),
            operation_id: command.operation_id.clone(),
            effect_kind: STARTUP_EFFECT.into(),
            target_kind: "provider_session".into(),
            target_id: SESSION.into(),
            phase: "started".into(),
            idempotency_key: STARTUP_EFFECT_KEY.into(),
            evidence: Some(json!({"grant":grant})),
            attempt_count: 1,
            created_at: 450,
            updated_at: 450,
        })
        .unwrap();
    authority_tx
        .append_transition(&runtime_core::OperationTransitionRecord {
            operation_id: command.operation_id,
            sequence: 1,
            from_phase: None,
            to_phase: runtime_core::OperationPhase::Requested,
            evidence: Some(json!({"native_resume":"first_attempt_granted","session_id":SESSION})),
            created_at: 450,
        })
        .unwrap();
    tx.commit().unwrap();
    grant
}

#[test]
fn legacy_started_resume_rechecks_detached_decision_before_accepting_native_ack() {
    for unknown in [false, true] {
        let fixture = Fixture::new(true);
        let (waiting, reserved, session, turn) = terminal_approval_fixture(&fixture);
        let grant = historical_started_startup(&fixture);
        if unknown {
            unknown_terminal_approval(&fixture, &reserved, &session, &turn);
        }
        let acknowledgement = ack(&grant, false);
        let before = snapshot(&fixture.repository);
        let result = fixture
            .repository
            .complete_startup_reattach(&grant, &acknowledgement, 550);
        assert!(
            matches!(result, Err(RuntimeError::Conflict(_))),
            "legacy Started resume ignored detached native decision: {result:?}"
        );
        assert_eq!(snapshot(&fixture.repository), before);
        assert!(matches!(
            fixture.repository.hold_startup_reattach(
                &grant, Some(&acknowledgement), "detached native decision remains unresolved", 560,
            ).unwrap(),
            runtime_core::StartupReattachHold::Held { operation_id }
                if operation_id == grant.intent.command.operation_id
        ));
        assert_attempt(
            &fixture.repository,
            &grant,
            "manual_review",
            "observed",
            true,
        );
        assert_eq!(fixture.session(), session);
        if unknown {
            let saved = fixture
                .repository
                .get_persisted_retirement_review_session(SESSION)
                .unwrap()
                .unwrap();
            assert_eq!(saved.approvals, vec![reserved]);
            assert_eq!(saved.session, session);
            assert_eq!(saved.turns, vec![turn]);
        } else {
            // The startup hold must not block settlement of a genuinely known,
            // already reserved approval ACK; terminal projection stays intact.
            let known = late_terminal_approval_ack(&fixture, &waiting, &reserved).unwrap();
            assert_eq!(known.turn, turn);
            assert_eq!(known.session, session);
            let before = snapshot(&fixture.repository);
            assert_unresolved(
                fixture.repository.admit_startup_reattach(
                    &fixture.command("startup_after_historical_hold", "after_historical_hold"),
                    600,
                ),
                &grant.intent.command.operation_id,
            );
            assert_eq!(snapshot(&fixture.repository), before);
        }
        assert_attempt(
            &fixture.repository,
            &grant,
            "manual_review",
            "observed",
            true,
        );
    }
}
