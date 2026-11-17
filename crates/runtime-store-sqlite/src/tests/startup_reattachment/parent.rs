use super::support::*;
use crate::db::open_connection;
use crate::SqliteRuntimeRepository;
use runtime_core::{
    ApprovalRecord, OperationActor, OperatorUserTurnCommand, OperatorUserTurnReplay, RuntimeError,
    TurnDispatchState,
};
use rusqlite::params;
use serde_json::json;
use std::collections::BTreeMap;

const PARENT: &str = "startup_original_operator_parent";
const PARENT_CLAIM: &str = "operator_user_turn_native";
const CHILD_TEST: &str = "tests::startup_reattachment::parent::original_operator_sender_child";

pub(super) struct OperatorParent {
    pub command: OperatorUserTurnCommand,
    pub pending: TurnFixture,
    pub approval: Option<ApprovalRecord>,
}

pub(super) fn prepare_operator_parent(
    fixture: &Fixture,
    approval: bool,
    explicit: bool,
) -> OperatorParent {
    let mut pending = pending_turn(&fixture.session());
    if approval {
        pending.admission.dispatch_policy.permission_mode = Some("require_approval".into());
        pending
            .admission
            .dispatch_policy
            .pre_dispatch_approval_required = true;
        pending.turn.status = "waiting_for_approval".into();
        pending.session.status = "waiting_for_approval".into();
    }
    let policy_approval = approval.then(|| ApprovalRecord {
        id: "startup_policy_approval".into(),
        session_id: SESSION.into(),
        turn_id: pending.turn.id.clone(),
        origin: "runtime_pre_dispatch_policy".into(),
        tool_call_id: None,
        provider_approval_ref: None,
        status: "pending".into(),
        request: json!({"reason":"manual approval before provider dispatch"}),
        response: None,
        created_at: 200,
        resolved_at: None,
    });
    let normalized = json!({
        "agent_id":SESSION,"input":pending.turn.input,"expected_turn_id":null,
        "permission_mode":if approval || explicit {
            json!(pending.admission.dispatch_policy.permission_mode)
        } else { serde_json::Value::Null },
        "turn_settings":pending.admission.dispatch_policy.turn_settings,
    });
    let command = OperatorUserTurnCommand {
        operation_id: PARENT.into(),
        agent_id: SESSION.into(),
        workspace_id: fixture.workspace.workspace_id.clone(),
        actor: OperationActor::operator("startup-original-operator"),
        idempotency_key: "startup-original-operator-key".into(),
        normalized_request_hash: runtime_core::normalized_json_hash(&normalized).unwrap(),
        normalized_request: normalized,
        requested_at: 200,
    };
    OperatorParent {
        command,
        pending,
        approval: policy_approval,
    }
}

pub(super) fn operator_parent(fixture: &Fixture, approval: bool, explicit: bool) -> OperatorParent {
    let parent = prepare_operator_parent(fixture, approval, explicit);
    fixture
        .repository
        .admit_operator_user_turn(
            &parent.command,
            &parent.pending.admission,
            &parent.pending.turn,
            &parent.pending.session,
            parent.approval.as_ref(),
        )
        .unwrap();
    parent
}

fn parent_rows(repository: &SqliteRuntimeRepository) -> BTreeMap<String, Vec<Vec<String>>> {
    let db = open_connection(&repository.database_path).unwrap();
    [
        ("runtime_operations", "operation_id"),
        ("runtime_operation_effects", "operation_id"),
        ("runtime_operation_transitions", "operation_id"),
        ("runtime_operation_resource_claims", "owner_operation_id"),
    ]
    .into_iter()
    .map(|(table, owner)| {
        let mut statement = db
            .prepare(&format!("SELECT * FROM {table} WHERE {owner}=?1"))
            .unwrap();
        let columns = statement.column_count();
        let mut rows = statement
            .query_map([PARENT], |row| {
                (0..columns)
                    .map(|column| Ok(format!("{:?}", row.get_ref(column)?)))
                    .collect::<Result<Vec<_>, rusqlite::Error>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.sort();
        (table.into(), rows)
    })
    .collect()
}

fn assert_parent_requested(repository: &SqliteRuntimeRepository, parent: &OperatorParent) {
    let db = open_connection(&repository.database_path).unwrap();
    let row: (String, String, i64, String, String) = db
        .query_row(
            "SELECT o.phase,e.phase,e.attempt_count,e.target_id,o.normalized_request_hash
         FROM runtime_operations o JOIN runtime_operation_effects e USING(operation_id)
         WHERE o.operation_id=?1",
            [PARENT],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (
            "requested".into(),
            "intended".into(),
            0,
            parent.pending.turn.id.clone(),
            parent.command.normalized_request_hash.clone()
        )
    );
    assert_eq!(
        repository
            .unresolved_operator_user_turn_for_agent(SESSION)
            .unwrap()
            .as_deref(),
        Some(PARENT)
    );
    let claims: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE resource_kind=?1
         AND resource_id=?2 AND owner_operation_id=?3",
            params![PARENT_CLAIM, SESSION, PARENT],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(claims, 1);
}

fn replay(repository: &SqliteRuntimeRepository, parent: &OperatorParent) -> OperatorUserTurnReplay {
    repository
        .operator_user_turn_replay(
            &parent.command.actor,
            &parent.command.idempotency_key,
            SESSION,
            &parent.command.normalized_request_hash,
        )
        .unwrap()
        .unwrap()
}

fn not_dispatched(repository: &SqliteRuntimeRepository, pending: &TurnFixture) -> TurnFixture {
    let message = "provider dispatch not_dispatched: rejected before native send";
    let mut admission = pending.admission.clone();
    admission.dispatch_state = TurnDispatchState::NotDispatched;
    admission.dispatch_error = Some(json!({"code":"startup_send_refused","message":message}));
    admission.updated_at = 220;
    let mut turn = pending.turn.clone();
    turn.status = "failed".into();
    turn.completed_at = Some(220);
    turn.error = Some(json!({"message":message,"provider_dispatch_outcome":"not_dispatched"}));
    let mut session = pending.session.clone();
    session.status = "ready".into();
    session.active_turn_id = None;
    session.updated_at = 220;
    let receipt = repository
        .commit_provider_dispatch_outcome(
            &pending.admission,
            &admission,
            &pending.turn,
            &turn,
            &pending.session,
            &session,
            &[],
            &event(
                &turn,
                "not_dispatched",
                "turn.failed",
                json!({"error":message,"provider_dispatch_outcome":"not_dispatched"}),
                220,
            ),
        )
        .unwrap();
    TurnFixture {
        session: receipt.session,
        turn: receipt.turn,
        admission: receipt.admission,
    }
}

#[test]
fn original_operator_sender_child() {
    let Some(root) = std::env::var_os("GOOSELAKE_STARTUP_PARENT_OWNED_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    assert_eq!(
        std::fs::read_to_string(root.join("operator-parent-marker")).unwrap(),
        "owned-startup-operator-parent-v1"
    );
    let repository = SqliteRuntimeRepository::new(root.join("runtime.sqlite3"));
    let projection = repository
        .get_persisted_retirement_review_session(SESSION)
        .unwrap()
        .unwrap();
    let mut requested = projection
        .admissions
        .into_iter()
        .find(|a| a.turn_id == "startup_sqlite_turn")
        .unwrap();
    requested.dispatch_state = TurnDispatchState::Dispatching;
    requested.updated_at = 210;
    let before = snapshot(&repository);
    let result = repository.claim_pending_turn_dispatch(&requested);
    assert!(
        matches!(result,
            Err(RuntimeError::TurnDispatchNotClaimed {
                ref turn_id, blocking_operation_id: Some(ref operation_id),
            }) if turn_id == &requested.turn_id && operation_id == "startup_parent_resume"
        ),
        "{result:?}"
    );
    assert_eq!(snapshot(&repository), before);
    assert_eq!(
        repository
            .unresolved_operator_user_turn_for_agent(SESSION)
            .unwrap()
            .as_deref(),
        Some(PARENT)
    );
}

#[test]
fn independent_original_sender_keeps_exact_pending_parent_until_resume_receipt() {
    let fixture = Fixture::new(true);
    let parent = operator_parent(&fixture, false, false);
    let original_rows = parent_rows(&fixture.repository);
    let pending = fixture
        .repository
        .settle_startup_operator_turn(SESSION, 202)
        .unwrap()
        .unwrap();
    assert_eq!(pending.operation_id, PARENT);
    assert_eq!(
        pending.normalized_request_hash,
        parent.command.normalized_request_hash
    );
    assert_eq!(pending.admission, parent.pending.admission);
    assert_eq!(parent_rows(&fixture.repository), original_rows);
    let grant = execute(
        &fixture.repository,
        &fixture.command("startup_parent_resume", "resume_instance"),
        205,
    );
    assert_eq!(grant.intent.pending_operator_turn.as_ref(), Some(&pending));
    assert_parent_requested(&fixture.repository, &parent);
    std::fs::write(
        fixture.temp.path().join("operator-parent-marker"),
        "owned-startup-operator-parent-v1",
    )
    .unwrap();
    let before = snapshot(&fixture.repository);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env("GOOSELAKE_STARTUP_PARENT_OWNED_ROOT", fixture.temp.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    assert_eq!(snapshot(&fixture.repository), before);
    assert_eq!(parent_rows(&fixture.repository), original_rows);
    assert!(matches!(replay(&fixture.repository, &parent),
        OperatorUserTurnReplay::Unresolved { operation_id } if operation_id == PARENT));
    assert_eq!(
        fixture.repository.list_turn_admissions().unwrap(),
        vec![parent.pending.admission.clone()]
    );
    fixture
        .repository
        .complete_startup_reattach(&grant, &ack(&grant, false), 206)
        .unwrap();
    let running = start_pending(&fixture.repository, &parent.pending);
    assert!(fixture
        .repository
        .settle_startup_operator_turn(SESSION, 230)
        .unwrap()
        .is_none());
    let OperatorUserTurnReplay::Completed {
        operation_id,
        accepted,
    } = replay(&fixture.repository, &parent)
    else {
        panic!("original parent did not settle from its single known dispatch");
    };
    assert_eq!(operation_id, PARENT);
    assert_eq!(
        (accepted.session_id, accepted.turn_id, accepted.status),
        (SESSION.into(), running.turn.id, "in_progress".into())
    );
    assert_eq!(
        fixture
            .repository
            .unresolved_operator_user_turn_for_agent(SESSION)
            .unwrap(),
        None
    );
    let mut stale = parent.pending.admission;
    stale.dispatch_state = TurnDispatchState::Dispatching;
    stale.updated_at = 240;
    let before = snapshot(&fixture.repository);
    assert!(matches!(
        fixture.repository.claim_pending_turn_dispatch(&stale),
        Err(RuntimeError::TurnDispatchNotClaimed {
            blocking_operation_id: None,
            ..
        })
    ));
    assert_eq!(snapshot(&fixture.repository), before);
}

#[test]
fn known_operator_results_settle_before_d4_or_provider_quarantine_blocks_resume() {
    for result_kind in 0..3 {
        let fixture = Fixture::new(true);
        let parent = operator_parent(&fixture, false, false);
        if result_kind == 0 {
            not_dispatched(&fixture.repository, &parent.pending);
        } else {
            let running = start_pending(&fixture.repository, &parent.pending);
            if result_kind == 2 {
                quarantine(&fixture.repository, &running);
            }
        }
        let db = open_connection(&fixture.repository.database_path).unwrap();
        if result_kind != 2 {
            db.execute(
                "UPDATE sessions SET status='policy_recovery_required',updated_at=400 WHERE id=?1",
                [SESSION],
            )
            .unwrap();
        }
        // Historical known-result settlement must not require a fresh Pending-input witness.
        db.execute(
            "UPDATE turn_admissions SET user_input_snapshot_json=?2 WHERE turn_id=?1",
            params![
                parent.pending.turn.id,
                json!({
                    "prompt_text":"historical flattened snapshot","image_refs":[],"invocation":null,
                })
                .to_string()
            ],
        )
        .unwrap();
        let projection = fixture
            .repository
            .get_persisted_retirement_review_session(SESSION)
            .unwrap()
            .unwrap();
        assert!(fixture
            .repository
            .settle_startup_operator_turn(SESSION, 500)
            .unwrap()
            .is_none());
        assert_eq!(
            fixture
                .repository
                .get_persisted_retirement_review_session(SESSION)
                .unwrap()
                .unwrap(),
            projection
        );
        match replay(&fixture.repository, &parent) {
            OperatorUserTurnReplay::NotDispatched {
                operation_id,
                code,
                message,
            } if result_kind == 0 => {
                assert_eq!(operation_id, PARENT);
                assert_eq!(code, "startup_send_refused");
                assert_eq!(message, "rejected before native send");
            }
            OperatorUserTurnReplay::Completed {
                operation_id,
                accepted,
            } if result_kind != 0 => {
                assert_eq!(operation_id, PARENT);
                assert_eq!(accepted.turn_id, parent.pending.turn.id);
                assert_eq!(accepted.status, "in_progress");
            }
            other => panic!("wrong known parent result: {other:?}"),
        }
        assert_eq!(
            fixture
                .repository
                .unresolved_operator_user_turn_for_agent(SESSION)
                .unwrap(),
            None
        );
        let before = snapshot(&fixture.repository);
        assert!(fixture
            .repository
            .admit_startup_reattach(
                &fixture.command("startup_after_known", "blocked_restart"),
                501,
            )
            .is_err());
        assert_eq!(snapshot(&fixture.repository), before);
    }
}

#[test]
fn known_parent_receipt_fault_rolls_back_then_settles_without_a_native_retry() {
    for dispatched in [false, true] {
        let fixture = Fixture::new(true);
        let parent = operator_parent(&fixture, false, false);
        if dispatched {
            start_pending(&fixture.repository, &parent.pending);
        } else {
            not_dispatched(&fixture.repository, &parent.pending);
        }
        let db = open_connection(&fixture.repository.database_path).unwrap();
        db.execute_batch(
            "CREATE TRIGGER startup_parent_receipt_fault BEFORE INSERT ON runtime_operation_transitions
             WHEN NEW.operation_id='startup_original_operator_parent' AND NEW.sequence=2
             BEGIN SELECT RAISE(ABORT,'parent receipt injection'); END;"
        ).unwrap();
        let before = snapshot(&fixture.repository);
        assert!(fixture
            .repository
            .settle_startup_operator_turn(SESSION, 500)
            .is_err());
        assert_eq!(snapshot(&fixture.repository), before);
        assert_parent_requested(&fixture.repository, &parent);
        db.execute_batch("DROP TRIGGER startup_parent_receipt_fault")
            .unwrap();
        assert!(fixture
            .repository
            .settle_startup_operator_turn(SESSION, 501)
            .unwrap()
            .is_none());
        let before = snapshot(&fixture.repository);
        assert!(fixture
            .repository
            .settle_startup_operator_turn(SESSION, 502)
            .unwrap()
            .is_none());
        assert_eq!(snapshot(&fixture.repository), before);
    }
}

#[test]
fn exact_pending_snapshot_policy_event_and_owner_damage_preserve_original_coordinate() {
    for corruption in 0..10 {
        let fixture = Fixture::new(true);
        let parent = operator_parent(&fixture, false, true);
        let db = open_connection(&fixture.repository.database_path).unwrap();
        let admitted = format!("evt_turn_{}_admitted", parent.pending.turn.id);
        match corruption {
            0 => {
                db.execute("UPDATE turn_admissions SET user_input_snapshot_json=json_set(user_input_snapshot_json,'$.prompt_text','other input') WHERE turn_id=?1", [&parent.pending.turn.id]).unwrap();
            }
            1 => {
                db.execute("UPDATE turn_admissions SET dispatch_policy_json=json_set(dispatch_policy_json,'$.permission_mode','read_only') WHERE turn_id=?1", [&parent.pending.turn.id]).unwrap();
            }
            2 => {
                db.execute("UPDATE turn_admissions SET dispatch_policy_json=json_set(dispatch_policy_json,'$.pre_dispatch_approval_required',json('true')) WHERE turn_id=?1", [&parent.pending.turn.id]).unwrap();
            }
            3 => {
                db.execute("UPDATE runtime_events SET payload_json=json_set(payload_json,'$.extra',true) WHERE event_id=?1", [&admitted]).unwrap();
            }
            4 => {
                db.execute(
                    "UPDATE runtime_events SET scope_id='other-owner' WHERE event_id=?1",
                    [&admitted],
                )
                .unwrap();
            }
            5 => {
                db.execute(
                    "UPDATE runtime_events SET created_at=created_at+1 WHERE event_id=?1",
                    [&admitted],
                )
                .unwrap();
            }
            6 => {
                db.execute(
                    "UPDATE runtime_events SET critical=0 WHERE event_id=?1",
                    [&admitted],
                )
                .unwrap();
            }
            7 => {
                db.execute(
                    "DELETE FROM runtime_operation_resource_claims WHERE owner_operation_id=?1",
                    [PARENT],
                )
                .unwrap();
            }
            8 => {
                db.execute("UPDATE runtime_operations SET normalized_request_hash='wrong-hash' WHERE operation_id=?1", [PARENT]).unwrap();
            }
            9 => {
                db.execute("UPDATE runtime_operation_effects SET target_id='other-turn' WHERE operation_id=?1", [PARENT]).unwrap();
            }
            _ => unreachable!(),
        }
        let before = snapshot(&fixture.repository);
        assert_unresolved(
            fixture
                .repository
                .settle_startup_operator_turn(SESSION, 300),
            PARENT,
        );
        assert_unresolved(
            fixture.repository.admit_startup_reattach(
                &fixture.command("startup_after_corruption", "corrupt_restart"),
                301,
            ),
            PARENT,
        );
        assert_eq!(
            snapshot(&fixture.repository),
            before,
            "corruption {corruption}"
        );
    }
}

#[test]
fn inherited_pending_policy_is_not_recomputed_and_local_approval_wait_is_known() {
    let fixture = Fixture::new(true);
    let parent = operator_parent(&fixture, false, false);
    open_connection(&fixture.repository.database_path)
        .unwrap()
        .execute(
            "UPDATE sessions SET permission_mode='read_only',updated_at=201 WHERE id=?1",
            [SESSION],
        )
        .unwrap();
    let before = snapshot(&fixture.repository);
    let pending = fixture
        .repository
        .settle_startup_operator_turn(SESSION, 202)
        .unwrap()
        .unwrap();
    assert_eq!(pending.admission, parent.pending.admission);
    assert_eq!(
        pending.admission.dispatch_policy.permission_mode.as_deref(),
        Some("workspace_write")
    );
    assert_eq!(snapshot(&fixture.repository), before);

    let fixture = Fixture::new(true);
    let parent = operator_parent(&fixture, true, true);
    let original_projection = fixture
        .repository
        .get_persisted_retirement_review_session(SESSION)
        .unwrap()
        .unwrap();
    assert!(fixture
        .repository
        .settle_startup_operator_turn(SESSION, 202)
        .unwrap()
        .is_none());
    let OperatorUserTurnReplay::Completed {
        operation_id,
        accepted,
    } = replay(&fixture.repository, &parent)
    else {
        panic!("local approval wait is a known accepted result");
    };
    assert_eq!(operation_id, PARENT);
    assert_eq!(accepted.status, "waiting_for_approval");
    assert_eq!(accepted.turn_id, parent.pending.turn.id);
    let grant = fixture.grant("startup_waiting_approval");
    assert!(grant.intent.pending_operator_turn.is_none());
    fixture
        .repository
        .complete_startup_reattach(&grant, &ack(&grant, false), 400)
        .unwrap();
    assert_eq!(
        fixture
            .repository
            .get_persisted_retirement_review_session(SESSION)
            .unwrap()
            .unwrap(),
        original_projection
    );
    assert_eq!(parent.approval.unwrap().status, "pending");
}

#[test]
fn dispatching_and_unknown_operator_parent_never_become_fresh_startup_authority() {
    for unknown in [false, true] {
        let fixture = Fixture::new(true);
        let parent = operator_parent(&fixture, false, false);
        let mut dispatching = parent.pending.admission.clone();
        dispatching.dispatch_state = TurnDispatchState::Dispatching;
        dispatching.updated_at = 210;
        fixture
            .repository
            .claim_pending_turn_dispatch(&dispatching)
            .unwrap();
        if unknown {
            let message = "native send outcome is unknown";
            let mut admission = dispatching.clone();
            admission.dispatch_state = TurnDispatchState::Unknown;
            admission.dispatch_error = Some(json!({"code":"send_unknown","message":message}));
            admission.updated_at = 220;
            let mut turn = parent.pending.turn.clone();
            turn.status = "dispatch_unknown".into();
            turn.error = Some(json!({"message":message,"provider_dispatch_outcome":"unknown"}));
            let mut session = parent.pending.session.clone();
            session.status = "turn_recovery_required".into();
            session.updated_at = 220;
            fixture
                .repository
                .commit_provider_dispatch_outcome(
                    &dispatching,
                    &admission,
                    &parent.pending.turn,
                    &turn,
                    &parent.pending.session,
                    &session,
                    &[],
                    &event(
                        &turn,
                        "dispatch_unknown",
                        "turn.dispatch_unknown",
                        json!({"error":message,"provider_dispatch_outcome":"unknown"}),
                        220,
                    ),
                )
                .unwrap();
        }
        let before = snapshot(&fixture.repository);
        assert_unresolved(
            fixture
                .repository
                .settle_startup_operator_turn(SESSION, 300),
            PARENT,
        );
        assert_unresolved(
            fixture.repository.admit_startup_reattach(
                &fixture.command("startup_unknown_parent", "unknown_parent_restart"),
                301,
            ),
            PARENT,
        );
        assert_eq!(snapshot(&fixture.repository), before);
        assert_parent_requested(&fixture.repository, &parent);
    }
}

#[test]
fn accepted_local_policy_keeps_original_pending_parent_available_for_one_later_dispatch() {
    let fixture = Fixture::new(true);
    let parent = operator_parent(&fixture, true, true);
    let mut accepted = parent.approval.clone().unwrap();
    accepted.status = "accept".into();
    accepted.response = Some(json!({"operator":"approved"}));
    accepted.resolved_at = Some(201);
    fixture.repository.upsert_approval(&accepted).unwrap();
    let mut accepted_event = event(
        &parent.pending.turn,
        "policy_accepted",
        "approval.resolved",
        json!({"approval_id":accepted.id,"origin":"runtime_pre_dispatch_policy","decision":"accept"}),
        201,
    );
    accepted_event.event_id = format!("evt_approval_{}_resolved", accepted.id);
    fixture
        .repository
        .append_runtime_event(&accepted_event)
        .unwrap();
    let before = snapshot(&fixture.repository);
    let pending = fixture
        .repository
        .settle_startup_operator_turn(SESSION, 202)
        .unwrap()
        .unwrap();
    assert_eq!(pending.operation_id, PARENT);
    assert_eq!(pending.admission, parent.pending.admission);
    assert_eq!(snapshot(&fixture.repository), before);
    let grant = execute(
        &fixture.repository,
        &fixture.command("startup_accepted_policy", "accepted_policy"),
        205,
    );
    assert_eq!(grant.intent.pending_operator_turn.as_ref(), Some(&pending));
    fixture
        .repository
        .complete_startup_reattach(&grant, &ack(&grant, false), 206)
        .unwrap();
    assert_parent_requested(&fixture.repository, &parent);
    assert_eq!(
        fixture.repository.list_turn_admissions().unwrap(),
        vec![parent.pending.admission]
    );
}
