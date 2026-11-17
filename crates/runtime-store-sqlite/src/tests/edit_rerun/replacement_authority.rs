//! Positive replacement evidence and revocation of a captured editor bundle.
use super::critical_receipts::{
    child, database, fixture, fork, native_started, pending_bundle, replacement, Fixture,
};
use crate::repository_turn_terminal::read_session;
use crate::tests::legacy_native_turn_guards::raw_state;
use crate::SqliteRuntimeRepository;
use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
    SessionControlCoordinate, TurnAdmissionRecord, TurnDispatchState,
};
use serde_json::json;

fn assert_original_owner(error: RuntimeError, f: &Fixture, replacement_id: Option<&str>) {
    match error {
        RuntimeError::SessionControlUnresolved {
            coordinate:
                SessionControlCoordinate::EditRerun {
                    agent_id,
                    operation_id,
                },
            replacement_turn_id,
            dispatch_outcome: None,
            ..
        } => {
            assert_eq!(agent_id, f.operation.agent_id);
            assert_eq!(operation_id, f.operation.operation_id);
            assert_eq!(replacement_turn_id.as_deref(), replacement_id);
        }
        other => panic!("lost original edit owner: {other:?}"),
    }
}

fn complete(f: &Fixture, id: &str) -> runtime_core::EditRerunTransitionReceipt {
    f.repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "completed",
            None,
            Some(id),
            None,
            500,
        )
        .unwrap()
}

fn terminalize(f: &Fixture, admission: &TurnAdmissionRecord) {
    let state = f.repo.hydrate_runtime_state().unwrap();
    let before = state
        .turns
        .into_iter()
        .find(|t| t.id == admission.turn_id)
        .unwrap();
    let previous_session = read_session(&database(f), &f.operation.agent_id).unwrap();
    let mut turn = before.clone();
    turn.status = "completed".into();
    turn.completed_at = Some(400);
    turn.usage = Some(json!({"last_message":"known replacement result"}));
    let mut session = previous_session.clone();
    session.status = "ready".into();
    session.active_turn_id = None;
    session.updated_at = 400;
    session.metadata = runtime_core::terminal_session_metadata(&session.metadata, &turn);
    let event = NewRuntimeEvent {
        event_id: format!("evt_turn_{}_terminal", turn.id),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: "turn.completed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: runtime_core::terminal_turn_event_payload(&turn),
        provider: None,
        provider_seq: None,
        created_at: 400,
    };
    f.repo
        .commit_provider_turn_result(
            &before,
            &turn,
            Some(&previous_session),
            Some(&session),
            &event,
        )
        .unwrap();
}

#[test]
fn captured_child_bundle_cannot_execute_after_other_handle_recovers_and_reviews_owner() {
    let f = fixture();
    fork(&f);
    let captured = pending_bundle(&f, "stale-original-replacement", true);
    let other = SqliteRuntimeRepository::new(f.temp.path().join("runtime.sqlite3"));
    let recovered = other
        .recover_session_edit_reruns_with_receipts(300)
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].operation.phase, "manual_review");
    let reviewed = other
        .resolve_session_edit_rerun_review_with_receipt(
            &f.operation.operation_id,
            "known child independently inspected; no replacement admitted",
            400,
        )
        .unwrap();
    assert_eq!(reviewed.operation.phase, "failed");
    assert_eq!(
        reviewed.operation.child_provider_session_ref,
        Some(child(&f).provider_session_ref)
    );
    let db = database(&f);
    let superseded: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM session_edit_rerun_superseded_turns WHERE operation_id=?1",
            [&f.operation.operation_id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(superseded > 0);
    let before = raw_state(&db);
    let error = f
        .repo
        .admit_turn(&captured.2, &captured.1, &captured.0, None)
        .unwrap_err();
    assert_original_owner(error, &f, None);
    assert_eq!(raw_state(&db), before);
    assert_eq!(
        read_session(&db, &f.operation.agent_id)
            .unwrap()
            .provider_session_ref,
        reviewed.operation.child_provider_session_ref
    );

    // Non-vacuity: the actual child is ready and ordinary new work is legal.
    let ordinary = pending_bundle(&f, "new-unrelated-turn", false);
    f.repo
        .admit_turn(&ordinary.2, &ordinary.1, &ordinary.0, None)
        .unwrap();
    assert_eq!(
        read_session(&db, &f.operation.agent_id)
            .unwrap()
            .active_turn_id,
        Some(ordinary.1.id)
    );
}

#[test]
fn terminal_and_already_admitted_edits_reject_a_distinct_second_replacement() {
    for completed in [false, true] {
        let f = fixture();
        fork(&f);
        let original = replacement(&f, "original-replacement");
        terminalize(&f, &original);
        if completed {
            complete(&f, &original.turn_id);
        }
        let captured = pending_bundle(&f, "different-second-replacement", true);
        assert_eq!(
            read_session(&database(&f), &f.operation.agent_id)
                .unwrap()
                .active_turn_id,
            None
        );
        let before = raw_state(&database(&f));
        let error = f
            .repo
            .admit_turn(&captured.2, &captured.1, &captured.0, None)
            .unwrap_err();
        assert_original_owner(error, &f, completed.then_some(original.turn_id.as_str()));
        assert_eq!(raw_state(&database(&f)), before);
    }
}

#[test]
fn exact_original_replacement_settles_after_known_terminal_and_replays_after_new_owner() {
    for recover in [false, true] {
        let f = fixture();
        fork(&f);
        let original = replacement(&f, "original-replacement");
        terminalize(&f, &original);
        let receipt = if recover {
            let mut receipts = f
                .repo
                .recover_session_edit_reruns_with_receipts(500)
                .unwrap();
            assert_eq!(receipts.len(), 1);
            receipts.remove(0)
        } else {
            complete(&f, &original.turn_id)
        };
        assert!(receipt.changed);
        assert_eq!(receipt.operation.phase, "completed");
        assert_eq!(
            receipt.operation.replacement_turn_id.as_deref(),
            Some(original.turn_id.as_str())
        );
        let ordinary = pending_bundle(&f, "later-owner-turn", false);
        f.repo
            .admit_turn(&ordinary.2, &ordinary.1, &ordinary.0, None)
            .unwrap();
        let saved = raw_state(&database(&f));
        let replay = complete(&f, &original.turn_id);
        assert!(!replay.changed);
        assert_eq!(replay.operation, receipt.operation);
        assert_eq!(raw_state(&database(&f)), saved);
    }
}

#[test]
fn fresh_replacement_must_match_child_snapshot_and_exact_ordered_native_input() {
    for mutation in [
        "provider",
        "source",
        "snapshot",
        "input",
        "child",
        "canonical",
    ] {
        let f = fixture();
        fork(&f);
        let (mut session, mut turn, mut admission) =
            pending_bundle(&f, "changed-replacement", true);
        match mutation {
            "provider" => admission.provider = "claude".into(),
            "source" => turn.source = Some("team".into()),
            "snapshot" => admission.user_input_snapshot.prompt_text = "other snapshot".into(),
            "input" => turn.input = json!([{"type":"text","text":"different native input"}]),
            "child" => session.provider_session_ref = Some("other-child".into()),
            "canonical" => session.canonical_provider_session_ref = Some("other-canonical".into()),
            _ => unreachable!(),
        }
        let before = raw_state(&database(&f));
        assert!(
            f.repo
                .admit_turn(&admission, &turn, &session, None)
                .is_err(),
            "{mutation}"
        );
        assert_eq!(raw_state(&database(&f)), before, "{mutation}");
    }
}

#[test]
fn missing_or_damaged_admission_witness_holds_replacement_and_never_proves_absence() {
    for corruption in [
        "missing", "scope_id", "payload", "time", "provider", "critical",
    ] {
        let f = fixture();
        fork(&f);
        let admission = replacement(&f, "actual-replacement");
        let db = database(&f);
        let event_id = format!("evt_turn_{}_admitted", admission.turn_id);
        match corruption {
            "missing" => {
                db.execute("DELETE FROM runtime_events WHERE event_id=?1", [&event_id])
                    .unwrap();
            }
            "scope_id" => {
                db.execute(
                    "UPDATE runtime_events SET scope_id='wrong-scope' WHERE event_id=?1",
                    [&event_id],
                )
                .unwrap();
            }
            "payload" => {
                db.execute(
                    "UPDATE runtime_events SET payload_json='{}' WHERE event_id=?1",
                    [&event_id],
                )
                .unwrap();
            }
            "time" => {
                db.execute(
                    "UPDATE runtime_events SET created_at=created_at+1 WHERE event_id=?1",
                    [&event_id],
                )
                .unwrap();
            }
            "provider" => {
                db.execute(
                    "UPDATE runtime_events SET provider='codex' WHERE event_id=?1",
                    [&event_id],
                )
                .unwrap();
            }
            "critical" => {
                db.execute(
                    "UPDATE runtime_events SET critical=0 WHERE event_id=?1",
                    [&event_id],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = raw_state(&db);
        assert!(
            f.repo
                .advance_session_edit_rerun_with_receipt(
                    &f.operation.operation_id,
                    "completed",
                    None,
                    Some(&admission.turn_id),
                    None,
                    500,
                )
                .is_err(),
            "{corruption}"
        );
        assert_eq!(raw_state(&db), before);
        let recovered = f
            .repo
            .recover_session_edit_reruns_with_receipts(600)
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].operation.phase, "manual_review");
        assert!(recovered[0].operation.replacement_turn_id.is_none());
        terminalize(&f, &admission);
        let held = raw_state(&db);
        assert!(f
            .repo
            .resolve_session_edit_rerun_review_with_receipt(
                &f.operation.operation_id,
                "cannot treat missing positive proof as no replacement",
                700,
            )
            .is_err());
        assert_eq!(raw_state(&db), held);
        assert!(f
            .repo
            .recover_session_edit_reruns_with_receipts(800)
            .unwrap()
            .is_empty());
        assert_eq!(raw_state(&db), held);
    }
}

#[test]
fn second_wrong_provider_or_orphan_candidate_cannot_hide_beside_valid_replacement() {
    for orphan in [false, true] {
        let f = fixture();
        fork(&f);
        let admission = replacement(&f, "actual-replacement");
        let db = database(&f);
        if orphan {
            // Deliberately damaged retained history, never executable admission.
            db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
        } else {
            let mut turn = f
                .repo
                .hydrate_runtime_state()
                .unwrap()
                .turns
                .into_iter()
                .find(|row| row.id == admission.turn_id)
                .unwrap();
            turn.id = "second-candidate".into();
            f.repo.upsert_turn(&turn).unwrap();
        }
        db.execute(
            "INSERT INTO turn_admissions
             (turn_id,session_id,provider,projection_source,user_input_snapshot_json,
              dispatch_policy_json,correlation_json,dispatch_state,provider_native_turn_id,
              dispatch_error_json,admitted_at,updated_at)
             SELECT 'second-candidate',session_id,'claude',projection_source,user_input_snapshot_json,
                    dispatch_policy_json,correlation_json,dispatch_state,NULL,
                    dispatch_error_json,admitted_at,updated_at
             FROM turn_admissions WHERE turn_id=?1", [&admission.turn_id],
        ).unwrap();
        let before = raw_state(&db);
        assert!(f
            .repo
            .advance_session_edit_rerun_with_receipt(
                &f.operation.operation_id,
                "completed",
                None,
                Some(&admission.turn_id),
                None,
                500,
            )
            .is_err());
        assert_eq!(raw_state(&db), before);
        let receipts = f
            .repo
            .recover_session_edit_reruns_with_receipts(600)
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].operation.phase, "manual_review");
        assert!(receipts[0].operation.replacement_turn_id.is_none());
        assert_eq!(
            f.repo
                .list_turn_admissions()
                .unwrap()
                .iter()
                .filter(|row| row.correlation.correlation_id.as_deref()
                    == Some(f.operation.operation_id.as_str()))
                .count(),
            2
        );
    }
}

#[test]
fn known_admission_settlement_preserves_each_original_dispatch_classification() {
    for dispatch in [
        TurnDispatchState::Pending,
        TurnDispatchState::Dispatched,
        TurnDispatchState::NotDispatched,
        TurnDispatchState::Unknown,
    ] {
        let f = fixture();
        fork(&f);
        let mut admission = replacement(&f, "actual-replacement");
        admission.dispatch_state = dispatch;
        admission.updated_at += 1;
        if dispatch == TurnDispatchState::Dispatched {
            admission.provider_native_turn_id = Some("native-known-replacement".into());
        }
        if matches!(
            dispatch,
            TurnDispatchState::NotDispatched | TurnDispatchState::Unknown
        ) {
            admission.dispatch_error =
                Some(json!({"code":"original-outcome","message":"retained native outcome"}));
        }
        f.repo.upsert_turn_admission(&admission).unwrap();
        let receipt = complete(&f, &admission.turn_id);
        assert_eq!(receipt.operation.phase, "completed");
        let saved = f
            .repo
            .list_turn_admissions()
            .unwrap()
            .into_iter()
            .find(|row| row.turn_id == admission.turn_id)
            .unwrap();
        assert_eq!(
            saved, admission,
            "edit settlement must not reclassify the native turn"
        );
    }
}

#[test]
fn recovery_batch_late_receipt_or_carrier_fault_rolls_back_every_changed_edit() {
    for fault in [
        "insert_failure",
        "earlier_sequence",
        "earlier_event",
        "earlier_carrier",
    ] {
        let f = fixture();
        native_started(&f);
        let db = database(&f);
        let source = read_session(&db, &f.operation.agent_id).unwrap();
        let agent = f
            .repo
            .get_workspace_agent_by_id(&source.id)
            .unwrap()
            .unwrap();
        let mut second_session = source.clone();
        second_session.id = "second-edit-agent".into();
        second_session.provider_session_ref = Some("provider-second-edit-agent".into());
        let mut second_agent = agent.clone();
        second_agent.agent_id = second_session.id.clone();
        second_agent.alias = "second-edit-agent".into();
        second_agent.provider_session_ref = second_session.provider_session_ref.clone();
        f.repo
            .create_workspace_agent(&second_session, &second_agent)
            .unwrap();
        let state = f.repo.hydrate_runtime_state().unwrap();
        let mut turn = state
            .turns
            .into_iter()
            .find(|row| row.id == f.operation.target_turn_id)
            .unwrap();
        turn.id = "second-edit-target".into();
        turn.session_id = second_session.id.clone();
        f.repo.upsert_turn(&turn).unwrap();
        let mut admission = f
            .repo
            .list_turn_admissions()
            .unwrap()
            .into_iter()
            .find(|row| row.turn_id == f.operation.target_turn_id)
            .unwrap();
        admission.turn_id = turn.id.clone();
        admission.session_id = second_session.id.clone();
        f.repo.upsert_turn_admission(&admission).unwrap();
        let mut second = f.operation.clone();
        second.operation_id = "zz-second-edit".into();
        second.agent_id = second_session.id;
        second.target_turn_id = turn.id;
        second.source_provider_session_ref = second_session.provider_session_ref.unwrap();
        second.idempotency_key = Some("second-edit-key".into());
        f.repo
            .begin_session_edit_rerun_with_receipt(&second)
            .unwrap();
        let (when, action) = match fault {
            "insert_failure" => ("BEFORE", "SELECT RAISE(ABORT,'second recovery receipt failed');"),
            "earlier_sequence" => ("AFTER", "UPDATE runtime_events SET seq=seq+10 WHERE kind='turn.edit_manual_review' AND json_extract(payload_json,'$.operation_id')='signed_edit_intent';"),
            "earlier_event" => ("AFTER", "UPDATE runtime_events SET critical=0 WHERE kind='turn.edit_manual_review' AND json_extract(payload_json,'$.operation_id')='signed_edit_intent';"),
            "earlier_carrier" => ("AFTER", "UPDATE session_edit_reruns SET record_json=json_set(record_json,'$.error_code','later_receipt_drift') WHERE operation_id='signed_edit_intent';"),
            _ => unreachable!(),
        };
        db.execute_batch(&format!(
            "CREATE TRIGGER n04_second_recovery_fault {when} INSERT ON runtime_events
             WHEN NEW.kind='turn.edit_manual_review' AND
                  json_extract(NEW.payload_json,'$.operation_id')='zz-second-edit'
             BEGIN {action} END;"
        ))
        .unwrap();
        let before = raw_state(&db);
        let error = f
            .repo
            .recover_session_edit_reruns_with_receipts(600)
            .unwrap_err();
        let expected_error = match fault {
            "insert_failure" => "second recovery receipt failed",
            "earlier_carrier" => "edit authority changed while journaling its receipt",
            _ => "edit transition receipt changed before commit",
        };
        assert!(
            error.to_string().contains(expected_error),
            "{fault}: {error}"
        );
        assert_eq!(raw_state(&db), before);
        db.execute_batch("DROP TRIGGER n04_second_recovery_fault")
            .unwrap();
        let receipts = f
            .repo
            .recover_session_edit_reruns_with_receipts(600)
            .unwrap();
        assert_eq!(receipts.len(), 2);
        assert!(receipts.iter().all(|receipt| receipt.changed
            && receipt.operation.phase == "manual_review"
            && receipt.journal_event.as_ref().unwrap().payload["origin"] == "startup"));
        for receipt in &receipts {
            assert_eq!(
                f.repo
                    .get_session_edit_rerun(&receipt.operation.operation_id)
                    .unwrap(),
                Some(receipt.operation.clone())
            );
            let event = receipt.journal_event.as_ref().unwrap();
            assert_eq!(
                crate::db::fetch_runtime_event_by_event_id(&db, &event.event_id).unwrap(),
                Some(event.clone())
            );
        }
        let saved = raw_state(&db);
        assert!(f
            .repo
            .recover_session_edit_reruns_with_receipts(900)
            .unwrap()
            .is_empty());
        assert_eq!(raw_state(&db), saved);
    }
}
