use super::*;
use runtime_core::{
    PersistedUserInputSnapshot, ProviderDispatchOutcomeCommit, RuntimeError, TurnAdmissionRecord,
    TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
};
use std::sync::{Arc, Barrier};

#[derive(Clone)]
struct Bundle {
    before_admission: TurnAdmissionRecord,
    after_admission: TurnAdmissionRecord,
    before_turn: TurnRecord,
    after_turn: TurnRecord,
    before_session: SessionRecord,
    after_session: SessionRecord,
    approvals: Vec<(ApprovalRecord, ApprovalRecord)>,
    event: NewRuntimeEvent,
}

fn seed(repo: &SqliteRuntimeRepository, approval: bool) -> Bundle {
    repo.initialize_schema().unwrap();
    let mut session = sample_session();
    session.active_turn_id = None;
    session.status = "ready".into();
    repo.upsert_session(&session).unwrap();
    let mut turn = sample_turn();
    turn.status = if approval {
        "waiting_for_approval"
    } else {
        "admitted"
    }
    .into();
    turn.provider_turn_ref = None;
    session.active_turn_id = Some(turn.id.clone());
    session.status = if approval {
        "waiting_for_approval"
    } else {
        "turn_admitted"
    }
    .into();
    session.updated_at = 102;
    let mut admission = TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: session.id.clone(),
        provider: session.provider.clone(),
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
        admitted_at: 102,
        updated_at: 102,
    };
    let pending = approval.then(|| ApprovalRecord {
        id: "approval_dispatch_outcome".into(),
        session_id: session.id.clone(),
        turn_id: turn.id.clone(),
        origin: "runtime_pre_dispatch_policy".into(),
        tool_call_id: None,
        provider_approval_ref: None,
        status: "pending".into(),
        request: serde_json::json!({"reason":"policy"}),
        response: None,
        created_at: 102,
        resolved_at: None,
    });
    repo.admit_turn(&admission, &turn, &session, pending.as_ref())
        .unwrap();
    if !approval {
        admission.dispatch_state = TurnDispatchState::Dispatching;
        admission.updated_at = 103;
        repo.upsert_turn_admission(&admission).unwrap();
    }
    let mut after_admission = admission.clone();
    after_admission.dispatch_state = if approval {
        TurnDispatchState::NotDispatched
    } else {
        TurnDispatchState::Unknown
    };
    after_admission.dispatch_error =
        Some(serde_json::json!({"code":"mock_send","message":"send failed"}));
    after_admission.updated_at = 110;
    let mut after_turn = turn.clone();
    after_turn.status = if approval {
        "failed"
    } else {
        "dispatch_unknown"
    }
    .into();
    if approval {
        after_turn.completed_at = Some(110);
    }
    after_turn.error = Some(serde_json::json!({
        "message":"send failed", "provider_dispatch_outcome":if approval { "not_dispatched" } else { "unknown" },
    }));
    let mut after_session = session.clone();
    after_session.status = if approval {
        "ready"
    } else {
        "turn_recovery_required"
    }
    .into();
    if approval {
        after_session.active_turn_id = None;
    }
    after_session.updated_at = 110;
    let approvals = pending
        .into_iter()
        .map(|before| {
            let mut after = before.clone();
            after.status = "cancelled".into();
            after.response = Some(
                serde_json::json!({"reason":"provider dispatch was proven not to have occurred"}),
            );
            after.resolved_at = Some(110);
            (before, after)
        })
        .collect();
    let event = NewRuntimeEvent {
        event_id: format!(
            "evt_turn_{}_{}",
            turn.id,
            if approval {
                "not_dispatched"
            } else {
                "dispatch_unknown"
            }
        ),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: if approval {
            "turn.failed"
        } else {
            "turn.dispatch_unknown"
        }
        .into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "error":"send failed", "provider_dispatch_outcome":if approval { "not_dispatched" } else { "unknown" },
        }),
        provider: None,
        provider_seq: None,
        created_at: 110,
    };
    Bundle {
        before_admission: admission,
        after_admission,
        before_turn: turn,
        after_turn,
        before_session: session,
        after_session,
        approvals,
        event,
    }
}

fn commit(
    repo: &SqliteRuntimeRepository,
    b: &Bundle,
) -> Result<ProviderDispatchOutcomeCommit, RuntimeError> {
    repo.commit_provider_dispatch_outcome(
        &b.before_admission,
        &b.after_admission,
        &b.before_turn,
        &b.after_turn,
        &b.before_session,
        &b.after_session,
        &b.approvals,
        &b.event,
    )
}

fn assert_state(repo: &SqliteRuntimeRepository, b: &Bundle, done: bool) {
    let hydrated = repo.hydrate_runtime_state().unwrap();
    let saved = repo.list_turn_admissions().unwrap();
    assert_eq!(
        saved.iter().find(|v| v.turn_id == b.before_turn.id),
        Some(if done {
            &b.after_admission
        } else {
            &b.before_admission
        })
    );
    assert_eq!(
        hydrated.turns.iter().find(|v| v.id == b.before_turn.id),
        Some(if done { &b.after_turn } else { &b.before_turn })
    );
    assert_eq!(
        hydrated
            .sessions
            .iter()
            .find(|v| v.id == b.before_session.id),
        Some(if done {
            &b.after_session
        } else {
            &b.before_session
        })
    );
    for (before, after) in &b.approvals {
        assert_eq!(
            hydrated.approvals.iter().find(|v| v.id == before.id),
            Some(if done { after } else { before })
        );
    }
    let events = repo
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &b.before_session.id)),
            None,
            100,
        )
        .unwrap();
    assert_eq!(
        events.iter().filter(|v| v.kind == b.event.kind).count(),
        usize::from(done)
    );
}

#[test]
fn unknown_final_critical_insert_fault_rolls_back_fence_and_restarts_safely() {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    let b = seed(&repo, false);
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER deny_unknown_event BEFORE INSERT ON runtime_events
        WHEN NEW.kind='turn.dispatch_unknown' BEGIN SELECT RAISE(ABORT,'injected critical fault'); END;").unwrap();
    assert!(commit(&repo, &b).is_err());
    assert_state(&repo, &b, false);
    connection
        .execute_batch("DROP TRIGGER deny_unknown_event")
        .unwrap();
    let restarted = super::repo(&temp);
    let first = commit(&restarted, &b).unwrap();
    assert_eq!(first.event.criticality, RuntimeEventCriticality::Critical);
    assert_state(&restarted, &b, true);
    assert_eq!(commit(&repo, &b).unwrap().event.row_id, first.event.row_id);
    assert_state(&repo, &b, true);
}

#[test]
fn proven_no_send_cancels_pending_approval_only_with_failed_critical_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    let b = seed(&repo, true);
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER deny_failed_event BEFORE INSERT ON runtime_events
        WHEN NEW.kind='turn.failed' BEGIN SELECT RAISE(ABORT,'injected critical fault'); END;",
        )
        .unwrap();
    assert!(commit(&repo, &b).is_err());
    assert_state(&repo, &b, false);
    connection
        .execute_batch("DROP TRIGGER deny_failed_event")
        .unwrap();
    let restarted = super::repo(&temp);
    assert_eq!(
        commit(&restarted, &b).unwrap().approvals,
        vec![b.approvals[0].1.clone()]
    );
    assert_state(&restarted, &b, true);
    assert!(commit(&restarted, &b).is_ok());
    assert_state(&repo, &b, true);
}

#[test]
fn independent_writers_cannot_publish_conflicting_native_outcomes() {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    let b = seed(&repo, false);
    let path = temp.path().join("runtime.sqlite3");
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];
    for n in 0..2 {
        let mut b = b.clone();
        if n != 0 {
            let message = "second outcome";
            b.after_admission.dispatch_error =
                Some(serde_json::json!({"code":"mock_send","message":message}));
            b.after_turn.error =
                Some(serde_json::json!({"message":message,"provider_dispatch_outcome":"unknown"}));
            b.event.payload =
                serde_json::json!({"error":message,"provider_dispatch_outcome":"unknown"});
        }
        let path = path.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let other = SqliteRuntimeRepository::new(path);
            barrier.wait();
            commit(&other, &b)
        }));
    }
    barrier.wait();
    let results = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|r| r.is_err()).count(), 1);
    let events = repo
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &b.before_session.id)),
            None,
            100,
        )
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "turn.dispatch_unknown")
            .count(),
        1
    );
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(
        winner.event.payload["error"],
        winner.admission.dispatch_error.as_ref().unwrap()["message"]
    );
}

#[test]
fn forged_receipt_and_missing_approval_cannot_fabricate_quarantine_or_unlock() {
    let temp = tempfile::tempdir().unwrap();
    let repo = repo(&temp);
    let b = seed(&repo, true);
    let mut missing = b.clone();
    missing.approvals.clear();
    assert!(matches!(
        commit(&repo, &missing),
        Err(RuntimeError::Conflict(_))
    ));
    assert_state(&repo, &b, false);
    let mut forged = b.event.clone();
    forged.payload =
        serde_json::json!({"error":"forged","provider_dispatch_outcome":"not_dispatched"});
    repo.append_runtime_event(&forged).unwrap();
    assert!(matches!(commit(&repo, &b), Err(RuntimeError::Conflict(_))));
    // The adversarial forged event is present by construction, but none of
    // the admission, turn, session or pending-approval projections may move.
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(repo.list_turn_admissions().unwrap()[0], b.before_admission);
    assert_eq!(hydrated.turns[0], b.before_turn);
    assert_eq!(hydrated.sessions[0], b.before_session);
    assert_eq!(hydrated.approvals[0], b.approvals[0].0);
    let events = repo
        .list_runtime_events(
            Some((RuntimeEventScope::Session, &b.before_session.id)),
            None,
            100,
        )
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|row| row.kind == "turn.failed")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .find(|row| row.kind == "turn.failed")
            .unwrap()
            .payload,
        forged.payload
    );
}
