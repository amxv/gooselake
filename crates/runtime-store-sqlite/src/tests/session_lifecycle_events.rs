use super::*;
use runtime_core::RuntimeError;
use std::sync::{Arc, Barrier};

fn fixture() -> (tempfile::TempDir, SqliteRuntimeRepository, SessionRecord) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut created = sample_session();
    created.status = "ready".into();
    created.active_turn_id = None;
    created.updated_at = created.created_at;
    (temp, repository, created)
}

fn event(after: &SessionRecord, kind: &str) -> NewRuntimeEvent {
    let (event_id, payload) = match kind {
        "session.created" => (
            format!("evt_session_{}_created", after.id),
            serde_json::json!({"provider":after.provider}),
        ),
        "session.resumed" => (
            format!("evt_session_{}_resumed_attempt_z", after.id),
            serde_json::json!({}),
        ),
        "session.closed" => (
            format!("evt_session_{}_closed", after.id),
            serde_json::json!({"reason":"operator_requested"}),
        ),
        _ => panic!("unexpected session lifecycle event kind"),
    };
    NewRuntimeEvent {
        event_id,
        scope: RuntimeEventScope::Session,
        scope_id: after.id.clone(),
        session_id: Some(after.id.clone()),
        team_id: None,
        turn_id: None,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: 500,
    }
}

fn resumed(before: &SessionRecord) -> SessionRecord {
    let mut after = before.clone();
    after.status = "ready".into();
    after.provider_session_ref = Some("provider-after-resume".into());
    after.canonical_provider_session_ref = Some("provider-canonical".into());
    after.updated_at += 4;
    after
}

fn closed(before: &SessionRecord) -> SessionRecord {
    let mut after = before.clone();
    after.status = "closed".into();
    after.closed_at = Some(before.updated_at + 5);
    after.updated_at = before.updated_at + 5;
    after.active_turn_id = None;
    after
}

#[test]
fn created_session_and_critical_receipt_are_one_transaction_with_restart_replay() {
    let (_temp, repository, created) = fixture();
    let event = event(&created, "session.created");
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_session_created BEFORE INSERT ON runtime_events
         WHEN NEW.kind='session.created'
         BEGIN SELECT RAISE(ABORT, 'native creation journal fault'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(repository
        .commit_session_lifecycle(None, &created, &event)
        .is_err());
    assert!(repository
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .is_empty());
    assert!(repository
        .list_runtime_events(None, None, 20)
        .unwrap()
        .is_empty());
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER reject_session_created")
        .unwrap();
    drop(connection);
    let (first, receipt) = repository
        .commit_session_lifecycle(None, &created, &event)
        .unwrap();
    assert_eq!(first, created);
    assert_eq!(receipt.as_ref().unwrap().event_id, event.event_id);
    assert_eq!(
        receipt.as_ref().unwrap().criticality,
        RuntimeEventCriticality::Critical
    );
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        reopened
            .commit_session_lifecycle(None, &created, &event)
            .unwrap(),
        (created.clone(), None)
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().sessions,
        vec![created]
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 20).unwrap().len(),
        1
    );
}

#[test]
fn resumed_session_receipt_fault_rolls_back_identity_and_replays_after_restart() {
    let (_temp, repository, created) = fixture();
    repository
        .commit_session_lifecycle(None, &created, &event(&created, "session.created"))
        .unwrap();
    let after = resumed(&created);
    let event = event(&after, "session.resumed");
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_resume BEFORE INSERT ON runtime_events
         WHEN NEW.kind='session.resumed'
         BEGIN SELECT RAISE(ABORT, 'native resume journal fault'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(repository
        .commit_session_lifecycle(Some(&created), &after, &event)
        .is_err());
    assert_eq!(
        repository.hydrate_runtime_state().unwrap().sessions,
        vec![created.clone()]
    );
    assert_eq!(
        repository
            .list_runtime_events(None, None, 20)
            .unwrap()
            .len(),
        1
    );
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER reject_resume")
        .unwrap();
    drop(connection);
    let (_, receipt) = repository
        .commit_session_lifecycle(Some(&created), &after, &event)
        .unwrap();
    assert_eq!(receipt.unwrap().kind, "session.resumed");
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        reopened
            .commit_session_lifecycle(Some(&created), &after, &event)
            .unwrap(),
        (after.clone(), None)
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().sessions,
        vec![after]
    );
}

#[test]
fn acknowledged_close_and_receipt_fault_rollback_then_restart_replay() {
    let (_temp, repository, created) = fixture();
    repository
        .commit_session_lifecycle(None, &created, &event(&created, "session.created"))
        .unwrap();
    let after = closed(&created);
    let event = event(&after, "session.closed");
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_close BEFORE INSERT ON runtime_events
         WHEN NEW.kind='session.closed'
         BEGIN SELECT RAISE(ABORT, 'native close journal fault'); END;",
        )
        .unwrap();
    drop(connection);
    assert!(repository
        .commit_session_lifecycle(Some(&created), &after, &event)
        .is_err());
    assert_eq!(
        repository.hydrate_runtime_state().unwrap().sessions,
        vec![created.clone()]
    );
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER reject_close")
        .unwrap();
    drop(connection);
    let (finalized, receipt) = repository
        .commit_session_lifecycle(Some(&created), &after, &event)
        .unwrap();
    assert_eq!(finalized.status, "closed");
    assert_eq!(
        receipt.as_ref().unwrap().payload["reason"],
        "operator_requested"
    );
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        reopened
            .commit_session_lifecycle(Some(&created), &after, &event)
            .unwrap(),
        (after.clone(), None)
    );
    assert_eq!(
        reopened.hydrate_runtime_state().unwrap().sessions,
        vec![after]
    );
    assert_eq!(
        reopened.list_runtime_events(None, None, 20).unwrap().len(),
        2
    );
}

#[test]
fn competing_native_resume_and_close_acks_cannot_both_mutate_session_authority() {
    let (_temp, repository, created) = fixture();
    repository
        .commit_session_lifecycle(None, &created, &event(&created, "session.created"))
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let jobs = [false, true]
        .into_iter()
        .map(|close| {
            let barrier = Arc::clone(&barrier);
            let repository = repository.clone();
            let before = created.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let after = if close {
                    closed(&before)
                } else {
                    resumed(&before)
                };
                let event = event(
                    &after,
                    if close {
                        "session.closed"
                    } else {
                        "session.resumed"
                    },
                );
                repository.commit_session_lifecycle(Some(&before), &after, &event)
            })
        })
        .collect::<Vec<_>>();
    let results = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(
        repository.hydrate_runtime_state().unwrap().sessions,
        vec![winner.0]
    );
    assert_eq!(
        repository
            .list_runtime_events(None, None, 20)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn stale_ownership_and_forged_receipt_never_confer_provider_session_state() {
    let (_temp, repository, created) = fixture();
    repository
        .commit_session_lifecycle(None, &created, &event(&created, "session.created"))
        .unwrap();
    let after = closed(&created);
    let mut concurrent = created.clone();
    concurrent.status = "turn_running".into();
    concurrent.updated_at += 1;
    repository.upsert_session(&concurrent).unwrap();
    assert!(matches!(
        repository.commit_session_lifecycle(
            Some(&created),
            &after,
            &event(&after, "session.closed")
        ),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository.hydrate_runtime_state().unwrap().sessions,
        vec![concurrent]
    );

    let (_temp, repository, created) = fixture();
    repository
        .commit_session_lifecycle(None, &created, &event(&created, "session.created"))
        .unwrap();
    let after = closed(&created);
    let mut forged = event(&after, "session.closed");
    forged.event_id = "evt_session_other_owner_closed".into();
    assert!(matches!(
        repository.commit_session_lifecycle(Some(&created), &after, &forged),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    let legitimate = event(&after, "session.closed");
    repository.append_runtime_event(&legitimate).unwrap();
    assert!(
        matches!(
            repository.commit_session_lifecycle(Some(&created), &after, &legitimate),
            Err(RuntimeError::Conflict(_))
        ),
        "a standalone prior event is NOT an atomic state-and-receipt proof"
    );
    assert_eq!(
        repository.hydrate_runtime_state().unwrap().sessions,
        vec![created]
    );
}
