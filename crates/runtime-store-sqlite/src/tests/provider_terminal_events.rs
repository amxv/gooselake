use super::*;
use runtime_core::RuntimeError;
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
    session.active_turn_id = Some(turn.id.clone());
    repository.upsert_session(&session).unwrap();
    repository.upsert_turn(&turn).unwrap();
    (temp, repository, session, turn)
}

fn terminal(
    session: &SessionRecord,
    turn: &TurnRecord,
    status: &str,
) -> (SessionRecord, TurnRecord, NewRuntimeEvent) {
    let mut updated_session = session.clone();
    updated_session.status = "ready".into();
    updated_session.active_turn_id = None;
    updated_session.updated_at = 200;
    updated_session.metadata =
        serde_json::json!({"transcript":[{"role":"assistant","text":"done"}]});
    let mut updated_turn = turn.clone();
    updated_turn.status = status.into();
    updated_turn.completed_at = Some(200);
    updated_turn.usage = Some(serde_json::json!({"input_tokens":42,"output_tokens":7}));
    let kind = match status {
        "completed" => "turn.completed",
        "interrupted" => "turn.interrupted",
        _ => "turn.failed",
    };
    let event = NewRuntimeEvent {
        event_id: format!("evt_turn_{}_terminal", turn.id),
        scope: RuntimeEventScope::Session,
        scope_id: session.id.clone(),
        session_id: Some(session.id.clone()),
        team_id: None,
        turn_id: Some(turn.id.clone()),
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "status": status, "usage": updated_turn.usage, "error": null, "assistant_text": null
        }),
        provider: None,
        provider_seq: None,
        created_at: 200,
    };
    (updated_session, updated_turn, event)
}

#[test]
fn provider_terminal_receipt_rolls_back_every_projection_on_event_fault_then_replays_after_restart()
{
    let (_temp, repository, session, turn) = fixture();
    let (ready, finished, event) = terminal(&session, &turn, "completed");
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_terminal_event BEFORE INSERT ON runtime_events
             WHEN NEW.kind = 'turn.completed'
             BEGIN SELECT RAISE(ABORT, 'injected terminal event fault'); END;",
        )
        .unwrap();
    drop(connection);

    assert!(repository
        .commit_provider_turn_result(&turn, &finished, Some(&session), Some(&ready), &event)
        .is_err());
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(
        hydrated.turns.iter().find(|row| row.id == turn.id),
        Some(&turn)
    );
    assert_eq!(
        hydrated.sessions.iter().find(|row| row.id == session.id),
        Some(&session)
    );
    assert!(repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 10)
        .unwrap()
        .is_empty());

    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER fail_terminal_event")
        .unwrap();
    drop(connection);
    let first = repository
        .commit_provider_turn_result(&turn, &finished, Some(&session), Some(&ready), &event)
        .unwrap();
    assert_eq!(first.turn, finished);
    assert_eq!(first.session, Some(ready.clone()));
    assert_eq!(first.event.seq, 1);
    assert_eq!(first.event.event_id, event.event_id);

    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    let mut different_timestamp = finished;
    different_timestamp.completed_at = Some(300);
    let replayed = reopened
        .commit_provider_turn_result(
            &turn,
            &different_timestamp,
            Some(&session),
            Some(&ready),
            &NewRuntimeEvent {
                created_at: 300,
                ..event
            },
        )
        .unwrap();
    assert_eq!(
        replayed, first,
        "receipt returns persisted time, not retry time"
    );
    assert_eq!(
        reopened
            .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 10)
            .unwrap()
            .len(),
        1,
    );
}

#[test]
fn provider_terminal_collision_and_stale_session_reject_without_partial_turn_update() {
    let (_temp, repository, session, turn) = fixture();
    let (ready, finished, event) = terminal(&session, &turn, "completed");
    let forged = NewRuntimeEvent {
        kind: "turn.failed".into(),
        payload: serde_json::json!({"status":"failed"}),
        ..event.clone()
    };
    repository.append_runtime_event(&forged).unwrap();
    assert!(matches!(
        repository.commit_provider_turn_result(
            &turn,
            &finished,
            Some(&session),
            Some(&ready),
            &event
        ),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository.hydrate_runtime_state().unwrap().turns[0].status,
        "in_progress",
    );

    let (_temp2, repository, session, turn) = fixture();
    let (ready, finished, event) = terminal(&session, &turn, "completed");
    let mut changed_session = session.clone();
    changed_session.status = "turn_recovery_required".into();
    changed_session.updated_at = 999;
    repository.upsert_session(&changed_session).unwrap();
    assert!(matches!(
        repository.commit_provider_turn_result(
            &turn,
            &finished,
            Some(&session),
            Some(&ready),
            &event
        ),
        Err(RuntimeError::Conflict(_))
    ));
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.turns[0], turn);
    assert_eq!(hydrated.sessions[0], changed_session);
    assert!(repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn competing_terminal_outcomes_only_one_state_and_event_can_commit() {
    let (_temp, repository, session, turn) = fixture();
    let gate = Arc::new(Barrier::new(2));
    let workers = ["completed", "failed"]
        .into_iter()
        .map(|status| {
            let repo = repository.clone();
            let gate = Arc::clone(&gate);
            let session = session.clone();
            let turn = turn.clone();
            std::thread::spawn(move || {
                let (ready, finished, event) = terminal(&session, &turn, status);
                gate.wait();
                repo.commit_provider_turn_result(
                    &turn,
                    &finished,
                    Some(&session),
                    Some(&ready),
                    &event,
                )
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    let persisted = repository.hydrate_runtime_state().unwrap();
    assert_eq!(persisted.turns[0], winner.turn);
    assert_eq!(persisted.sessions[0], winner.session.unwrap());
    let events = repository
        .list_runtime_events(Some((RuntimeEventScope::Session, &session.id)), None, 10)
        .unwrap();
    assert_eq!(events, vec![winner.event]);
}

#[test]
fn stale_provider_result_terminalizes_only_historical_turn_not_current_session() {
    let (_temp, repository, session, turn) = fixture();
    let (ready, finished, event) = terminal(&session, &turn, "interrupted");
    let mut newer = session.clone();
    newer.active_turn_id = Some("turn_new".into());
    newer.updated_at = 500;
    repository.upsert_session(&newer).unwrap();
    let committed = repository
        .commit_provider_turn_result(&turn, &finished, None, None, &event)
        .unwrap();
    assert_eq!(committed.turn, finished);
    assert!(committed.session.is_none());
    let hydrated = repository.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.sessions[0], newer);
    assert_ne!(hydrated.sessions[0], ready);
}
