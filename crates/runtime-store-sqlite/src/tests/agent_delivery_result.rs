use super::*;
use runtime_core::{AgentDeliveryRecord, RuntimeError};

fn setup() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    AgentDeliveryRecord,
) {
    let (temp, repository) = super::agent_message_cancellation::seeded_messages();
    let mut session = sample_session();
    session.id = "sess_a".into();
    session.status = "ready".into();
    session.worktree_id = None;
    session.active_turn_id = None;
    repository.upsert_session(&session).unwrap();
    let mut turn = sample_turn();
    turn.id = "admitted-agent-turn".into();
    turn.session_id = session.id;
    turn.source = Some("agent_message_delivery_transport".into());
    repository.upsert_turn(&turn).unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    db.execute(
        "INSERT INTO turn_admissions(
          turn_id,session_id,provider,projection_source,user_input_snapshot_json,
          dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at)
         VALUES (?1,'sess_a','codex','agent_message_delivery_transport',
          '{}','{}','{\"correlation_id\":\"msg_cancel\"}','dispatched',100,101)",
        [turn.id],
    )
    .unwrap();
    let pending = repository
        .get_agent_delivery("msg_cancel_a")
        .unwrap()
        .unwrap();
    let mut injecting = pending.clone();
    injecting.status = "injecting".into();
    injecting.updated_at += 1;
    assert!(repository
        .compare_and_swap_agent_delivery_status(&pending, &injecting)
        .unwrap());
    (temp, repository, injecting)
}

fn event_count(repository: &SqliteRuntimeRepository, kind: &str) -> i64 {
    open_connection(&repository.database_path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM runtime_events WHERE kind=?1 AND critical=1",
            [kind],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn one_correlated_result_commits_turn_status_and_critical_receipt_across_restart() {
    let (temp, repository, injecting) = setup();
    assert_eq!(event_count(&repository, "agent_delivery.injecting"), 1);
    let updated = repository
        .commit_agent_delivery_injected(
            &injecting,
            "admitted-agent-turn",
            injecting.updated_at + 10,
        )
        .unwrap();
    assert_eq!(updated.status, "injected");
    assert_eq!(
        updated.injected_turn_id.as_deref(),
        Some("admitted-agent-turn")
    );
    assert_eq!(event_count(&repository, "agent_delivery.injected"), 1);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        reopened.get_agent_delivery(&updated.id).unwrap(),
        Some(updated.clone())
    );
    assert_eq!(
        reopened
            .commit_agent_delivery_injected(
                &injecting,
                "admitted-agent-turn",
                injecting.updated_at + 99,
            )
            .unwrap(),
        updated,
        "a lost acknowledgement must replay the original receipt"
    );
    assert_eq!(event_count(&reopened, "agent_delivery.injected"), 1);
    assert!(matches!(
        reopened.upsert_agent_delivery(&updated),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn last_critical_event_failure_rolls_back_native_result_and_legacy_mirror() {
    let (_temp, repository, injecting) = setup();
    let db = open_connection(&repository.database_path).unwrap();
    db.execute_batch(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
         VALUES ('team_receipt','Legacy','sess_sender','test',1,1);
         INSERT INTO team_messages(id,team_id,scope,sender_agent_id,
            recipient_agent_ids_json,input_json,priority,policy,created_at)
         VALUES ('msg_cancel','team_receipt','direct','sess_sender',
            '[\"sess_a\"]','[]','normal','non_interrupting',1);",
    )
    .unwrap();
    db.execute(
        "INSERT INTO team_deliveries(id,message_id,team_id,recipient_agent_id,
            provider,status,created_at,updated_at)
         VALUES (?1,'msg_cancel','team_receipt','sess_a','codex','injecting',?2,?3)",
        params![injecting.id, injecting.created_at, injecting.updated_at],
    )
    .unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_critical_ack BEFORE INSERT ON runtime_events
         WHEN NEW.kind='agent_delivery.injected'
         BEGIN SELECT RAISE(FAIL,'forced final receipt failure'); END;",
    )
    .unwrap();
    assert!(
        repository
            .commit_agent_delivery_injected(
                &injecting,
                "admitted-agent-turn",
                injecting.updated_at + 1,
            )
            .is_err()
    );
    assert_eq!(
        repository.get_agent_delivery(&injecting.id).unwrap(),
        Some(injecting.clone())
    );
    let mirror: (String, Option<String>) = db
        .query_row(
            "SELECT status,injected_turn_id FROM team_deliveries WHERE id=?1",
            [&injecting.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(mirror, ("injecting".into(), None));
    assert_eq!(event_count(&repository, "agent_delivery.injected"), 0);
    db.execute_batch("DROP TRIGGER fail_critical_ack").unwrap();
    let updated = repository
        .commit_agent_delivery_injected(&injecting, "admitted-agent-turn", injecting.updated_at + 2)
        .unwrap();
    assert_eq!(
        db.query_row(
            "SELECT status,injected_turn_id FROM team_deliveries WHERE id=?1",
            [&injecting.id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .unwrap(),
        ("injected".into(), updated.injected_turn_id.clone())
    );
}

#[test]
fn missing_or_forged_native_admission_claim_or_receipt_fails_closed() {
    let (_temp, repository, injecting) = setup();
    let db = open_connection(&repository.database_path).unwrap();
    for (field, original, replacement) in [
        ("provider", "codex", "claude"),
        (
            "projection_source",
            "agent_message_delivery_transport",
            "user_visible",
        ),
        ("dispatch_state", "dispatched", "unknown"),
    ] {
        db.execute(
            &format!("UPDATE turn_admissions SET {field}=?1 WHERE turn_id='admitted-agent-turn'"),
            [replacement],
        )
        .unwrap();
        assert!(
            matches!(
                repository.commit_agent_delivery_injected(
                    &injecting,
                    "admitted-agent-turn",
                    injecting.updated_at + 1,
                ),
                Err(RuntimeError::Conflict(_))
            ),
            "{field} must match the native authority"
        );
        db.execute(
            &format!("UPDATE turn_admissions SET {field}=?1 WHERE turn_id='admitted-agent-turn'"),
            [original],
        )
        .unwrap();
    }
    db.execute("UPDATE turn_admissions SET correlation_json='{\"correlation_id\":\"wrong\"}' WHERE turn_id='admitted-agent-turn'",[]).unwrap();
    assert!(
        repository
            .commit_agent_delivery_injected(
                &injecting,
                "admitted-agent-turn",
                injecting.updated_at + 1,
            )
            .is_err()
    );
    db.execute("UPDATE turn_admissions SET correlation_json='{\"correlation_id\":\"msg_cancel\"}' WHERE turn_id='admitted-agent-turn'",[]).unwrap();
    assert!(repository
        .commit_agent_delivery_injected(&injecting, "made-up-turn", injecting.updated_at + 1,)
        .is_err());
    db.execute(
        "DELETE FROM runtime_events WHERE kind='agent_delivery.injecting'",
        [],
    )
    .unwrap();
    assert!(
        repository
            .commit_agent_delivery_injected(
                &injecting,
                "admitted-agent-turn",
                injecting.updated_at + 1,
            )
            .is_err(),
        "historical unjournaled injecting is never certified by inference"
    );
    assert_eq!(
        repository.get_agent_delivery(&injecting.id).unwrap(),
        Some(injecting)
    );
}

#[test]
fn event_id_collision_and_preclaim_event_fault_do_not_grant_native_authority() {
    let (_temp, repository, injecting) = setup();
    let db = open_connection(&repository.database_path).unwrap();
    let event_id = format!(
        "evt_agent_delivery_{}_injected_{}",
        injecting.id, injecting.updated_at
    );
    db.execute(
        "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES (?1,'message','msg_cancel',800,'agent_delivery.injected',1,'{}',1)",
        [&event_id],
    ).unwrap();
    assert!(matches!(
        repository.commit_agent_delivery_injected(
            &injecting,
            "admitted-agent-turn",
            injecting.updated_at + 1,
        ),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository.get_agent_delivery(&injecting.id).unwrap(),
        Some(injecting.clone())
    );
    db.execute("DELETE FROM runtime_events WHERE event_id=?1", [event_id])
        .unwrap();
    let mut next = injecting.clone();
    next.status = "deferred".into();
    next.updated_at += 1;
    assert!(
        repository
            .compare_and_swap_agent_delivery_status(&injecting, &next)
            .is_err(),
        "native unknown cannot become a retryable delivery via generic status CAS"
    );
    assert_eq!(event_count(&repository, "agent_delivery.injected"), 0);
    let deferred = repository
        .get_agent_delivery("msg_cancel_b")
        .unwrap()
        .unwrap();
    let mut pending = deferred.clone();
    pending.status = "pending".into();
    pending.updated_at += 1;
    db.execute_batch("CREATE TRIGGER fail_preclaim BEFORE INSERT ON runtime_events WHEN NEW.kind='agent_delivery.pending' BEGIN SELECT RAISE(FAIL,'critical preclaim fault'); END;").unwrap();
    assert!(repository
        .compare_and_swap_agent_delivery_status(&deferred, &pending)
        .is_err());
    assert_eq!(
        repository.get_agent_delivery(&deferred.id).unwrap(),
        Some(deferred)
    );
}

#[test]
fn independent_acknowledgers_converge_on_one_admitted_turn_and_receipt() {
    let (_temp, repository, injecting) = setup();
    std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|i| {
                let repository = &repository;
                let injecting = injecting.clone();
                scope.spawn(move || {
                    repository.commit_agent_delivery_injected(
                        &injecting,
                        "admitted-agent-turn",
                        injecting.updated_at + i + 1,
                    )
                })
            })
            .collect::<Vec<_>>();
        let responses = workers
            .into_iter()
            .map(|worker| worker.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(responses.iter().all(|row| row == &responses[0]));
    });
    assert_eq!(event_count(&repository, "agent_delivery.injected"), 1);
}
