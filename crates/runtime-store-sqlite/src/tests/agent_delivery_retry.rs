use super::*;
use runtime_core::{
    prepare_agent_delivery_retry, prepare_agent_message_cancellation,
    prepare_workspace_registration, AgentRetryDeliveryMutationCommand, OperationActor,
    OperationPhase, RuntimeError, WorkspaceRegisterRequest,
};

fn seed() -> (tempfile::TempDir, SqliteRuntimeRepository) {
    super::agent_message_cancellation::seeded_messages()
}

fn command(id: &str, key: &str) -> AgentRetryDeliveryMutationCommand {
    prepare_agent_delivery_retry(
        id,
        OperationActor::operator("runtime_operator"),
        Some(key.to_string()),
    )
    .expect("retry operator command")
}

#[test]
fn exact_retry_admission_replays_after_restart_without_reverting_native_delivery_state() {
    let (temp, repository) = seed();
    let first = repository
        .retry_agent_delivery_operator(&command("msg_cancel_b", "retry-1"))
        .expect("admit deferred delivery");
    assert_eq!(first.delivery.status, "pending");
    assert_eq!(first.delivery.injected_turn_id, None);
    assert_eq!(first.delivery.last_error_code, None);
    let inspection = repository
        .get_operation(&first.operation_id)
        .unwrap()
        .expect("retry operation");
    assert_eq!(inspection.operation.kind, "agent_delivery_retry_admission");
    assert_eq!(inspection.operation.phase, OperationPhase::Completed);
    assert_eq!(
        inspection.operation.exact_terminal_result,
        Some(serde_json::to_value(&first).unwrap())
    );
    assert!(inspection.claims.is_empty());

    // A later provider attempt may fail or defer. The keyed retry receipt is
    // a stable admission snapshot, NOT its current mutable delivery status.
    let mut later = first.delivery.clone();
    later.status = "deferred".into();
    later.last_error_code = Some("busy".into());
    later.updated_at += 100;
    repository.upsert_agent_delivery(&later).unwrap();

    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        reopened
            .retry_agent_delivery_operator(&command("msg_cancel_b", "retry-1"))
            .unwrap(),
        first
    );
    assert_eq!(
        reopened
            .get_agent_delivery("msg_cancel_b")
            .unwrap()
            .unwrap()
            .status,
        "deferred"
    );
    assert!(matches!(
        reopened.retry_agent_delivery_operator(&command("msg_other_b", "retry-1")),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(matches!(
        reopened.cancel_agent_message_operator(
            &prepare_agent_message_cancellation(
                "msg_other",
                OperationActor::operator("runtime_operator"),
                Some("retry-1".into()),
            )
            .unwrap()
        ),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn retry_rolls_back_legacy_mirror_and_operation_on_failure() {
    let (temp, repository) = seed();
    let path = temp.path().join("runtime.sqlite3");
    let db = open_connection(&path).unwrap();
    db.execute_batch(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
         VALUES ('team_retry','Legacy','sess_sender','test',1,1);
         INSERT INTO team_messages(
           id,team_id,scope,sender_agent_id,recipient_agent_ids_json,input_json,
           priority,policy,created_at
         ) VALUES (
           'msg_cancel','team_retry','direct','sess_sender','[\"sess_b\"]','[]',
           'normal','non_interrupting',1
         );
         INSERT INTO team_deliveries(
           id,message_id,team_id,recipient_agent_id,provider,status,created_at,updated_at
         ) VALUES (
           'msg_cancel_b','msg_cancel','team_retry','sess_b','codex','deferred',1,1
         );
         CREATE TRIGGER block_retry_mirror BEFORE UPDATE ON team_deliveries
           WHEN NEW.status='pending'
           BEGIN SELECT RAISE(FAIL,'forced legacy mirror error'); END;",
    )
    .unwrap();
    let input = command("msg_cancel_b", "legacy-retry");
    assert!(repository.retry_agent_delivery_operator(&input).is_err());
    assert_eq!(
        repository
            .get_agent_delivery("msg_cancel_b")
            .unwrap()
            .unwrap()
            .status,
        "deferred"
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM runtime_operations WHERE kind='agent_delivery_retry_admission'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER block_retry_mirror;")
        .unwrap();
    db.execute(
        "UPDATE team_deliveries SET status='injected' WHERE id='msg_cancel_b'",
        [],
    )
    .unwrap();
    assert!(matches!(
        repository.retry_agent_delivery_operator(&input),
        Err(RuntimeError::Conflict(_))
    ));
    db.execute(
        "UPDATE team_deliveries SET status='deferred' WHERE id='msg_cancel_b'",
        [],
    )
    .unwrap();
    let result = repository
        .retry_agent_delivery_operator(&input)
        .expect("retry after proven rollback");
    assert_eq!(result.delivery.status, "pending");
    assert_eq!(
        db.query_row(
            "SELECT status FROM team_deliveries WHERE id='msg_cancel_b'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "pending"
    );
}

#[test]
fn cancelled_or_already_pending_delivery_cannot_be_retried() {
    let (_temp, repository) = seed();
    assert!(matches!(
        repository.retry_agent_delivery_operator(&command("msg_cancel_a", "pending-retry")),
        Err(RuntimeError::Conflict(_))
    ));
    let cancellation = prepare_agent_message_cancellation(
        "msg_cancel",
        OperationActor::operator("runtime_operator"),
        Some("cancel-before-retry".into()),
    )
    .unwrap();
    repository
        .cancel_agent_message_operator(&cancellation)
        .expect("atomically cancel message");
    assert!(matches!(
        repository.retry_agent_delivery_operator(&command("msg_cancel_b", "terminal-retry")),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn eight_concurrent_same_key_delivery_retry_admissions_converge_once() {
    let (_temp, repository) = seed();
    std::thread::scope(|scope| {
        let handles = (0..8)
            .map(|_| {
                let repository = &repository;
                scope.spawn(move || {
                    repository
                        .retry_agent_delivery_operator(&command("msg_cancel_b", "concurrent-retry"))
                        .expect("convergent exact retry receipt")
                })
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert!(results.iter().all(|receipt| receipt == &results[0]));
    });
}

#[test]
fn retry_admission_is_fenced_by_active_or_retired_workspace_before_native_effect() {
    let (temp, repository) = seed();
    let root = temp.path().join("retirement-owner");
    std::fs::create_dir(&root).unwrap();
    let workspace = repository
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.display().to_string(),
                    display_name: None,
                },
                OperationActor::operator("retry-retirement-test"),
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .workspace;
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE agent_messages SET context_kind='workspace_team', workspace_id=?1
         WHERE id='msg_cancel'",
        [&workspace.workspace_id],
    )
    .unwrap();
    let retry = command("msg_cancel_b", "retirement-fence-key");
    db.execute(
        "UPDATE workspaces SET lifecycle_state='retired' WHERE workspace_id=?1",
        [&workspace.workspace_id],
    )
    .unwrap();
    assert!(matches!(
        repository.retry_agent_delivery_operator(&retry),
        Err(RuntimeError::Conflict(_))
    ));
    db.execute(
        "UPDATE workspaces SET lifecycle_state='active' WHERE workspace_id=?1",
        [&workspace.workspace_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO runtime_operations(
           operation_id,workspace_id,kind,actor_kind,actor_id,
           normalized_request_hash,normalized_request_json,phase,created_at,updated_at
         ) VALUES ('op_freeze',?1,'workspace_retire','operator','test','sha','{}',
                   'requested',1,1)",
        [&workspace.workspace_id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO workspace_retirement_fences(
           workspace_id,operation_id,phase,snapshot_json,created_at,updated_at
         ) VALUES (?1,'op_freeze','frozen','{}',1,1)",
        [&workspace.workspace_id],
    )
    .unwrap();
    assert!(matches!(
        repository.retry_agent_delivery_operator(&retry),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository
            .get_agent_delivery("msg_cancel_b")
            .unwrap()
            .unwrap()
            .status,
        "deferred"
    );
    db.execute(
        "DELETE FROM workspace_retirement_fences WHERE workspace_id=?1",
        [&workspace.workspace_id],
    )
    .unwrap();
    let admitted = repository
        .retry_agent_delivery_operator(&retry)
        .expect("unfrozen active owner permits admission");
    assert_eq!(admitted.delivery.status, "pending");
}
