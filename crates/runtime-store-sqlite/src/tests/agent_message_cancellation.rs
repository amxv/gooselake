use super::*;
use runtime_core::{
    prepare_agent_message_cancellation, AgentDeliveryRecord, AgentMessageContextKind,
    AgentMessageRecord, OperationActor, OperationPhase,
};

pub(super) fn seeded_messages() -> (tempfile::TempDir, SqliteRuntimeRepository) {
    let temp = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema");
    for message_id in ["msg_cancel", "msg_other"] {
        let message = AgentMessageRecord {
            id: message_id.to_string(),
            scope: "direct".into(),
            context_kind: AgentMessageContextKind::GlobalDirect,
            workspace_id: None,
            legacy_team_id: None,
            sender_agent_id: "sess_sender".into(),
            recipient_agent_ids: vec!["sess_a".into(), "sess_b".into()],
            input: serde_json::json!([{"type":"text","text":"hello"}]),
            image_paths: Vec::new(),
            priority: "normal".into(),
            policy: "non_interrupting".into(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
            created_at: 100,
        };
        let deliveries = ["a", "b"]
            .iter()
            .enumerate()
            .map(|(index, suffix)| AgentDeliveryRecord {
                id: format!("{message_id}_{suffix}"),
                message_id: message_id.to_string(),
                recipient_agent_id: format!("sess_{suffix}"),
                provider: "codex".into(),
                status: if index == 0 { "pending" } else { "deferred" }.into(),
                effective_policy: None,
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: None,
                last_error_message: None,
                created_at: 100 + index as i64,
                updated_at: 100 + index as i64,
            })
            .collect::<Vec<_>>();
        repository
            .insert_agent_message_with_deliveries(&message, &deliveries)
            .expect("seed message fanout");
    }
    (temp, repository)
}

fn command(message_id: &str, key: &str) -> runtime_core::AgentCancelMessageMutationCommand {
    prepare_agent_message_cancellation(
        message_id,
        OperationActor::operator("runtime_operator"),
        Some(key.to_string()),
    )
    .expect("command")
}

#[test]
fn cancellation_commits_all_recipients_and_exact_receipt_across_restarts() {
    let (temp, repository) = seeded_messages();
    let input = command("msg_cancel", "lost-response");
    let first = repository
        .cancel_agent_message_operator(&input)
        .expect("first cancellation");
    assert_eq!(first.deliveries.len(), 2);
    assert!(first.deliveries.iter().all(|row| row.status == "cancelled"));
    let operation = repository
        .get_operation(&first.operation_id)
        .expect("lookup")
        .expect("operation");
    assert_eq!(operation.operation.phase, OperationPhase::Completed);
    assert_eq!(
        operation.operation.exact_terminal_result,
        Some(serde_json::to_value(&first).expect("receipt"))
    );
    assert!(operation.claims.is_empty());

    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let replay = reopened
        .cancel_agent_message_operator(&command("msg_cancel", "lost-response"))
        .expect("lost response replay after reopen");
    assert_eq!(replay, first);
    let mismatch = reopened
        .cancel_agent_message_operator(&command("msg_other", "lost-response"))
        .expect_err("same identity on another message");
    assert!(matches!(mismatch, runtime_core::RuntimeError::Conflict(_)));
    assert!(matches!(
        reopened.cancel_agent_message_operator(&command("msg_cancel", "new-key")),
        Err(runtime_core::RuntimeError::Conflict(_))
    ));

    let old = AgentDeliveryRecord {
        status: "deferred".into(),
        ..first.deliveries[0].clone()
    };
    assert!(matches!(
        reopened.upsert_agent_delivery(&old),
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    assert_eq!(
        reopened
            .get_agent_delivery(&old.id)
            .expect("load")
            .expect("delivery")
            .status,
        "cancelled"
    );
}

#[test]
fn cancellation_rolls_back_fanout_and_receipt_if_any_update_fails() {
    let (temp, repository) = seeded_messages();
    let path = temp.path().join("runtime.sqlite3");
    let connection = open_connection(&path).expect("connection");
    connection
        .execute_batch(
            "CREATE TRIGGER prevent_second_cancel BEFORE UPDATE ON agent_deliveries
             WHEN NEW.id='msg_cancel_b' AND NEW.status='cancelled'
             BEGIN SELECT RAISE(FAIL,'failpoint second recipient'); END;",
        )
        .expect("failpoint");
    drop(connection);
    let input = command("msg_cancel", "failed-tx");
    assert!(repository.cancel_agent_message_operator(&input).is_err());

    let connection = open_connection(&path).expect("connection");
    let statuses: Vec<String> = {
        let mut statement = connection
            .prepare(
                "SELECT status FROM agent_deliveries WHERE message_id='msg_cancel' ORDER BY id",
            )
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert_eq!(statuses, ["pending", "deferred"]);
    let receipts: i64 = connection
        .query_row(
            "SELECT count(*) FROM runtime_operations WHERE kind='agent_message_cancel'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 0, "operation cannot commit ahead of fanout");
    connection
        .execute_batch("DROP TRIGGER prevent_second_cancel")
        .unwrap();
    drop(connection);
    let complete = repository
        .cancel_agent_message_operator(&input)
        .expect("retry after proven rollback");
    assert!(complete
        .deliveries
        .iter()
        .all(|row| row.status == "cancelled"));
}

#[test]
fn concurrent_same_key_cancellations_converge_to_one_receipt() {
    let (_temp, repository) = seeded_messages();
    std::thread::scope(|scope| {
        let handles = (0..8)
            .map(|_| {
                let repository = &repository;
                scope.spawn(move || {
                    repository
                        .cancel_agent_message_operator(&command("msg_cancel", "concurrent"))
                        .expect("convergent cancellation")
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
fn migrated_legacy_mirror_blocks_unsafe_cancel_and_rolls_back_on_mirror_failure() {
    let (temp, repository) = seeded_messages();
    let path = temp.path().join("runtime.sqlite3");
    let db = open_connection(&path).unwrap();
    db.execute_batch(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
         VALUES ('team_legacy','Legacy','sess_sender','test',1,1);
         INSERT INTO team_messages(
           id,team_id,scope,sender_agent_id,recipient_agent_ids_json,input_json,
           priority,policy,created_at
         ) VALUES (
           'msg_cancel','team_legacy','direct','sess_sender','[\"sess_a\"]','[]',
           'normal','non_interrupting',1
         );
         INSERT INTO team_deliveries(
           id,message_id,team_id,recipient_agent_id,provider,status,created_at,updated_at
         ) VALUES (
           'msg_cancel_a','msg_cancel','team_legacy','sess_a','codex','injected',1,1
         );",
    )
    .unwrap();
    let refused = repository.cancel_agent_message_operator(&command("msg_cancel", "legacy-state"));
    assert!(matches!(
        refused,
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository
            .get_agent_delivery("msg_cancel_a")
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    db.execute("UPDATE team_deliveries SET status='pending'", [])
        .unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_legacy_mirror BEFORE UPDATE ON team_deliveries
         WHEN NEW.status='cancelled'
         BEGIN SELECT RAISE(FAIL,'failpoint legacy mirror'); END;",
    )
    .unwrap();
    assert!(repository
        .cancel_agent_message_operator(&command("msg_cancel", "legacy-failure"))
        .is_err());
    let statuses = repository
        .list_agent_delivery_records(&runtime_core::AgentDeliveryListRequest {
            message_id: Some("msg_cancel".into()),
            recipient_agent_id: None,
        })
        .unwrap();
    assert_eq!(statuses[0].status, "pending");
    assert_eq!(statuses[1].status, "deferred");
    db.execute_batch("DROP TRIGGER fail_legacy_mirror").unwrap();

    let result = repository
        .cancel_agent_message_operator(&command("msg_cancel", "legacy-failure"))
        .expect("one atomic cancellation after failpoint removal");
    assert_eq!(result.deliveries.len(), 2);
    assert!(result
        .deliveries
        .iter()
        .all(|delivery| delivery.status == "cancelled"));
    let mirror: String = db
        .query_row(
            "SELECT status FROM team_deliveries WHERE id='msg_cancel_a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mirror, "cancelled");
}
