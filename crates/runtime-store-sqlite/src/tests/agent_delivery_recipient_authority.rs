use super::*;
use runtime_core::{AgentDeliveryRecord, AgentMessageRecord, RuntimeError};

fn copied_message(
    repository: &SqliteRuntimeRepository,
    new_id: &str,
    recipients: &[&str],
) -> AgentMessageRecord {
    let mut original = repository
        .hydrate_runtime_state()
        .unwrap()
        .agent_messages
        .into_iter()
        .find(|row| row.id == "msg_cancel")
        .unwrap();
    original.id = new_id.into();
    original.recipient_agent_ids = recipients.iter().map(|s| s.to_string()).collect();
    original
}

fn copied_delivery(
    repository: &SqliteRuntimeRepository,
    new_id: &str,
    message_id: &str,
    recipient: &str,
) -> AgentDeliveryRecord {
    let mut original = repository
        .get_agent_delivery("msg_cancel_b")
        .unwrap()
        .unwrap();
    original.id = new_id.into();
    original.message_id = message_id.into();
    original.recipient_agent_id = recipient.into();
    original.status = "pending".into();
    original
}

fn assert_no_partial_admission(
    repository: &SqliteRuntimeRepository,
    message_id: &str,
    delivery_ids: &[&str],
) {
    let db = open_connection(&repository.database_path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM agent_messages WHERE id=?1",
            [message_id],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM runtime_events WHERE event_id=?1",
            [format!("agent-message:{message_id}:sent")],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    for id in delivery_ids {
        assert!(repository.get_agent_delivery(id).unwrap().is_none());
    }
}

#[test]
fn message_admission_rejects_out_of_snapshot_recipient_and_rolls_back_all_before_journal() {
    let (_temp, repository) = super::agent_message_cancellation::seeded_messages();
    let message = copied_message(&repository, "forged_recipient_message", &["sess_a"]);
    let delivery = copied_delivery(
        &repository,
        "forged_recipient_delivery",
        &message.id,
        "sess_b",
    );
    assert!(matches!(
        repository.insert_agent_message_with_deliveries(&message, &[delivery]),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert_no_partial_admission(&repository, &message.id, &["forged_recipient_delivery"]);
}

#[test]
fn duplicate_delivery_recipient_in_single_admission_cannot_duplicate_native_authority() {
    let (_temp, repository) = super::agent_message_cancellation::seeded_messages();
    let message = copied_message(
        &repository,
        "duplicate_recipient_message",
        &["sess_a", "sess_b"],
    );
    let first = copied_delivery(
        &repository,
        "duplicate_recipient_first",
        &message.id,
        "sess_a",
    );
    let second = copied_delivery(
        &repository,
        "duplicate_recipient_second",
        &message.id,
        "sess_a",
    );
    assert!(matches!(
        repository.insert_agent_message_with_deliveries(&message, &[first, second]),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert_no_partial_admission(
        &repository,
        &message.id,
        &["duplicate_recipient_first", "duplicate_recipient_second"],
    );
}

#[test]
fn missing_delivery_cannot_publish_message_with_incomplete_native_authority() {
    let (_temp, repository) = super::agent_message_cancellation::seeded_messages();
    let message = copied_message(
        &repository,
        "missing_recipient_message",
        &["sess_a", "sess_b"],
    );
    let partial = copied_delivery(
        &repository,
        "missing_recipient_delivery",
        &message.id,
        "sess_a",
    );
    assert!(matches!(
        repository.insert_agent_message_with_deliveries(&message, &[partial]),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert_no_partial_admission(&repository, &message.id, &["missing_recipient_delivery"]);
}

#[test]
fn duplicate_snapshot_recipient_cannot_publish_message_or_delivery() {
    let (_temp, repository) = super::agent_message_cancellation::seeded_messages();
    let message = copied_message(
        &repository,
        "duplicate_snapshot_message",
        &["sess_a", "sess_a"],
    );
    let delivery = copied_delivery(
        &repository,
        "duplicate_snapshot_delivery",
        &message.id,
        "sess_a",
    );
    assert!(matches!(
        repository.insert_agent_message_with_deliveries(&message, &[delivery]),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    assert_no_partial_admission(&repository, &message.id, &["duplicate_snapshot_delivery"]);
}

#[test]
fn native_claim_requires_recipient_in_original_message_snapshot_on_every_restart() {
    let (temp, repository) = super::agent_message_cancellation::seeded_messages();
    let pending = repository
        .get_agent_delivery("msg_cancel_a")
        .unwrap()
        .unwrap();
    let mut injecting = pending.clone();
    injecting.status = "injecting".into();
    injecting.updated_at += 1;
    let db = open_connection(&repository.database_path).unwrap();
    db.execute(
        "UPDATE agent_messages SET recipient_agent_ids_json='[\"sess_b\"]' WHERE id='msg_cancel'",
        [],
    )
    .unwrap();
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert!(matches!(
        reopened.compare_and_swap_agent_delivery_status(&pending, &injecting),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        reopened.get_agent_delivery(&pending.id).unwrap(),
        Some(pending.clone())
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM runtime_events WHERE kind='agent_delivery.injecting' AND critical=1",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0,
        "an injected status or Critical claim must not exist for a forged recipient"
    );
    db.execute(
        "UPDATE agent_messages SET recipient_agent_ids_json='[\"sess_a\",\"sess_a\",\"sess_b\"]'
         WHERE id='msg_cancel'",
        [],
    )
    .unwrap();
    assert!(matches!(
        reopened.compare_and_swap_agent_delivery_status(&pending, &injecting),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        reopened.get_agent_delivery(&pending.id).unwrap(),
        Some(pending.clone())
    );
    db.execute(
        "UPDATE agent_messages SET recipient_agent_ids_json='[\"sess_a\",\"sess_b\"]'
         WHERE id='msg_cancel'",
        [],
    )
    .unwrap();
    assert!(reopened
        .compare_and_swap_agent_delivery_status(&pending, &injecting)
        .unwrap());
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM runtime_events WHERE kind='agent_delivery.injecting' AND critical=1",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}

#[test]
fn concurrent_independent_claimants_cannot_ignore_changed_recipient_snapshot() {
    let (temp, repository) = super::agent_message_cancellation::seeded_messages();
    let pending = repository
        .get_agent_delivery("msg_cancel_a")
        .unwrap()
        .unwrap();
    let mut injecting = pending.clone();
    injecting.status = "injecting".into();
    injecting.updated_at += 1;
    let db = open_connection(&repository.database_path).unwrap();
    db.execute(
        "UPDATE agent_messages SET recipient_agent_ids_json='[\"sess_b\"]' WHERE id='msg_cancel'",
        [],
    )
    .unwrap();
    let path = temp.path().join("runtime.sqlite3");
    std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|_| {
                let pending = pending.clone();
                let injecting = injecting.clone();
                let path = path.clone();
                scope.spawn(move || {
                    let independent = SqliteRuntimeRepository::new(path);
                    assert!(matches!(
                        independent.compare_and_swap_agent_delivery_status(&pending, &injecting),
                        Err(RuntimeError::Conflict(_))
                    ));
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    assert_eq!(
        repository.get_agent_delivery(&pending.id).unwrap(),
        Some(pending)
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM runtime_events WHERE kind='agent_delivery.injecting' AND critical=1",
            [],|row|row.get::<_,i64>(0)
        ).unwrap(),0
    );
}
