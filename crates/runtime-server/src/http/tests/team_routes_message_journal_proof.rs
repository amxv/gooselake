//! Independent SQLite reopen of an authenticated /v1 message admission.
pub(super) fn assert_admission(
    temp_path: &std::path::Path,
    team_id: &str,
    message_id: &str,
    delivery_id: &str,
) {
    // Independent reopened durable proof: HTTP message admission persisted
    // its initial delivery and both Critical event receipts before native
    // recipient work. Subsequent delivery status transitions may advance.
    let fresh_store =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp_path.join("runtime.sqlite3"));
    let events = fresh_store
        .list_runtime_events(
            Some((runtime_core::RuntimeEventScope::Team, team_id)),
            None,
            100,
        )
        .expect("reopen legacy message and delivery admission events");
    let message_event = events
        .iter()
        .find(|event| event.kind == "team_message.created")
        .expect("critical legacy message event");
    let pending_event = events
        .iter()
        .find(|event| {
            event.kind == "team_delivery.pending"
                && event.payload["delivery"]["id"].as_str() == Some(delivery_id)
        })
        .expect("critical legacy delivery pending event");
    assert_eq!(message_event.seq, 5);
    assert_eq!(pending_event.seq, 6);
    assert_eq!(
        message_event.criticality,
        runtime_core::RuntimeEventCriticality::Critical
    );
    assert_eq!(
        pending_event.criticality,
        runtime_core::RuntimeEventCriticality::Critical
    );
    assert_eq!(
        message_event.payload["message"]["id"].as_str(),
        Some(message_id)
    );
    assert_eq!(
        pending_event.payload["delivery"]["status"].as_str(),
        Some("pending")
    );
    let persisted = fresh_store
        .hydrate_runtime_state()
        .expect("reopen message authority");
    assert!(persisted
        .team_messages
        .iter()
        .any(|message| message.id == message_id));
    assert!(persisted
        .team_deliveries
        .iter()
        .any(|delivery| delivery.id == delivery_id));
}
