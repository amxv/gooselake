use super::*;
use runtime_store_sqlite::SqliteRuntimeRepository;

pub(super) fn assert_terminal_receipts_match_reopened_sqlite(
    temp: &tempfile::TempDir,
    session_id: &str,
    events: &[runtime_core::RuntimeEventRecord],
) {
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let durable = reopened
        .hydrate_runtime_state()
        .expect("reopen after HTTP turns");
    let session = durable
        .sessions
        .iter()
        .find(|s| s.id == session_id)
        .unwrap();
    assert_eq!(session.active_turn_id, None);
    assert_eq!(session.status, "ready");
    let completed = events
        .iter()
        .filter(|event| event.kind == "turn.completed")
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    for event in completed {
        assert_eq!(
            event.event_id,
            format!("evt_turn_{}_terminal", event.turn_id.as_deref().unwrap())
        );
        assert_eq!(
            durable
                .turns
                .iter()
                .find(|turn| Some(turn.id.as_str()) == event.turn_id.as_deref())
                .unwrap()
                .status,
            "completed"
        );
    }
    assert_eq!(
        reopened
            .list_runtime_events(Some((RuntimeEventScope::Session, session_id)), None, 100)
            .unwrap(),
        events,
        "HTTP replay and restarted SQLite must expose the same terminal journal",
    );
}
