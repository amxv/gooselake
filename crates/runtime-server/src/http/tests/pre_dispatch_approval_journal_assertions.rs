use super::*;

// Called by the genuine authenticated workspace/agent HTTP test *before*
// interrupt: approval admission and both critical events must already be
// recoverable in a fresh SQLite repository, with no provider dispatch.
pub(super) async fn assert_pre_dispatch_approval_events(
    runtime: &Arc<RuntimeSessionManager>,
    temp_dir: &tempfile::TempDir,
    agent_id: &str,
) {
    let session = runtime.get_session(agent_id).await.unwrap();
    assert_eq!(session.status, "waiting_for_approval");
    let turn_id = session.active_turn_id.unwrap();
    let restarted_repository =
        runtime_store_sqlite::SqliteRuntimeRepository::new(temp_dir.path().join("runtime.sqlite3"));
    let events = restarted_repository
        .list_runtime_events(
            Some((runtime_core::RuntimeEventScope::Session, agent_id)),
            None,
            100,
        )
        .unwrap();
    let turn_admitted = events
        .iter()
        .filter(|event| {
            event.turn_id.as_deref() == Some(turn_id.as_str()) && event.kind == "turn.admitted"
        })
        .collect::<Vec<_>>();
    let approval_requested = events
        .iter()
        .filter(|event| {
            event.turn_id.as_deref() == Some(turn_id.as_str()) && event.kind == "approval.requested"
        })
        .collect::<Vec<_>>();
    assert_eq!(turn_admitted.len(), 1);
    assert_eq!(approval_requested.len(), 1);
    assert_eq!(approval_requested[0].seq, turn_admitted[0].seq + 1);
    assert_eq!(
        approval_requested[0].criticality,
        runtime_core::RuntimeEventCriticality::Critical
    );
    assert_eq!(
        approval_requested[0].payload["origin"],
        "runtime_pre_dispatch_policy"
    );
    assert!(approval_requested[0].event_id.starts_with("evt_approval_"));
}
