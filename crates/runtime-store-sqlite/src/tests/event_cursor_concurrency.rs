use super::*;
use std::collections::HashSet;
use std::sync::{Arc, Barrier};

#[test]
fn simultaneous_producers_keep_global_and_scoped_cursors_unique_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    repo(&temp).initialize_schema().unwrap();

    // Four independent SQLite connections contend for the same two scopes.
    // The immediate append transaction must serialize MAX(seq)+INSERT while
    // SQLite's row ID independently orders the entire mixed-scope journal.
    let barrier = Arc::new(Barrier::new(4));
    let workers = (0..4)
        .map(|worker| {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let repository = SqliteRuntimeRepository::new(path);
                barrier.wait();
                let mut receipts = Vec::new();
                for n in 0..40 {
                    let workspace = n % 2 == 0;
                    let event = NewRuntimeEvent {
                        event_id: format!("concurrent_{worker}_{n}"),
                        scope: if workspace {
                            RuntimeEventScope::Workspace
                        } else {
                            RuntimeEventScope::Process
                        },
                        scope_id: if workspace { "ws_hot" } else { "proc_hot" }.into(),
                        session_id: None,
                        team_id: None,
                        turn_id: None,
                        kind: "synthetic.concurrent".into(),
                        criticality: RuntimeEventCriticality::Critical,
                        payload: serde_json::json!({"worker":worker,"n":n}),
                        provider: None,
                        provider_seq: None,
                        created_at: 1_000 + n,
                    };
                    let first = repository.append_runtime_event(&event).unwrap();
                    // Replayed provider callbacks must not allocate extra seq
                    // or emit a second global row, even under contention.
                    assert_eq!(repository.append_runtime_event(&event).unwrap(), first);
                    receipts.push(first);
                }
                receipts
            })
        })
        .collect::<Vec<_>>();
    let receipts = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(receipts.len(), 160);
    let ids = receipts
        .iter()
        .map(|row| row.row_id)
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 160, "global row IDs must be unique");
    let mut ordered = ids.into_iter().collect::<Vec<_>>();
    ordered.sort_unstable();
    assert_eq!(ordered, (1..=160).collect::<Vec<_>>());

    // Opening a new repository simulates restart. Page from cursor zero
    // through the complete retained history without materializing a global
    // process, delivery, or session projection.
    let restarted = SqliteRuntimeRepository::new(path);
    for scope in [
        None,
        Some((RuntimeEventScope::Workspace, "ws_hot")),
        Some((RuntimeEventScope::Process, "proc_hot")),
    ] {
        let expected = if scope.is_some() { 80 } else { 160 };
        let mut cursor = 0;
        let mut observed = HashSet::new();
        loop {
            let page = restarted
                .list_runtime_events(scope, Some(cursor), 13)
                .unwrap();
            if page.is_empty() {
                break;
            }
            for row in page {
                let current = if scope.is_some() { row.seq } else { row.row_id };
                assert_eq!(current, cursor + 1, "no cursor gaps after restart");
                assert!(observed.insert(row.event_id), "no duplicate after replay");
                cursor = current;
            }
        }
        assert_eq!(cursor, expected);
        assert_eq!(observed.len(), expected as usize);
    }
}
