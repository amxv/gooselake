use super::*;
use std::collections::HashSet;

#[test]
fn retained_journal_replays_two_thousand_mixed_scope_events_after_restart_without_gaps() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();

    // Populate the authoritative journal in one transaction, with different
    // scopes interleaved by global row ID and independent scope sequences.
    let mut db = open_connection(&path).unwrap();
    let transaction = db.transaction().unwrap();
    let scopes = [
        ("workspace", "ws_alpha"),
        ("session", "agent_alpha"),
        ("process", "proc_alpha"),
        ("workspace", "ws_beta"),
        ("team", "team_alpha"),
    ];
    for index in 0..2_000i64 {
        let (scope, scope_id) = scopes[(index as usize) % scopes.len()];
        let seq = index / scopes.len() as i64 + 1;
        transaction
            .execute(
                "INSERT INTO runtime_events
                 (event_id, scope, scope_id, seq, kind, critical, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'synthetic.retained', 1, ?5, ?6)",
                params![
                    format!("event_large_{index:05}"),
                    scope,
                    scope_id,
                    seq,
                    serde_json::json!({"opaque_provider_data":"must not affect cursors"})
                        .to_string(),
                    1000 + index
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(db);

    // A new repository instance stands in for a stopped/restarted runtime.
    // Global resume uses SQLite row ID; scoped resume uses scope-local seq.
    let restarted = SqliteRuntimeRepository::new(path.clone());
    let mut global_cursor = 0i64;
    let mut seen = HashSet::new();
    let mut pages = 0;
    loop {
        let page = restarted
            .list_runtime_events(None, Some(global_cursor), 47)
            .unwrap();
        if page.is_empty() {
            break;
        }
        for event in &page {
            assert!(event.row_id > global_cursor, "global cursor must increase");
            global_cursor = event.row_id;
            assert!(seen.insert(event.event_id.clone()), "replayed global row");
        }
        pages += 1;
    }
    assert!(pages > 40, "multiple bounded journal pages were exercised");
    assert_eq!(seen.len(), 2_000);
    assert_eq!(global_cursor, 2_000);

    let mut scoped_cursor = 0i64;
    let mut scoped_seen = HashSet::new();
    loop {
        let page = restarted
            .list_runtime_events(
                Some((RuntimeEventScope::Workspace, "ws_alpha")),
                Some(scoped_cursor),
                13,
            )
            .unwrap();
        if page.is_empty() {
            break;
        }
        for event in &page {
            assert_eq!(event.scope, RuntimeEventScope::Workspace);
            assert_eq!(event.scope_id, "ws_alpha");
            assert_eq!(event.seq, scoped_cursor + 1, "no scoped cursor gap");
            scoped_cursor = event.seq;
            assert!(scoped_seen.insert(event.event_id.clone()));
        }
    }
    assert_eq!(scoped_seen.len(), 400);
    assert_eq!(scoped_cursor, 400);

    // Replay of an observed tail is empty. The next committed append must
    // advance both durable cursor domains, even after opening a fresh store.
    assert!(restarted
        .list_runtime_events(None, Some(global_cursor), 17)
        .unwrap()
        .is_empty());
    assert!(restarted
        .list_runtime_events(
            Some((RuntimeEventScope::Workspace, "ws_alpha")),
            Some(scoped_cursor),
            17,
        )
        .unwrap()
        .is_empty());
    let next = NewRuntimeEvent {
        event_id: "event_after_recovery".into(),
        scope: RuntimeEventScope::Workspace,
        scope_id: "ws_alpha".into(),
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: "workspace.recovered".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"replayed":true}),
        provider: None,
        provider_seq: None,
        created_at: 9_999,
    };
    let appended = restarted.append_runtime_event(&next).unwrap();
    assert_eq!(appended.row_id, 2_001);
    assert_eq!(appended.seq, 401);
    assert_eq!(restarted.append_runtime_event(&next).unwrap(), appended);
    let global_tail = restarted
        .list_runtime_events(None, Some(global_cursor), 17)
        .unwrap();
    assert_eq!(global_tail, [appended.clone()]);
    let scoped_tail = restarted
        .list_runtime_events(
            Some((RuntimeEventScope::Workspace, "ws_alpha")),
            Some(scoped_cursor),
            17,
        )
        .unwrap();
    assert_eq!(scoped_tail, [appended]);

    let db = Connection::open(path).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        2_001,
        "event history must remain retained across runtime restart"
    );
    let plan = |query: &str| {
        let mut statement = db.prepare(query).unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" | ")
    };
    let scoped = plan(
        "EXPLAIN QUERY PLAN SELECT id, event_id FROM runtime_events
         WHERE scope='workspace' AND scope_id='ws_alpha' AND seq>250
         ORDER BY seq ASC LIMIT 17",
    );
    assert!(
        scoped.contains("idx_runtime_events_scope_seq"),
        "scoped index: {scoped}"
    );
    assert!(!scoped.contains("USE TEMP B-TREE"), "scoped sort: {scoped}");
    let global = plan(
        "EXPLAIN QUERY PLAN SELECT id, event_id FROM runtime_events
         WHERE id>1500 ORDER BY id ASC LIMIT 17",
    );
    assert!(
        global.contains("SEARCH runtime_events USING INTEGER PRIMARY KEY"),
        "global rowid index: {global}"
    );
    assert!(!global.contains("USE TEMP B-TREE"), "global sort: {global}");
}
