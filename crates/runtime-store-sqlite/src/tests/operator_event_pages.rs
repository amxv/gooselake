use super::*;
use std::collections::HashSet;

#[test]
fn indexed_event_history_pages_preserve_global_cursor_scope_order_and_payload_privacy() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let tx = db.transaction().unwrap();
    for index in 0..180i64 {
        let (scope, scope_id) = match index % 3 {
            0 => ("workspace", "ws_alpha"),
            1 => ("workspace", "ws_beta"),
            _ => ("process", "proc_gamma"),
        };
        tx.execute(
            "INSERT INTO runtime_events (
                 event_id, scope, scope_id, seq, kind, critical, payload_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, 'synthetic.event', 1, ?5, 100)",
            params![
                format!("evt_{index:04}"),
                scope,
                scope_id,
                index / 3 + 1,
                serde_json::json!({"secret":"private provider payload"}).to_string(),
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let mut cursor = None;
    let mut prior_row_id = 0;
    let mut seen = HashSet::new();
    loop {
        let rows = repository
            .list_operator_events_page(None, cursor, 13)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            assert!(row.row_id > prior_row_id);
            prior_row_id = row.row_id;
            assert!(seen.insert(row.event_id.clone()));
            let public = serde_json::to_value(row).unwrap();
            assert_eq!(public.as_object().unwrap().len(), 8);
            for hidden in [
                "payload",
                "provider",
                "provider_seq",
                "session_id",
                "turn_id",
            ] {
                assert!(public.get(hidden).is_none(), "leaked {hidden}");
            }
        }
        cursor = rows.last().map(|row| row.row_id);
    }
    assert_eq!(seen.len(), 180);
    let mut cursor = None;
    let mut seen_scoped = HashSet::new();
    loop {
        let rows = repository
            .list_operator_events_page(Some((RuntimeEventScope::Workspace, "ws_alpha")), cursor, 17)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            assert_eq!(row.scope, RuntimeEventScope::Workspace);
            assert_eq!(row.scope_id, "ws_alpha");
            assert!(seen_scoped.insert(row.event_id.clone()));
        }
        cursor = rows.last().map(|row| row.row_id);
    }
    assert_eq!(seen_scoped.len(), 60);
    assert!(repository
        .list_operator_events_page(Some((RuntimeEventScope::Workspace, "missing")), None, 3)
        .unwrap()
        .is_empty());
    assert!(repository
        .list_operator_events_page(None, None, 102)
        .is_err());
    assert!(repository
        .list_operator_events_page(None, Some(-1), 1)
        .is_err());
    assert_eq!(
        repository
            .get_operator_event("evt_0001")
            .unwrap()
            .unwrap()
            .payload["secret"],
        "private provider payload"
    );
    assert!(repository
        .get_operator_event("evt_missing")
        .unwrap()
        .is_none());

    for (sql, expected) in [
        (
            "EXPLAIN QUERY PLAN SELECT id,event_id FROM runtime_events
             WHERE id>20 ORDER BY id ASC LIMIT 20",
            "INTEGER PRIMARY KEY",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id,event_id FROM runtime_events
             WHERE scope='workspace' AND scope_id='ws_alpha' AND id>20
             ORDER BY id ASC LIMIT 20",
            "idx_runtime_events_scope_row_id",
        ),
    ] {
        let details = db
            .prepare(sql)
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(details.contains(expected), "index plan: {details}");
        assert!(!details.contains("USE TEMP B-TREE"), "sort plan: {details}");
    }

    // The additive index must upgrade a populated prior schema without
    // rewriting existing event IDs, sequence numbers or JSON payloads.
    db.execute_batch(
        "DROP INDEX idx_runtime_events_scope_row_id;
         DELETE FROM schema_migrations WHERE version=22;",
    )
    .unwrap();
    drop(db);
    repository.initialize_schema().unwrap();
    assert_eq!(
        repository
            .list_operator_events_page(None, None, 101)
            .unwrap()
            .len(),
        101
    );
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        180
    );
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        22
    );
}
