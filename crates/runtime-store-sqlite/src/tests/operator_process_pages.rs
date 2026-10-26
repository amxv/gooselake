use super::*;
use std::collections::HashSet;

#[test]
fn operator_process_pages_use_indexed_stable_keysets_without_loading_commands() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let tx = db.transaction().unwrap();
    for n in 0..180i64 {
        tx.execute(
            "INSERT INTO managed_processes (
                process_id, workspace_id, command_json, status, admission_order,
                queue_order, capture_limit_bytes, stdout_path, stderr_path, admitted_at, updated_at
             ) VALUES (?1, ?2, ?3, 'completed', ?4, ?4, 1024, 'secret.stdout',
                       'secret.stderr', ?5, ?5)",
            params![
                format!("proc_{n:04}"),
                if n % 3 == 0 { "ws_one" } else { "ws_two" },
                serde_json::json!({"command":"private process input"}).to_string(),
                n + 1,
                10_000 + n / 9,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let mut cursor = None;
    let mut seen = HashSet::new();
    let mut last = (i64::MAX, String::from("zzz"));
    loop {
        let rows = repository
            .list_managed_processes_page(None, cursor.clone(), 17)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let key = (row.admitted_at, row.process_id.clone());
            assert!(key < last, "descending `(admitted_at, process_id)` cursor");
            last = key.clone();
            assert!(seen.insert(row.process_id.clone()), "duplicate process");
            assert_eq!(row.completion_state, "not_required");
            let object = serde_json::to_value(row).unwrap();
            assert_eq!(object.as_object().unwrap().len(), 8);
            for sensitive in ["command", "cwd", "pid", "stdout_path", "tool_call_id"] {
                assert!(object.get(sensitive).is_none());
            }
        }
        cursor = rows
            .last()
            .map(|row| (row.admitted_at, row.process_id.clone()));
    }
    assert_eq!(seen.len(), 180);

    let mut cursor = None;
    let mut scoped = HashSet::new();
    loop {
        let rows = repository
            .list_managed_processes_page(Some("ws_one"), cursor.clone(), 11)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            assert_eq!(row.workspace_id.as_deref(), Some("ws_one"));
            assert!(scoped.insert(row.process_id.clone()));
        }
        cursor = rows
            .last()
            .map(|row| (row.admitted_at, row.process_id.clone()));
    }
    assert_eq!(scoped.len(), 60);
    assert!(repository
        .list_managed_processes_page(Some("ws_absent"), None, 1)
        .unwrap()
        .is_empty());
    assert!(repository
        .list_managed_processes_page(None, None, 102)
        .is_err());

    for (sql, expected_index) in [
        (
            "EXPLAIN QUERY PLAN SELECT process_id, status FROM managed_processes
             WHERE (admitted_at, process_id) < (10020,'proc_0040')
             ORDER BY admitted_at DESC,process_id DESC LIMIT 20",
            "idx_managed_processes_admitted_page",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT process_id, status FROM managed_processes
             WHERE workspace_id='ws_one' AND (admitted_at,process_id) < (10020,'proc_0040')
             ORDER BY admitted_at DESC,process_id DESC LIMIT 20",
            "idx_managed_processes_workspace_admitted_page",
        ),
    ] {
        let mut statement = db.prepare(sql).unwrap();
        let details = statement
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(details.contains(expected_index), "index plan: {details}");
        assert!(!details.contains("USE TEMP B-TREE"), "sort: {details}");
    }

    // Simulate an existing populated schema-20 installation upgrading in
    // place, without rewriting any of its 180 process/history rows.
    db.execute_batch(
        "DROP INDEX idx_managed_processes_admitted_page;
         DROP INDEX idx_managed_processes_workspace_admitted_page;
         DROP INDEX idx_runtime_events_scope_row_id;
         DELETE FROM schema_migrations WHERE version IN (21,22);",
    )
    .unwrap();
    drop(db);
    repository.initialize_schema().unwrap();
    assert_eq!(
        repository
            .list_managed_processes_page(None, None, 101)
            .unwrap()
            .len(),
        101
    );
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM managed_processes", [], |row| row
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
