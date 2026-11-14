use super::*;
use std::collections::BTreeMap;

#[test]
fn managed_process_status_projection_is_index_only_and_matches_full_history_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL").unwrap();

    let statuses = [
        "queued",
        "launch_reserved",
        "running",
        "completed",
        "failed",
        "interrupted",
    ];
    let command_json = serde_json::json!({"private_command": "x".repeat(2048)}).to_string();
    let tx = db.transaction().unwrap();
    for i in 0..2_400i64 {
        tx.execute(
            "INSERT INTO managed_processes (
                process_id, command_json, status, admission_order, queue_order,
                capture_limit_bytes, stdout_path, stderr_path, admitted_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?4, 1024, '/private/out', '/private/err', ?4, ?4)",
            params![
                format!("process_{i:05}"),
                command_json,
                statuses[(i as usize) % statuses.len()],
                i + 1
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    // Prove the old full-record path's exact counts, including all statuses.
    let full = repository.list_managed_processes().unwrap();
    let mut legacy_counts = BTreeMap::new();
    for record in &full {
        *legacy_counts.entry(record.status.clone()).or_insert(0usize) += 1;
    }
    assert_eq!(full.len(), 2_400);
    assert_eq!(
        repository.managed_process_status_counts().unwrap(),
        legacy_counts
    );

    let plan = db
        .prepare(
            "EXPLAIN QUERY PLAN SELECT status, COUNT(*) FROM managed_processes GROUP BY status",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" ");
    assert!(
        plan.contains("COVERING INDEX idx_managed_processes_queue"),
        "{plan}"
    );
    assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");

    // A separate writer cannot leak an uncommitted status into this read.
    let writer = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    writer
        .execute_batch("BEGIN IMMEDIATE; UPDATE managed_processes SET status='killed' WHERE process_id='process_00000';")
        .unwrap();
    assert_eq!(
        repository.managed_process_status_counts().unwrap(),
        legacy_counts
    );
    writer.execute_batch("COMMIT").unwrap();

    let mut updated = legacy_counts;
    *updated.get_mut("queued").unwrap() -= 1;
    updated.insert("killed".into(), 1);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(reopened.managed_process_status_counts().unwrap(), updated);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert_eq!(
        RuntimeStore::managed_process_status_counts(&store).unwrap(),
        Some(updated)
    );
}
