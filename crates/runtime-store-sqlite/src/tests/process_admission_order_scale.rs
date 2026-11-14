use super::workspace_agent::apply_schema_through;
use super::*;
use runtime_core::ManagedProcessAdmission;
use std::sync::{Arc, Barrier};

fn admission(process_id: &str) -> ManagedProcessAdmission {
    ManagedProcessAdmission {
        process_id: process_id.into(),
        owner_session_id: Some("fixture_owner".into()),
        workspace_id: Some("fixture_workspace".into()),
        tool_call_id: Some(format!("call_{process_id}")),
        command: serde_json::json!({"command":"inert admission only"}),
        cwd: Some("/disposable/process-fixture".into()),
        timeout_ms: Some(1000),
        stdout_path: format!("/disposable/{process_id}.out"),
        stderr_path: format!("/disposable/{process_id}.err"),
        capture_limit_bytes: 1024,
        admitted_at: 10_000,
    }
}

fn plan(connection: &Connection, sql: &str) -> String {
    let mut statement = connection
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" ")
}

fn max_order(connection: &Connection, column: &str) -> i64 {
    assert!(matches!(column, "queue_order" | "admission_order"));
    connection
        .query_row(
            &format!("SELECT COALESCE(MAX({column}),0) FROM managed_processes"),
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn process_admission_global_queue_max_uses_ordered_index_after_populated_schema42_upgrade() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    // Genuine historical schema42 with substantial terminal process history;
    // no real DB, native command or live provider is consulted.
    apply_schema_through(&path, 42);
    let mut db = open_connection(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    let private_command = serde_json::json!({"private": "x".repeat(1024)}).to_string();
    let mut expected_max = 0i64;
    let tx = db.transaction().unwrap();
    for i in 0..3_600i64 {
        // Queue order is intentionally unrelated to admission order and may
        // repeat; the global maximum must not come from a status-first scan.
        let queue_order = (i * 67) % 5_003 + 100;
        expected_max = expected_max.max(queue_order);
        tx.execute(
            "INSERT INTO managed_processes (
                process_id, workspace_id, command_json, status, admission_order,
                queue_order, capture_limit_bytes, stdout_path, stderr_path,
                admitted_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1024, '/private/out',
                       '/private/err', ?5, ?5)",
            params![
                format!("legacy_{i:05}"),
                if i % 2 == 0 {
                    "workspace_a"
                } else {
                    "workspace_b"
                },
                private_command,
                if i % 3 == 0 { "failed" } else { "completed" },
                i + 1,
                queue_order,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(max_order(&db, "queue_order"), expected_max);
    let old_plan = plan(
        &db,
        "SELECT COALESCE(MAX(queue_order),0)+1 FROM managed_processes",
    );
    assert!(
        !old_plan.contains("idx_managed_processes_queue_allocation"),
        "historical index unexpectedly exists: {old_plan}"
    );
    assert!(
        old_plan.contains("managed_processes"),
        "missing historical read: {old_plan}"
    );
    drop(db);

    // Migration installs only the new index; historical state remains intact.
    let repository = SqliteRuntimeRepository::new(path.clone());
    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM managed_processes", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        3_600
    );
    assert_eq!(max_order(&db, "queue_order"), expected_max);
    let new_plan = plan(
        &db,
        "SELECT COALESCE(MAX(queue_order),0)+1 FROM managed_processes",
    );
    assert!(
        new_plan.contains("COVERING INDEX idx_managed_processes_queue_allocation"),
        "global queue maximum must use the new ordered index: {new_plan}"
    );
    let admission_plan = plan(
        &db,
        "SELECT COALESCE(MAX(admission_order),0)+1 FROM managed_processes",
    );
    assert!(
        admission_plan.contains("COVERING INDEX"),
        "the existing admission uniqueness index must stay usable: {admission_plan}"
    );
    assert!(!new_plan.contains("USE TEMP B-TREE"), "{new_plan}");
    drop(db);

    let first = repository
        .admit_managed_process(&admission("new_after_upgrade"))
        .unwrap();
    assert_eq!(first.admission_order, 3_601);
    assert_eq!(first.queue_order, expected_max + 1);
    let original_event = repository
        .list_runtime_events(
            Some((RuntimeEventScope::Process, "new_after_upgrade")),
            None,
            5,
        )
        .unwrap();
    assert_eq!(original_event.len(), 1);
    assert_eq!(original_event[0].kind, "process.queued");
    assert_eq!(original_event[0].payload["queue_order"], expected_max + 1);

    let restarted = SqliteRuntimeRepository::new(path.clone());
    restarted.initialize_schema().unwrap();
    assert_eq!(
        restarted
            .admit_managed_process(&admission("new_after_upgrade"))
            .unwrap(),
        first,
        "lost-response replay must preserve the original exact admission"
    );
    assert_eq!(
        restarted
            .list_runtime_events(
                Some((RuntimeEventScope::Process, "new_after_upgrade")),
                None,
                5,
            )
            .unwrap(),
        original_event,
        "replay must not fabricate a second queued event"
    );

    // Cross-connection WAL visibility: a pending writer's larger queue order
    // is not visible to readers, but a committed row is after reopen.
    let writer = open_connection(&path).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    writer
        .execute(
            "INSERT INTO managed_processes (
                process_id,command_json,status,admission_order,queue_order,
                capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at
             ) VALUES ('external_pending','{}','completed',5000,99000,1024,'/out','/err',5000,5000)",
            [],
        )
        .unwrap();
    let reader = open_connection(&path).unwrap();
    assert_eq!(max_order(&reader, "queue_order"), expected_max + 1);
    writer.execute_batch("COMMIT").unwrap();
    let reopened = open_connection(&path).unwrap();
    assert_eq!(max_order(&reopened, "queue_order"), 99_000);
    let after_external = restarted
        .admit_managed_process(&admission("new_after_external"))
        .unwrap();
    assert_eq!(after_external.queue_order, 99_001);
    assert_eq!(after_external.admission_order, 5_001);
}

#[test]
fn independent_disposable_admission_writers_keep_distinct_queue_positions_and_exact_replay() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for index in 0..2 {
        let peer = SqliteRuntimeRepository::new(repository.database_path.clone());
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            peer.admit_managed_process(&admission(&format!("racing_{index}")))
        }));
    }
    let rows = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    let mut queues = rows.iter().map(|row| row.queue_order).collect::<Vec<_>>();
    queues.sort();
    assert_eq!(queues, [1, 2]);
    let mut admissions = rows
        .iter()
        .map(|row| row.admission_order)
        .collect::<Vec<_>>();
    admissions.sort();
    assert_eq!(admissions, [1, 2]);
    let restarted = SqliteRuntimeRepository::new(repository.database_path.clone());
    for row in rows {
        assert_eq!(
            restarted
                .admit_managed_process(&admission(&row.process_id))
                .unwrap(),
            row
        );
        let events = restarted
            .list_runtime_events(Some((RuntimeEventScope::Process, &row.process_id)), None, 5)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "process.queued");
    }
}
