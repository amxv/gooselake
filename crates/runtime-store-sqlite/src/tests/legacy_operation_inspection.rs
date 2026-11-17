use super::*;
use crate::repository_legacy_operation_inspection::DIAGNOSTIC_PAGE_SQL;

#[test]
fn legacy_inspection_upgrade_preserves_raw_history_and_uses_indexed_keyset_pages() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut db = open_connection(&repository.database_path).unwrap();
    // This is only a disposable v49 fixture; no existing user database opens.
    db.execute_batch(
        "DROP INDEX idx_team_operation_diagnostics_operation_id;
        DELETE FROM schema_migrations WHERE version=50;",
    )
    .unwrap();
    db.execute("INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES('op_spawn_target','history-team','spawn_member_with_worktree','planned','{}',10,20)",[]).unwrap();
    let mut target_ids = Vec::new();
    let tx = db.transaction().unwrap();
    for i in 0..300 {
        tx.execute("INSERT INTO team_operation_diagnostics(operation_id,code,message,payload_json,created_at)
            VALUES(?1,'foreign','original','{malformed-foreign',30)",[format!("foreign-{i}")]).unwrap();
        if i % 100 == 0 {
            tx.execute("INSERT INTO team_operation_diagnostics(operation_id,team_id,code,message,payload_json,created_at)
                VALUES('op_spawn_target','history-team','target','original','{\"keep\":true}',30)",[]).unwrap();
            target_ids.push(tx.last_insert_rowid());
        }
    }
    tx.commit().unwrap();
    let raw_rows = |connection: &Connection| -> Vec<(i64, String, String)> {
        connection.prepare("SELECT id,operation_id,hex(CAST(payload_json AS BLOB)) FROM team_operation_diagnostics ORDER BY id")
            .unwrap().query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
            .collect::<Result<_,_>>().unwrap()
    };
    let original = raw_rows(&db);
    repository.initialize_schema().unwrap();
    assert_eq!(raw_rows(&db), original);
    let version: i64 = db
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(version, crate::schema::SCHEMA_VERSION);
    let plan = db
        .prepare(&format!("EXPLAIN QUERY PLAN {DIAGNOSTIC_PAGE_SQL}"))
        .unwrap()
        .query_map(params!["op_spawn_target", 0_i64, 4096_i64, 3_i64], |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" ");
    assert!(
        plan.contains("idx_team_operation_diagnostics_operation_id"),
        "{plan}"
    );
    assert!(plan.contains("SEARCH"), "{plan}");
    assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
    let first = repository
        .inspect_legacy_team_operation("op_spawn_target", None, 2)
        .unwrap()
        .unwrap();
    assert_eq!(
        first.diagnostics.iter().map(|r| r.id).collect::<Vec<_>>(),
        target_ids[..2]
    );
    assert_eq!(first.next_diagnostic_id, Some(target_ids[1]));
    let next = repository
        .inspect_legacy_team_operation("op_spawn_target", first.next_diagnostic_id, 2)
        .unwrap()
        .unwrap();
    assert_eq!(
        next.diagnostics.iter().map(|r| r.id).collect::<Vec<_>>(),
        target_ids[2..]
    );
    assert!(next.next_diagnostic_id.is_none());
    assert_eq!(
        raw_rows(&db),
        original,
        "inspection does not consume or rewrite history"
    );
    assert!(
        repository.hydrate_runtime_state().is_err(),
        "unrelated malformed history remains strict in full export"
    );
    db.execute_batch(
        "ALTER TABLE team_operation_diagnostics RENAME TO fixture_unavailable_diagnostics;",
    )
    .unwrap();
    let failed = repository.inspect_legacy_team_operation("op_spawn_target", None, 2);
    db.execute_batch(
        "ALTER TABLE fixture_unavailable_diagnostics RENAME TO team_operation_diagnostics;",
    )
    .unwrap();
    assert!(
        failed.is_err(),
        "SQL read errors are not absent or partial evidence"
    );
}

#[test]
fn legacy_inspection_preserves_healthy_full_hydration_history_and_strict_authority_reads() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let journal = runtime_core::TeamOperationJournalRecord {
        operation_id: "op_spawn_healthy_history".into(),
        team_id: "original-team".into(),
        kind: "spawn_member_with_worktree".into(),
        stage: "rollback_pending".into(),
        payload: serde_json::json!({"original":["keep",null,7]}),
        created_at: 10,
        updated_at: 20,
    };
    repository.upsert_team_operation_journal(&journal).unwrap();
    let diagnostic = repository
        .append_team_operation_diagnostic(
            Some(&journal.operation_id),
            Some(&journal.team_id),
            "native_unknown",
            "preserve evidence",
            &serde_json::json!({"review":true,"original":[1,2]}),
            30,
        )
        .unwrap();
    let before = repository.hydrate_runtime_state().unwrap();
    assert_eq!(before.team_operation_journal, vec![journal.clone()]);
    assert_eq!(before.team_operation_diagnostics, vec![diagnostic.clone()]);
    let view = repository
        .inspect_legacy_team_operation(&journal.operation_id, None, 50)
        .unwrap()
        .unwrap();
    assert_eq!(
        view.operation.unwrap().evidence.payload,
        Some(journal.payload.clone())
    );
    assert_eq!(
        view.diagnostics[0].evidence.payload,
        Some(diagnostic.payload)
    );
    assert_eq!(
        repository
            .get_team_operation_journal(&journal.operation_id)
            .unwrap(),
        Some(journal)
    );
    assert_eq!(repository.hydrate_runtime_state().unwrap(), before);
}
