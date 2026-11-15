use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::*;

fn plan(db: &Connection) -> String {
    let mut statement = db
        .prepare(
            "EXPLAIN QUERY PLAN SELECT id,session_id,provider_turn_ref,status,input_json,source,
                    started_at,completed_at,usage_json,error_json
             FROM turns WHERE session_id='retirement_target' ORDER BY rowid ASC",
        )
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ")
}

#[test]
fn schema44_session_turn_projection_uses_seek_index_after_populated_schema43_upgrade() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 43);
    let repository = SqliteRuntimeRepository::new(path.clone());
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    for (id, alias) in [
        ("retirement_target", "target-retire"),
        ("retirement_other_a", "other-retire-a"),
        ("retirement_other_b", "other-retire-b"),
    ] {
        let (session, agent) = workspace_agent_fixture(&workspace, id, alias);
        repository.create_workspace_agent(&session, &agent).unwrap();
    }

    let mut db = open_connection(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    let tx = db.transaction().unwrap();
    for i in 0..3_600 {
        let owner = if i % 9 == 0 {
            "retirement_target"
        } else if i % 2 == 0 {
            "retirement_other_a"
        } else {
            "retirement_other_b"
        };
        tx.execute(
            "INSERT INTO turns (
                id,session_id,status,input_json,source,started_at,completed_at
             ) VALUES (?1,?2,'completed',?3,'user',?4,?4)",
            params![
                format!("turn_{i:05}"),
                owner,
                serde_json::json!({"private_fixture_turn": i}).to_string(),
                i as i64 + 100,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let historical_plan = plan(&db);
    assert!(historical_plan.contains("SCAN turns"), "{historical_plan}");
    let original = repository
        .get_persisted_session_with_turns("retirement_target")
        .unwrap()
        .unwrap();
    assert_eq!(original.1.len(), 400);
    assert_eq!(original.1.first().unwrap().id, "turn_00000");
    assert_eq!(original.1.last().unwrap().id, "turn_03591");
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM turns", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        3_600
    );
    drop(db);

    // Migration is additive to a genuinely populated historical schema43 DB;
    // original id ordering, full turn content, and ownership remain exact.
    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    let upgraded_plan = plan(&db);
    assert!(
        upgraded_plan.contains("SEARCH turns USING INDEX idx_turns_session_projection"),
        "targeted session projection must seek its own rows, not scan 3,600 turns: {upgraded_plan}"
    );
    assert!(
        !upgraded_plan.contains("USE TEMP B-TREE"),
        "{upgraded_plan}"
    );
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert_eq!(
        repository
            .get_persisted_session_with_turns("retirement_target")
            .unwrap()
            .unwrap(),
        original
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM turns", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        3_600
    );

    // A malformed unrelated historical payload still cannot obstruct this
    // narrow read; neither the migration nor query repairs that bad row.
    db.execute(
        "UPDATE sessions SET metadata_json='{broken-json' WHERE id='retirement_other_a'",
        [],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    let reopened = SqliteRuntimeRepository::new(path);
    reopened.initialize_schema().unwrap();
    assert_eq!(
        reopened
            .get_persisted_session_with_turns("retirement_target")
            .unwrap()
            .unwrap(),
        original
    );
    assert!(reopened.hydrate_runtime_state().is_err());
}
