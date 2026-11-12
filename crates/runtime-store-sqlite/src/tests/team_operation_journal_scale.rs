use super::*;
use runtime_core::{RuntimeError, TeamOperationJournalRecord};

fn operation(id: &str, team: &str, stage: &str, created_at: i64) -> TeamOperationJournalRecord {
    TeamOperationJournalRecord {
        operation_id: id.to_string(),
        team_id: team.to_string(),
        kind: "spawn_member_with_worktree".to_string(),
        stage: stage.to_string(),
        payload: serde_json::json!({"stage":stage}),
        created_at,
        updated_at: created_at,
    }
}

#[test]
fn independent_stages_keep_first_admission_under_large_history_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut connection = open_connection(&repository.database_path).unwrap();
    let tx = connection.transaction().unwrap();
    for index in 0..2_000_i64 {
        tx.execute(
            "INSERT INTO team_operation_journal
             (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
             VALUES (?1,?2,'spawn_member_with_worktree','planned','{}',?3,?3)",
            params![
                format!("spawn_{index:04}"),
                if index % 2 == 0 { "team_a" } else { "team_b" },
                100_i64 + index,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let lookup_plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT created_at FROM team_operation_journal
             WHERE operation_id='spawn_1000' AND team_id='team_a'",
            [],
            |row| row.get(3),
        )
        .unwrap();
    assert!(
        lookup_plan.contains("sqlite_autoindex_team_operation_journal"),
        "operation identity must use primary-key lookup: {lookup_plan}"
    );
    let exact_lookup_plan: String = connection.query_row(
        "EXPLAIN QUERY PLAN SELECT operation_id,team_id,kind,stage,payload_json,created_at,updated_at
         FROM team_operation_journal WHERE operation_id='spawn_1000'",
        [], |row| row.get(3),
    ).unwrap();
    assert!(
        exact_lookup_plan.contains("sqlite_autoindex_team_operation_journal"),
        "recovery inspection must use exact primary-key lookup: {exact_lookup_plan}"
    );
    let exact = repository
        .get_team_operation_journal("spawn_1000")
        .unwrap()
        .unwrap();
    assert_eq!(exact.operation_id, "spawn_1000");
    assert_eq!(exact.team_id, "team_a");
    assert_eq!(exact.stage, "planned");
    assert!(repository
        .get_team_operation_journal("spawn_absent")
        .unwrap()
        .is_none());

    let mut updated = operation("spawn_1000", "team_a", "session_created", 900_000);
    repository.upsert_team_operation_journal(&updated).unwrap();
    let first: (String, String, i64, i64) = connection
        .query_row(
            "SELECT kind,stage,created_at,updated_at FROM team_operation_journal
             WHERE operation_id='spawn_1000'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        first,
        (
            "spawn_member_with_worktree".into(),
            "session_created".into(),
            1100,
            900_000
        )
    );

    updated.stage = "onboarding_sent".into();
    updated.updated_at = 900_001;
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    reopened.upsert_team_operation_journal(&updated).unwrap();
    let again: (String, i64, i64) = connection
        .query_row(
            "SELECT stage,created_at,updated_at FROM team_operation_journal WHERE operation_id='spawn_1000'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(again, ("onboarding_sent".into(), 1100, 900_001));
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM team_operation_journal", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        2_000
    );

    let mut foreign = updated.clone();
    foreign.team_id = "team_b".into();
    assert!(matches!(
        reopened.upsert_team_operation_journal(&foreign),
        Err(RuntimeError::Conflict(_))
    ));
    foreign = updated.clone();
    foreign.kind = "other_kind".into();
    assert!(matches!(
        reopened.upsert_team_operation_journal(&foreign),
        Err(RuntimeError::Conflict(_))
    ));
    let preserved: (String, String, i64) = connection
        .query_row(
            "SELECT team_id,stage,created_at FROM team_operation_journal WHERE operation_id='spawn_1000'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(preserved, ("team_a".into(), "onboarding_sent".into(), 1100));
}

#[test]
fn failed_stage_update_does_not_overwrite_durable_prior_operation() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    // A new native spawn plan now requires a live persisted team. This is
    // deliberately a real roster fixture, not an orphan synthetic journal.
    repository
        .upsert_team(&runtime_core::TeamRecord {
            id: "team_a".into(),
            name: "Journal stage-fault team".into(),
            lead_agent_id: "fixture_lead".into(),
            created_by: "fixture".into(),
            created_at: 100,
            updated_at: 100,
            deleted_at: None,
        })
        .unwrap();
    let initial = operation("spawn_fault", "team_a", "planned", 500);
    repository.upsert_team_operation_journal(&initial).unwrap();
    let connection = open_connection(&repository.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fault_spawn_journal BEFORE UPDATE ON team_operation_journal
         WHEN NEW.operation_id='spawn_fault'
         BEGIN SELECT RAISE(ABORT,'injected post-native journal failure'); END;",
        )
        .unwrap();
    let mut advanced = initial.clone();
    advanced.stage = "session_created".into();
    advanced.payload = serde_json::json!({"session":"native_unknown"});
    advanced.created_at = 900;
    advanced.updated_at = 901;
    assert!(repository.upsert_team_operation_journal(&advanced).is_err());
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        reopened
            .list_team_operation_journal(Some("team_a"))
            .unwrap(),
        vec![initial]
    );
    // A failure after a native effect must remain unresolved; this test does
    // not grant permission to create the session or worktree again.
}
