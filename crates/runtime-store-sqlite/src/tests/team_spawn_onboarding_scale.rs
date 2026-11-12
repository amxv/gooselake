//! Bounded indexed onboarding provenance lookup after upgrading from v31.
use super::legacy_team_messages::fixture;
use super::*;

#[test]
fn schema_31_upgrade_preserves_two_thousand_deliveries_and_indexes_original_onboarding_lookup() {
    let (_temp, repo, team) = fixture();
    let mut connection = open_connection(&repo.database_path).unwrap();

    // Simulate an isolated v31 database, not a designated live user's DB.
    connection
        .execute_batch(
            "DROP INDEX idx_workspace_agents_active_route_cwd;
             DROP INDEX idx_managed_worktrees_repo_name_id;
             DROP TRIGGER trg_spawn_deletion_fence_journal_insert;
             DROP TRIGGER trg_spawn_deletion_fence_journal_update;
             DROP TRIGGER trg_spawn_deletion_fence_journal_delete;
             DROP TRIGGER trg_spawn_deletion_fence_admission_insert;
             DROP TRIGGER trg_spawn_deletion_fence_admission_update;
             DROP TRIGGER trg_spawn_deletion_fence_admission_delete;
             DROP VIEW team_spawn_deletion_fence_candidates;
             DROP TABLE team_spawn_deletion_fence;
             DROP INDEX idx_team_deliveries_by_message;
             DELETE FROM schema_migrations WHERE version IN (32,33,34,35);",
        )
        .unwrap();
    let transaction = connection.transaction().unwrap();
    for i in 0..2_000 {
        let message_id = format!("historical_spawn_onboarding_{i}");
        let delivery_id = format!("historical_spawn_delivery_{i}");
        transaction
            .execute(
                r#"INSERT INTO team_messages (
                   id,team_id,scope,sender_agent_id,recipient_agent_ids_json,
                   input_json,image_paths_json,priority,policy,created_at
                 ) VALUES (?1,?2,'direct','session_lead','["session_a"]',
                           '[]','[]','normal','start_new_turn_only',1000)"#,
                params![message_id, team.id],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO team_deliveries (
                   id,message_id,team_id,recipient_agent_id,provider,status,created_at,updated_at
                 ) VALUES (?1,?2,?3,'session_a','codex','injected',1000,1000)",
                params![delivery_id, message_id, team.id],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let original: (i64, String) = connection
        .query_row("SELECT COUNT(*),MIN(id) FROM team_deliveries", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(original.0, 2_000);
    drop(connection);

    // Reopening applies only additive migrations and keeps every delivery.
    let upgraded = SqliteRuntimeRepository::new(repo.database_path.clone());
    upgraded.initialize_schema().unwrap();
    let connection = open_connection(&repo.database_path).unwrap();
    let version: i64 = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, crate::schema::SCHEMA_VERSION);
    let preserved: (i64, String) = connection
        .query_row("SELECT COUNT(*),MIN(id) FROM team_deliveries", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(preserved, original);
    let plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT id,team_id,recipient_agent_id
             FROM team_deliveries WHERE message_id=?1",
            ["historical_spawn_onboarding_999"],
            |row| row.get(3),
        )
        .unwrap();
    assert!(plan.contains("idx_team_deliveries_by_message"), "{plan}");
    let identity: (String, String) = connection
        .query_row(
            "SELECT id,recipient_agent_id FROM team_deliveries WHERE message_id=?1",
            ["historical_spawn_onboarding_999"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        identity,
        (
            "historical_spawn_delivery_999".to_string(),
            "session_a".to_string()
        )
    );
    // An already upgraded restart must not add/change any original deliveries.
    drop(connection);
    upgraded.initialize_schema().unwrap();
    let connection = open_connection(&repo.database_path).unwrap();
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM team_deliveries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2_000);
}
