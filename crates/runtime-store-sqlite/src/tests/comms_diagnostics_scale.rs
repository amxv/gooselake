use super::*;
use crate::repository_comms_diagnostics::{
    LEGACY_DELIVERY_STATUS_COUNTS_SQL, LEGACY_ROW_COUNTS_SQL,
};
use runtime_core::LegacyCommsDiagnosticCounts;
use std::collections::BTreeMap;

fn hydrated_legacy_counts(
    state: &runtime_core::RuntimeHydratedState,
) -> LegacyCommsDiagnosticCounts {
    let mut statuses = BTreeMap::new();
    for delivery in &state.team_deliveries {
        *statuses.entry(delivery.status.clone()).or_insert(0) += 1;
    }
    LegacyCommsDiagnosticCounts {
        team_count: state.teams.len(),
        member_count: state.team_members.len(),
        message_count: state.team_messages.len(),
        delivery_total: state.team_deliveries.len(),
        delivery_status_counts: statuses,
    }
}

#[test]
fn indexed_legacy_comms_counts_preserve_hydrated_semantics_and_wal_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db_path = temp.path().join("runtime.sqlite3");
    let mut seed = Connection::open(&db_path).unwrap();
    seed.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    let transaction = seed.transaction().unwrap();
    let private_input = serde_json::json!({"private": "s".repeat(1024)}).to_string();
    let statuses = ["pending", "failed", "injected", "legacy_unknown"];
    for i in 0..1_600i64 {
        let team_id = format!("team_{i:04}");
        let session_id = format!("session_{i:04}");
        let message_id = format!("message_{i:04}");
        transaction
            .execute(
                "INSERT INTO sessions (id, provider, status, created_at, updated_at)
                 VALUES (?1, 'inert', 'ready', ?2, ?2)",
                params![session_id, i + 1],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO teams (id, name, lead_agent_id, created_by, created_at,
                                    updated_at, deleted_at)
                 VALUES (?1, 'legacy', ?2, 'fixture', ?3, ?3, ?4)",
                params![team_id, session_id, i + 1, (i % 5 == 0).then_some(i + 2)],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO team_members (team_id, agent_id, joined_at, added_by)
                 VALUES (?1, ?2, ?3, 'fixture')",
                params![team_id, session_id, i + 1],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO team_messages (id, team_id, scope, sender_agent_id,
                     recipient_agent_ids_json, input_json, priority, policy, created_at)
                 VALUES (?1, ?2, 'direct', ?3, '[]', ?4, 'normal', 'auto', ?5)",
                params![message_id, team_id, session_id, private_input, i + 1],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO team_deliveries (id, message_id, team_id, recipient_agent_id,
                     provider, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'inert', ?5, ?6, ?6)",
                params![
                    format!("delivery_{i:04}"),
                    message_id,
                    team_id,
                    session_id,
                    statuses[i as usize % 4],
                    i + 1
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let old = hydrated_legacy_counts(&repository.hydrate_runtime_state().unwrap());
    assert_eq!(old.team_count, 1_600, "even deleted historical teams count");
    assert_eq!(old.member_count, 1_600);
    assert_eq!(old.message_count, 1_600);
    assert_eq!(old.delivery_total, 1_600);
    assert_eq!(old.delivery_status_counts["legacy_unknown"], 400);
    assert_eq!(repository.legacy_comms_diagnostic_counts().unwrap(), old);

    // This must be a covering status index scan, not a delivery payload scan
    // nor an unbounded temporary sort.
    let plan = seed
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {LEGACY_DELIVERY_STATUS_COUNTS_SQL}"
        ))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" ");
    assert!(
        plan.contains("COVERING INDEX idx_team_deliveries_status"),
        "{plan}"
    );
    assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");

    // Simulate another SQLite writer between the scalar counts and status
    // grouping. One read transaction must observe only the first revision.
    let mut reader = Connection::open(&db_path).unwrap();
    let snapshot = reader.transaction().unwrap();
    let before: (i64, i64, i64) = snapshot
        .query_row(LEGACY_ROW_COUNTS_SQL, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(before, (1_600, 1_600, 1_600));
    let writer = Connection::open(&db_path).unwrap();
    writer
        .execute(
            "INSERT INTO teams (id, name, lead_agent_id, created_by, created_at, updated_at)
                    VALUES ('new_team', 'new', 'session_0000', 'fixture', 9999, 9999)",
            [],
        )
        .unwrap();
    writer
        .execute(
            "UPDATE team_deliveries SET status='failed' WHERE id='delivery_0000'",
            [],
        )
        .unwrap();
    let during: (i64, i64, i64) = snapshot
        .query_row(LEGACY_ROW_COUNTS_SQL, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(during, before);
    let seen = snapshot
        .prepare(LEGACY_DELIVERY_STATUS_COUNTS_SQL)
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .collect::<Result<BTreeMap<_, _>, _>>()
        .unwrap();
    assert_eq!(seen["pending"], 400);
    assert_eq!(seen["failed"], 400);
    snapshot.commit().unwrap();

    let reopened = SqliteRuntimeRepository::new(db_path.clone());
    let updated = hydrated_legacy_counts(&reopened.hydrate_runtime_state().unwrap());
    assert_eq!(updated.team_count, 1_601);
    assert_eq!(updated.delivery_status_counts["pending"], 399);
    assert_eq!(updated.delivery_status_counts["failed"], 401);
    assert_eq!(reopened.legacy_comms_diagnostic_counts().unwrap(), updated);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: db_path,
    });
    assert_eq!(
        RuntimeStore::legacy_comms_diagnostic_counts(&store).unwrap(),
        Some(updated)
    );
}

#[test]
fn migration_38_adds_index_without_rewriting_historical_legacy_delivery() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at)
         VALUES('t','old','s','fixture',1,1);
         INSERT INTO team_messages(id,team_id,scope,sender_agent_id,
            recipient_agent_ids_json,input_json,priority,policy,created_at)
         VALUES('m','t','direct','s','[]','[]','normal','auto',1);
         INSERT INTO team_deliveries(id,message_id,team_id,recipient_agent_id,
            provider,status,created_at,updated_at)
         VALUES('d','m','t','s','inert','historical_unknown',1,1);
         DROP INDEX idx_team_deliveries_status;
         DROP INDEX idx_sessions_active_turn_diagnostics;
         DROP INDEX idx_approvals_pending_diagnostics;
         DROP INDEX idx_worktree_claims_active_diagnostics;
         DELETE FROM schema_migrations WHERE version IN (38,39);",
    )
    .unwrap();
    drop(db);

    repository.initialize_schema().unwrap();
    let counts = repository.legacy_comms_diagnostic_counts().unwrap();
    assert_eq!(counts.team_count, 1);
    assert_eq!(counts.delivery_total, 1);
    assert_eq!(counts.delivery_status_counts["historical_unknown"], 1);
    let db = Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_team_deliveries_status'",
            [],
            |row| row.get::<_, i64>(0),
        ).unwrap(),
        1
    );
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        39
    );
}
