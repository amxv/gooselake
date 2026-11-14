use super::*;
use crate::repository_recovery_diagnostics::{
    ACTIVE_TURNS_DIAGNOSTICS_SQL, ORPHAN_CLAIMS_DIAGNOSTICS_SQL, PENDING_APPROVALS_DIAGNOSTICS_SQL,
};
use std::collections::{HashMap, HashSet};

/// Independent parity oracle over the historical full hydration path.
fn hydrated_anomalies(state: &runtime_core::RuntimeHydratedState) -> Vec<String> {
    let mut result = Vec::new();
    let turns = state
        .turns
        .iter()
        .map(|t| (t.id.as_str(), t))
        .collect::<HashMap<_, _>>();
    let session_ids = state
        .sessions
        .iter()
        .map(|s| s.id.as_str())
        .collect::<HashSet<_>>();
    let terminal = |status: &str| matches!(status, "completed" | "interrupted" | "failed");
    for session in &state.sessions {
        if let Some(active) = session.active_turn_id.as_deref() {
            match turns.get(active) {
                None => result.push(format!(
                    "session {} references missing active turn {}",
                    session.id, active
                )),
                Some(turn) if turn.session_id != session.id => result.push(format!(
                    "session {} active turn {} belongs to {}",
                    session.id, active, turn.session_id
                )),
                Some(turn) if terminal(&turn.status) => result.push(format!(
                    "session {} retains terminal active turn {} ({})",
                    session.id, active, turn.status
                )),
                _ => {}
            }
        }
    }
    for approval in &state.approvals {
        if approval.status != "pending" {
            continue;
        }
        match turns.get(approval.turn_id.as_str()) {
            None => result.push(format!(
                "pending approval {} references missing turn {}",
                approval.id, approval.turn_id
            )),
            Some(turn) if terminal(&turn.status) => result.push(format!(
                "pending approval {} references terminal turn {} ({})",
                approval.id, turn.id, turn.status
            )),
            _ => {}
        }
    }
    for claim in &state.managed_worktree_claims {
        if claim.released_at.is_none() && !session_ids.contains(claim.session_id.as_str()) {
            result.push(format!(
                "active worktree claim {} -> {} references missing session",
                claim.worktree_id, claim.session_id
            ));
        }
    }
    result
}

#[test]
fn recovery_anomaly_projection_preserves_hydrated_order_and_wal_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    // Historical corrupt references must be read and reported, never repaired
    // or silently filtered by foreign key enforcement during diagnostics.
    let mut db = Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA foreign_keys=OFF; PRAGMA journal_mode=WAL;")
        .unwrap();
    let transaction = db.transaction().unwrap();
    let private = serde_json::json!({"private": "q".repeat(1024)}).to_string();
    for i in 0..1_200i64 {
        let active = match i % 5 {
            0 => Some(format!("missing_turn_{i}")),
            1 => Some(format!("foreign_turn_{i}")),
            2 => Some(format!("terminal_turn_{i}")),
            3 => Some(format!("running_turn_{i}")),
            _ => None,
        };
        transaction
            .execute(
                "INSERT INTO sessions(id,provider,status,active_turn_id,metadata_json,
                                   created_at,updated_at)
             VALUES(?1,'inert','ready',?2,?3,?4,?4)",
                params![format!("session_{i:04}"), active, private, i % 13],
            )
            .unwrap();
    }
    for i in 0..1_200i64 {
        let (id, owner, status) = match i % 5 {
            1 => (
                format!("foreign_turn_{i}"),
                "session_0000".to_string(),
                "running",
            ),
            2 => (
                format!("terminal_turn_{i}"),
                format!("session_{i:04}"),
                ["completed", "interrupted", "failed"][i as usize % 3],
            ),
            3 => (
                format!("running_turn_{i}"),
                format!("session_{i:04}"),
                "running",
            ),
            _ => continue,
        };
        transaction
            .execute(
                "INSERT INTO turns(id,session_id,status,input_json) VALUES(?1,?2,?3,?4)",
                params![id, owner, status, private],
            )
            .unwrap();
    }
    for i in 0..1_200i64 {
        let turn = match i % 5 {
            0 => format!("missing_approval_turn_{i}"),
            1 => format!("terminal_turn_{}", i + 1),
            2 => format!("terminal_turn_{i}"),
            3 => format!("running_turn_{i}"),
            _ => format!("running_turn_{}", i - 1),
        };
        transaction
            .execute(
                "INSERT INTO approvals(id,session_id,turn_id,status,request_json,created_at)
             VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    format!("approval_{i:04}"),
                    format!("session_{i:04}"),
                    turn,
                    if i % 5 == 4 { "resolved" } else { "pending" },
                    private,
                    i % 17
                ],
            )
            .unwrap();
    }
    for i in 0..450i64 {
        let worktree = format!("worktree_{i:04}");
        transaction
            .execute(
                "INSERT INTO managed_worktrees(id,repo_root,worktree_root,worktree_cwd,
                                           branch_name,worktree_name,unified_workspace_path,
                                           deletion_policy,created_at,updated_at)
             VALUES(?1,'/fixture',?2,?2,?1,?1,?2,'retain',?3,?3)",
                params![
                    worktree,
                    "/fixture/private/".to_string() + &"p".repeat(1024),
                    i
                ],
            )
            .unwrap();
        transaction.execute(
            "INSERT INTO managed_worktree_claims(worktree_id,session_id,claim_role,created_at,released_at)
             VALUES(?1,?2,'primary',?3,?4)",
            params![worktree, if i % 3 == 0 { format!("session_{i:04}") }
                      else { format!("missing_session_{i:04}") }, i % 11,
                    if i % 5 == 0 { Some(i + 1) } else { None }],
        ).unwrap();
    }
    for i in 0..75i64 {
        transaction
            .execute(
                "INSERT INTO processes(id,command_json,status,started_at)
             VALUES(?1,?2,'running',?3)",
                params![format!("legacy_process_{i}"), private, i],
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let historical = hydrated_anomalies(&repository.hydrate_runtime_state().unwrap());
    assert!(
        historical.len() > 900,
        "fixture must include multiple anomaly classes"
    );
    assert!(historical
        .iter()
        .any(|a| a.contains("belongs to session_0000")));
    assert!(historical.iter().any(|a| a.contains("pending approval")));
    assert!(historical
        .iter()
        .any(|a| a.contains("active worktree claim")));
    assert!(!historical.iter().any(|a| a.contains("legacy_process")));
    assert_eq!(
        repository.recovery_diagnostic_anomalies().unwrap(),
        historical
    );

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    assert_eq!(
        RuntimeStore::recovery_diagnostic_anomalies(&store).unwrap(),
        Some(historical)
    );

    for (sql, index) in [
        (
            ACTIVE_TURNS_DIAGNOSTICS_SQL,
            "idx_sessions_active_turn_diagnostics",
        ),
        (
            PENDING_APPROVALS_DIAGNOSTICS_SQL,
            "idx_approvals_pending_diagnostics",
        ),
        (
            ORPHAN_CLAIMS_DIAGNOSTICS_SQL,
            "idx_worktree_claims_active_diagnostics",
        ),
    ] {
        let plan = db
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" ");
        assert!(plan.contains(index), "{index}: {plan}");
        assert!(!plan.contains("USE TEMP B-TREE"), "{sql}: {plan}");
    }

    // An independent WAL writer must not split the three anomaly families
    // across different revisions within a read transaction.
    let mut reader = Connection::open(&path).unwrap();
    let snapshot = reader.transaction().unwrap();
    let before: i64 = snapshot
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE active_turn_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    db.execute(
        "INSERT INTO sessions(id,provider,status,active_turn_id,created_at,updated_at)
         VALUES('new_session','inert','ready','unresolvable',9999,9999)",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT INTO approvals(id,session_id,turn_id,status,request_json,created_at)
         VALUES('new_approval','new_session','lost_turn','pending','{}',9999)",
        [],
    )
    .unwrap();
    assert_eq!(
        snapshot
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE active_turn_id IS NOT NULL",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        before
    );
    assert_eq!(
        snapshot
            .query_row(
                "SELECT COUNT(*) FROM approvals WHERE id='new_approval'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    snapshot.commit().unwrap();
    let reopened = SqliteRuntimeRepository::new(path);
    let latest = hydrated_anomalies(&reopened.hydrate_runtime_state().unwrap());
    assert!(latest
        .iter()
        .any(|a| a.contains("new_session references missing")));
    assert!(latest
        .iter()
        .any(|a| a.contains("pending approval new_approval")));
    assert_eq!(reopened.recovery_diagnostic_anomalies().unwrap(), latest);
}

#[test]
fn recovery_indexes_upgrade_from_previous_schema_without_rewriting_history() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(
        "INSERT INTO sessions(id,provider,status,active_turn_id,created_at,updated_at)
         VALUES('legacy','inert','ready','lost',1,1);
         DROP INDEX idx_sessions_active_turn_diagnostics;
         DROP INDEX idx_approvals_pending_diagnostics;
         DROP INDEX idx_worktree_claims_active_diagnostics;
         DROP INDEX idx_agent_deliveries_message_recipient_created;
         DELETE FROM schema_migrations WHERE version IN (39,40);",
    )
    .unwrap();
    drop(db);
    repository.initialize_schema().unwrap();
    assert_eq!(
        repository.recovery_diagnostic_anomalies().unwrap(),
        vec!["session legacy references missing active turn lost".to_string()]
    );
    let db = Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    for name in [
        "idx_sessions_active_turn_diagnostics",
        "idx_approvals_pending_diagnostics",
        "idx_worktree_claims_active_diagnostics",
    ] {
        assert_eq!(
            db.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                [name],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }
}
