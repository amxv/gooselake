//! Full production-schema fixtures, exact SQL-builder plans, and snapshot tests.
//! These are source drafts only; they do not establish measured scan budgets.
use super::legacy_spawn_obligations_support::*;
use super::{open_connection, repo, sample_session};
use crate::repository_legacy_spawn_obligations::{
    classify_legacy_spawn_obligations, metadata_page_sql, stream_has_more_sql,
};
use runtime_core::legacy_spawn_obligations::{
    LegacyNativeTarget, LegacySpawnScanBudget, LegacySpawnStream, LegacyTargetBlockReason as Reason,
};
use rusqlite::{params_from_iter, types::Value as SqlValue, Connection, TransactionBehavior};

const ALL_STAGE_INDEX: &str = "idx_team_spawn_journal_all_operations";

fn explain(db: &Connection, sql: &str, values: Vec<SqlValue>) -> Vec<String> {
    db.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map(params_from_iter(values.iter()), |row| row.get(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<String>>>()
        .unwrap()
}

fn admission_operation_index(db: &Connection) -> String {
    let indexes = db
        .prepare("PRAGMA index_list('team_spawn_admissions')")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, bool>(2)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    indexes
        .into_iter()
        .find_map(|(name, unique)| {
            if !unique {
                return None;
            }
            let quoted = name.replace('\'', "''");
            let columns = db
                .prepare(&format!("PRAGMA index_info('{quoted}')"))
                .unwrap()
                .query_map([], |row| row.get::<_, String>(2))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (columns == ["operation_id"]).then_some(name)
        })
        .expect("migration 30 must retain a unique operation_id index")
}

fn candidate_ids(db: &Connection, stream: LegacySpawnStream) -> Vec<String> {
    let sql = metadata_page_sql(stream, false);
    db.prepare(&sql)
        .unwrap()
        .query_map([128_i64], |row| row.get::<_, Vec<u8>>(2))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .into_iter()
        .map(|bytes| String::from_utf8(bytes).unwrap())
        .collect()
}

#[test]
fn migration_51_preserves_raw_v50_history_and_indexes_all_candidate_stages() {
    assert_eq!(
        crate::schema::SCHEMA_VERSION,
        51,
        "this disposable downgrade fixture must be reviewed when later migrations are added"
    );
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    // Undo only this index migration in this new disposable fixture.
    db.execute_batch(
        "DROP INDEX idx_team_spawn_journal_all_operations;
         DELETE FROM schema_migrations WHERE version=51;",
    )
    .unwrap();
    let stages = [
        "planned",
        "worktree_created",
        "session_create_review_required",
        "session_created",
        "team_joined",
        "onboarding_sent",
        "completed",
        "rollback_pending",
        "rollback_review_required",
        "rolled_back",
        "unknown_future_stage",
    ];
    let mut expected_keyed = Vec::new();
    let mut expected_journal = Vec::new();
    for (index, stage) in stages.into_iter().enumerate() {
        for keyed in [false, true] {
            let op = format!("{}_{index:02}", if keyed { "key" } else { "raw" });
            journal(&db, &op, TEAM, KIND, stage, "{original-malformed");
            expected_journal.push(op.clone());
            if keyed {
                admission(&db, &op, TEAM, Some("{original-receipt-malformed"));
                expected_keyed.push(op);
            }
        }
    }
    admission(&db, "missing_journal", TEAM, None);
    expected_keyed.push("missing_journal".into());
    journal(
        &db,
        "wrong_kind",
        "foreign-team",
        "remove_member",
        "completed",
        "{}",
    );
    admission(&db, "wrong_kind", TEAM, Some("{}"));
    expected_keyed.push("wrong_kind".into());
    // Preserve SQLite type and invalid UTF-8 bytes, not just JSON equivalence.
    db.execute_batch(
        "UPDATE team_operation_journal SET payload_json=CAST(x'80' AS TEXT)
         WHERE operation_id='raw_00';
         UPDATE team_spawn_admissions SET completed_response_json=x'80ff00'
         WHERE operation_id='key_00';",
    )
    .unwrap();
    let before = (
        raw_rows(&db, "team_operation_journal"),
        raw_rows(&db, "team_spawn_admissions"),
    );
    repository.initialize_schema().unwrap();
    assert_eq!(
        (
            raw_rows(&db, "team_operation_journal"),
            raw_rows(&db, "team_spawn_admissions")
        ),
        before
    );
    let versions = db
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(versions, (1..=51).collect::<Vec<i64>>());
    let index_sql: String = db
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?1",
            [ALL_STAGE_INDEX],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        index_sql.contains("WHERE kind='spawn_member_with_worktree'"),
        "{index_sql}"
    );
    assert!(
        !index_sql.contains("stage"),
        "weak terminal history must stay indexed: {index_sql}"
    );
    let columns = db
        .prepare("PRAGMA index_info('idx_team_spawn_journal_all_operations')")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(2))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(columns, ["operation_id"]);
    expected_keyed.sort();
    expected_journal.sort();
    assert_eq!(
        candidate_ids(&db, LegacySpawnStream::KeyedAdmissions),
        expected_keyed
    );
    assert_eq!(
        candidate_ids(&db, LegacySpawnStream::SpawnJournal),
        expected_journal
    );
    assert_eq!(
        (
            raw_rows(&db, "team_operation_journal"),
            raw_rows(&db, "team_spawn_admissions")
        ),
        before
    );
}

#[test]
fn exact_classifier_page_and_exhaustion_sql_use_production_indexes_with_and_without_cursor() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    certified(&db, "a_certified", "{not-read");
    journal(
        &db,
        "z_terminal",
        TEAM,
        KIND,
        "rolled_back",
        &child_payload("other"),
    );
    let admission_index = admission_operation_index(&db);
    for stream in [
        LegacySpawnStream::KeyedAdmissions,
        LegacySpawnStream::SpawnJournal,
    ] {
        for has_cursor in [false, true] {
            for metadata in [false, true] {
                let sql = if metadata {
                    metadata_page_sql(stream, has_cursor)
                } else {
                    stream_has_more_sql(stream, has_cursor)
                };
                let mut values = Vec::new();
                if has_cursor {
                    values.push(SqlValue::Text("a".into()));
                }
                if metadata {
                    values.push(SqlValue::Integer(64));
                }
                let plan = explain(&db, &sql, values);
                let expected = if stream == LegacySpawnStream::KeyedAdmissions {
                    admission_index.as_str()
                } else {
                    ALL_STAGE_INDEX
                };
                let access = plan
                    .iter()
                    .find(|line| line.contains(expected))
                    .unwrap_or_else(|| {
                        panic!("expected {expected} for actual SQL {sql}: {plan:?}")
                    });
                assert!(
                    !plan.iter().any(|line| line.contains("USE TEMP B-TREE")),
                    "keyset order must not sort the candidate history: {plan:?}"
                );
                assert!(!sql.contains("OFFSET"));
                if has_cursor {
                    assert!(
                        access.contains("SEARCH") && access.contains("operation_id>?"),
                        "cursor must constrain the indexed candidate stream: {plan:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn production_candidate_streams_ignore_unrelated_journal_payloads_without_hiding_spawn_history() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut db = open_connection(&repository.database_path).unwrap();
    let tx = db.transaction().unwrap();
    for index in 0..256 {
        journal(
            &tx,
            &format!("foreign_{index:03}"),
            "foreign-team",
            "remove_member",
            "completed",
            "{foreign-malformed",
        );
    }
    certified(&tx, "certified", "{irrelevant-malformed");
    journal(
        &tx,
        "unkeyed",
        TEAM,
        KIND,
        "rolled_back",
        &child_payload("another-child"),
    );
    tx.commit().unwrap();
    let before = (
        raw_rows(&db, "team_operation_journal"),
        raw_rows(&db, "team_spawn_admissions"),
    );
    let result = scan(&mut db, &[session("target")], budget(3, 1024 * 1024)).unwrap();
    assert_clear(&result);
    assert_eq!(
        result.stats.source_rows_read, 3,
        "one admission plus both spawn journal rows; unrelated kinds are not source candidates"
    );
    assert_eq!(result.stats.certified_receipts, 1);
    assert_eq!(
        (
            raw_rows(&db, "team_operation_journal"),
            raw_rows(&db, "team_spawn_admissions")
        ),
        before
    );
    assert!(
        repository.hydrate_runtime_state().is_err(),
        "a classifier must not depend on exhaustive mutable-history hydration"
    );
}

#[test]
fn saved_completion_does_not_depend_on_current_session_or_roster_hydration() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut current = sample_session();
    current.id = "historical-child".into();
    current.active_turn_id = None;
    current.worktree_id = None;
    repository.upsert_session(&current).unwrap();
    let mut db = open_connection(&repository.database_path).unwrap();
    certified(&db, "original", "{}");
    db.execute_batch(
        "UPDATE sessions SET status='closed',metadata_json='{mutable-malformed'
         WHERE id='historical-child';",
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    let result = scan(
        &mut db,
        &[
            session("historical-child"),
            checkout(None, Some("/some/checkout")),
        ],
        LegacySpawnScanBudget::default(),
    )
    .unwrap();
    assert_clear(&result);
    assert_eq!(result.stats.certified_receipts, 1);
    // This clears the spawn obligation only, not native quiescence or cleanup.
}

#[test]
fn one_caller_owned_snapshot_is_stable_and_a_fresh_transaction_sees_late_obligations() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let mut reader = open_connection(&repository.database_path).unwrap();
    let writer = open_connection(&repository.database_path).unwrap();
    let mode: String = reader
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        mode, "wal",
        "fixture needs independent writer commit during a read snapshot"
    );
    let tx = reader
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    let count: i64 = tx
        .query_row("SELECT COUNT(*) FROM team_operation_journal", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 0); // Establish this transaction's read snapshot.
    journal(&writer, "late-original", TEAM, KIND, "planned", "{}");
    let old = classify_legacy_spawn_obligations(
        &tx,
        &[session("target")],
        LegacySpawnScanBudget::default(),
    )
    .unwrap();
    assert_clear(&old);
    assert!(!tx.is_autocommit());
    tx.rollback().unwrap();
    let tx = reader
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let fresh = classify_legacy_spawn_obligations(
        &tx,
        &[session("target")],
        LegacySpawnScanBudget::default(),
    )
    .unwrap();
    assert_blocked(&fresh, 0, "late-original", Reason::UnknownSessionScope);
    assert!(!tx.is_autocommit());
    tx.rollback().unwrap();
    // A previous preflight result cannot replace transaction-local admission.
}

#[cfg(unix)]
#[test]
fn retargeting_a_real_symlink_never_converts_historical_checkout_uncertainty_to_clearance() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir_all(first.join("checkout")).unwrap();
    std::fs::create_dir_all(second.join("checkout")).unwrap();
    let link = temp.path().join("historical-link");
    symlink(&first, &link).unwrap();
    let historical = link.join("checkout");
    let mut f = Fixture::new();
    let mut saved = worktree();
    saved.worktree_root = link.to_str().unwrap().into();
    saved.worktree_cwd = historical.to_str().unwrap().into();
    journal(
        &f.db,
        "original",
        TEAM,
        KIND,
        "worktree_created",
        &serde_json::json!({"worktree":saved}).to_string(),
    );
    let target = first.join("checkout").canonicalize().unwrap();
    assert_eq!(historical.canonicalize().unwrap(), target);
    let before = f.scan(
        &[checkout(None, Some(target.to_str().unwrap()))],
        LegacySpawnScanBudget::default(),
    );
    assert_blocked(&before, 0, "original", Reason::HistoricalPathAliasUncertain);
    std::fs::remove_file(&link).unwrap();
    symlink(&second, &link).unwrap();
    assert_ne!(historical.canonicalize().unwrap(), target);
    let after = f.scan(
        &[checkout(None, Some(target.to_str().unwrap()))],
        LegacySpawnScanBudget::default(),
    );
    assert_blocked(&after, 0, "original", Reason::HistoricalPathAliasUncertain);
    // Present-day canonical inequality is not evidence of a historical negative.
}

#[test]
fn repository_branch_positive_match_is_independent_of_checkout_spelling() {
    let mut f = Fixture::new();
    let saved = worktree();
    journal(
        &f.db,
        "original",
        TEAM,
        KIND,
        "worktree_created",
        &serde_json::json!({"worktree":saved}).to_string(),
    );
    let target = LegacyNativeTarget::Checkout {
        worktree_id: Some("different-id".into()),
        checkout_path: Some("/different/spelling".into()),
        repository_root: Some(saved.repo_root.clone()),
        branch_name: Some(saved.branch_name.clone()),
    };
    let result = f.scan(&[target], LegacySpawnScanBudget::default());
    assert_blocked(&result, 0, "original", Reason::ExactRepositoryBranch);
}
