use super::workspace_agent::apply_schema_through;
use super::*;

const TARGET: &str = "session_1";
const FOREIGN: &str = "foreign_claim_session";

fn active_claim_plan(db: &Connection) -> String {
    db.prepare(
        "EXPLAIN QUERY PLAN SELECT worktree_id FROM managed_worktree_claims
         WHERE session_id='session_1' AND worktree_id!='wt_excluded'
           AND released_at IS NULL
         ORDER BY created_at ASC,worktree_id ASC LIMIT 1",
    )
    .unwrap()
    .query_map([], |row| row.get::<_, String>(3))
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
    .join(" | ")
}

#[test]
fn schema48_active_foreign_claim_seek_preserves_ties_and_does_not_rewrite_old_rows() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 47);
    let repository = SqliteRuntimeRepository::new(path.clone());
    repository.upsert_session(&sample_session()).unwrap();
    let mut foreign = sample_session();
    foreign.id = FOREIGN.into();
    foreign.provider_session_ref = Some("native-foreign-claim".into());
    repository.upsert_session(&foreign).unwrap();

    let mut db = open_connection(&path).unwrap();
    let transaction = db.transaction().unwrap();
    // A realistic large historical table with many foreign claims; a couple
    // of legacy same-session ties demonstrate identical first-error ordering.
    for i in 0..2_400 {
        let id = format!("wt_history_{i:04}");
        let cwd = format!("/tmp/claim-wt-history-{i:04}");
        transaction
            .execute(
                "INSERT INTO managed_worktrees (
                    id,repo_root,worktree_root,worktree_cwd,branch_name,
                    worktree_name,unified_workspace_path,deletion_policy,created_at,updated_at
                 ) VALUES(?1,'/tmp/claim-root','/tmp',?2,?3,?4,
                   'claim-root','retain_on_last_claim',?5,?5)",
                params![id, cwd, format!("fixture/{id}"), id, i as i64],
            )
            .unwrap();
        let target = matches!(i, 111 | 112 | 114);
        transaction
            .execute(
                "INSERT INTO managed_worktree_claims (
                    worktree_id,session_id,claim_role,created_at,released_at
                 ) VALUES(?1,?2,'owner',?3,?4)",
                params![
                    id,
                    if target { TARGET } else { FOREIGN },
                    if target { 12 } else { i as i64 },
                    if i == 114 { Some(50_i64) } else { None },
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let old_plan = active_claim_plan(&db);
    assert!(!old_plan.contains("idx_worktree_claims_active_session_order"));
    assert_eq!(
        repository
            .first_active_foreign_worktree_claim_for_session(TARGET, "wt_excluded")
            .unwrap(),
        Some("wt_history_0111".into())
    );
    assert_eq!(
        repository
            .first_active_foreign_worktree_claim_for_session(TARGET, "wt_history_0111")
            .unwrap(),
        Some("wt_history_0112".into())
    );
    assert_eq!(
        repository
            .first_active_foreign_worktree_claim_for_session("absent", "wt_excluded")
            .unwrap(),
        None
    );
    let original_count: i64 = db
        .query_row("SELECT COUNT(*) FROM managed_worktree_claims", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(original_count, 2_400);
    drop(db);

    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    let new_plan = active_claim_plan(&db);
    assert!(
        new_plan.contains(
            "SEARCH managed_worktree_claims USING INDEX idx_worktree_claims_active_session_order"
        ),
        "{new_plan}"
    );
    assert!(!new_plan.contains("USE TEMP B-TREE"), "{new_plan}");
    assert_eq!(
        repository
            .first_active_foreign_worktree_claim_for_session(TARGET, "wt_excluded")
            .unwrap(),
        Some("wt_history_0111".into())
    );
    assert_eq!(
        repository
            .first_active_foreign_worktree_claim_for_session(TARGET, "wt_history_0111")
            .unwrap(),
        Some("wt_history_0112".into())
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM managed_worktree_claims", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        original_count
    );
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );

    // Foreign malformed persisted session metadata breaks global hydration,
    // but the original owning-session claim remains readable after reopen.
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid' WHERE id=?1",
        [FOREIGN],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    let reopened = SqliteRuntimeRepository::new(path);
    assert_eq!(
        reopened
            .first_active_foreign_worktree_claim_for_session(TARGET, "wt_excluded")
            .unwrap(),
        Some("wt_history_0111".into())
    );
    assert_eq!(
        reopened
            .first_active_foreign_worktree_claim_for_session(FOREIGN, "wt_history_0000")
            .unwrap(),
        Some("wt_history_0001".into())
    );
    assert!(reopened.hydrate_runtime_state().is_err());
}
