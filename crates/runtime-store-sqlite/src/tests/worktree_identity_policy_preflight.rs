use super::*;

#[test]
fn indexed_managed_worktree_natural_key_seek_survives_corrupt_unrelated_history() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    repository.upsert_session(&sample_session()).unwrap();

    let target = ManagedWorktreeRecord {
        id: "wt_policy_target".into(),
        repo_root: "/tmp/retention-policy-repo".into(),
        worktree_root: "/tmp/retention-policy-root".into(),
        worktree_cwd: "/tmp/retention-policy-root/target".into(),
        branch_name: "gg/retention-policy-target".into(),
        worktree_name: "policy-target".into(),
        unified_workspace_path: "policy-target".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 100,
        updated_at: 100,
    };
    repository.upsert_managed_worktree(&target).unwrap();

    let path = temp.path().join("runtime.sqlite3");
    let mut db = open_connection(&path).unwrap();
    let transaction = db.transaction().unwrap();
    for index in 0..2_400 {
        transaction
            .execute(
                "INSERT INTO managed_worktrees (
                    id,repo_root,worktree_root,worktree_cwd,branch_name,
                    worktree_name,unified_workspace_path,deletion_policy,created_at,updated_at
                 ) VALUES(?1,'/tmp/foreign-repo','/tmp/foreign',?2,?3,?4,
                          '/tmp/foreign','retain_on_last_claim',?5,?5)",
                params![
                    format!("wt_foreign_{index:04}"),
                    format!("/tmp/foreign/wt_{index:04}"),
                    format!("gg/foreign-{index:04}"),
                    format!("foreign-{index:04}"),
                    index as i64,
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let plan = db
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                    worktree_name, unified_workspace_path, deletion_policy,
                    created_by_session_id, created_by_operation_id, created_at, updated_at
             FROM managed_worktrees
             WHERE repo_root=?1 AND worktree_cwd=?2 AND branch_name=?3",
        )
        .unwrap()
        .query_map(
            params![&target.repo_root, &target.worktree_cwd, &target.branch_name],
            |row| row.get::<_, String>(3),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        plan.contains("SEARCH managed_worktrees USING INDEX sqlite_autoindex_managed_worktrees_2"),
        "{plan}"
    );
    assert!(!plan.contains("USE TEMP B-TREE"), "{plan}");
    assert_eq!(
        repository
            .get_managed_worktree_by_identity(
                &target.repo_root,
                &target.worktree_cwd,
                &target.branch_name,
            )
            .unwrap(),
        Some(target.clone())
    );
    assert_eq!(
        repository
            .get_managed_worktree_by_identity(
                &target.repo_root,
                &target.worktree_cwd,
                "gg/unrelated",
            )
            .unwrap(),
        None
    );

    // A foreign historical session may be malformed; the exact natural-key
    // worktree identity and retention policy are still independently valid.
    db.execute(
        "UPDATE sessions SET metadata_json='{broken-json' WHERE id='session_1'",
        [],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    let reopened = SqliteRuntimeRepository::new(path);
    assert_eq!(
        reopened
            .get_managed_worktree_by_identity(
                &target.repo_root,
                &target.worktree_cwd,
                &target.branch_name,
            )
            .unwrap(),
        Some(target)
    );
    assert!(reopened.hydrate_runtime_state().is_err());
}
