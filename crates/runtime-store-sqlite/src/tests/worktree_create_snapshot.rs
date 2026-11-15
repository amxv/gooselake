use super::*;

#[test]
fn worktree_create_natural_key_and_active_claims_share_indexed_reopenable_sqlite_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    for (session_id, provider_ref) in [
        ("session_1", "native_1"),
        ("session_2", "native_2"),
        ("session_3", "native_3"),
    ] {
        let mut session = sample_session();
        session.id = session_id.into();
        session.provider_session_ref = Some(provider_ref.into());
        session.active_turn_id = None;
        session.worktree_id = None;
        repository.upsert_session(&session).unwrap();
    }
    let target = ManagedWorktreeRecord {
        id: "wt_create_target".into(),
        repo_root: "/tmp/creation-snapshot-repo".into(),
        worktree_root: "/tmp/creation-snapshot-root".into(),
        worktree_cwd: "/tmp/creation-snapshot-root/target".into(),
        branch_name: "gg/creation-target".into(),
        worktree_name: "creation-target".into(),
        unified_workspace_path: "target".into(),
        deletion_policy: "delete_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 100,
        updated_at: 100,
    };
    repository.upsert_managed_worktree(&target).unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: target.id.clone(),
            session_id: "session_1".into(),
            claim_role: "owner".into(),
            created_at: 110,
            released_at: None,
        })
        .unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: target.id.clone(),
            session_id: "session_2".into(),
            claim_role: "consumer".into(),
            created_at: 111,
            released_at: Some(200),
        })
        .unwrap();
    let tombstone = ManagedWorktreeRecord {
        id: "wt_partial_tombstone".into(),
        branch_name: "__gg_tombstoned__/wt_partial_tombstone".into(),
        worktree_cwd: "/tmp/creation-snapshot-root/tombstone".into(),
        ..target.clone()
    };
    repository.upsert_managed_worktree(&tombstone).unwrap();

    let path = temp.path().join("runtime.sqlite3");
    let mut db = open_connection(&path).unwrap();
    let tx = db.transaction().unwrap();
    for index in 0..2_400 {
        tx.execute(
            "INSERT INTO managed_worktrees (
                id,repo_root,worktree_root,worktree_cwd,branch_name,
                worktree_name,unified_workspace_path,deletion_policy,created_at,updated_at
             ) VALUES (?1,'/tmp/unrelated-repo','/tmp/unrelated',?2,?3,?4,
                       '/tmp/unrelated','retain_on_last_claim',?5,?5)",
            params![
                format!("wt_unrelated_{index:04}"),
                format!("/tmp/unrelated/wt_{index:04}"),
                format!("gg/unrelated-{index:04}"),
                format!("unrelated-{index:04}"),
                index as i64,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_unrelated_0001".into(),
            session_id: "session_3".into(),
            claim_role: "owner".into(),
            created_at: 115,
            released_at: None,
        })
        .unwrap();

    let identity_plan = db
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
        identity_plan
            .contains("SEARCH managed_worktrees USING INDEX sqlite_autoindex_managed_worktrees_2"),
        "{identity_plan}"
    );
    assert!(
        !identity_plan.contains("USE TEMP B-TREE"),
        "{identity_plan}"
    );

    let claims_plan = db
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT COUNT(*) FROM managed_worktree_claims
             WHERE worktree_id=?1 AND released_at IS NULL",
        )
        .unwrap()
        .query_map([&target.id], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        claims_plan.contains("SEARCH managed_worktree_claims USING INDEX"),
        "{claims_plan}"
    );
    assert!(claims_plan.contains("worktree_id=?"), "{claims_plan}");
    assert!(!claims_plan.contains("USE TEMP B-TREE"), "{claims_plan}");

    assert_eq!(
        repository
            .managed_worktree_creation_snapshot(
                &target.repo_root,
                &target.worktree_cwd,
                &target.branch_name,
            )
            .unwrap(),
        Some((target.clone(), 1))
    );
    assert_eq!(
        repository
            .managed_worktree_creation_snapshot(
                &target.repo_root,
                &target.worktree_cwd,
                "gg/not-the-target",
            )
            .unwrap(),
        None
    );
    assert_eq!(
        repository
            .managed_worktree_creation_snapshot(
                &tombstone.repo_root,
                &tombstone.worktree_cwd,
                &tombstone.branch_name,
            )
            .unwrap(),
        None,
        "historic partial tombstone must not become a native checkout identity"
    );

    db.execute(
        "UPDATE sessions SET metadata_json='{unrelated-broken-json' WHERE id='session_3'",
        [],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    let reopened = SqliteRuntimeRepository::new(path.clone());
    assert_eq!(
        reopened
            .managed_worktree_creation_snapshot(
                &target.repo_root,
                &target.worktree_cwd,
                &target.branch_name,
            )
            .unwrap(),
        Some((target.clone(), 1)),
        "original target and active claim still readable after restart"
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path,
    });
    assert_eq!(
        store
            .managed_worktree_creation_snapshot(
                &target.repo_root,
                &target.worktree_cwd,
                &target.branch_name,
            )
            .unwrap(),
        Some((target.clone(), 1)),
        "RuntimeStore trait must use the same indexed projection"
    );
    db.execute(
        "UPDATE managed_worktree_claims SET released_at=300
         WHERE worktree_id=?1 AND session_id='session_1'",
        [&target.id],
    )
    .unwrap();
    assert_eq!(
        reopened
            .managed_worktree_creation_snapshot(
                &target.repo_root,
                &target.worktree_cwd,
                &target.branch_name,
            )
            .unwrap(),
        Some((target, 0)),
        "released claims cannot keep a stale checkout retained"
    );
    assert!(reopened.hydrate_runtime_state().is_err());
}
