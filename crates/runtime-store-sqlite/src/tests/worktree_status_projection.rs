use super::workspace_agent::apply_schema_through;
use super::*;

fn worktree(id: &str, repo_root: &str, root: &std::path::Path) -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: id.into(),
        repo_root: repo_root.into(),
        worktree_root: root.display().to_string(),
        worktree_cwd: root.join(format!("checkout-{id}")).display().to_string(),
        branch_name: format!("gg/{id}"),
        worktree_name: id.into(),
        unified_workspace_path: "managed".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 100,
        updated_at: 100,
    }
}

#[test]
fn indexed_worktree_status_projects_only_target_repo_and_live_claims_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let repository_root = temp.path().join("repository");
    let unrelated_root = temp.path().join("unrelated-repository");
    std::fs::create_dir_all(repository_root.join("nested")).unwrap();
    std::fs::create_dir_all(&unrelated_root).unwrap();
    let root = std::fs::canonicalize(&repository_root).unwrap();
    let raw_alias = repository_root.join("nested/..").display().to_string();

    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let primary = worktree("wt_a", &root.display().to_string(), temp.path());
    let alias = worktree("wt_alias", &raw_alias, temp.path());
    repository.upsert_managed_worktree(&primary).unwrap();
    repository.upsert_managed_worktree(&alias).unwrap();

    let mut session = sample_session();
    repository.upsert_session(&session).unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: primary.id.clone(),
            session_id: session.id.clone(),
            claim_role: "owner".into(),
            created_at: 110,
            released_at: None,
        })
        .unwrap();
    session.id = "session_2".into();
    repository.upsert_session(&session).unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: alias.id.clone(),
            session_id: session.id.clone(),
            claim_role: "consumer".into(),
            created_at: 110,
            released_at: Some(120),
        })
        .unwrap();

    // A substantial unrelated catalog must not enter the status page or
    // its active-claim / per-worktree revision query.
    for index in 0..500 {
        let other = worktree(
            &format!("wt_unrelated_{index:04}"),
            &unrelated_root.display().to_string(),
            temp.path(),
        );
        repository.upsert_managed_worktree(&other).unwrap();
    }
    let mut updated = primary.clone();
    updated.deletion_policy = "delete_on_last_claim".into();
    updated.updated_at += 1;
    repository.upsert_managed_worktree(&updated).unwrap();

    assert_eq!(
        repository.get_managed_worktree("wt_a").unwrap(),
        Some(updated.clone()),
        "native route uses the current authoritative worktree record"
    );
    assert_eq!(
        repository.get_managed_worktree("wt_alias").unwrap(),
        Some(alias.clone()),
        "point lookup must preserve historical repository-root spelling"
    );
    assert_eq!(repository.get_managed_worktree("wt_missing").unwrap(), None);

    let projected = repository
        .list_managed_worktree_status_for_root(&root.display().to_string())
        .unwrap();
    assert_eq!(projected.len(), 2);
    let by_id = projected
        .iter()
        .map(|item| (item.worktree.id.as_str(), item))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(by_id["wt_a"].active_claim_session_ids, ["session_1"]);
    assert_eq!(by_id["wt_alias"].active_claim_session_ids.len(), 0);
    assert_eq!(
        by_id["wt_a"].revision,
        repository.managed_worktree_revision("wt_a").unwrap(),
        "scoped status must use the same durable per-worktree revision"
    );
    assert_eq!(
        by_id["wt_a"].worktree.deletion_policy,
        "delete_on_last_claim"
    );
    assert_eq!(by_id["wt_alias"].worktree.repo_root, raw_alias);

    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        restarted.get_managed_worktree("wt_a").unwrap(),
        Some(updated.clone()),
        "point lookup must recover after a new SQLite store opens"
    );
    assert_eq!(
        restarted
            .list_managed_worktree_status_for_root(&root.display().to_string())
            .unwrap(),
        projected,
        "status projection must rebuild from the journaled SQLite rows on restart"
    );
    assert_eq!(
        restarted
            .list_managed_worktree_status_for_root(&unrelated_root.display().to_string())
            .unwrap()
            .len(),
        500
    );

    let connection = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let plan = |query: &str| {
        let mut statement = connection.prepare(query).unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" | ")
    };
    let worktrees = plan(
        "EXPLAIN QUERY PLAN SELECT id, revision FROM managed_worktrees
         WHERE repo_root IN ('repo-1','repo-2') ORDER BY worktree_name, id",
    );
    assert!(
        worktrees.contains("SEARCH managed_worktrees USING INDEX"),
        "repository-scoped worktree search must use the repo_root index: {worktrees}"
    );
    let point = plan(
        "EXPLAIN QUERY PLAN SELECT id, repo_root, worktree_root, worktree_cwd
         FROM managed_worktrees WHERE id='wt_a'",
    );
    assert!(
        point.contains("SEARCH managed_worktrees USING INDEX"),
        "native route must use the worktree ID primary-key index: {point}"
    );
    assert!(
        !point.contains("SCAN managed_worktrees"),
        "point scan: {point}"
    );
    let claims = plan(
        "EXPLAIN QUERY PLAN SELECT c.worktree_id, c.session_id
         FROM managed_worktrees w CROSS JOIN managed_worktree_claims c
         WHERE w.repo_root IN ('repo-1','repo-2')
           AND c.worktree_id=w.id AND c.released_at IS NULL
         ORDER BY c.worktree_id, c.session_id",
    );
    assert!(
        claims.contains("SEARCH w USING"),
        "repo lookup plan: {claims}"
    );
    assert!(
        claims.contains("idx_managed_worktree_claims_active_worktree"),
        "only live claims may enter the indexed worktree status join: {claims}"
    );
    assert!(
        !claims.contains("SCAN c"),
        "do not scan claim history: {claims}"
    );
}

#[test]
fn migration_25_builds_live_claim_index_without_discarding_version_24_rows() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 24);
    let repository = SqliteRuntimeRepository::new(path.clone());
    repository.upsert_session(&sample_session()).unwrap();
    let root = temp.path().join("repository");
    std::fs::create_dir_all(&root).unwrap();
    let row = worktree("wt_legacy_24", &root.display().to_string(), temp.path());
    repository.upsert_managed_worktree(&row).unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: row.id.clone(),
            session_id: "session_1".into(),
            claim_role: "owner".into(),
            created_at: 110,
            released_at: None,
        })
        .unwrap();

    repository.initialize_schema().unwrap();
    let result = repository
        .list_managed_worktree_status_for_root(&root.display().to_string())
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].active_claim_session_ids, ["session_1"]);
    assert_eq!(result[0].worktree.id, row.id);
    let connection = Connection::open(path).unwrap();
    let version = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    assert_eq!(version, crate::schema::SCHEMA_VERSION);
    let index = connection
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='index'
             AND name='idx_managed_worktree_claims_active_worktree'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert_eq!(index, "idx_managed_worktree_claims_active_worktree");
}
