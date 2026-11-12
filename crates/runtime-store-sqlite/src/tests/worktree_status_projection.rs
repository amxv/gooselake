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

#[test]
fn migration_34_pages_two_thousand_worktrees_with_aliases_claims_and_exact_cursor_membership() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    // This disposable fixture is genuinely at schema 33 before migration.
    apply_schema_through(&path, 33);
    let root = temp.path().join("canonical-repository");
    let foreign_root = temp.path().join("another-repository");
    std::fs::create_dir_all(root.join("nested")).unwrap();
    std::fs::create_dir_all(&foreign_root).unwrap();
    let canonical_root = std::fs::canonicalize(&root).unwrap();
    let alias_root = root.join("nested/..").display().to_string();
    let canonical_text = canonical_root.display().to_string();

    let mut connection = Connection::open(&path).unwrap();
    let transaction = connection.transaction().unwrap();
    for i in 0..2_000 {
        let id = format!("worktree_{i:04}");
        let name = if i % 5 == 0 {
            "same-name".to_owned()
        } else {
            format!("name-{i:04}")
        };
        let rooted_at = if i % 11 == 0 {
            alias_root.as_str()
        } else {
            canonical_text.as_str()
        };
        transaction
            .execute(
                "INSERT INTO managed_worktrees
                 (id,repo_root,worktree_root,worktree_cwd,branch_name,
                  worktree_name,unified_workspace_path,deletion_policy,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,'managed','retain_on_last_claim',1,1)",
                params![
                    &id,
                    rooted_at,
                    temp.path().display().to_string(),
                    temp.path()
                        .join(format!("checkout-{id}"))
                        .display()
                        .to_string(),
                    format!("gg/{id}"),
                    name,
                ],
            )
            .unwrap();
    }
    for (id, repo_root, cwd) in [
        (
            "tombstoned_worktree",
            canonical_text.as_str(),
            "__gg_tombstoned__/tombstoned_worktree",
        ),
        (
            "foreign_worktree",
            foreign_root.to_str().unwrap(),
            "/unrelated/checkout",
        ),
    ] {
        transaction
            .execute(
                "INSERT INTO managed_worktrees
                 (id,repo_root,worktree_root,worktree_cwd,branch_name,
                  worktree_name,unified_workspace_path,deletion_policy,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,'managed','retain_on_last_claim',1,1)",
                params![
                    id,
                    repo_root,
                    temp.path().display().to_string(),
                    cwd,
                    format!("gg/{id}"),
                    id
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);
    let repository = SqliteRuntimeRepository::new(path.clone());
    repository.upsert_session(&sample_session()).unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "worktree_0000".into(),
            session_id: "session_1".into(),
            claim_role: "owner".into(),
            created_at: 2,
            released_at: None,
        })
        .unwrap();

    let before: i64 = Connection::open(&path)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM managed_worktrees", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(before, 2_002);
    repository.initialize_schema().unwrap();
    let full = repository
        .list_managed_worktree_status_for_root(&canonical_text)
        .unwrap()
        .into_iter()
        .filter(|row| !row.worktree.worktree_cwd.starts_with("__gg_tombstoned__/"))
        .collect::<Vec<_>>();
    assert_eq!(full.len(), 2_000);
    let restarted = SqliteRuntimeRepository::new(path.clone());
    let mut cursor: Option<(String, String)> = None;
    let mut found = Vec::new();
    loop {
        let (part, valid) = restarted
            .list_managed_worktree_status_for_root_page(
                &canonical_text,
                cursor
                    .as_ref()
                    .map(|(name, id)| (name.as_str(), id.as_str())),
                19,
            )
            .unwrap();
        assert!(valid);
        assert!(part.len() <= 19);
        if part.is_empty() {
            break;
        }
        cursor = part
            .last()
            .map(|row| (row.worktree.worktree_name.clone(), row.worktree.id.clone()));
        found.extend(part);
    }
    assert_eq!(
        found, full,
        "keyset pages must equal the complete authority after restart"
    );
    assert_eq!(
        found
            .iter()
            .find(|row| row.worktree.id == "worktree_0000")
            .unwrap()
            .active_claim_session_ids,
        ["session_1"]
    );
    for (name, id) in [
        ("wrong-name", "worktree_0000"),
        ("same-name", "does-not-exist"),
        ("foreign_worktree", "foreign_worktree"),
        ("tombstoned_worktree", "tombstoned_worktree"),
    ] {
        let (empty, valid) = restarted
            .list_managed_worktree_status_for_root_page(&canonical_text, Some((name, id)), 19)
            .unwrap();
        assert!(
            !valid,
            "foreign or missing cursor should not skip a page: {name} {id}"
        );
        assert!(empty.is_empty());
    }
    let connection = Connection::open(&path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        crate::schema::SCHEMA_VERSION,
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM managed_worktrees", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        before
    );
    let index_plan = connection
        .prepare(
            "EXPLAIN QUERY PLAN SELECT id,revision FROM managed_worktrees
             WHERE repo_root IN ('repo-one') AND (worktree_name,id) > ('same-name','worktree_0000')
             ORDER BY worktree_name,id LIMIT 19",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        index_plan.contains("idx_managed_worktrees_repo_name_id"),
        "{index_plan}"
    );
    assert!(
        !index_plan.contains("SCAN managed_worktrees"),
        "{index_plan}"
    );
}

#[test]
fn paged_worktree_inventory_preserves_all_aliases_across_bounded_root_batches() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let root = temp.path().join("repository");
    let foreign = temp.path().join("foreign");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&foreign).unwrap();
    let canonical = std::fs::canonicalize(&root).unwrap();
    let canonical_text = canonical.display().to_string();
    let repository = SqliteRuntimeRepository::new(path.clone());
    repository.initialize_schema().unwrap();

    // 245 distinct aliases point at the same real repo, and 900 additional
    // distinct historical paths do not. Neither count may change the page
    // membership, claim authority, cursor validity, or sorting on ties.
    let mut connection = Connection::open(&path).unwrap();
    let transaction = connection.transaction().unwrap();
    for index in 0..245 {
        let alias_dir = root.join(format!("old-spelling-{index:04}"));
        std::fs::create_dir_all(&alias_dir).unwrap();
        let id = format!("alias_{index:04}");
        let name = if index % 4 == 0 {
            "shared-name".to_string()
        } else {
            format!("name-{index:04}")
        };
        transaction
            .execute(
                "INSERT INTO managed_worktrees
             (id,repo_root,worktree_root,worktree_cwd,branch_name,
              worktree_name,unified_workspace_path,deletion_policy,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,'managed','retain_on_last_claim',1,1)",
                params![
                    id,
                    alias_dir.join("..").display().to_string(),
                    temp.path().display().to_string(),
                    temp.path()
                        .join(format!("checkout-{index}"))
                        .display()
                        .to_string(),
                    format!("gg/{index}"),
                    name,
                ],
            )
            .unwrap();
    }
    for index in 0..900 {
        let id = format!("foreign_{index:04}");
        transaction
            .execute(
                "INSERT INTO managed_worktrees
             (id,repo_root,worktree_root,worktree_cwd,branch_name,
              worktree_name,unified_workspace_path,deletion_policy,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,'managed','retain_on_last_claim',1,1)",
                params![
                    id,
                    foreign
                        .join(format!("old-root-{index:04}"))
                        .display()
                        .to_string(),
                    temp.path().display().to_string(),
                    format!("/other/checkout-{index}"),
                    format!("gg/foreign-{index}"),
                    format!("name-{index:04}"),
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);

    repository.upsert_session(&sample_session()).unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "alias_0000".into(),
            session_id: "session_1".into(),
            claim_role: "owner".into(),
            created_at: 2,
            released_at: None,
        })
        .unwrap();
    let full = repository
        .list_managed_worktree_status_for_root(&canonical_text)
        .unwrap();
    assert_eq!(full.len(), 245);
    let restarted = SqliteRuntimeRepository::new(path.clone());
    let mut collected = Vec::new();
    let mut cursor: Option<(String, String)> = None;
    loop {
        let (part, valid) = restarted
            .list_managed_worktree_status_for_root_page(
                &canonical_text,
                cursor
                    .as_ref()
                    .map(|(name, id)| (name.as_str(), id.as_str())),
                17,
            )
            .unwrap();
        assert!(valid);
        assert!(part.len() <= 17);
        if part.is_empty() {
            break;
        }
        cursor = part
            .last()
            .map(|row| (row.worktree.worktree_name.clone(), row.worktree.id.clone()));
        collected.extend(part);
    }
    assert_eq!(
        collected, full,
        "aliased keyset pages must reproduce full authority"
    );
    assert_eq!(
        collected
            .iter()
            .find(|row| row.worktree.id == "alias_0000")
            .unwrap()
            .active_claim_session_ids,
        ["session_1"]
    );
    let (part, valid) = restarted
        .list_managed_worktree_status_for_root_page(
            &canonical_text,
            Some(("name-0001", "foreign_0001")),
            17,
        )
        .unwrap();
    assert!(!valid);
    assert!(part.is_empty());
}

#[cfg(unix)]
#[test]
fn paged_worktree_inventory_revalidates_symlink_alias_after_filesystem_retarget() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let root = temp.path().join("repository");
    let other = temp.path().join("other");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&other).unwrap();
    let alias = temp.path().join("moving-alias");
    symlink(&root, &alias).unwrap();

    let repository = SqliteRuntimeRepository::new(path);
    repository.initialize_schema().unwrap();
    repository
        .upsert_managed_worktree(&worktree(
            "moving",
            &alias.display().to_string(),
            temp.path(),
        ))
        .unwrap();
    let root_text = std::fs::canonicalize(&root).unwrap().display().to_string();
    let (initial, valid) = repository
        .list_managed_worktree_status_for_root_page(&root_text, None, 3)
        .unwrap();
    assert!(valid);
    assert_eq!(initial.len(), 1);

    std::fs::remove_file(&alias).unwrap();
    symlink(&other, &alias).unwrap();
    let (disappeared, valid) = repository
        .list_managed_worktree_status_for_root_page(&root_text, None, 3)
        .unwrap();
    assert!(valid);
    assert!(
        disappeared.is_empty(),
        "filesystem retarget must affect current alias membership"
    );
    let (missing_cursor, valid) = repository
        .list_managed_worktree_status_for_root_page(&root_text, Some(("moving", "moving")), 3)
        .unwrap();
    assert!(!valid);
    assert!(missing_cursor.is_empty());

    std::fs::remove_file(&alias).unwrap();
    symlink(&root, &alias).unwrap();
    let (restored, valid) = repository
        .list_managed_worktree_status_for_root_page(&root_text, None, 3)
        .unwrap();
    assert!(valid);
    assert_eq!(
        restored, initial,
        "alias may recover without a SQLite write"
    );
}
