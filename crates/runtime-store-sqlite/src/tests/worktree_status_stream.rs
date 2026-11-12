use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use std::collections::HashMap;

fn worktree(root: &str, id: &str, cwd: &str, name: &str) -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: id.into(),
        repo_root: root.into(),
        worktree_root: root.into(),
        worktree_cwd: cwd.into(),
        branch_name: format!("gg/{id}"),
        worktree_name: name.into(),
        unified_workspace_path: "stream-test".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 100,
        updated_at: 100,
    }
}

#[test]
fn streamed_worktree_status_is_exact_source_and_route_parity_across_alias_batches() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("repo");
    let foreign = temp.path().join("foreign");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&foreign).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let foreign_workspace = registered_workspace(&repository, &foreign);
    let attached_cwd = root.join("checkout-attached").display().to_string();
    for (id, owner, active) in [
        ("attached_a", &workspace, true),
        ("attached_b", &workspace, true),
        ("archived", &workspace, false),
        ("foreign_agent", &foreign_workspace, true),
    ] {
        let (mut session, mut agent) = workspace_agent_fixture(owner, id, id);
        session.cwd = Some(attached_cwd.clone());
        agent.recreation_policy.authoritative_cwd = attached_cwd.clone();
        repository.create_workspace_agent(&session, &agent).unwrap();
        if !active {
            Connection::open(temp.path().join("runtime.sqlite3"))
                .unwrap()
                .execute(
                    "UPDATE workspace_agents SET lifecycle_state='archived',archived_at=123 WHERE session_id=?1",
                    [id],
                )
                .unwrap();
        }
    }

    // The first 200 aliases, the next 65 aliases and all foreign records
    // deliberately interleave duplicate worktree names and ID ordering.
    for index in 0..265 {
        let alias_dir = root.join(format!("alias-{index:04}"));
        std::fs::create_dir_all(&alias_dir).unwrap();
        let alias = alias_dir.join("..").display().to_string();
        let id = format!("wt_{index:04}");
        let cwd = if index == 7 || index == 207 {
            attached_cwd.clone()
        } else {
            root.join(format!("missing-{index:04}"))
                .display()
                .to_string()
        };
        repository
            .upsert_managed_worktree(&worktree(
                &alias,
                &id,
                &cwd,
                &format!("repeated-name-{:02}", index % 19),
            ))
            .unwrap();
    }
    for index in 0..320 {
        repository
            .upsert_managed_worktree(&worktree(
                &foreign.display().to_string(),
                &format!("foreign_{index:04}"),
                &attached_cwd,
                "repeated-name-00",
            ))
            .unwrap();
    }
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_0007".into(),
            session_id: "attached_a".into(),
            claim_role: "owner".into(),
            created_at: 101,
            released_at: None,
        })
        .unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_0207".into(),
            session_id: "foreign_agent".into(),
            claim_role: "consumer".into(),
            created_at: 101,
            released_at: None,
        })
        .unwrap();

    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let routes = reopened
        .list_workspace_active_agent_routes(&workspace.workspace_id)
        .unwrap();
    let mut attached: HashMap<String, Vec<String>> = HashMap::new();
    for route in routes {
        attached
            .entry(route.authoritative_cwd)
            .or_default()
            .push(route.agent_id);
    }
    for ids in attached.values_mut() {
        ids.sort();
    }
    let expected = reopened
        .list_managed_worktree_status_for_root(&root.display().to_string())
        .unwrap()
        .into_iter()
        .map(|projection| {
            let agents = attached
                .get(&projection.worktree.worktree_cwd)
                .cloned()
                .unwrap_or_default();
            (projection, agents)
        })
        .collect::<Vec<_>>();
    let mut streamed = Vec::new();
    reopened
        .visit_managed_worktree_status_for_root(
            &root.display().to_string(),
            &workspace.workspace_id,
            &mut |projection, agents| {
                streamed.push((projection, agents));
                Ok(())
            },
        )
        .unwrap();
    streamed.sort_by(|left, right| {
        (&left.0.worktree.worktree_name, &left.0.worktree.id)
            .cmp(&(&right.0.worktree.worktree_name, &right.0.worktree.id))
    });
    assert_eq!(streamed, expected);
    assert_eq!(streamed.len(), 265);
    let selected = streamed
        .iter()
        .find(|(row, _)| row.worktree.id == "wt_0007")
        .unwrap();
    assert_eq!(selected.1, ["attached_a", "attached_b"]);
    let conflicting = streamed
        .iter()
        .find(|(row, _)| row.worktree.id == "wt_0207")
        .unwrap();
    assert_eq!(conflicting.0.active_claim_session_ids, ["foreign_agent"]);
    assert_eq!(conflicting.1, ["attached_a", "attached_b"]);
}

#[test]
fn streamed_status_keeps_one_snapshot_when_another_connection_commits_mid_walk() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("repository");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let root_name = root.display().to_string();
    for index in 0..225 {
        repository
            .upsert_managed_worktree(&worktree(
                &root_name,
                &format!("wt_{index:04}"),
                &root.join(format!("path-{index:04}")).display().to_string(),
                &format!("name-{index:04}"),
            ))
            .unwrap();
    }
    let mut seen = Vec::new();
    let mut committed = false;
    repository
        .visit_managed_worktree_status_for_root(
            &root_name,
            &workspace.workspace_id,
            &mut |row, _| {
                if !committed {
                    committed = true;
                    let mut changed = worktree(&root_name, "wt_0150", "/new/target", "name-0150");
                    changed.updated_at = 200;
                    repository.upsert_managed_worktree(&changed).unwrap();
                    repository
                        .upsert_managed_worktree(&worktree(
                            &root_name,
                            "wt_new",
                            "/new/path",
                            "name-0160-extra",
                        ))
                        .unwrap();
                }
                seen.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        seen.len(),
        225,
        "mid-walk insert must not leak into the snapshot"
    );
    let original = seen
        .iter()
        .find(|row| row.worktree.id == "wt_0150")
        .unwrap();
    assert_eq!(original.worktree.updated_at, 100);
    assert_eq!(original.revision, 1);

    let mut restarted_seen = Vec::new();
    SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"))
        .visit_managed_worktree_status_for_root(
            &root_name,
            &workspace.workspace_id,
            &mut |row, _| {
                restarted_seen.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(restarted_seen.len(), 226);
    let updated = restarted_seen
        .iter()
        .find(|row| row.worktree.id == "wt_0150")
        .unwrap();
    assert_eq!(updated.worktree.updated_at, 200);
    assert!(updated.revision > original.revision);
}

#[test]
fn streamed_status_query_plans_seek_source_claims_and_selected_active_routes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("repository");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let worktree_path = root.join("chosen").display().to_string();
    let worktree = worktree(
        &root.display().to_string(),
        "wt_selected",
        &worktree_path,
        "selected",
    );
    repository.upsert_managed_worktree(&worktree).unwrap();
    let (mut session, mut agent) = workspace_agent_fixture(&workspace, "attached", "attached");
    session.cwd = Some(worktree_path.clone());
    agent.recreation_policy.authoritative_cwd = worktree_path;
    repository.create_workspace_agent(&session, &agent).unwrap();
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: worktree.id,
            session_id: session.id,
            claim_role: "owner".into(),
            created_at: 110,
            released_at: None,
        })
        .unwrap();

    let connection = Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let plan = |sql: &str| -> String {
        connection
            .prepare(sql)
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" | ")
    };
    let roots = plan(
        "EXPLAIN QUERY PLAN SELECT DISTINCT repo_root
                      FROM managed_worktrees ORDER BY repo_root",
    );
    assert!(
        roots.contains("COVERING INDEX"),
        "root source index: {roots}"
    );
    let batch = plan(
        "EXPLAIN QUERY PLAN SELECT id, repo_root, worktree_cwd, revision
                      FROM managed_worktrees
                      WHERE repo_root IN ('repo-a','repo-b')
                        AND (worktree_name, id) > ('cursor','cursor')
                      ORDER BY worktree_name, id LIMIT 100",
    );
    assert!(
        batch.contains("idx_managed_worktrees_repo_name_id"),
        "root-batch keyset must use the composite source index: {batch}"
    );
    assert!(!batch.contains("SCAN managed_worktrees"), "{batch}");
    let claims = plan(
        "EXPLAIN QUERY PLAN SELECT worktree_id, session_id
                       FROM managed_worktree_claims
                       WHERE worktree_id IN ('wt_selected') AND released_at IS NULL
                       ORDER BY worktree_id, session_id",
    );
    assert!(
        claims.contains("idx_managed_worktree_claims_active_worktree"),
        "live-claim source must be indexed: {claims}"
    );
    let routes = plan(
        "EXPLAIN QUERY PLAN SELECT a.session_id, a.recreation_policy_json
                       FROM workspace_agents a
                       JOIN workspace_agent_profiles p
                         ON p.session_id=a.session_id AND p.workspace_id=a.workspace_id
                       JOIN sessions s ON s.id=a.session_id
                       WHERE a.workspace_id='workspace' AND a.lifecycle_state='active'
                         AND json_extract(a.recreation_policy_json, '$.authoritative_cwd')
                             IN ('/selected/checkout')
                       ORDER BY a.created_at, a.session_id",
    );
    assert!(
        routes.contains("idx_workspace_agents_active_route_cwd"),
        "attached route CWD must use the partial expression index: {routes}"
    );
    assert!(!routes.contains("SCAN a"), "{routes}");
}

#[cfg(unix)]
#[test]
fn streamed_status_rechecks_historical_symlink_root_membership_without_db_writes() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    let alias = temp.path().join("moving-alias");
    symlink(&first, &alias).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let first_workspace = registered_workspace(&repository, &first);
    let second_workspace = registered_workspace(&repository, &second);
    repository
        .upsert_managed_worktree(&worktree(
            &alias.display().to_string(),
            "wt_retarg",
            "/missing/native/worktree",
            "move-with-symlink",
        ))
        .unwrap();

    let visit = |root: &std::path::Path, workspace_id: &str| {
        let mut records = Vec::new();
        repository
            .visit_managed_worktree_status_for_root(
                &root.display().to_string(),
                workspace_id,
                &mut |row, _| {
                    records.push(row.worktree.id);
                    Ok(())
                },
            )
            .unwrap();
        records
    };
    assert_eq!(visit(&first, &first_workspace.workspace_id), ["wt_retarg"]);
    assert!(visit(&second, &second_workspace.workspace_id).is_empty());
    std::fs::remove_file(&alias).unwrap();
    symlink(&second, &alias).unwrap();
    assert!(visit(&first, &first_workspace.workspace_id).is_empty());
    assert_eq!(
        visit(&second, &second_workspace.workspace_id),
        ["wt_retarg"]
    );
    std::fs::remove_file(&alias).unwrap();
    symlink(&first, &alias).unwrap();
    assert_eq!(visit(&first, &first_workspace.workspace_id), ["wt_retarg"]);
}
