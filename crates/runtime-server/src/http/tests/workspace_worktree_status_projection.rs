use super::workspace_worktree_routes::{call, fixture};
use super::*;
use runtime_core::{ManagedWorktreeClaimRecord, ManagedWorktreeRecord};

#[tokio::test]
async fn operator_worktree_inventory_preserves_alias_claim_blockers_and_restart_parity() {
    let (router, token, temp, provider, workspace, agent, linked) = fixture().await;
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let repository_root = std::path::Path::new(&workspace.canonical_root);
    fs::create_dir_all(repository_root.join("nested")).unwrap();
    let alias = repository_root.join("nested/..").display().to_string();
    let new_row = |id: &str, root: &str, cwd: &str, name: &str| ManagedWorktreeRecord {
        id: id.into(),
        repo_root: root.into(),
        worktree_root: temp.path().display().to_string(),
        worktree_cwd: cwd.into(),
        branch_name: format!("gg/{id}"),
        worktree_name: name.into(),
        unified_workspace_path: "projection".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: None,
        created_at: 10,
        updated_at: 10,
    };

    let alias_path = temp.path().join("not-a-git-worktree");
    let alias_row = new_row(
        "wt_alias",
        &alias,
        &alias_path.display().to_string(),
        "alias",
    );
    store.upsert_managed_worktree(&alias_row).unwrap();
    store
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_alias".into(),
            session_id: agent.agent_id.clone(),
            claim_role: "owner".into(),
            created_at: 20,
            released_at: None,
        })
        .unwrap();

    let released = new_row(
        "wt_released",
        &workspace.canonical_root,
        &temp.path().join("released").display().to_string(),
        "released",
    );
    store.upsert_managed_worktree(&released).unwrap();
    store
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_released".into(),
            session_id: agent.agent_id.clone(),
            claim_role: "owner".into(),
            created_at: 20,
            released_at: Some(30),
        })
        .unwrap();
    store
        .upsert_managed_worktree(&new_row(
            "wt_tombstoned",
            &workspace.canonical_root,
            "__gg_tombstoned__/wt_tombstoned",
            "tombstoned",
        ))
        .unwrap();

    let unrelated = temp.path().join("unrelated");
    fs::create_dir_all(&unrelated).unwrap();
    for index in 0..130 {
        store
            .upsert_managed_worktree(&new_row(
                &format!("wt_other_{index:03}"),
                &unrelated.display().to_string(),
                &temp
                    .path()
                    .join(format!("other-{index:03}"))
                    .display()
                    .to_string(),
                "unrelated",
            ))
            .unwrap();
    }

    let endpoint = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let (status, result) = call(&router, &token, "GET", &endpoint, None, None).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let inventory: runtime_core::WorkspaceWorktreeInventory =
        serde_json::from_value(result).unwrap();
    assert_eq!(inventory.worktrees.len(), 3);
    let by_id = inventory
        .worktrees
        .iter()
        .map(|entry| (entry.worktree_id.as_str(), entry))
        .collect::<HashMap<_, _>>();
    assert!(by_id["wt_linked"].eligible_for_assignment);
    assert_eq!(by_id["wt_linked"].worktree_path, linked);
    assert_eq!(
        by_id["wt_alias"].eligibility_blockers,
        [
            "worktree_path_missing",
            "claim_conflicts_with_workspace_roster"
        ]
    );
    assert_eq!(
        by_id["wt_released"].eligibility_blockers,
        ["worktree_path_missing"]
    );
    assert_eq!(
        by_id["wt_alias"].revision,
        store.managed_worktree_revision("wt_alias").unwrap()
    );

    let restarted = super::operator_agent_create::reopened_runtime(&temp, provider).await;
    assert_eq!(
        restarted
            .list_workspace_worktree_inventory(&workspace.workspace_id)
            .unwrap(),
        inventory,
        "workspace inventory must reconstruct identical eligibility and revisions on restart"
    );
}

#[tokio::test]
async fn model_status_uses_bounded_one_snapshot_summary_with_full_inventory_parity() {
    let (_, _, temp, provider, workspace, agent, linked) = fixture().await;
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let root = std::path::Path::new(&workspace.canonical_root);
    // Every spelling resolves to the same canonical Git root, but is a
    // distinct persisted original alias. Repeated names cross source batches.
    for index in 0..25 {
        let directory = root.join(format!("historical-{index:02}"));
        fs::create_dir_all(&directory).unwrap();
        store
            .upsert_managed_worktree(&ManagedWorktreeRecord {
                id: format!("alias_{index:02}"),
                repo_root: directory.join("..").display().to_string(),
                worktree_root: temp.path().display().to_string(),
                worktree_cwd: linked.clone(),
                branch_name: "gg/linked".into(),
                worktree_name: if index % 4 == 0 {
                    "early-tie".into()
                } else {
                    format!("feature-{index:02}")
                },
                unified_workspace_path: "status-projection".into(),
                deletion_policy: "retain_on_last_claim".into(),
                created_by_session_id: None,
                created_by_operation_id: None,
                created_at: index,
                updated_at: index,
            })
            .unwrap();
    }
    // Archived checkout and a missing native path must not enter the count.
    store
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            id: "blocked_path".into(),
            repo_root: workspace.canonical_root.clone(),
            worktree_root: temp.path().display().to_string(),
            worktree_cwd: root.join("no-checkout").display().to_string(),
            branch_name: "gg/missing".into(),
            worktree_name: "before-everything".into(),
            unified_workspace_path: "status-projection".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 1,
            updated_at: 1,
        })
        .unwrap();
    let connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let original_policy: String = connection
        .query_row(
            "SELECT recreation_policy_json FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| row.get(0),
        )
        .unwrap();
    let mut policy: serde_json::Value = serde_json::from_str(&original_policy).unwrap();
    policy["authoritative_cwd"] = serde_json::json!(linked);
    connection
        .execute(
            "UPDATE workspace_agents SET recreation_policy_json=?1 WHERE session_id=?2",
            rusqlite::params![policy.to_string(), agent.agent_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE sessions SET cwd=?1 WHERE id=?2",
            rusqlite::params![linked, agent.agent_id],
        )
        .unwrap();
    drop(connection);

    let reopened = super::operator_agent_create::reopened_runtime(&temp, provider).await;
    let full = reopened
        .list_workspace_worktree_inventory(&workspace.workspace_id)
        .unwrap();
    let mut expected = full
        .worktrees
        .iter()
        .filter(|entry| entry.eligible_for_assignment)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), 26);
    expected.sort_by(|left, right| {
        (&left.normalized_name, &left.branch_name, &left.worktree_id).cmp(&(
            &right.normalized_name,
            &right.branch_name,
            &right.worktree_id,
        ))
    });
    expected.truncate(20);
    let full_attached = full
        .worktrees
        .iter()
        .filter(|entry| entry.attached_agent_ids.contains(&agent.agent_id))
        .min_by(|left, right| {
            (&left.normalized_name, &left.worktree_id)
                .cmp(&(&right.normalized_name, &right.worktree_id))
        })
        .unwrap();
    let displayed = vec![(agent.agent_id.clone(), linked.clone())];
    let summary = reopened
        .summarize_workspace_worktrees_for_status(&workspace.workspace_id, &displayed, 20)
        .unwrap();
    assert_eq!(summary.eligible_total, 26);
    assert_eq!(summary.available, expected);
    assert_eq!(
        summary.displayed_agent_worktrees[&agent.agent_id],
        *full_attached
    );
    let repeated = reopened
        .summarize_workspace_worktrees_for_status(&workspace.workspace_id, &displayed, 20)
        .unwrap();
    assert_eq!(repeated.available, summary.available);
    assert_eq!(repeated.eligible_total, summary.eligible_total);
}
