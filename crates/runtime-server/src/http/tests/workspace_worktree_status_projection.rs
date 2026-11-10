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
