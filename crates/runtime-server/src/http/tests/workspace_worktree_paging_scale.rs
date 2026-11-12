use super::workspace_worktree_routes::fixture;
use super::*;

#[tokio::test]
async fn http_worktree_pages_match_full_authority_across_large_alias_catalog() {
    let (router, token, temp, provider, workspace, _, _) = fixture().await;
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let root = std::path::Path::new(&workspace.canonical_root);
    fs::create_dir_all(root.join("nested")).unwrap();
    let alias = root.join("nested/..").display().to_string();
    let foreign = temp.path().join("foreign-repository");
    fs::create_dir_all(&foreign).unwrap();
    for index in 0..215 {
        let id = format!("page_{index:04}");
        let name = if index % 9 == 0 {
            "tie-name".into()
        } else {
            format!("worktree-{index:04}")
        };
        store
            .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
                id: id.clone(),
                repo_root: if index % 17 == 0 {
                    alias.clone()
                } else {
                    workspace.canonical_root.clone()
                },
                worktree_root: temp.path().display().to_string(),
                worktree_cwd: temp.path().join(&id).display().to_string(),
                branch_name: format!("gg/{id}"),
                worktree_name: name,
                unified_workspace_path: "bounded page fixture".into(),
                deletion_policy: "retain_on_last_claim".into(),
                created_by_session_id: None,
                created_by_operation_id: None,
                created_at: index,
                updated_at: index,
            })
            .unwrap();
    }
    store
        .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
            id: "foreign_record".into(),
            repo_root: foreign.display().to_string(),
            worktree_root: temp.path().display().to_string(),
            worktree_cwd: foreign.join("checkout").display().to_string(),
            branch_name: "gg/foreign_record".into(),
            worktree_name: "aaa-foreign".into(),
            unified_workspace_path: "foreign".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 1,
            updated_at: 1,
        })
        .unwrap();

    let uri = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let mut cursor: Option<String> = None;
    let mut observed = Vec::new();
    let mut pages = 0;
    loop {
        let query = cursor.as_ref().map_or_else(
            || format!("{uri}?limit=37"),
            |cursor| format!("{uri}?limit=37&cursor={cursor}"),
        );
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(query)
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        cursor = response
            .headers()
            .get("x-next-cursor")
            .map(|header| header.to_str().unwrap().to_owned());
        let page: runtime_core::WorkspaceWorktreeInventory =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert!(page.worktrees.len() <= 37);
        observed.extend(page.worktrees);
        pages += 1;
        assert!(pages < 12, "keyset cursor must make forward progress");
        if cursor.is_none() {
            break;
        }
    }
    let restarted = super::operator_agent_create::reopened_runtime(&temp, provider).await;
    let complete = restarted
        .list_workspace_worktree_inventory(&workspace.workspace_id)
        .unwrap();
    assert_eq!(observed.len(), 216, "linked worktree plus 215 candidates");
    assert_eq!(
        observed, complete.worktrees,
        "HTTP pages must exactly match the full authoritative projection"
    );
    assert!(pages >= 5);
}
