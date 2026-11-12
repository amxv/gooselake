use super::workspace_worktree_routes::fixture;
use super::*;

#[tokio::test]
async fn http_worktree_pages_match_full_authority_across_large_alias_catalog() {
    let (router, token, temp, provider, workspace, source_agent, linked) = fixture().await;
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

    // More than one model-facing roster page of independent active agents
    // must not be decoded for every 37-worktree HTTP page. Some are actually
    // attached to selected routes, the rest have unrelated CWDs.
    for index in 0..150 {
        let agent_id = format!("inventory_agent_{index:03}");
        let cwd = if index < 35 {
            linked.clone()
        } else if index < 70 {
            temp.path().join("page_0000").display().to_string()
        } else {
            temp.path()
                .join(format!("unrelated-agent-{index:03}"))
                .display()
                .to_string()
        };
        let session = runtime_core::SessionRecord {
            id: agent_id.clone(),
            provider: "codex".into(),
            status: "ready".into(),
            cwd: Some(cwd.clone()),
            model: Some("test-model".into()),
            permission_mode: Some("workspace_write".into()),
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some(format!("native-{agent_id}")),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: 100,
            updated_at: 100,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        };
        let mut agent = source_agent.clone();
        agent.agent_id = agent_id.clone();
        agent.alias = format!("inventory-{index:03}");
        agent.profile.title = Some(format!("Inventory agent {index}"));
        agent.profile.added_by = "runtime_operator".into();
        agent.profile.creator_session_id = None;
        agent.recreation_policy.authoritative_cwd = cwd;
        agent.provider_session_ref = session.provider_session_ref.clone();
        agent.canonical_provider_session_ref = None;
        agent.metadata = session.metadata.clone();
        store.create_workspace_agent(&session, &agent).unwrap();
    }

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
        observed
            .iter()
            .find(|entry| entry.worktree_id == "wt_linked")
            .unwrap()
            .attached_agent_ids
            .len(),
        35,
    );
    assert_eq!(
        observed
            .iter()
            .find(|entry| entry.worktree_id == "page_0000")
            .unwrap()
            .attached_agent_ids
            .len(),
        35,
    );
    assert_eq!(
        observed, complete.worktrees,
        "HTTP pages must exactly match the full authoritative projection"
    );
    assert!(pages >= 5);
}
