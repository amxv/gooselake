use super::workspace_worktree_routes::{call, fixture};
use super::*;

#[tokio::test]
async fn failed_commit_after_provider_proof_fences_the_unverified_route() {
    let (router, token, temp, provider, workspace, agent, _) = fixture().await;
    let database_path = temp.path().join("runtime.sqlite3");
    // The provider can successfully rebind, then durable commit can fail.
    // No partial session, policy, or worktree-claim mutation may escape.
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_rebind_commit BEFORE UPDATE OF cwd ON sessions
             BEGIN SELECT RAISE(ABORT, 'injected route commit failure'); END;",
        )
        .unwrap();
    drop(connection);

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!(
            "/v2/workspaces/{}/agents/{}/worktree",
            workspace.workspace_id, agent.agent_id
        ),
        Some(serde_json::json!({"worktree_id":"wt_linked","expected_revision":0})),
        Some("commit-failpoint"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.rebind_calls().await, 1);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig { database_path });
    let operation = store
        .get_workspace_agent_rebind_by_key(
            &workspace.workspace_id,
            &agent.agent_id,
            "commit-failpoint",
        )
        .unwrap()
        .unwrap();
    assert_eq!(operation.phase, "manual_review");
    assert!(store
        .unresolved_workspace_agent_rebind(&agent.agent_id)
        .unwrap());
    let hydrated = store.hydrate_runtime_state().unwrap();
    let persisted = hydrated
        .sessions
        .iter()
        .find(|row| row.id == agent.agent_id)
        .unwrap();
    assert_eq!(
        persisted.cwd.as_deref(),
        Some(workspace.canonical_root.as_str())
    );
    assert_eq!(persisted.worktree_id, None);
    assert_eq!(
        store
            .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
            .unwrap()
            .unwrap()
            .revision,
        0
    );
    assert!(hydrated.managed_worktree_claims.iter().any(|claim| {
        claim.worktree_id == "wt_linked"
            && claim.session_id == agent.agent_id
            && claim.claim_role == "rebind_reservation"
            && claim.released_at.is_none()
    }));
}

#[tokio::test]
async fn conflicting_prior_claim_denies_destination_reservation_without_provider_dispatch() {
    let (router, token, temp, provider, workspace, agent, _) = fixture().await;
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    store
        .upsert_managed_worktree_claim(&runtime_core::ManagedWorktreeClaimRecord {
            worktree_id: "wt_linked".into(),
            session_id: agent.agent_id.clone(),
            claim_role: "owner".into(),
            created_at: 12,
            released_at: None,
        })
        .unwrap();
    let (status, created) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{}/worktrees", workspace.workspace_id),
        Some(serde_json::json!({
            "worktree_name":"conflicted-destination","run_init_script":false
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created:?}");
    let id = created["worktree"]["id"].as_str().unwrap();
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!(
            "/v2/workspaces/{}/agents/{}/worktree",
            workspace.workspace_id, agent.agent_id
        ),
        Some(serde_json::json!({"worktree_id":id,"expected_revision":0})),
        Some("claim-rejected"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.rebind_calls().await, 0);
    assert!(store
        .get_workspace_agent_rebind_by_key(
            &workspace.workspace_id,
            &agent.agent_id,
            "claim-rejected"
        )
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktree_claims
            .into_iter()
            .filter(|claim| claim.session_id == agent.agent_id && claim.released_at.is_none())
            .count(),
        1
    );
}

#[tokio::test]
async fn tombstoned_worktree_is_neither_in_inventory_nor_a_rebind_target() {
    let (router, token, temp, provider, workspace, agent, _) = fixture().await;
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let mut record = store
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktrees
        .into_iter()
        .find(|record| record.id == "wt_linked")
        .unwrap();
    record.worktree_cwd = "__gg_tombstoned__/wt_linked".into();
    store.upsert_managed_worktree(&record).unwrap();
    let (status, inventory) = call(
        &router,
        &token,
        "GET",
        &format!("/v2/workspaces/{}/worktrees", workspace.workspace_id),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(inventory["worktrees"].as_array().unwrap().is_empty());
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!(
            "/v2/workspaces/{}/agents/{}/worktree",
            workspace.workspace_id, agent.agent_id
        ),
        Some(serde_json::json!({"worktree_id":"wt_linked","expected_revision":0})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.rebind_calls().await, 0);
}
