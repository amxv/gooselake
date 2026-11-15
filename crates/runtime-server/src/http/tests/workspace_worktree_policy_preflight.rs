use super::workspace_worktree_routes::{call, fixture};
use super::*;

#[tokio::test]
async fn v2_explicit_worktree_retention_conflict_ignores_foreign_corrupt_session_before_native_admission(
) {
    let (router, token, temp, provider, workspace, foreign_agent, _) = fixture().await;
    let uri = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let name = "policy-preflight-corrupt-foreign";
    let (created_status, created) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({
            "worktree_name":name,
            "run_init_script":false,
            "deletion_policy":"retain_on_last_claim"
        })),
        Some("policy-preflight-initial"),
    )
    .await;
    assert_eq!(created_status, StatusCode::OK, "{created:?}");
    assert_eq!(
        created["worktree"]["deletion_policy"],
        "retain_on_last_claim"
    );
    let created_id = created["worktree"]["id"].as_str().unwrap().to_string();

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let operation_count = || -> i64 {
        db.query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_worktree_create'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    let managed_count = || -> i64 {
        db.query_row("SELECT COUNT(*) FROM managed_worktrees", [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    let git_inventory = || -> Vec<u8> {
        let result = std::process::Command::new("git")
            .arg("-C")
            .arg(&workspace.canonical_root)
            .args(["worktree", "list", "--porcelain"])
            .output()
            .expect("disposable native Git inventory");
        assert!(result.status.success());
        result.stdout
    };
    let before_ops = operation_count();
    let before_worktrees = managed_count();
    let before_git = git_inventory();
    let before_provider = provider.create_call_count();

    // Only the unrelated preexisting agent's historical JSON is malformed.
    // The exact managed worktree identity and its policy are still durable.
    let corrupted = db
        .execute(
            "UPDATE sessions SET metadata_json='{malformed-json' WHERE id=?1",
            [&foreign_agent.agent_id],
        )
        .unwrap();
    assert_eq!(corrupted, 1);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(store.hydrate_runtime_state().is_err());

    let (status, rejected) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({
            "worktree_name":name,
            "run_init_script":false,
            "deletion_policy":"delete_on_last_claim"
        })),
        Some("policy-preflight-different-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected:?}");
    assert_eq!(rejected["error"]["code"], "conflict");
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("different retention policy"),
        "{rejected}"
    );
    assert_eq!(operation_count(), before_ops, "no new native Git admission");
    assert_eq!(managed_count(), before_worktrees, "no extra managed row");
    assert_eq!(git_inventory(), before_git, "no native Git checkout change");
    assert_eq!(provider.create_call_count(), before_provider);
    assert_eq!(
        db.query_row(
            "SELECT id FROM managed_worktrees WHERE worktree_name=?1",
            [name],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        created_id
    );
    assert!(store.hydrate_runtime_state().is_err());

    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
}
