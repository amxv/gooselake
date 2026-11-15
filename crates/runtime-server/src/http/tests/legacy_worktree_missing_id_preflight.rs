use super::*;

async fn post_legacy_worktree(
    router: &axum::Router,
    token: Option<&str>,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn missing_legacy_release_and_cleanup_ids_do_not_decode_unrelated_corrupt_sessions_or_touch_native_state(
) {
    let (router, token, temp, provider) =
        build_test_router_with_provider(TeamMcpPolicy::default()).await;
    let root = temp.path().join("disposable-repo");
    std::fs::create_dir_all(&root).unwrap();
    let source_body = serde_json::json!({
        "provider":"codex", "model":"test-model", "cwd":root,
    });
    let (status, first) =
        post_legacy_worktree(&router, Some(&token), "/v1/sessions", source_body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let session_id = first["id"].as_str().unwrap();
    let (status, foreign) =
        post_legacy_worktree(&router, Some(&token), "/v1/sessions", source_body).await;
    assert_eq!(status, StatusCode::OK, "{foreign}");
    let foreign_id = foreign["id"].as_str().unwrap();

    let database_path = temp.path().join("runtime.sqlite3");
    let db = rusqlite::Connection::open(&database_path).unwrap();
    let existing_cwd = temp.path().join("managed-checkout").display().to_string();
    assert_eq!(
        db.execute(
            "INSERT INTO managed_worktrees
          (id,repo_root,worktree_root,worktree_cwd,branch_name,worktree_name,
           unified_workspace_path,deletion_policy,created_at,updated_at)
         VALUES('wt_guarded',?1,?2,?3,'gg/guarded','guarded',?3,
                'delete_on_last_claim',100,100)",
            rusqlite::params![
                root.display().to_string(),
                temp.path().display().to_string(),
                existing_cwd
            ],
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.execute(
            "INSERT INTO managed_worktrees
              (id,repo_root,worktree_root,worktree_cwd,branch_name,worktree_name,
               unified_workspace_path,deletion_policy,created_at,updated_at)
             VALUES('wt_tombstoned',?1,?2,?3,'__gg_tombstoned__/wt_tombstoned',
                    'tombstoned',?3,'delete_on_last_claim',101,101)",
            rusqlite::params![
                root.display().to_string(),
                temp.path().display().to_string(),
                temp.path()
                    .join("tombstoned-checkout")
                    .display()
                    .to_string()
            ],
        )
        .unwrap(),
        1
    );
    assert_eq!(
        db.execute(
            "UPDATE sessions SET metadata_json='{broken-foreign-history' WHERE id=?1",
            [foreign_id],
        )
        .unwrap(),
        1
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig { database_path });
    assert!(store.hydrate_runtime_state().is_err());
    let count = |table: &str| -> i64 {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    let counts_before = (
        count("managed_worktrees"),
        count("managed_worktree_claims"),
        count("team_operation_journal"),
    );
    let provider_before = provider.create_call_count();
    let cases = [
        (
            "/v1/worktrees/wt_not_found/release".to_string(),
            serde_json::json!({"session_id":session_id, "cleanup_if_last_claim":false}),
        ),
        (
            "/v1/worktrees/wt_not_found/cleanup".to_string(),
            serde_json::json!({"reason":"missing-id-readonly-preflight"}),
        ),
        (
            "/v1/worktrees/wt_tombstoned/release".to_string(),
            serde_json::json!({"session_id":session_id, "cleanup_if_last_claim":false}),
        ),
        (
            "/v1/worktrees/wt_tombstoned/cleanup".to_string(),
            serde_json::json!({"reason":"tombstoned-id-readonly-preflight"}),
        ),
        (
            "/v1/worktrees/wt_guarded/release".to_string(),
            serde_json::json!({"session_id":session_id,"cleanup_if_last_claim":false}),
        ),
        (
            "/v1/worktrees/wt_guarded/cleanup".to_string(),
            serde_json::json!({"reason":"existing-id-must-fail-closed"}),
        ),
    ];
    for (index, (uri, body)) in cases.into_iter().enumerate() {
        let (status, result) =
            post_legacy_worktree(&router, Some(&token), &uri, body.clone()).await;
        let expected = if index < 4 {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        assert_eq!(status, expected, "{uri}: {result}");
        if index < 4 {
            let id = if index < 2 {
                "wt_not_found"
            } else {
                "wt_tombstoned"
            };
            assert_eq!(result["error"], format!("worktree {id}"));
        } else {
            assert!(
                result["error"].as_str().unwrap_or_default().contains("failed collecting sqlite rows"),
                "existing target must still fail closed on unreadable global safety evidence: {result}"
            );
        }
        let (unauthorized, _) = post_legacy_worktree(&router, None, &uri, body).await;
        assert_eq!(unauthorized, StatusCode::UNAUTHORIZED, "{uri}");
    }
    assert_eq!(
        counts_before,
        (
            count("managed_worktrees"),
            count("managed_worktree_claims"),
            count("team_operation_journal")
        )
    );
    assert_eq!(provider.create_call_count(), provider_before);
    assert!(store.hydrate_runtime_state().is_err());
}
