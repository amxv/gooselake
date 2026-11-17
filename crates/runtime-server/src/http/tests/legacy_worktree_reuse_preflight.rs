use super::*;

async fn call_legacy(
    router: &axum::Router,
    token: Option<&str>,
    method: &str,
    uri: &str,
    body: serde_json::Value,
    idempotency_key: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(key) = idempotency_key {
        request = request.header("Idempotency-Key", key);
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
async fn historical_reuse_receipt_ignores_unrelated_corrupt_session_without_native_effects() {
    let (router, token, temp, provider, _runtime, app, history) =
        build_test_router_with_history(TeamMcpPolicy::default(), |root| {
            vec![
                json!({
                    "provider":"codex", "model":"test-model", "cwd":root.join("source-repository"),
                });
                2
            ]
        })
        .await;
    let repo = temp.path().join("source-repository");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("README.md"), "disposable source\n").unwrap();
    for args in [
        vec!["init", "-b", "main"],
        vec!["add", "README.md"],
        vec![
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.org",
            "commit",
            "-m",
            "seed",
        ],
    ] {
        assert!(std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .output()
            .unwrap()
            .status
            .success());
    }
    let lead = historical_session_json(&history[0]);
    let source = lead["id"].as_str().unwrap();
    let (status, team) = call_legacy(
        &router,
        Some(&token),
        "POST",
        "/v1/teams",
        serde_json::json!({"name":"reuse corrupt history", "lead_agent_id":source}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{team}");
    let team_id = team["team"]["id"].as_str().unwrap();
    let (status, created) = legacy_worktree_fixture_json(
        &app,
        serde_json::json!({
            "team_id":team_id,"source_session_id":source,
            "worktree_name":"reuse-corrupt-history", "branch_prefix":"gg",
            "run_init_script":false,"deletion_policy":"retain_on_last_claim",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let worktree_id = created["worktree"]["id"].as_str().unwrap().to_owned();

    let foreign = historical_session_json(&history[1]);
    let foreign_id = foreign["id"].as_str().unwrap();
    let body = serde_json::json!({
        "source_session_id":source,
        "title":"Reuse existing managed checkout",
        "prompt":"Use the existing checkout",
        "worktree":{
            "mode":"use_existing", "name":"reuse-corrupt-history",
            "branch_prefix":"gg", "run_init_script":false,
        },
    });
    let before_seed = provider.create_call_count();
    let (status, spawned) = legacy_spawn_fixture_with_assignment_json(
        &app,
        team_id,
        Some("reuse-foreign-history-key"),
        body.clone(),
        Some(
            super::legacy_spawn_history::HistoricalWorktreeAssignment::Reused(
                serde_json::from_value(created["worktree"].clone()).unwrap(),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{spawned}");
    assert_eq!(
        provider.create_call_count(),
        before_seed,
        "history seeding cannot create another provider attachment"
    );
    assert_eq!(spawned["journal_stage"], "completed");
    assert_eq!(spawned["worktree_assignment_mode"], "reused");
    assert_eq!(spawned["worktree"]["id"], worktree_id);
    assert_eq!(spawned["worktree_created_by_operation"], false);
    let (router, _reopened_runtime, _reopened_app) =
        reopen_test_router_with_services(&app, temp.path(), &token, TeamMcpPolicy::default()).await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    assert_eq!(
        db.execute(
            "UPDATE sessions SET metadata_json='{bad-foreign-session-json' WHERE id=?1",
            [foreign_id],
        )
        .unwrap(),
        1
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(store.hydrate_runtime_state().is_err());
    let native_inventory = || {
        let result = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["worktree", "list", "--porcelain"])
            .output()
            .unwrap();
        assert!(result.status.success());
        result.stdout
    };
    let count = |table: &str| -> i64 {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    let previous_git = native_inventory();
    let previous_worktrees = count("managed_worktrees");
    let previous_claims = count("managed_worktree_claims");
    let previous_provider = provider.create_call_count();
    let uri = format!("/v1/teams/{team_id}/members/spawn");
    let (status, replay) = call_legacy(
        &router,
        Some(&token),
        "POST",
        &uri,
        body.clone(),
        Some("reuse-foreign-history-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(
        replay, spawned,
        "lost-reply inspection returns the exact saved response"
    );
    assert_eq!(provider.create_call_count(), previous_provider);
    assert_eq!(native_inventory(), previous_git);
    assert_eq!(count("managed_worktrees"), previous_worktrees);
    assert_eq!(count("managed_worktree_claims"), previous_claims);
    let (unauthorized, _) = call_legacy(
        &router,
        None,
        "POST",
        &uri,
        body,
        Some("unauthorized-reuse"),
    )
    .await;
    assert_eq!(unauthorized, StatusCode::UNAUTHORIZED);
    assert_eq!(provider.create_call_count(), previous_provider);
    assert_eq!(native_inventory(), previous_git);
    assert_eq!(count("managed_worktree_claims"), previous_claims);
    assert!(store.hydrate_runtime_state().is_err());
}
