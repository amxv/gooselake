use super::*;

async fn call_legacy_create(
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
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn legacy_create_existing_retained_checkout_scopes_identity_and_claims_with_corrupt_foreign_session(
) {
    let (router, token, temp, provider, runtime, app) =
        build_test_router_with_services(TeamMcpPolicy::default()).await;
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("README.md"), "disposable Git workspace\n").unwrap();
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
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let native_inventory = || {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["worktree", "list", "--porcelain"])
            .output()
            .unwrap();
        assert!(output.status.success());
        output.stdout
    };
    let session_body = serde_json::json!({
        "provider":"codex", "model":"test-model", "cwd":repo,
    });
    let (status, lead) = legacy_session_fixture_json(&runtime, session_body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{lead}");
    let lead_id = lead["id"].as_str().unwrap();
    let (status, team) = call_legacy_create(
        &router,
        Some(&token),
        "/v1/teams",
        serde_json::json!({"name":"existing worktree policy", "lead_agent_id":lead_id}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{team}");
    let team_id = team["team"]["id"].as_str().unwrap();
    let body = serde_json::json!({
        "source_session_id":lead_id,"team_id":team_id,
        "worktree_name":"legacy-create-snapshot", "branch_prefix":"gg",
        "deletion_policy":"retain_on_last_claim","run_init_script":false,
    });
    let (status, first) = legacy_worktree_fixture_json(&app, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["created"], true);
    let worktree_id = first["worktree"]["id"].as_str().unwrap();

    let (status, claim) = call_legacy_create(
        &router,
        Some(&token),
        &format!("/v1/worktrees/{worktree_id}/claims"),
        serde_json::json!({
            "session_id":lead_id,"claim_role":"owner",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{claim}");
    let (status, foreign) = legacy_session_fixture_json(&runtime, session_body).await;
    assert_eq!(status, StatusCode::OK, "{foreign}");
    let foreign_id = foreign["id"].as_str().unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    assert_eq!(
        db.execute(
            "UPDATE sessions SET metadata_json='{invalid-foreign-json' WHERE id=?1",
            [foreign_id],
        )
        .unwrap(),
        1
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(store.hydrate_runtime_state().is_err());
    let count = |table: &str| -> i64 {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    let before_git = native_inventory();
    let before_rows = count("managed_worktrees");
    let before_claims = count("managed_worktree_claims");
    let before_provider = provider.create_call_count();
    let (status, reused) = legacy_worktree_fixture_json(&app, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{reused}");
    assert_eq!(reused["worktree"]["id"], worktree_id);
    assert_eq!(reused["created"], false);
    assert_eq!(reused["init_script_status"], "skipped_existing");
    assert_eq!(native_inventory(), before_git);
    assert_eq!(count("managed_worktrees"), before_rows);
    assert_eq!(count("managed_worktree_claims"), before_claims);
    assert_eq!(provider.create_call_count(), before_provider);

    let mut conflicting_policy = body.clone();
    conflicting_policy["deletion_policy"] = serde_json::json!("delete_on_last_claim");
    let (conflict, _) = legacy_worktree_fixture_json(&app, conflicting_policy).await;
    assert_eq!(conflict, StatusCode::CONFLICT);
    let (unauthorized, _) = call_legacy_create(&router, None, "/v1/worktrees", body).await;
    assert_eq!(unauthorized, StatusCode::UNAUTHORIZED);
    assert_eq!(native_inventory(), before_git);
    assert_eq!(count("managed_worktrees"), before_rows);
    assert_eq!(count("managed_worktree_claims"), before_claims);
    assert_eq!(provider.create_call_count(), before_provider);
    assert!(store.hydrate_runtime_state().is_err());
}
