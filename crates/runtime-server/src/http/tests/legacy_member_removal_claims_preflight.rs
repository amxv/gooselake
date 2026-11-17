use super::*;

async fn call_member_removal_fixture(
    router: &axum::Router,
    token: Option<&str>,
    method: &str,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn removing_legacy_team_member_releases_own_claim_despite_foreign_corrupt_history_without_native_cleanup(
) {
    let (router, token, temp, provider, runtime, app) =
        build_test_router_with_services(TeamMcpPolicy::default()).await;
    let repo = temp.path().join("disposable-git-repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("README.md"), "fake provider, temporary Git\n").unwrap();
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
            "git: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
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
    let session_body = serde_json::json!({
        "provider":"codex", "model":"test-model", "cwd":repo,
    });
    let mut ids = Vec::new();
    for _ in 0..3 {
        let (status, result) = legacy_session_fixture_json(&runtime, session_body.clone()).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        ids.push(result["id"].as_str().unwrap().to_owned());
    }
    let (lead, member, foreign) = (&ids[0], &ids[1], &ids[2]);
    let (status, team) = call_member_removal_fixture(
        &router,
        Some(&token),
        "POST",
        "/v1/teams",
        serde_json::json!({"name":"claim cleanup despite corruption", "lead_agent_id":lead}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{team}");
    let team_id = team["team"]["id"].as_str().unwrap();
    let (status, joined) = call_member_removal_fixture(
        &router,
        Some(&token),
        "POST",
        &format!("/v1/teams/{team_id}/members"),
        serde_json::json!({"agent_id":member,"title":"Disposable worker"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{joined}");
    let (status, created) = legacy_worktree_fixture_json(
        &app,
        serde_json::json!({
            "source_session_id":lead,"team_id":team_id,
            "worktree_name":"member-removal-corrupt-foreign-history", "branch_prefix":"gg",
            "deletion_policy":"retain_on_last_claim", "run_init_script":false,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let worktree_id = created["worktree"]["id"].as_str().unwrap().to_owned();
    let (status, claim) = call_member_removal_fixture(
        &router,
        Some(&token),
        "POST",
        &format!("/v1/worktrees/{worktree_id}/claims"),
        serde_json::json!({"session_id":member,"claim_role":"consumer"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{claim}");

    let db_path = temp.path().join("runtime.sqlite3");
    let db = rusqlite::Connection::open(&db_path).unwrap();
    let current_release = || -> Option<i64> {
        db.query_row(
            "SELECT released_at FROM managed_worktree_claims
             WHERE worktree_id=?1 AND session_id=?2",
            rusqlite::params![&worktree_id, member],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(current_release(), None);
    let diagnostics_before: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM team_operation_diagnostics
         WHERE team_id=?1 AND code='worktree_cleanup_failed_on_member_remove'",
            [team_id],
            |row| row.get(0),
        )
        .unwrap();
    let claims_before: i64 = db
        .query_row("SELECT COUNT(*) FROM managed_worktree_claims", [], |row| {
            row.get(0)
        })
        .unwrap();
    let managed_before: i64 = db
        .query_row("SELECT COUNT(*) FROM managed_worktrees", [], |row| {
            row.get(0)
        })
        .unwrap();
    let provider_before = provider.create_call_count();
    let git_before = native_inventory();
    assert_eq!(
        db.execute(
            "UPDATE sessions SET metadata_json='{unrelated-invalid-historical-session'
         WHERE id=?1",
            [foreign],
        )
        .unwrap(),
        1
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: db_path,
    });
    assert!(store.hydrate_runtime_state().is_err());
    let uri = format!("/v1/teams/{team_id}/members/{member}");
    let (unauthorized, _) =
        call_member_removal_fixture(&router, None, "DELETE", &uri, serde_json::json!({})).await;
    assert_eq!(unauthorized, StatusCode::UNAUTHORIZED);
    assert_eq!(current_release(), None);

    let (status, removed) =
        call_member_removal_fixture(&router, Some(&token), "DELETE", &uri, serde_json::json!({}))
            .await;
    assert_eq!(status, StatusCode::OK, "{removed}");
    assert!(
        removed["members"]
            .as_array()
            .unwrap()
            .iter()
            .all(|record| record["agent_id"] != member.as_str()),
        "team membership removal itself must persist despite best-effort cleanup"
    );
    assert!(
        current_release().is_some(),
        "member's durable active worktree claim should release despite foreign corrupt session"
    );
    let diagnostics_after: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM team_operation_diagnostics
         WHERE team_id=?1 AND code='worktree_cleanup_failed_on_member_remove'",
            [team_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        diagnostics_after,
        diagnostics_before + 1,
        "existing worktree's native cleanup must fail closed and report diagnostics"
    );
    assert_eq!(
        native_inventory(),
        git_before,
        "no native checkout deletion is authorized on unreadable safety evidence"
    );
    assert_eq!(provider.create_call_count(), provider_before);
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM managed_worktrees", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        managed_before
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM managed_worktree_claims", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        claims_before
    );
    assert!(store.hydrate_runtime_state().is_err());
}
