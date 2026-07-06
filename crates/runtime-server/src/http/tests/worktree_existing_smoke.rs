use super::*;

#[tokio::test]
async fn phase6_spawn_acp_member_use_existing_mode_reuses_existing_worktree() {
    let (router, token, temp_dir, acp_provider) = build_mixed_provider_test_router().await;
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::write(repo_dir.join("README.md"), "phase6 acp use_existing\n").expect("write readme");
    let init_status = std::process::Command::new("git")
        .arg("init")
        .arg("-b")
        .arg("main")
        .arg(repo_dir.as_os_str())
        .status()
        .expect("git init");
    assert!(init_status.success(), "git init should succeed");
    let add_status = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir.as_os_str())
        .args(["add", "."])
        .status()
        .expect("git add");
    assert!(add_status.success(), "git add should succeed");
    let commit_status = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir.as_os_str())
        .args([
            "-c",
            "user.name=GG Runtime",
            "-c",
            "user.email=runtime@example.com",
            "commit",
            "-m",
            "init",
        ])
        .status()
        .expect("git commit");
    assert!(commit_status.success(), "git commit should succeed");

    let lead_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "codex",
                        "model": "test-model",
                        "cwd": repo_dir.display().to_string(),
                        "metadata": {"suite":"phase6-acp"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create lead session");
    assert_eq!(lead_response.status(), StatusCode::OK);
    let lead_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(lead_response.into_body(), usize::MAX)
            .await
            .expect("lead body"),
    )
    .expect("lead json");
    let lead_session_id = lead_json["id"].as_str().expect("lead id").to_string();

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "Phase6 ACP Existing Team",
                        "lead_agent_id": lead_session_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("team body"),
    )
    .expect("team json");
    let team_id = create_team_json["team"]["id"]
        .as_str()
        .expect("team id")
        .to_string();

    let create_worktree_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/worktrees")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "team_id": team_id,
                        "source_session_id": lead_session_id,
                        "worktree_name": "phase6-acp-existing-worker",
                        "branch_prefix": "gg",
                        "run_init_script": false,
                        "deletion_policy": "retain_on_last_claim",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create worktree response");
    assert_eq!(create_worktree_response.status(), StatusCode::OK);
    let create_worktree_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_worktree_response.into_body(), usize::MAX)
            .await
            .expect("create worktree body"),
    )
    .expect("create worktree json");
    let existing_worktree_id = create_worktree_json["worktree"]["id"]
        .as_str()
        .expect("existing worktree id")
        .to_string();
    let existing_worktree_cwd = create_worktree_json["worktree"]["worktree_cwd"]
        .as_str()
        .expect("existing worktree cwd")
        .to_string();

    let spawn_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/members/spawn"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "source_session_id": lead_session_id,
                        "provider": "acp",
                        "title": "Phase 6 ACP Existing",
                        "prompt": "Use existing worktree.",
                        "worktree": {
                            "mode": "use_existing",
                            "name": "phase6-acp-existing-worker",
                            "branch_prefix": "gg",
                            "run_init_script": false
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("spawn response");
    assert_eq!(spawn_response.status(), StatusCode::OK);
    let spawn_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(spawn_response.into_body(), usize::MAX)
            .await
            .expect("spawn body"),
    )
    .expect("spawn json");
    let spawned_session_id = spawn_json["spawned_session"]["id"]
        .as_str()
        .expect("spawned session id")
        .to_string();
    assert_eq!(
        spawn_json["worktree_assignment_mode"].as_str(),
        Some("reused")
    );
    assert_eq!(
        spawn_json["worktree"]["id"].as_str(),
        Some(existing_worktree_id.as_str())
    );

    let created_sessions = acp_provider.created_sessions().await;
    let acp_spawn = created_sessions
        .iter()
        .find(|session| session.runtime_session_id == spawned_session_id)
        .expect("captured ACP reused session");
    assert_eq!(
        acp_spawn.cwd.as_deref(),
        Some(existing_worktree_cwd.as_str())
    );
}

#[tokio::test]
#[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json and phase6 worktree tooling"]
async fn smoke_real_codex_phase6_spawn_worktree_and_cleanup() {
    let home_dir = std::env::var("HOME").expect("HOME must be set");
    let source_auth = std::path::PathBuf::from(home_dir)
        .join(".gg")
        .join("codex")
        .join("auth.json");
    assert!(source_auth.exists(), "missing {}", source_auth.display());

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::write(repo_dir.join("README.md"), "phase6 smoke\n").expect("write readme");
    assert!(std::process::Command::new("git")
        .arg("init")
        .arg("-b")
        .arg("main")
        .arg(repo_dir.as_os_str())
        .status()
        .expect("git init")
        .success());
    assert!(std::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir.as_os_str())
        .args(["add", "."])
        .status()
        .expect("git add")
        .success());
    assert!(std::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir.as_os_str())
        .args([
            "-c",
            "user.name=GG Runtime",
            "-c",
            "user.email=runtime@example.com",
            "commit",
            "-m",
            "init",
        ])
        .status()
        .expect("git commit")
        .success());

    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.claude.enabled = false;
    config.providers.codex.enabled = true;
    let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");

    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let lead_session_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "codex",
                        "model": codex_test_model(),
                        "cwd": repo_dir.display().to_string(),
                        "metadata": {"smoke":"phase6"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create lead session");
    assert_eq!(lead_session_response.status(), StatusCode::OK);
    let lead_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(lead_session_response.into_body(), usize::MAX)
            .await
            .expect("lead body"),
    )
    .expect("lead json");
    let lead_session_id = lead_json["id"].as_str().expect("lead id").to_string();

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "Phase6 Smoke Team",
                        "lead_agent_id": lead_session_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("team body"),
    )
    .expect("team json");
    let team_id = team_json["team"]["id"]
        .as_str()
        .expect("team id")
        .to_string();

    let spawn_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/members/spawn"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "source_session_id": lead_session_id,
                        "provider": "codex",
                        "model": codex_test_model(),
                        "title": "Phase 6 Implementer",
                        "prompt": "Execute phase 6 smoke instructions.",
                        "worktree": {
                            "mode": "create",
                            "name": "phase6-smoke-worker",
                            "run_init_script": false
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("spawn member");
    assert_eq!(spawn_response.status(), StatusCode::OK);
    let spawn_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(spawn_response.into_body(), usize::MAX)
            .await
            .expect("spawn body"),
    )
    .expect("spawn json");
    let spawned_session_id = spawn_json["spawned_session"]["id"]
        .as_str()
        .expect("spawned id")
        .to_string();
    let worktree_id = spawn_json["worktree"]["id"]
        .as_str()
        .expect("worktree id")
        .to_string();

    tokio::time::sleep(Duration::from_millis(300)).await;
    let deliveries_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/deliveries"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("deliveries");
    assert_eq!(deliveries_response.status(), StatusCode::OK);
    let deliveries_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(deliveries_response.into_body(), usize::MAX)
            .await
            .expect("deliveries body"),
    )
    .expect("deliveries json");
    assert!(
        deliveries_json.as_array().map(|rows| !rows.is_empty()) == Some(true),
        "expected onboarding delivery rows"
    );

    let remove_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/teams/{team_id}/members/{spawned_session_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("remove member");
    assert_eq!(remove_response.status(), StatusCode::OK);

    let cleanup_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/worktrees/{worktree_id}/cleanup"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({"reason":"phase6_smoke"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("cleanup");
    assert_eq!(cleanup_response.status(), StatusCode::OK);

    let diagnostics_response = router
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations?team_id={team_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("diagnostics");
    assert_eq!(diagnostics_response.status(), StatusCode::OK);
}
