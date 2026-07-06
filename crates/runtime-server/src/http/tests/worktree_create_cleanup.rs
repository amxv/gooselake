use super::*;

#[tokio::test]
async fn phase6_spawn_member_with_created_worktree_and_cleanup_on_remove() {
    let (router, token, temp_dir) = build_test_router().await;
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::write(repo_dir.join("README.md"), "phase6\n").expect("write readme");
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
                        "metadata": {"suite":"phase6"}
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
                        "name": "Phase6 Team",
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
                        "title": "Phase 6 Implementer",
                        "prompt": "Implement phase 6.",
                        "worktree": {
                            "mode": "create",
                            "name": "phase6-worker",
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
    let worktree_id = spawn_json["worktree"]["id"]
        .as_str()
        .expect("worktree id")
        .to_string();
    let worktree_cwd = spawn_json["worktree"]["worktree_cwd"]
        .as_str()
        .expect("worktree cwd")
        .to_string();
    assert!(
        Path::new(worktree_cwd.as_str()).exists(),
        "spawn-created worktree path should exist"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
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
        .expect("deliveries response");
    assert_eq!(deliveries_response.status(), StatusCode::OK);
    let deliveries: serde_json::Value = serde_json::from_slice(
        &to_bytes(deliveries_response.into_body(), usize::MAX)
            .await
            .expect("deliveries body"),
    )
    .expect("deliveries json");
    assert!(
        deliveries.as_array().map(|rows| !rows.is_empty()) == Some(true),
        "onboarding delivery should be persisted"
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
        .expect("remove response");
    assert_eq!(remove_response.status(), StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(250)).await;
    let cleanup_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/worktrees/{worktree_id}/cleanup"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("cleanup response");
    assert_eq!(cleanup_response.status(), StatusCode::OK);
    let cleanup_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(cleanup_response.into_body(), usize::MAX)
            .await
            .expect("cleanup body"),
    )
    .expect("cleanup json");
    assert!(
        cleanup_json["status"].as_str() == Some("deleted")
            || cleanup_json["status"].as_str() == Some("cleanup_failed")
            || cleanup_json["status"].as_str() == Some("retained_by_policy")
            || cleanup_json["status"].as_str() == Some("skipped_live_claims"),
        "cleanup endpoint should report structured status"
    );
}

#[tokio::test]
async fn phase6_spawn_member_use_existing_mode_reuses_existing_worktree() {
    let (router, token, temp_dir) = build_test_router().await;
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::write(repo_dir.join("README.md"), "phase6 use_existing\n").expect("write readme");
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
                        "metadata": {"suite":"phase6"}
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
                        "name": "Phase6 Existing Team",
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
                        "worktree_name": "phase6-existing-worker",
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
                        "title": "Phase 6 Existing",
                        "prompt": "Use existing worktree.",
                        "worktree": {
                            "mode": "use_existing",
                            "name": "phase6-existing-worker",
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
    assert_eq!(
        spawn_json["worktree_assignment_mode"].as_str(),
        Some("reused"),
        "documented use_existing mode must select the reuse path"
    );
    assert_eq!(
        spawn_json["worktree_created_by_operation"].as_bool(),
        Some(false),
        "reused path must not report created worktree ownership"
    );
    assert_eq!(
        spawn_json["worktree"]["id"].as_str(),
        Some(existing_worktree_id.as_str())
    );
}

#[tokio::test]
async fn phase6_spawn_acp_member_with_created_worktree_assigns_runtime_cwd_and_cleanup_on_remove() {
    let (router, token, temp_dir, acp_provider) = build_mixed_provider_test_router().await;
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir_all(&repo_dir).expect("create repo dir");
    std::fs::write(repo_dir.join("README.md"), "phase6 acp\n").expect("write readme");
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
                        "name": "Phase6 ACP Team",
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
                        "title": "Phase 6 ACP Implementer",
                        "prompt": "Implement phase 6 for ACP.",
                        "worktree": {
                            "mode": "create",
                            "name": "phase6-acp-worker",
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
    let worktree_id = spawn_json["worktree"]["id"]
        .as_str()
        .expect("worktree id")
        .to_string();
    let worktree_cwd = spawn_json["worktree"]["worktree_cwd"]
        .as_str()
        .expect("worktree cwd")
        .to_string();
    assert!(Path::new(worktree_cwd.as_str()).exists());

    let created_sessions = acp_provider.created_sessions().await;
    let acp_spawn = created_sessions
        .iter()
        .find(|session| session.runtime_session_id == spawned_session_id)
        .expect("captured ACP spawned session");
    assert_eq!(acp_spawn.cwd.as_deref(), Some(worktree_cwd.as_str()));

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
        .expect("remove response");
    assert_eq!(remove_response.status(), StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(250)).await;
    let cleanup_response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/worktrees/{worktree_id}/cleanup"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("cleanup response");
    assert_eq!(cleanup_response.status(), StatusCode::OK);
}
