use super::workspace_worktree_routes::{call, fixture, git};
use super::*;

#[tokio::test]
async fn workspace_agents_start_on_root_existing_or_new_managed_worktree() {
    let (router, token, temp, _, workspace, _, linked) = fixture().await;
    let agents_uri = format!("/v2/workspaces/{}/agents", workspace.workspace_id);
    let (status, root) = call(
        &router,
        &token,
        "POST",
        &agents_uri,
        Some(serde_json::json!({
            "provider":"codex", "model":"test-model", "worktree":{"mode":"root"}
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{root:?}");
    assert_eq!(
        root["recreation_policy"]["authoritative_cwd"],
        workspace.canonical_root
    );

    let (status, existing) = call(
        &router,
        &token,
        "POST",
        &agents_uri,
        Some(serde_json::json!({
            "provider":"codex", "model":"test-model",
            "worktree":{"mode":"existing","worktree_id":"wt_linked"}
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{existing:?}");
    assert_eq!(existing["recreation_policy"]["authoritative_cwd"], linked);
    let persisted = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let hydrated = persisted.hydrate_runtime_state().unwrap();
    let existing_id = existing["agent_id"].as_str().unwrap();
    assert_eq!(
        hydrated
            .sessions
            .iter()
            .find(|s| s.id == existing_id)
            .unwrap()
            .worktree_id
            .as_deref(),
        Some("wt_linked")
    );
    assert!(hydrated.managed_worktree_claims.iter().any(|claim| {
        claim.session_id == existing_id
            && claim.worktree_id == "wt_linked"
            && claim.claim_role == "owner"
            && claim.released_at.is_none()
    }));

    let input = serde_json::json!({
        "provider":"codex", "model":"test-model",
        "worktree": {"mode":"new","worktree_name":"new-route","run_init_script":false,
                     "deletion_policy":"retain_on_last_claim"}
    });
    let (status, created) = call(&router, &token, "POST", &agents_uri, Some(input), None).await;
    assert_eq!(status, StatusCode::OK, "{created:?}");
    let cwd = created["recreation_policy"]["authoritative_cwd"]
        .as_str()
        .unwrap();
    assert!(Path::new(cwd).is_dir());
    let expected = runtime_core::resolve_repository_identity(Path::new(&workspace.canonical_root))
        .expect("workspace Git identity");
    let actual = runtime_core::resolve_repository_identity(Path::new(cwd))
        .expect("new Git worktree identity");
    assert_eq!(actual.fingerprint, expected.fingerprint);
    let created_id = created["agent_id"].as_str().unwrap();
    let hydrated = persisted.hydrate_runtime_state().unwrap();
    let session = hydrated
        .sessions
        .iter()
        .find(|row| row.id == created_id)
        .unwrap();
    let worktree_id = session
        .worktree_id
        .as_ref()
        .expect("assigned managed route");
    assert!(hydrated.managed_worktree_claims.iter().any(|claim| {
        claim.session_id == created_id
            && claim.worktree_id == *worktree_id
            && claim.claim_role == "owner"
            && claim.released_at.is_none()
    }));

    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{}/worktrees", workspace.workspace_id),
        Some(serde_json::json!({"worktree_name":"new-route","run_init_script":false})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay["created"], false);
    assert_eq!(replay["worktree"]["id"], worktree_id.as_str());
}

#[tokio::test]
async fn concurrent_named_managed_worktree_creation_converges_and_rejects_invalid_names() {
    let (router, token, temp, _, workspace, _, _) = fixture().await;
    let endpoint = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let input = serde_json::json!({"worktree_name":"shared","run_init_script":false});
    let (a, b) = tokio::join!(
        call(
            &router,
            &token,
            "POST",
            &endpoint,
            Some(input.clone()),
            None
        ),
        call(&router, &token, "POST", &endpoint, Some(input), None),
    );
    assert_eq!(a.0, StatusCode::OK, "{:?}", a.1);
    assert_eq!(b.0, StatusCode::OK, "{:?}", b.1);
    assert_eq!(a.1["worktree"]["id"], b.1["worktree"]["id"]);
    assert_ne!(a.1["created"], b.1["created"]);
    let persisted = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert_eq!(
        persisted
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktrees
            .iter()
            .filter(|row| row.worktree_name == "shared")
            .count(),
        1
    );
    for bad in ["../unsafe", "-option", "hidden/child", "..", "a..b"] {
        let (status, _) = call(
            &router,
            &token,
            "POST",
            &endpoint,
            Some(serde_json::json!({"worktree_name":bad})),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn failing_init_script_cannot_delete_newly_created_worktree_files() {
    use std::os::unix::fs::PermissionsExt;

    // Keep the temporary repository and SQLite database alive until the
    // requests finish; `_` drops the TempDir immediately at destructuring.
    let (router, token, _temp, _, workspace, _, _) = fixture().await;
    let repo = Path::new(&workspace.canonical_root);
    let script_dir = repo.join(".agents/gg");
    std::fs::create_dir_all(&script_dir).unwrap();
    let script = script_dir.join("worktree-init.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\nprintf 'retained user output\\n' > init-output.txt\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    git(repo, &["add", ".agents/gg/worktree-init.sh"]);
    git(
        repo,
        &["commit", "-m", "add intentionally failing init script"],
    );

    let uri = format!("/v2/workspaces/{}/worktrees", workspace.workspace_id);
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({
            "worktree_name":"retain-failed-init", "run_init_script":true
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The failed initializer created an untracked file. Compensating cleanup
    // must not run git worktree remove --force and destroy that data.
    let (status, recovered) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({
            "worktree_name":"retain-failed-init", "run_init_script":false
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{recovered:?}");
    assert_eq!(
        recovered["init_script_status"],
        "recovered_existing_checkout"
    );
    let cwd = recovered["worktree"]["worktree_cwd"].as_str().unwrap();
    assert_eq!(
        std::fs::read_to_string(Path::new(cwd).join("init-output.txt")).unwrap(),
        "retained user output\n"
    );
}

#[tokio::test]
async fn managed_cleanup_preserves_dirty_and_unmerged_checkouts() {
    let (router, token, temp, _, workspace, _, _) = fixture().await;
    let (status, created) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{}/worktrees", workspace.workspace_id),
        Some(serde_json::json!({
            "worktree_name":"cleanup-protected",
            "run_init_script":false,
            "deletion_policy":"delete_on_last_claim"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created:?}");
    let id = created["worktree"]["id"].as_str().unwrap();
    let path = created["worktree"]["worktree_cwd"].as_str().unwrap();
    let cleanup_uri = format!("/v1/worktrees/{id}/cleanup");
    let dirty = Path::new(path).join("untracked.txt");
    std::fs::write(&dirty, "private workspace changes\n").unwrap();
    std::fs::write(Path::new(path).join(".gitignore"), "secret.log\n").unwrap();
    let (status, preserved) = call(
        &router,
        &token,
        "POST",
        &cleanup_uri,
        Some(serde_json::json!({"reason":"operator_requested_cleanup"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preserved:?}");
    assert_eq!(preserved["status"], "skipped_dirty_worktree");
    assert!(dirty.exists());

    git(Path::new(path), &["add", "."]);
    git(Path::new(path), &["commit", "-m", "preserve unmerged work"]);
    let (status, unmerged) = call(
        &router,
        &token,
        "POST",
        &cleanup_uri,
        Some(serde_json::json!({"reason":"operator_requested_cleanup"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{unmerged:?}");
    assert_eq!(unmerged["status"], "skipped_unmerged_branch");
    assert!(dirty.exists());

    git(
        Path::new(&workspace.canonical_root),
        &["merge", "--ff-only", "gg/cleanup-protected"],
    );
    let ignored = Path::new(path).join("secret.log");
    std::fs::write(&ignored, "never delete ignored user data\n").unwrap();
    let (status, ignored_outcome) = call(
        &router,
        &token,
        "POST",
        &cleanup_uri,
        Some(serde_json::json!({"reason":"operator_requested_cleanup"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ignored_outcome:?}");
    assert_eq!(ignored_outcome["status"], "skipped_dirty_worktree");
    assert!(ignored.exists());
    std::fs::remove_file(ignored).unwrap();
    let (status, cleaned) = call(
        &router,
        &token,
        "POST",
        &cleanup_uri,
        Some(serde_json::json!({"reason":"operator_requested_cleanup"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cleaned:?}");
    assert_eq!(cleaned["status"], "deleted");
    assert!(!Path::new(path).exists());
    let persisted = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(persisted
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktrees
        .iter()
        .any(|row| row.id == id));
}

#[tokio::test]
async fn cleanup_refuses_live_agent_route_even_if_legacy_claim_was_released() {
    let (router, token, temp, _, workspace, _, _) = fixture().await;
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{}/agents", workspace.workspace_id),
        Some(serde_json::json!({
            "provider":"codex", "model":"test-model",
            "worktree":{"mode":"new","worktree_name":"still-live",
                        "deletion_policy":"delete_on_last_claim","run_init_script":false}
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let agent_id = agent["agent_id"].as_str().unwrap();
    let cwd = agent["recreation_policy"]["authoritative_cwd"]
        .as_str()
        .unwrap();
    let sqlite = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let active = sqlite
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims
        .into_iter()
        .find(|claim| claim.session_id == agent_id && claim.released_at.is_none())
        .unwrap();
    let worktree_id = active.worktree_id.clone();
    sqlite
        .upsert_managed_worktree_claim(&runtime_core::ManagedWorktreeClaimRecord {
            released_at: Some(1000000000),
            ..active
        })
        .unwrap();

    let (status, outcome) = call(
        &router,
        &token,
        "POST",
        &format!("/v1/worktrees/{worktree_id}/cleanup"),
        Some(serde_json::json!({"reason":"legacy_orphan_cleanup"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{outcome:?}");
    assert_eq!(outcome["status"], "skipped_live_binding");
    assert!(
        Path::new(cwd).is_dir(),
        "a live provider route must not be deleted"
    );
}

#[tokio::test]
async fn verified_rebind_cleanup_retries_safely_after_an_interrupted_native_cleanup() {
    let (router, token, temp, provider, workspace, _, _) = fixture().await;
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{}/agents", workspace.workspace_id),
        Some(serde_json::json!({
            "provider":"codex", "model":"test-model",
            "worktree":{"mode":"new","worktree_name":"cleanup-retry",
                        "deletion_policy":"delete_on_last_claim","run_init_script":false}
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let agent_id = agent["agent_id"].as_str().unwrap();
    let previous = agent["recreation_policy"]["authoritative_cwd"]
        .as_str()
        .unwrap();
    let untracked = Path::new(previous).join("must-stay.txt");
    std::fs::write(&untracked, "not committed\n").unwrap();

    let uri = format!(
        "/v2/workspaces/{}/agents/{agent_id}/worktree",
        workspace.workspace_id
    );
    let input = serde_json::json!({
        "worktree_id":null, "expected_revision":0, "cleanup_previous_worktree":true
    });
    let (status, first) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(input.clone()),
        Some("recover-cleanup"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first:?}");
    assert_eq!(
        first["agent"]["recreation_policy"]["authoritative_cwd"],
        workspace.canonical_root
    );
    assert_eq!(first["operation"]["phase"], "completed");
    assert_eq!(first["operation"]["previous_cleanup_status"], "pending");
    assert_eq!(
        first["operation"]["previous_cleanup_diagnostic"],
        "skipped_dirty_worktree"
    );
    assert!(untracked.exists());
    assert_eq!(provider.rebind_calls().await, 1);

    // The route is already committed. Replaying the same request must never
    // repeat the provider call, even while native cleanup remains pending.
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(input),
        Some("recover-cleanup"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay["operation"]["previous_cleanup_status"], "pending");
    assert_eq!(provider.rebind_calls().await, 1);

    let operation_id = first["operation"]["operation_id"].as_str().unwrap();
    let sqlite = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let persisted = sqlite
        .get_workspace_agent_rebind(operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.previous_cleanup_status.as_deref(),
        Some("pending")
    );

    // Idempotent replay must not secretly retry a previously blocked native
    // cleanup. Only the explicit recovery endpoint authorizes that effect.
    std::fs::remove_file(&untracked).unwrap();
    let (status, replay_after_fix) = call(
        &router,
        &token,
        "POST",
        &uri,
        Some(serde_json::json!({
            "worktree_id":null,"expected_revision":0,"cleanup_previous_worktree":true
        })),
        Some("recover-cleanup"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay_after_fix:?}");
    assert_eq!(
        replay_after_fix["operation"]["previous_cleanup_status"],
        "pending"
    );
    assert!(
        Path::new(previous).is_dir(),
        "idempotency replay cannot repeat a native cleanup effect"
    );
    let retry = format!(
        "/v2/workspaces/{}/agents/{agent_id}/rebinds/{operation_id}/cleanup",
        workspace.workspace_id
    );
    let (status, result) = call(&router, &token, "POST", &retry, None, None).await;
    assert_eq!(status, StatusCode::OK, "{result:?}");
    assert_eq!(result["previous_cleanup_status"], "deleted");
    assert!(!Path::new(previous).exists());
    assert_eq!(provider.rebind_calls().await, 1);
    let (status, again) = call(&router, &token, "POST", &retry, None, None).await;
    assert_eq!(status, StatusCode::OK, "{again:?}");
    assert_eq!(again["previous_cleanup_status"], "deleted");
    assert_eq!(provider.rebind_calls().await, 1);
}
