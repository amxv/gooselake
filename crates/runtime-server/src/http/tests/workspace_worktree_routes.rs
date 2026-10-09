use super::*;
use runtime_core::WorktreeService;
use std::process::Command;

pub(super) async fn call(
    router: &Router,
    token: &str,
    method: &str,
    uri: &str,
    input: Option<serde_json::Value>,
    key: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    let response = router
        .clone()
        .oneshot(
            request
                .body(Body::from(
                    input.map(|item| item.to_string()).unwrap_or_default(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&body).expect("JSON response"),
    )
}

pub(super) fn git(repo: &Path, args: &[&str]) {
    let result = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .expect("git command");
    assert!(
        result.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&result.stderr)
    );
}

fn seeded_repo(path: &Path) {
    std::fs::create_dir_all(path).expect("create repo");
    git(path, &["init"]);
    git(path, &["config", "user.email", "runtime@example.invalid"]);
    git(path, &["config", "user.name", "Runtime Test"]);
    std::fs::write(path.join("README.md"), "fixture\n").expect("write fixture");
    git(path, &["add", "."]);
    git(path, &["commit", "-m", "init"]);
}

pub(super) async fn fixture() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    runtime_core::WorkspaceRecord,
    runtime_core::WorkspaceAgentRecord,
    String,
) {
    let (router, token, temp, provider) =
        build_test_router_with_provider(TeamMcpPolicy::default()).await;
    let repo = temp.path().join("repo");
    seeded_repo(&repo);
    let (status, workspace) = call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root": repo, "display_name":"Routes"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workspace: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_value(workspace).expect("workspace");
    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{}/agents", workspace.workspace.workspace_id),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let agent: runtime_core::WorkspaceAgentRecord = serde_json::from_value(agent).expect("agent");
    let linked = temp.path().join("linked");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "gg/linked",
            linked.to_str().expect("linked path"),
        ],
    );
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    store
        .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
            id: "wt_linked".into(),
            repo_root: repo.to_string_lossy().to_string(),
            worktree_root: temp.path().to_string_lossy().to_string(),
            worktree_cwd: linked.to_string_lossy().to_string(),
            branch_name: "gg/linked".into(),
            worktree_name: "linked".into(),
            unified_workspace_path: "routes".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 1,
            updated_at: 1,
        })
        .expect("register linked worktree");
    (
        router,
        token,
        temp,
        provider,
        workspace.workspace,
        agent,
        linked.to_string_lossy().to_string(),
    )
}

#[tokio::test]
async fn workspace_inventory_and_verified_rebind_update_session_policy_and_claim_atomically() {
    let (router, token, temp, provider, workspace, agent, linked) = fixture().await;
    let base = format!(
        "/v2/workspaces/{}/agents/{}",
        workspace.workspace_id, agent.agent_id
    );
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
    assert_eq!(inventory["worktrees"][0]["worktree_id"], "wt_linked");
    assert!(inventory["worktrees"][0]["revision"].as_u64().unwrap() > 0);
    assert_eq!(inventory["worktrees"][0]["routing_state"], "routable");
    assert!(inventory["repository_fingerprint"]
        .as_str()
        .unwrap()
        .starts_with("repo_v2_"));

    let request = serde_json::json!({"worktree_id":"wt_linked","expected_revision":agent.revision});
    let (status, first) = call(
        &router,
        &token,
        "POST",
        &format!("{base}/worktree"),
        Some(request.clone()),
        Some("assign-linked"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["operation"]["phase"], "completed");
    assert_eq!(
        first["agent"]["recreation_policy"]["authoritative_cwd"],
        linked
    );
    assert_eq!(first["agent"]["revision"], agent.revision + 1);
    assert_eq!(provider.rebind_calls().await, 1);
    let op_id = first["operation"]["operation_id"].as_str().unwrap();
    let (status, exact) = call(
        &router,
        &token,
        "GET",
        &format!("{base}/rebinds/{op_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(exact, first["operation"]);

    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &format!("{base}/worktree"),
        Some(request),
        Some("assign-linked"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["operation"], first["operation"]);
    assert_eq!(
        provider.rebind_calls().await,
        1,
        "idempotent replay must never reach provider"
    );

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let persisted = store.hydrate_runtime_state().expect("hydrated");
    let canonical = store
        .get_operation(op_id)
        .expect("canonical operation")
        .expect("canonical worktree rebind operation");
    assert_eq!(
        canonical.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(canonical.operation.kind, "workspace_agent_rebind");
    assert!(
        canonical.claims.is_empty(),
        "completed rebind releases its exclusive claim"
    );
    assert_eq!(canonical.transitions.len(), 2);
    let session = persisted
        .sessions
        .iter()
        .find(|row| row.id == agent.agent_id)
        .unwrap();
    assert_eq!(session.worktree_id.as_deref(), Some("wt_linked"));
    assert_eq!(session.cwd.as_deref(), Some(linked.as_str()));
    let claims = persisted
        .managed_worktree_claims
        .iter()
        .filter(|row| row.session_id == agent.agent_id && row.released_at.is_none())
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].worktree_id, "wt_linked");

    let (status, back) = call(
        &router,
        &token,
        "POST",
        &format!("{base}/worktree"),
        Some(serde_json::json!({"worktree_id":null,"expected_revision":agent.revision+1})),
        Some("assign-root"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        back["agent"]["recreation_policy"]["authoritative_cwd"],
        workspace.canonical_root
    );
    let persisted = store.hydrate_runtime_state().expect("hydrated after root");
    assert_eq!(
        persisted
            .sessions
            .iter()
            .find(|row| row.id == agent.agent_id)
            .unwrap()
            .worktree_id,
        None
    );
    assert_eq!(
        persisted
            .managed_worktree_claims
            .iter()
            .filter(|row| row.session_id == agent.agent_id && row.released_at.is_none())
            .count(),
        0
    );
    assert!(
        Path::new(&linked).exists(),
        "old linked worktree must be preserved by default"
    );
}

#[tokio::test]
async fn ambiguous_or_unverified_rebind_is_durably_fenced_and_cannot_run_another_turn() {
    let (router, token, temp, provider, workspace, agent, _) = fixture().await;
    provider.set_rebind_mode(Some("bad_evidence")).await;
    let base = format!(
        "/v2/workspaces/{}/agents/{}",
        workspace.workspace_id, agent.agent_id
    );
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{base}/worktree"),
        Some(serde_json::json!({"worktree_id":"wt_linked","expected_revision":0})),
        Some("ambiguous-binding"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let operation = store
        .get_workspace_agent_rebind_by_key(
            &workspace.workspace_id,
            &agent.agent_id,
            "ambiguous-binding",
        )
        .expect("operation read")
        .expect("operation");
    assert_eq!(operation.phase, "manual_review");
    let canonical = store
        .get_operation(&operation.operation_id)
        .unwrap()
        .expect("durable canonical operation");
    assert_eq!(
        canonical.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(
        canonical.claims.len(),
        1,
        "recovery-required rebind retains an exclusive fenced agent operation"
    );
    assert!(store
        .unresolved_workspace_agent_rebind(&agent.agent_id)
        .unwrap());
    let persisted = store
        .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.recreation_policy.authoritative_cwd,
        workspace.canonical_root
    );
    assert_eq!(persisted.revision, agent.revision);

    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("/v1/sessions/{}/turns", agent.agent_id),
        Some(serde_json::json!({"input":[{"type":"text","text":"should not dispatch"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &format!("{base}/worktree"),
        Some(serde_json::json!({"worktree_id":null,"expected_revision":0})),
        Some("another-key"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.rebind_calls().await, 1);
    let (status, exact) = call(
        &router,
        &token,
        "GET",
        &format!("{base}/rebinds/{}", operation.operation_id),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(exact["phase"], "manual_review");
    let (status, _) = call(
        &router,
        &token,
        "POST",
        "/v1/worktrees/wt_linked/release",
        Some(serde_json::json!({
            "session_id": agent.agent_id,
            "cleanup_if_last_claim": true,
        })),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "legacy release cannot discard a pending provider rebind reservation"
    );

    // A separate manager after process restart must respect the same SQLite
    // fence rather than lazily resuming an unverified native provider session.
    let mut registry = runtime_core::ProviderRegistry::new();
    registry
        .register(Arc::new(TestProvider::default()))
        .expect("restart provider");
    let restarted = Arc::new(
        RuntimeSessionManager::new(
            Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
                database_path: temp.path().join("runtime.sqlite3"),
            })),
            Arc::new(registry),
            128,
        )
        .expect("new runtime manager"),
    );
    let error = restarted
        .send_turn(
            &agent.agent_id,
            runtime_core::SendTurnInput {
                input: vec![serde_json::json!({"type":"text","text":"restart must not replay"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect_err("unverified binding must fence restarted runtime");
    assert!(matches!(error, RuntimeError::Conflict(_)));
}

#[tokio::test]
async fn lost_provider_rebind_response_never_changes_route_or_retries_automatically() {
    let (router, token, temp, provider, workspace, agent, _) = fixture().await;
    provider.set_rebind_mode(Some("unknown")).await;
    let base = format!(
        "/v2/workspaces/{}/agents/{}/worktree",
        workspace.workspace_id, agent.agent_id
    );
    let input = serde_json::json!({"worktree_id":"wt_linked","expected_revision":0});
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(input.clone()),
        Some("lost-native-response"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(input),
        Some("lost-native-response"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.rebind_calls().await, 1);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let operation = store
        .get_workspace_agent_rebind_by_key(
            &workspace.workspace_id,
            &agent.agent_id,
            "lost-native-response",
        )
        .unwrap()
        .unwrap();
    assert_eq!(operation.phase, "manual_review");
    assert_eq!(
        operation.error_code.as_deref(),
        Some("reassignment_recovery_required")
    );
    assert_eq!(
        store
            .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
            .unwrap()
            .unwrap()
            .recreation_policy
            .authoritative_cwd,
        workspace.canonical_root
    );
    let active_claims = store
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims
        .into_iter()
        .filter(|claim| {
            claim.session_id == agent.agent_id
                && claim.worktree_id == "wt_linked"
                && claim.released_at.is_none()
        })
        .count();
    assert_eq!(
        active_claims, 1,
        "destination reservation remains while outcome is unknown"
    );
}

#[tokio::test]
async fn foreign_or_stale_managed_worktrees_are_never_provider_rebind_targets() {
    let (router, token, temp, provider, workspace, agent, linked) = fixture().await;
    let other = temp.path().join("other");
    seeded_repo(&other);
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    store
        .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
            id: "wt_foreign".into(),
            repo_root: other.to_string_lossy().to_string(),
            worktree_root: other.to_string_lossy().to_string(),
            worktree_cwd: other.to_string_lossy().to_string(),
            branch_name: "main".into(),
            worktree_name: "foreign".into(),
            unified_workspace_path: "foreign".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 2,
            updated_at: 2,
        })
        .expect("foreign fixture");
    let base = format!(
        "/v2/workspaces/{}/agents/{}/worktree",
        workspace.workspace_id, agent.agent_id
    );
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(serde_json::json!({"worktree_id":"wt_foreign","expected_revision":0})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    git(
        Path::new(&workspace.canonical_root),
        &["worktree", "remove", "--force", &linked],
    );
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
    assert_eq!(
        inventory["worktrees"][0]["eligibility_blockers"][0],
        "worktree_path_missing"
    );
    let (status, _) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(serde_json::json!({"worktree_id":"wt_linked","expected_revision":0})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(provider.rebind_calls().await, 0);
}

#[tokio::test]
async fn unresolved_destination_reservation_survives_worktree_service_restart() {
    let (router, token, temp, provider, workspace, agent, linked) = fixture().await;
    let repository = Path::new(&workspace.canonical_root);
    let other = temp.path().join("other-linked");
    git(
        repository,
        &[
            "worktree",
            "add",
            "-b",
            "gg/other-linked",
            other.to_str().unwrap(),
        ],
    );
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    store
        .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
            id: "wt_other".into(),
            repo_root: workspace.canonical_root.clone(),
            worktree_root: temp.path().to_string_lossy().into_owned(),
            worktree_cwd: other.to_string_lossy().into_owned(),
            branch_name: "gg/other-linked".into(),
            worktree_name: "other-linked".into(),
            unified_workspace_path: "routes".into(),
            deletion_policy: "delete_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 2,
            updated_at: 2,
        })
        .unwrap();
    let route = format!(
        "/v2/workspaces/{}/agents/{}/worktree",
        workspace.workspace_id, agent.agent_id
    );
    let (status, first) = call(
        &router,
        &token,
        "POST",
        &route,
        Some(serde_json::json!({"worktree_id":"wt_linked","expected_revision":0})),
        Some("initial-linked-route"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first:?}");
    assert_eq!(
        first["agent"]["recreation_policy"]["authoritative_cwd"],
        linked
    );

    provider.set_rebind_mode(Some("unknown")).await;
    let (status, second) = call(
        &router,
        &token,
        "POST",
        &route,
        Some(serde_json::json!({"worktree_id":"wt_other","expected_revision":1})),
        Some("ambiguous-second-route"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{second:?}");
    assert!(store
        .unresolved_workspace_agent_rebind(&agent.agent_id)
        .unwrap());

    let mut registry = runtime_core::ProviderRegistry::new();
    registry
        .register(provider)
        .expect("register provider after restart");
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128)
            .expect("restarted runtime"),
    );
    let comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("restarted comms");
    let worktrees = RuntimeWorktreeService::new(
        store.clone(),
        runtime,
        comms,
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp.path().join("worktrees").to_string_lossy().into_owned(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "delete_on_last_claim".into(),
        },
    )
    .expect("worktree startup recovery");

    let hydrated = store.hydrate_runtime_state().unwrap();
    let active = hydrated
        .managed_worktree_claims
        .iter()
        .filter(|claim| claim.session_id == agent.agent_id && claim.released_at.is_none())
        .collect::<Vec<_>>();
    assert_eq!(active.len(), 2, "both bindings must survive startup repair");
    assert!(active
        .iter()
        .any(|claim| claim.worktree_id == "wt_linked" && claim.claim_role == "owner"));
    assert!(active
        .iter()
        .any(|claim| claim.worktree_id == "wt_other" && claim.claim_role == "rebind_reservation"));

    let release = worktrees
        .release_worktree(runtime_core::WorktreeReleaseRequest {
            worktree_id: "wt_linked".into(),
            session_id: agent.agent_id.clone(),
            cleanup_if_last_claim: Some(true),
        })
        .await;
    assert!(matches!(release, Err(RuntimeError::Conflict(_))));
    let claim = worktrees
        .claim_worktree(runtime_core::WorktreeClaimRequest {
            worktree_id: "wt_other".into(),
            session_id: agent.agent_id.clone(),
            claim_role: "owner".into(),
        })
        .await;
    assert!(matches!(claim, Err(RuntimeError::Conflict(_))));
    assert!(
        other.is_dir(),
        "recovery-required destination must not be deleted"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_rebind_and_native_cleanup_never_bind_to_a_deleted_checkout() {
    // Retain the fixture's TempDir for both concurrent HTTP requests.
    let (router, token, _temp, provider, workspace, agent, _) = fixture().await;
    let (status, created) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{}/worktrees", workspace.workspace_id),
        Some(serde_json::json!({
            "worktree_name":"concurrent-route", "run_init_script":false,
            "deletion_policy":"delete_on_last_claim"
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created:?}");
    let worktree_id = created["worktree"]["id"].as_str().unwrap().to_owned();
    let worktree_cwd = created["worktree"]["worktree_cwd"]
        .as_str()
        .unwrap()
        .to_owned();
    let rebind_uri = format!(
        "/v2/workspaces/{}/agents/{}/worktree",
        workspace.workspace_id, agent.agent_id
    );
    let cleanup_uri = format!("/v1/worktrees/{worktree_id}/cleanup");

    // Both requests compete for the same repository lock, even though they
    // enter through separate runtime and worktree services.
    let lock = runtime_core::repository_worktree_lock(&workspace.canonical_root).await;
    let initial_guard = lock.lock().await;
    let rebind = {
        let router = router.clone();
        let token = token.clone();
        tokio::spawn(async move {
            call(
                &router,
                &token,
                "POST",
                &rebind_uri,
                Some(serde_json::json!({
                    "worktree_id":worktree_id, "expected_revision":0
                })),
                Some("race-with-cleanup"),
            )
            .await
        })
    };
    let cleanup = {
        let router = router.clone();
        let token = token.clone();
        tokio::spawn(async move {
            call(
                &router,
                &token,
                "POST",
                &cleanup_uri,
                Some(serde_json::json!({"reason":"race_with_assignment"})),
                None,
            )
            .await
        })
    };
    tokio::task::yield_now().await;
    drop(initial_guard);

    let (rebind, cleanup) = timeout(Duration::from_secs(20), async {
        tokio::join!(rebind, cleanup)
    })
    .await
    .expect("concurrent Git cleanup and provider reassignment must not deadlock");
    let (rebind_status, rebind) = rebind.unwrap();
    let (cleanup_status, cleanup) = cleanup.unwrap();
    assert_eq!(cleanup_status, StatusCode::OK, "{cleanup:?}");
    if rebind_status == StatusCode::OK {
        assert!(
            matches!(
                cleanup["status"].as_str(),
                Some("skipped_live_claims" | "skipped_live_binding")
            ),
            "verified binding must keep the checkout: {cleanup:?}"
        );
        assert_eq!(
            rebind["agent"]["recreation_policy"]["authoritative_cwd"],
            worktree_cwd
        );
        assert!(Path::new(&worktree_cwd).is_dir());
        assert_eq!(provider.rebind_calls().await, 1);
    } else {
        assert_eq!(rebind_status, StatusCode::CONFLICT, "{rebind:?}");
        assert_eq!(cleanup["status"], "deleted");
        assert!(!Path::new(&worktree_cwd).exists());
        assert_eq!(provider.rebind_calls().await, 0);
    }
}
