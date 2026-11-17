use super::workspace_worktree_routes::{call, git};
use super::*;

pub(super) async fn fixture() -> (
    AppState,
    tempfile::TempDir,
    Arc<TestProvider>,
    runtime_core::WorkspaceRecord,
    runtime_core::WorkspaceAgentRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.unwrap();
    let provider = Arc::new(TestProvider::default());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let registry = Arc::new(registry);
    let runtime =
        Arc::new(RuntimeSessionManager::new(store.clone(), registry.clone(), 128).unwrap());
    let processes = RuntimeProcessManager::new_with_runtime(
        store.clone(),
        runtime.clone(),
        ProcessManagerConfig {
            enabled: false,
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
            allow_shell: false,
            completed_retention_ms: 600_000,
            output_event_sample_bytes: 8 * 1024,
            log_dir: temp.path().join("process-logs"),
        },
    )
    .await
    .unwrap();
    let comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .unwrap();
    let root = temp.path().join("worktrees");
    let settings = runtime_core::WorktreeSettings {
        enabled: true,
        root_dir: root.to_string_lossy().into_owned(),
        init_script_path: ".agents/gg/worktree-init.sh".into(),
        deletion_policy_default: "delete_on_last_claim".into(),
    };
    let worktrees = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        comms.clone(),
        WorktreeServiceConfig {
            enabled: true,
            root_dir: settings.root_dir.clone(),
            init_script_path: settings.init_script_path.clone(),
            deletion_policy_default: settings.deletion_policy_default.clone(),
        },
    )
    .unwrap();
    let gateway = Arc::new(RuntimeToolGateway::new(RuntimeToolGatewayDeps {
        process_manager: processes.clone(),
        runtime: Some(runtime.clone()),
        team_comms: comms.clone(),
        worktrees: worktrees.clone(),
        team_policy: TeamMcpPolicy::default(),
        team_model_presets: test_team_model_presets(),
    }));
    let app = RuntimeApp::new(
        registry,
        runtime_core::RuntimeServices {
            store: store.clone(),
            tool_gateway: gateway,
            process_manager: processes,
            team_comms: comms,
            worktrees,
        },
        runtime_core::EventQueueLimits {
            live_queue_capacity: 128,
            critical_queue_capacity: 128,
            team_queue_capacity: 128,
        },
        runtime_core::ProcessLimits {
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
        },
        settings,
    )
    .unwrap();
    app.initialize().await.unwrap();
    let state = AppState {
        app: Arc::new(app),
        runtime,
        bearer_token: "rebind-receipt-test-token".into(),
        public_base_url: "http://localhost:8080".into(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    };
    let router = build_router(state.clone());
    let repo = temp.path().join("repo");
    fs::create_dir(&repo).unwrap();
    git(&repo, &["init"]);
    git(&repo, &["config", "user.email", "runtime@example.invalid"]);
    git(&repo, &["config", "user.name", "Runtime Test"]);
    fs::write(repo.join("README.md"), "fixture\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "init"]);
    let (status, workspace) = call(
        &router,
        &state.bearer_token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":repo})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{workspace}");
    let workspace: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_value(workspace).unwrap();
    let workspace = workspace.workspace;
    let (status, agent) = call(
        &router,
        &state.bearer_token,
        "POST",
        &format!("/v2/workspaces/{}/agents", workspace.workspace_id),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent}");
    let agent: runtime_core::WorkspaceAgentRecord = serde_json::from_value(agent).unwrap();
    finish_test_onboarding(&state.runtime, &workspace.workspace_id, &agent.agent_id).await;
    let linked = root.join("linked");
    fs::create_dir_all(&root).unwrap();
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "gg/linked",
            linked.to_str().unwrap(),
        ],
    );
    store
        .upsert_managed_worktree(&runtime_core::ManagedWorktreeRecord {
            id: "wt_receipt".into(),
            repo_root: repo.to_string_lossy().into_owned(),
            worktree_root: root.to_string_lossy().into_owned(),
            worktree_cwd: linked.to_string_lossy().into_owned(),
            branch_name: "gg/linked".into(),
            worktree_name: "linked".into(),
            unified_workspace_path: "receipt-test".into(),
            deletion_policy: "delete_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 1,
            updated_at: 1,
        })
        .unwrap();
    (state, temp, provider, workspace, agent)
}

async fn rebind(
    router: &Router,
    token: Option<&str>,
    uri: &str,
    body: &serde_json::Value,
    key: Option<&str>,
    etag: Option<&str>,
) -> (StatusCode, HeaderMap, serde_json::Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    if let Some(etag) = etag {
        request = request.header(header::IF_MATCH, etag);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, headers, body)
}

// Compare every row, not only counts: replay must not update a receipt, route,
// claim, recovery obligation, hook/notice, or canonical operation in place.
fn durable_snapshot(db: &rusqlite::Connection) -> Vec<(String, Vec<Vec<String>>)> {
    let tables = db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    tables
        .into_iter()
        .map(|name| {
            let mut statement = db
                .prepare(&format!("SELECT * FROM \"{}\"", name.replace('"', "\"\"")))
                .unwrap();
            let columns = statement.column_count();
            let mut rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| row.get_ref(index).map(|value| format!("{value:?}")))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            rows.sort();
            (name, rows)
        })
        .collect()
}

async fn completed_receipt_during_unknown_native_operation(edit: bool) {
    let (state, temp, provider, workspace, agent) = fixture().await;
    let router = build_router(state.clone());
    let token = Some(state.bearer_token.as_str());
    let uri = format!(
        "/v2/workspaces/{}/agents/{}/worktree",
        workspace.workspace_id, agent.agent_id
    );
    let (status, _, entered) = rebind(
        &router,
        token,
        &uri,
        &serde_json::json!({"worktree_id":"wt_receipt","expected_revision":agent.revision}),
        Some("enter-linked"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{entered}");
    let linked = temp.path().join("worktrees/linked");
    let dirty = linked.join("keep-until-reviewed.txt");
    fs::write(&dirty, "uncommitted fixture data").unwrap();
    let input = serde_json::json!({
        "worktree_id":null,
        "expected_revision":entered["agent"]["revision"],
        "cleanup_previous_worktree":true
    });
    let etag = format!("\"{}\"", input["expected_revision"]);
    let key = Some("saved-rebind");
    let (status, original_headers, original) =
        rebind(&router, token, &uri, &input, key, Some(&etag)).await;
    assert_eq!(status, StatusCode::OK, "{original}");
    assert_eq!(original["operation"]["phase"], "completed");
    assert_eq!(original["operation"]["previous_cleanup_status"], "pending");
    assert_eq!(
        original["operation"]["previous_cleanup_diagnostic"],
        "skipped_dirty_worktree"
    );
    assert_eq!(provider.rebind_calls().await, 2);
    // A replay must not retry cleanup even after the dirty fixture becomes
    // eligible. Only the separate explicit cleanup endpoint may do that.
    fs::remove_file(dirty).unwrap();

    let blocker = if edit {
        provider.set_edit_mode(2);
        let (status, turn) = call(
            &router,
            &state.bearer_token,
            "POST",
            &format!("/v2/agents/{}/turns", agent.agent_id),
            Some(serde_json::json!({"input":[{"type":"text","text":"historical request"}]})),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{turn}");
        timeout(Duration::from_secs(5), async {
            loop {
                let session = state.runtime.get_session(&agent.agent_id).await.unwrap();
                if session.status == "ready" && session.active_turn_id.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (status, unknown) = call(
            &router,
            &state.bearer_token,
            "POST",
            &format!(
                "/v2/agents/{}/turns/{}/edit-rerun",
                agent.agent_id,
                turn["turn_id"].as_str().unwrap()
            ),
            Some(serde_json::json!({"edited_snapshot":{
                "prompt_text":"later unknown native edit","image_refs":[],"invocation":null
            }})),
            Some("later-unknown-edit"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{unknown}");
        assert_eq!(provider.edit_calls(), 1);
        assert!(state
            .app
            .services
            .store
            .unresolved_session_edit_rerun(&agent.agent_id)
            .unwrap());
        "edit-and-rerun remains unresolved"
    } else {
        provider.set_compaction_mode(3);
        let (status, unknown) = call(
            &router,
            &state.bearer_token,
            "POST",
            &format!("/v2/agents/{}/compact", agent.agent_id),
            None,
            Some("later-unknown-compaction"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{unknown}");
        assert_eq!(provider.compaction_calls(), 1);
        assert!(state
            .app
            .services
            .store
            .unresolved_session_compaction(&agent.agent_id)
            .unwrap());
        "unresolved provider compaction"
    };
    let database_path = temp.path().join("runtime.sqlite3");
    let db = rusqlite::Connection::open(&database_path).unwrap();
    let before = durable_snapshot(&db);
    let inventory_before = std::process::Command::new("git")
        .args([
            "-C",
            &workspace.canonical_root,
            "worktree",
            "list",
            "--porcelain",
        ])
        .output()
        .unwrap();
    assert!(inventory_before.status.success());

    // This is a fresh manager over reopened disposable SQLite, without any
    // provider registration or startup recovery. It is not an OS restart.
    let reopened_store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig { database_path }));
    let empty_registry = Arc::new(runtime_core::ProviderRegistry::new());
    let reopened =
        Arc::new(RuntimeSessionManager::new(reopened_store, empty_registry.clone(), 128).unwrap());
    let mut reopened_app = (*state.app).clone();
    reopened_app.provider_registry = empty_registry;
    let reopened_state = AppState {
        app: Arc::new(reopened_app.clone()),
        runtime: reopened,
        ..state.clone()
    };
    let reopened_router = build_router(reopened_state.clone());

    for active_router in [&router, &reopened_router] {
        for fresh_key in [Some("new-rebind"), None] {
            let fresh = serde_json::json!({"worktree_id":"wt_receipt","expected_revision":original["agent"]["revision"]});
            let (status, headers, denied) =
                rebind(active_router, token, &uri, &fresh, fresh_key, None).await;
            assert_eq!(status, StatusCode::CONFLICT, "{denied}");
            assert!(denied["error"]["message"]
                .as_str()
                .unwrap()
                .contains(blocker));
            assert!(!headers.contains_key("x-operation-id"));
        }
        let (status, headers, replay) =
            rebind(active_router, token, &uri, &input, key, Some(&etag)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "completed rebind receipt must remain readable: {replay}"
        );
        assert_eq!(replay["operation"], original["operation"]);
        assert_eq!(
            headers["x-operation-id"],
            original_headers["x-operation-id"]
        );
        assert_eq!(headers[header::ETAG], original_headers[header::ETAG]);
        assert_eq!(
            replay["agent"]["recreation_policy"]["authoritative_cwd"],
            workspace.canonical_root
        );
        assert!(replay.get("newly_completed").is_none());

        for (field, value) in [
            ("worktree_id", serde_json::json!("wt_receipt")),
            ("expected_revision", serde_json::json!(999)),
            ("cleanup_previous_worktree", serde_json::json!(false)),
        ] {
            let mut changed = input.clone();
            changed[field] = value;
            let (status, headers, denied) =
                rebind(active_router, token, &uri, &changed, key, None).await;
            assert_eq!(status, StatusCode::CONFLICT, "{denied}");
            assert!(denied["error"]["message"]
                .as_str()
                .unwrap()
                .contains("different input"));
            assert!(!headers.contains_key("x-operation-id"));
        }
        let scoped = runtime_core::mint_scoped_mcp_token(&state.bearer_token, &agent.agent_id);
        for credential in [None, Some("wrong-operator-token"), Some(scoped.as_str())] {
            let (status, headers, denied) =
                rebind(active_router, credential, &uri, &input, key, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{denied}");
            assert!(!headers.contains_key("x-operation-id"));
        }
        for wrong_uri in [
            format!("/v2/workspaces/foreign/agents/{}/worktree", agent.agent_id),
            format!(
                "/v2/workspaces/{}/agents/foreign/worktree",
                workspace.workspace_id
            ),
        ] {
            let (status, headers, denied) =
                rebind(active_router, token, &wrong_uri, &input, key, None).await;
            assert!(status.is_client_error(), "{denied}");
            assert!(denied.get("operation").is_none());
            assert!(!headers.contains_key("x-operation-id"));
        }
        let (status, _, denied) =
            rebind(active_router, token, &uri, &input, key, Some("\"999\"")).await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{denied}");
        let (status, _, denied) =
            rebind(active_router, token, &uri, &input, Some("   "), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{denied}");
    }
    reopened_app.worktree_settings.enabled = false;
    let disabled_router = build_router(AppState {
        app: Arc::new(reopened_app),
        ..reopened_state
    });
    let (status, headers, denied) =
        rebind(&disabled_router, token, &uri, &input, key, Some(&etag)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{denied}");
    assert_eq!(denied["error"]["code"], "unsupported_capability");
    assert!(denied["error"]["message"]
        .as_str()
        .unwrap()
        .contains("managed worktrees are disabled"));
    assert!(!headers.contains_key("x-operation-id"));

    assert_eq!(
        durable_snapshot(&db),
        before,
        "receipt inspection and rejected requests must not change any durable row"
    );
    assert_eq!(
        provider.rebind_calls().await,
        2,
        "saved provider work must never repeat"
    );
    assert_eq!(provider.edit_calls(), usize::from(edit));
    assert_eq!(provider.compaction_calls(), usize::from(!edit));
    assert!(
        linked.is_dir(),
        "saved receipt must not retry pending Git cleanup"
    );
    let inventory_after = std::process::Command::new("git")
        .args([
            "-C",
            &workspace.canonical_root,
            "worktree",
            "list",
            "--porcelain",
        ])
        .output()
        .unwrap();
    assert!(inventory_after.status.success());
    assert_eq!(inventory_before.stdout, inventory_after.stdout);
}

#[tokio::test]
async fn completed_rebind_receipt_replays_during_later_unknown_edit() {
    completed_receipt_during_unknown_native_operation(true).await;
}

#[tokio::test]
async fn completed_rebind_receipt_replays_during_later_unknown_compaction() {
    completed_receipt_during_unknown_native_operation(false).await;
}
