use super::support::test_team_model_presets;
use super::*;

/// Rebuild only the HTTP test composition over healthy disposable history.
/// Keep the SAME provider registry: original fake native attachments and
/// counters survive, while runtime/team/gateway caches are newly hydrated.
/// This does not invoke RuntimeSessionManager::recover_startup.
pub(super) async fn reopen_test_router_with_services(
    previous: &Arc<RuntimeApp>,
    fixture_root: &Path,
    bearer_token: &str,
    team_policy: TeamMcpPolicy,
) -> (Router, Arc<RuntimeSessionManager>, Arc<RuntimeApp>) {
    let store: Arc<dyn RuntimeStore> = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: fixture_root.join("runtime.sqlite3"),
    }));
    assemble_test_router(
        fixture_root,
        store,
        previous.provider_registry.clone(),
        bearer_token,
        team_policy,
    )
    .await
}

pub(super) async fn assemble_test_router(
    fixture_root: &Path,
    store: Arc<dyn RuntimeStore>,
    provider_registry: Arc<runtime_core::ProviderRegistry>,
    bearer_token: &str,
    team_policy: TeamMcpPolicy,
) -> (Router, Arc<RuntimeSessionManager>, Arc<RuntimeApp>) {
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), provider_registry.clone(), 512)
            .expect("build runtime"),
    );
    runtime
        .install_pre_add_hook_executor(Arc::new(runtime_tools::NativeTeamHookExecutor::new(
            store.clone(),
        )))
        .await;
    let process_manager = RuntimeProcessManager::new_with_runtime(
        store.clone(),
        runtime.clone(),
        ProcessManagerConfig {
            enabled: true,
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
            allow_shell: false,
            completed_retention_ms: 600_000,
            output_event_sample_bytes: 8 * 1024,
            log_dir: fixture_root.join("process-logs"),
        },
    )
    .await
    .expect("process manager");
    let team_comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("team comms");

    let worktrees = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        team_comms.clone(),
        WorktreeServiceConfig {
            enabled: true,
            root_dir: fixture_root.join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("worktree service");
    let tool_gateway = Arc::new(RuntimeToolGateway::new(RuntimeToolGatewayDeps {
        process_manager: process_manager.clone(),
        runtime: Some(runtime.clone()),
        team_comms: team_comms.clone(),
        worktrees: worktrees.clone(),
        team_policy,
        team_model_presets: test_team_model_presets(),
    }));

    let app = runtime_core::RuntimeApp::new(
        provider_registry.clone(),
        runtime_core::RuntimeServices {
            store: store.clone(),
            tool_gateway,
            process_manager,
            team_comms,
            worktrees,
        },
        runtime_core::EventQueueLimits {
            live_queue_capacity: 512,
            critical_queue_capacity: 512,
            team_queue_capacity: 512,
        },
        runtime_core::ProcessLimits {
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
        },
        runtime_core::WorktreeSettings {
            enabled: true,
            root_dir: fixture_root.join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("build app");
    app.initialize().await.expect("initialize app");
    let bearer_token = bearer_token.to_string();

    let app = Arc::new(app);
    let router = build_router(AppState {
        app: app.clone(),
        runtime: runtime.clone(),
        bearer_token: bearer_token.clone(),
        public_base_url: "http://localhost:8080".to_string(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    (router, runtime, app)
}
