use super::support::test_team_model_presets;
use super::*;

pub(super) async fn build_mixed_provider_test_router_with_claude() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestAcpProvider>,
    Arc<TestClaudeProvider>,
    Arc<RuntimeSessionManager>,
) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");

    let mut registry = runtime_core::ProviderRegistry::new();
    let acp_provider = Arc::new(TestAcpProvider::default());
    let claude_provider = Arc::new(TestClaudeProvider::default());
    registry
        .register(Arc::new(TestProvider::default()))
        .expect("register codex test provider");
    registry
        .register(claude_provider.clone())
        .expect("register claude test provider");
    registry
        .register(acp_provider.clone())
        .expect("register acp test provider");
    let provider_registry = Arc::new(registry);
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), provider_registry.clone(), 512)
            .expect("build runtime"),
    );
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
            log_dir: temp_dir.path().join("process-logs"),
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
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
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
        team_policy: TeamMcpPolicy::default(),
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
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("build app");
    app.initialize().await.expect("initialize app");
    let bearer_token = "mixed-provider-token".to_string();

    let router = build_router(AppState {
        app: Arc::new(app),
        runtime: runtime.clone(),
        bearer_token: bearer_token.clone(),
        public_base_url: "http://localhost:8080".to_string(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    (
        router,
        bearer_token,
        temp_dir,
        acp_provider,
        claude_provider,
        runtime,
    )
}
