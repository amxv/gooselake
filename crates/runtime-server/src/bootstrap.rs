use std::collections::BTreeMap;
use std::sync::Arc;

use crate::config::{ResolvedAuth, RuntimeServerConfig};
use anyhow::{Context, Result};
use runtime_core::{
    EventQueueLimits, ProcessLimits, ProviderRegistry, RuntimeApp, RuntimeServices,
    RuntimeSessionManager, RuntimeStore, RuntimeTeamCommsConfig, RuntimeTeamCommsService,
    WorktreeSettings,
};
use runtime_provider_claude::{
    standalone_claude_bridge_command_path, standalone_gg_mcp_server_command_path,
    ClaudeGgMcpConfig, ClaudeProvider, ClaudeProviderConfig,
};
use runtime_provider_codex::{copy_codex_auth_file, CodexProvider, CodexProviderConfig};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
use runtime_tools::{
    ProcessManagerConfig, RuntimeProcessManager, RuntimeToolGateway, RuntimeWorktreeService,
    WorktreeServiceConfig,
};

#[derive(Clone)]
pub struct BootstrappedRuntime {
    pub app: Arc<RuntimeApp>,
    pub runtime: Arc<RuntimeSessionManager>,
    pub auth: ResolvedAuth,
    pub bind_address: String,
    pub public_base_url: String,
}

pub async fn bootstrap_runtime(config: RuntimeServerConfig) -> Result<BootstrappedRuntime> {
    config.ensure_data_dirs()?;
    let auth = config.bootstrap_auth()?;

    let sqlite_path = config.resolve_sqlite_path();
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: sqlite_path,
    }));
    store
        .initialize()
        .await
        .context("failed to initialize runtime store")?;

    let codex_home = config.resolve_provider_dir("codex").join("home");
    let codex_provider = Arc::new(CodexProvider::new(CodexProviderConfig {
        enabled: config.providers.codex.enabled,
        home_dir: codex_home.clone(),
        max_transports: config.providers.codex.max_instances,
        max_sessions_per_transport: config.providers.codex.max_sessions_per_instance,
    }));

    if config.providers.codex.enabled {
        let default_auth_source = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .map(|path| path.join(".gg").join("codex").join("auth.json"));
        if let Some(source_auth_path) = default_auth_source.as_ref() {
            if source_auth_path.exists() {
                copy_codex_auth_file(source_auth_path, codex_home.as_path())
                    .context("failed to stage codex auth.json into runtime provider home")?;
            }
        }
    }

    let (claude_bridge_command, claude_bridge_args) = resolve_claude_bridge_launch();
    let mut claude_bridge_env = BTreeMap::new();
    if let Some(override_config_dir) = std::env::var("GG_CLAUDE_BRIDGE_CLAUDE_CONFIG_DIR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        claude_bridge_env.insert("CLAUDE_CONFIG_DIR".to_string(), override_config_dir);
    }
    if let Some(override_home) = std::env::var("GG_CLAUDE_BRIDGE_HOME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        claude_bridge_env.insert("HOME".to_string(), override_home);
    }
    let mcp_gateway_url = format!(
        "{}/v1/mcp",
        config.server.public_base_url.trim_end_matches('/')
    );
    let claude_provider = Arc::new(ClaudeProvider::new(ClaudeProviderConfig {
        enabled: config.providers.claude.enabled,
        config_dir: config.resolve_provider_dir("claude").join("config"),
        bridge_command: claude_bridge_command,
        bridge_args: claude_bridge_args,
        max_bridges: config.providers.claude.max_instances,
        max_sessions_per_bridge: config.providers.claude.max_sessions_per_instance,
        request_timeout_ms: 30_000,
        default_wait_timeout_ms: 300_000,
        heartbeat_interval_ms: 10_000,
        heartbeat_failure_threshold: 3,
        gg_mcp: ClaudeGgMcpConfig {
            enabled: true,
            server_name: "gg".to_string(),
            command: std::env::var("GG_MCP_SERVER_PATH").unwrap_or_else(|_| {
                standalone_gg_mcp_server_command_path()
                    .display()
                    .to_string()
            }),
            args: Vec::new(),
            enable_process_tools: config.processes.enabled,
            gateway_url: Some(mcp_gateway_url),
            gateway_token: Some(auth.bearer_token.clone()),
        },
        bridge_env: claude_bridge_env,
    }));

    let mut provider_registry = ProviderRegistry::new();
    if config.providers.codex.enabled {
        provider_registry
            .register(codex_provider)
            .context("failed to register codex provider")?;
    }
    if config.providers.claude.enabled {
        provider_registry
            .register(claude_provider)
            .context("failed to register claude provider")?;
    }

    let provider_registry = Arc::new(provider_registry);
    let runtime = Arc::new(
        RuntimeSessionManager::new(
            store.clone(),
            provider_registry.clone(),
            config.events.live_queue_capacity,
        )
        .context("failed to initialize runtime session manager")?,
    );

    let process_manager = RuntimeProcessManager::new(
        store.clone(),
        ProcessManagerConfig {
            enabled: config.processes.enabled,
            max_concurrent: config.processes.max_concurrent,
            default_timeout_ms: config.processes.default_timeout_ms,
            max_output_bytes_per_process: config.processes.max_output_bytes_per_process,
            allow_shell: config.processes.allow_shell,
            completed_retention_ms: 600_000,
            output_event_sample_bytes: 64 * 1024,
            log_dir: config
                .resolve_data_path(&config.data.logs_dir)
                .join("processes"),
        },
    )
    .await
    .context("failed to initialize process manager")?;
    let tool_gateway = Arc::new(RuntimeToolGateway::new(process_manager.clone()));
    let team_comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 10_000,
        },
    )
    .context("failed to initialize team comms service")?;

    let worktrees = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        team_comms.clone(),
        WorktreeServiceConfig {
            enabled: config.worktrees.enabled,
            root_dir: config.resolve_worktree_root().display().to_string(),
            init_script_path: config.worktrees.init_script_path.display().to_string(),
            deletion_policy_default: config.worktrees.deletion_policy_default.clone(),
        },
    )
    .context("failed to initialize worktree service")?;

    let services = RuntimeServices {
        store: store.clone(),
        tool_gateway,
        process_manager,
        team_comms,
        worktrees,
    };

    let app = RuntimeApp::new(
        provider_registry.clone(),
        services,
        EventQueueLimits {
            live_queue_capacity: config.events.live_queue_capacity,
            critical_queue_capacity: config.events.critical_queue_capacity,
            team_queue_capacity: config.events.team_queue_capacity,
        },
        ProcessLimits {
            max_concurrent: config.processes.max_concurrent,
            default_timeout_ms: config.processes.default_timeout_ms,
            max_output_bytes_per_process: config.processes.max_output_bytes_per_process,
        },
        WorktreeSettings {
            enabled: config.worktrees.enabled,
            root_dir: config.resolve_worktree_root().display().to_string(),
            init_script_path: config.worktrees.init_script_path.display().to_string(),
            deletion_policy_default: config.worktrees.deletion_policy_default.clone(),
        },
    )
    .context("failed to build runtime app composition")?;

    let app = Arc::new(app);
    app.initialize()
        .await
        .context("runtime initialization failed")?;

    Ok(BootstrappedRuntime {
        app,
        runtime,
        auth,
        bind_address: config.server.bind_address,
        public_base_url: config.server.public_base_url,
    })
}

fn resolve_claude_bridge_launch() -> (String, Vec<String>) {
    if let Ok(command) = std::env::var("GG_CLAUDE_BRIDGE_COMMAND") {
        let trimmed = command.trim();
        if !trimmed.is_empty() {
            if let Ok(raw_args) = std::env::var("GG_CLAUDE_BRIDGE_ARGS_JSON") {
                if let Ok(args) = serde_json::from_str::<Vec<String>>(raw_args.as_str()) {
                    return (trimmed.to_string(), args);
                }
            }
            return (trimmed.to_string(), Vec::new());
        }
    }

    (
        standalone_claude_bridge_command_path()
            .display()
            .to_string(),
        Vec::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bootstrap_fails_when_all_providers_disabled() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.codex.enabled = false;
        config.providers.claude.enabled = false;

        let result = bootstrap_runtime(config).await;
        assert!(result.is_err(), "bootstrap should fail");
    }

    #[tokio::test]
    async fn bootstrap_registers_enabled_providers() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.providers.codex.enabled = true;
        config.providers.claude.enabled = false;

        let runtime = bootstrap_runtime(config).await.expect("bootstrap");
        assert_eq!(runtime.app.provider_registry.len(), 1);
    }

    #[tokio::test]
    async fn bootstrap_succeeds_when_processes_and_worktrees_are_disabled() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.processes.enabled = false;
        config.worktrees.enabled = false;

        let runtime = bootstrap_runtime(config).await.expect("bootstrap");
        assert_eq!(runtime.app.provider_registry.len(), 2);
        assert!(!runtime.app.worktree_settings.enabled);
    }

    #[tokio::test]
    async fn bootstrap_wires_worktree_deletion_policy_default() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = RuntimeServerConfig::default();
        config.data.root_dir = temp_dir.path().to_path_buf();
        config.worktrees.deletion_policy_default = "retain_on_last_claim".to_string();

        let runtime = bootstrap_runtime(config).await.expect("bootstrap");
        assert_eq!(
            runtime.app.worktree_settings.deletion_policy_default,
            "retain_on_last_claim"
        );
    }

    #[test]
    fn default_standalone_gg_mcp_path_is_branch_owned() {
        let path = standalone_gg_mcp_server_command_path();
        assert!(path.ends_with("sidecars/gg-mcp-server/bin/gg-mcp-server-dev"));
        assert!(path.exists(), "expected repo-owned gg-mcp launcher");
        assert!(
            !path.to_string_lossy().contains("CARGO_MANIFEST_DIR"),
            "default path should not bake build-time source-tree locations"
        );
    }

    #[test]
    fn default_standalone_claude_bridge_path_is_branch_owned() {
        let (command, args) = resolve_claude_bridge_launch();
        assert!(command.ends_with("sidecars/claude-bridge/bin/claude-bridge-dev"));
        assert!(
            std::path::Path::new(command.as_str()).exists(),
            "expected repo-owned claude launcher"
        );
        assert!(args.is_empty());
        assert!(!command.ends_with("src/main.ts"));
        assert!(!command.contains("CARGO_MANIFEST_DIR"));
    }
}
