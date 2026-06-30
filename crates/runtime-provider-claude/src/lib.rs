use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCloseSessionRequest, ProviderCreateSessionRequest, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderSession, ProviderTurnAck, ProviderTurnResult,
    ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError, RuntimeProvider,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock};

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_WAIT_TIMEOUT_MS: u32 = 300_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 10_000;
const DEFAULT_HEARTBEAT_FAILURE_THRESHOLD: u64 = 3;
const DEFAULT_MAX_BRIDGE_PROCESSES: usize = 4;
const DEFAULT_SESSIONS_PER_BRIDGE_SOFT_LIMIT: usize = 4;
const CLAUDE_BRIDGE_STDIN_QUEUE_CAPACITY: usize = 256;
const CLAUDE_BRIDGE_STDIN_FLUSH_BATCH_MAX: usize = 32;
const CLAUDE_STDOUT_WORKER_LANE_COUNT: usize = 16;
const CLAUDE_STDOUT_WORKER_QUEUE_CAPACITY: usize = 128;
const GG_MCP_MISSING_BAD_REQUEST: &str = "Missing ggMcpServer config for SDK mode session";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaudeAuthMode {
    HostMachine,
    RuntimeManaged,
}

impl ClaudeAuthMode {
    fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("runtime_managed") | Some("runtime-managed") | Some("runtime") => {
                Self::RuntimeManaged
            }
            _ => Self::HostMachine,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::HostMachine => "host_machine",
            Self::RuntimeManaged => "runtime_managed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeGgMcpConfig {
    pub enabled: bool,
    pub server_name: String,
    pub command: String,
    pub args: Vec<String>,
    pub enable_process_tools: bool,
    pub gateway_url: Option<String>,
    pub gateway_token: Option<String>,
}

impl Default for ClaudeGgMcpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            server_name: "gg".to_string(),
            command: default_gg_mcp_server_command(),
            args: Vec::new(),
            enable_process_tools: true,
            gateway_url: None,
            gateway_token: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeProviderConfig {
    pub enabled: bool,
    pub config_dir: PathBuf,
    pub bridge_command: String,
    pub bridge_args: Vec<String>,
    pub max_bridges: usize,
    pub max_sessions_per_bridge: usize,
    pub request_timeout_ms: u64,
    pub default_wait_timeout_ms: u32,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_failure_threshold: u64,
    pub gg_mcp: ClaudeGgMcpConfig,
    pub bridge_env: BTreeMap<String, String>,
}

impl Default for ClaudeProviderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            config_dir: PathBuf::from(".gg-runtime/providers/claude/config"),
            bridge_command: default_bridge_command(),
            bridge_args: default_bridge_args(),
            max_bridges: DEFAULT_MAX_BRIDGE_PROCESSES,
            max_sessions_per_bridge: DEFAULT_SESSIONS_PER_BRIDGE_SOFT_LIMIT,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            default_wait_timeout_ms: DEFAULT_WAIT_TIMEOUT_MS,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            heartbeat_failure_threshold: DEFAULT_HEARTBEAT_FAILURE_THRESHOLD,
            gg_mcp: ClaudeGgMcpConfig::default(),
            bridge_env: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClaudeProvider {
    inner: Arc<ClaudeProviderInner>,
}

#[derive(Debug)]
struct ClaudeProviderInner {
    config: ClaudeProviderConfig,
    next_bridge_instance_id: AtomicU64,
    next_bridge_selection: AtomicU64,
    next_request_id: AtomicU64,
    bridges: RwLock<BTreeMap<u64, Arc<ClaudeBridgeHandle>>>,
    bridge_allocation_lock: Mutex<()>,
    sessions: RwLock<BTreeMap<String, Arc<ClaudeSessionHandle>>>,
    sessions_by_bridge_key: RwLock<BTreeMap<String, Arc<ClaudeSessionHandle>>>,
}

#[derive(Debug)]
struct ClaudeSessionHandle {
    runtime_session_id: String,
    bridge_session_id: String,
    provider_session_ref: RwLock<String>,
    canonical_provider_session_ref: RwLock<Option<String>>,
    bridge: Arc<ClaudeBridgeHandle>,
    active_turn_id: RwLock<Option<String>>,
    bridge_turn_by_runtime_turn: Mutex<BTreeMap<String, String>>,
    runtime_turn_by_bridge_turn: Mutex<BTreeMap<String, String>>,
    completed_turns: Mutex<BTreeMap<String, ProviderTurnResult>>,
}

#[derive(Debug)]
struct ClaudeBridgeHandle {
    instance_id: u64,
    process: Mutex<ClaudeBridgeProcessState>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<RpcResponseResult>>>,
    writer_tx: mpsc::Sender<OutboundJsonLine>,
    writer_shutdown: Notify,
    closed: AtomicBool,
    shutdown_requested: AtomicBool,
    last_event_seq_by_session: Mutex<BTreeMap<String, u64>>,
}

#[derive(Debug)]
struct ClaudeBridgeProcessState {
    child: Child,
    closed: bool,
}

type OutboundJsonLine = Vec<u8>;
type RpcResponseResult = Result<Value, RuntimeError>;

#[derive(Debug)]
struct ClaudeBridgeEventWorkItem {
    payload: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaudeConfigResolutionSource {
    EnvOverride,
    GgFallback,
    UpstreamDefault,
}

impl ClaudeConfigResolutionSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::EnvOverride => "env_override",
            Self::GgFallback => "gg_fallback",
            Self::UpstreamDefault => "upstream_default",
        }
    }
}

#[derive(Debug, Clone)]
struct ClaudeAuthPathsResolution {
    credentials_path: PathBuf,
    config_path: PathBuf,
    config_dir: Option<PathBuf>,
    config_source: ClaudeConfigResolutionSource,
}

#[derive(Debug)]
struct ClaudeAuthImportPayload {
    credentials_json: Option<Value>,
    config_json: Option<Value>,
}

#[derive(Debug, Clone)]
struct ClaudeBridgeAuthEnvironment {
    home_dir: PathBuf,
    claude_config_dir: Option<PathBuf>,
    auth_paths: ClaudeAuthPathsResolution,
}

impl ClaudeProvider {
    pub fn new(config: ClaudeProviderConfig) -> Self {
        let max_bridges = config.max_bridges.max(1);
        let max_sessions_per_bridge = config.max_sessions_per_bridge.max(1);
        Self {
            inner: Arc::new(ClaudeProviderInner {
                config: ClaudeProviderConfig {
                    max_bridges,
                    max_sessions_per_bridge,
                    ..config
                },
                next_bridge_instance_id: AtomicU64::new(1),
                next_bridge_selection: AtomicU64::new(0),
                next_request_id: AtomicU64::new(1),
                bridges: RwLock::new(BTreeMap::new()),
                bridge_allocation_lock: Mutex::new(()),
                sessions: RwLock::new(BTreeMap::new()),
                sessions_by_bridge_key: RwLock::new(BTreeMap::new()),
            }),
        }
    }

    pub fn config_dir(&self) -> &Path {
        self.inner.config.config_dir.as_path()
    }

    fn build_gg_mcp_server_session_config(&self, runtime_session_id: &str) -> Value {
        let mut env = serde_json::Map::new();
        env.insert(
            "GG_MCP_ENABLE_PROCESS_TOOLS".to_string(),
            Value::String(if self.inner.config.gg_mcp.enable_process_tools {
                "1".to_string()
            } else {
                "0".to_string()
            }),
        );
        env.insert(
            "GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID".to_string(),
            Value::String("1".to_string()),
        );
        env.insert(
            "GG_MCP_CALLER_AGENT_ID".to_string(),
            Value::String(runtime_session_id.to_string()),
        );
        if let Some(gateway_url) = self
            .inner
            .config
            .gg_mcp
            .gateway_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "GG_MCP_GATEWAY_URL".to_string(),
                Value::String(gateway_url.to_string()),
            );
        }
        if let Some(gateway_token) = self
            .inner
            .config
            .gg_mcp
            .gateway_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.insert(
                "GG_MCP_GATEWAY_TOKEN".to_string(),
                Value::String(gateway_token.to_string()),
            );
        }
        if let Some(home) = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.insert(
                "HOME".to_string(),
                Value::String(home.display().to_string()),
            );
        }
        if let Some(cargo_home) = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.insert(
                "CARGO_HOME".to_string(),
                Value::String(cargo_home.display().to_string()),
            );
        }
        if let Some(rustup_home) = std::env::var_os("RUSTUP_HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.insert(
                "RUSTUP_HOME".to_string(),
                Value::String(rustup_home.display().to_string()),
            );
        }

        let config = serde_json::json!({
            "serverName": self.inner.config.gg_mcp.server_name.clone(),
            "callerAgentId": runtime_session_id,
            "command": self.inner.config.gg_mcp.command.clone(),
            "args": self.inner.config.gg_mcp.args.clone(),
            "env": env,
        });
        if claude_smoke_debug_enabled() {
            let mut redacted = config.clone();
            if let Some(token) = redacted
                .get_mut("env")
                .and_then(Value::as_object_mut)
                .and_then(|env| env.get_mut("GG_MCP_GATEWAY_TOKEN"))
            {
                *token = Value::String("<redacted>".to_string());
            }
            eprintln!(
                "[claude-provider] ggMcpServer session config for {}: {}",
                runtime_session_id, redacted
            );
        }
        config
    }

    fn runtime_home_dir(&self) -> PathBuf {
        self.inner
            .config
            .config_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.inner.config.config_dir.clone())
            .join("home")
    }

    fn process_home_dir() -> Option<PathBuf> {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
    }

    fn auth_mode(&self) -> ClaudeAuthMode {
        let env_mode = std::env::var("GG_CLAUDE_AUTH_MODE").ok();
        let configured = self
            .inner
            .config
            .bridge_env
            .get("GG_CLAUDE_AUTH_MODE")
            .map(String::as_str)
            .or(env_mode.as_deref());
        ClaudeAuthMode::parse(configured)
    }

    fn bridge_home_dir(&self) -> PathBuf {
        if let Some(home_override) = self
            .inner
            .config
            .bridge_env
            .get("HOME")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
        {
            return home_override;
        }

        match self.auth_mode() {
            ClaudeAuthMode::HostMachine => {
                Self::process_home_dir().unwrap_or_else(|| self.runtime_home_dir())
            }
            ClaudeAuthMode::RuntimeManaged => self.runtime_home_dir(),
        }
    }

    fn bridge_claude_config_dir_override(&self) -> Option<PathBuf> {
        if let Some(config_override) = self
            .inner
            .config
            .bridge_env
            .get("CLAUDE_CONFIG_DIR")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
        {
            return Some(config_override);
        }

        match self.auth_mode() {
            ClaudeAuthMode::HostMachine => std::env::var_os("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .filter(|path| !path.as_os_str().is_empty()),
            ClaudeAuthMode::RuntimeManaged => Some(self.inner.config.config_dir.clone()),
        }
    }

    fn bridge_auth_overrides_active(&self) -> bool {
        self.inner
            .config
            .bridge_env
            .get("HOME")
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
            || self
                .inner
                .config
                .bridge_env
                .get("CLAUDE_CONFIG_DIR")
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
    }

    fn resolve_bridge_auth_environment(&self) -> Result<ClaudeBridgeAuthEnvironment, RuntimeError> {
        let home_dir = self.bridge_home_dir();
        let claude_config_dir = self.bridge_claude_config_dir_override();
        let auth_paths =
            resolve_claude_auth_paths(claude_config_dir.clone(), Some(home_dir.clone()))
                .ok_or_else(|| {
                    RuntimeError::InvalidState(
                        "unable to resolve Claude auth/config paths".to_string(),
                    )
                })?;
        Ok(ClaudeBridgeAuthEnvironment {
            home_dir,
            claude_config_dir: auth_paths.config_dir.clone(),
            auth_paths,
        })
    }

    fn validate_bridge_auth_environment(
        &self,
        env: &ClaudeBridgeAuthEnvironment,
    ) -> Result<(), RuntimeError> {
        let runtime_api_key_present = std::fs::read_to_string(self.api_key_path())
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        let bridge_api_key_present = self
            .inner
            .config
            .bridge_env
            .get("ANTHROPIC_API_KEY")
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        let process_api_key_present = std::env::var("ANTHROPIC_API_KEY")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        if runtime_api_key_present || bridge_api_key_present || process_api_key_present {
            return Ok(());
        }

        let credentials_path = env.auth_paths.credentials_path.as_path();
        if !credentials_path.exists() {
            return Err(RuntimeError::NotFound(format!(
                "Claude OAuth credentials file missing at {}. Run `claude login`, or import credentials via /v1/providers/claude/auth/import-json or /v1/providers/claude/auth/import-file.",
                credentials_path.display()
            )));
        }

        if read_claude_oauth_access_token(credentials_path).is_none() {
            return Err(RuntimeError::InvalidState(format!(
                "Claude OAuth credentials file at {} is missing a non-empty claudeAiOauth.accessToken.",
                credentials_path.display()
            )));
        }

        let config_path = env.auth_paths.config_path.as_path();
        if !config_path.exists() {
            return Err(RuntimeError::NotFound(format!(
                "Claude config file missing at {}. Ensure canonical Claude config is present (for example ~/.gg/claude/.claude.json or ~/.claude.json).",
                config_path.display()
            )));
        }

        let config_content = std::fs::read_to_string(config_path).map_err(|error| {
            RuntimeError::Io(format!(
                "failed reading Claude config file {}: {error}",
                config_path.display()
            ))
        })?;
        let parsed = serde_json::from_str::<Value>(config_content.as_str()).map_err(|error| {
            RuntimeError::InvalidState(format!(
                "Claude config file {} is not valid JSON: {error}",
                config_path.display()
            ))
        })?;
        if !parsed.is_object() {
            return Err(RuntimeError::InvalidState(format!(
                "Claude config file {} must contain a JSON object",
                config_path.display()
            )));
        }

        Ok(())
    }

    fn runtime_claude_auth_paths(&self) -> ClaudeAuthPathsResolution {
        resolve_claude_auth_paths(
            Some(self.inner.config.config_dir.clone()),
            Some(self.runtime_home_dir()),
        )
        .expect("runtime Claude auth paths should always resolve with runtime home")
    }

    fn claude_credentials_path(&self) -> PathBuf {
        self.runtime_claude_auth_paths().credentials_path
    }

    fn claude_config_path(&self) -> PathBuf {
        self.runtime_claude_auth_paths().config_path
    }

    fn api_key_path(&self) -> PathBuf {
        self.inner.config.config_dir.join("api_key")
    }

    async fn ensure_provider_enabled(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap(
                "claude provider disabled".to_string(),
            ));
        }
        let auth_paths = self.runtime_claude_auth_paths();
        if let Some(config_dir) = auth_paths.config_dir.as_ref() {
            tokio::fs::create_dir_all(config_dir)
                .await
                .map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to create Claude config dir {}: {error}",
                        config_dir.display()
                    ))
                })?;
        }
        if let Some(credentials_dir) = auth_paths.credentials_path.parent() {
            tokio::fs::create_dir_all(credentials_dir)
                .await
                .map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to create Claude credentials dir {}: {error}",
                        credentials_dir.display()
                    ))
                })?;
        }
        tokio::fs::create_dir_all(&self.inner.config.config_dir)
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create Claude config dir {}: {error}",
                    self.inner.config.config_dir.display()
                ))
            })?;
        Ok(())
    }

    async fn read_api_key(&self) -> Result<Option<String>, RuntimeError> {
        let path = self.api_key_path();
        match tokio::fs::read_to_string(path.as_path()).await {
            Ok(content) => {
                let trimmed = content.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed.to_string()))
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(RuntimeError::Io(format!(
                "failed reading Claude API key file {}: {error}",
                path.display()
            ))),
        }
    }

    async fn write_api_key(&self, api_key: &str) -> Result<(), RuntimeError> {
        tokio::fs::create_dir_all(&self.inner.config.config_dir)
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create Claude config dir {}: {error}",
                    self.inner.config.config_dir.display()
                ))
            })?;
        let path = self.api_key_path();
        tokio::fs::write(path.as_path(), api_key.as_bytes())
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed writing Claude API key file {}: {error}",
                    path.display()
                ))
            })?;
        set_permissions_if_unix(path.as_path(), 0o600)?;
        Ok(())
    }

    async fn write_claude_json_file(
        &self,
        path: &Path,
        payload: &Value,
    ) -> Result<(), RuntimeError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                RuntimeError::Io(format!(
                    "failed creating Claude auth parent dir {}: {error}",
                    parent.display()
                ))
            })?;
            set_permissions_if_unix(parent, 0o700)?;
        }
        let encoded = serde_json::to_vec_pretty(payload).map_err(|error| {
            RuntimeError::InvalidState(format!(
                "invalid Claude auth JSON payload for {}: {error}",
                path.display()
            ))
        })?;
        tokio::fs::write(path, encoded).await.map_err(|error| {
            RuntimeError::Io(format!(
                "failed writing Claude auth file {}: {error}",
                path.display()
            ))
        })?;
        set_permissions_if_unix(path, 0o600)?;
        Ok(())
    }

    async fn write_oauth_credentials_json(
        &self,
        credentials_json: &Value,
    ) -> Result<(), RuntimeError> {
        let credentials_path = self.claude_credentials_path();
        self.write_claude_json_file(credentials_path.as_path(), credentials_json)
            .await
    }

    async fn write_claude_config_json(&self, config_json: &Value) -> Result<(), RuntimeError> {
        let config_path = self.claude_config_path();
        self.write_claude_json_file(config_path.as_path(), config_json)
            .await
    }

    async fn recycle_after_live_auth_change(&self) {
        let sessions = {
            let mut sessions = self.inner.sessions.write().await;
            let mut by_bridge = self.inner.sessions_by_bridge_key.write().await;
            by_bridge.clear();
            std::mem::take(&mut *sessions)
        };
        drop(sessions);

        let bridges = {
            let mut bridges = self.inner.bridges.write().await;
            std::mem::take(&mut *bridges)
                .into_values()
                .collect::<Vec<_>>()
        };
        for bridge in bridges {
            fail_bridge(
                &self.inner,
                &bridge,
                "Claude provider auth-change recycle".to_string(),
            )
            .await;
        }
    }

    async fn provider_auth_status_internal(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let auth_mode = self.auth_mode();
        let runtime_auth_paths = self.runtime_claude_auth_paths();
        let runtime_credentials_present =
            has_claude_oauth_access_token(runtime_auth_paths.credentials_path.as_path());
        let runtime_config_json_present = runtime_auth_paths.config_path.exists();
        let runtime_oauth_ready = runtime_credentials_present && runtime_config_json_present;
        let bridge_auth_env = self.resolve_bridge_auth_environment().ok();
        let bridge_credentials_present = bridge_auth_env
            .as_ref()
            .map(|env| has_claude_oauth_access_token(env.auth_paths.credentials_path.as_path()))
            .unwrap_or(false);
        let bridge_config_json_present = bridge_auth_env
            .as_ref()
            .map(|env| env.auth_paths.config_path.exists())
            .unwrap_or(false);
        let bridge_oauth_ready = bridge_credentials_present && bridge_config_json_present;
        let api_key_present = self.read_api_key().await?.is_some();
        let authenticated = match auth_mode {
            ClaudeAuthMode::HostMachine => {
                runtime_oauth_ready || bridge_oauth_ready || api_key_present
            }
            ClaudeAuthMode::RuntimeManaged => {
                runtime_oauth_ready
                    || (self.bridge_auth_overrides_active() && bridge_oauth_ready)
                    || api_key_present
            }
        };
        let oauth_mode_ready = match auth_mode {
            ClaudeAuthMode::HostMachine => runtime_oauth_ready || bridge_oauth_ready,
            ClaudeAuthMode::RuntimeManaged => {
                runtime_oauth_ready || (self.bridge_auth_overrides_active() && bridge_oauth_ready)
            }
        };
        Ok(ProviderAuthStatus {
            authenticated,
            mode: if oauth_mode_ready {
                Some("claude_code_oauth".to_string())
            } else if api_key_present {
                Some("api_key".to_string())
            } else {
                None
            },
            detail: Some(format!(
                "auth_mode={} runtime_credentials_path={} runtime_config_path={} runtime_config_source={} runtime_credentials_present={} runtime_config_present={} runtime_oauth_ready={} bridge_credentials_path={} bridge_config_path={} bridge_config_source={} bridge_credentials_present={} bridge_config_present={} bridge_oauth_ready={} bridge_override_active={} api_key_present={}",
                auth_mode.as_str(),
                runtime_auth_paths.credentials_path.display(),
                runtime_auth_paths.config_path.display(),
                runtime_auth_paths.config_source.as_str(),
                runtime_credentials_present,
                runtime_config_json_present,
                runtime_oauth_ready,
                bridge_auth_env
                    .as_ref()
                    .map(|env| env.auth_paths.credentials_path.display().to_string())
                    .unwrap_or_else(|| "<unresolved>".to_string()),
                bridge_auth_env
                    .as_ref()
                    .map(|env| env.auth_paths.config_path.display().to_string())
                    .unwrap_or_else(|| "<unresolved>".to_string()),
                bridge_auth_env
                    .as_ref()
                    .map(|env| env.auth_paths.config_source.as_str())
                    .unwrap_or("unknown"),
                bridge_credentials_present,
                bridge_config_json_present,
                bridge_oauth_ready,
                self.bridge_auth_overrides_active(),
                api_key_present,
            )),
        })
    }

    async fn acquire_bridge_for_new_session(
        &self,
    ) -> Result<Arc<ClaudeBridgeHandle>, RuntimeError> {
        let _allocation_guard = self.inner.bridge_allocation_lock.lock().await;
        self.prune_closed_bridges_locked().await;

        if let Some(existing) = self.select_reusable_bridge_locked().await {
            return Ok(existing);
        }

        let max_bridges = self.inner.config.max_bridges.max(1);
        let current_count = {
            let bridges = self.inner.bridges.read().await;
            bridges.len()
        };

        if current_count < max_bridges {
            let spawned = self.spawn_bridge_handle().await?;
            let mut bridges = self.inner.bridges.write().await;
            bridges.insert(spawned.instance_id, Arc::clone(&spawned));
            return Ok(spawned);
        }

        if let Some(existing) = self.select_existing_bridge_locked().await {
            return Ok(existing);
        }

        let spawned = self.spawn_bridge_handle().await?;
        let mut bridges = self.inner.bridges.write().await;
        bridges.insert(spawned.instance_id, Arc::clone(&spawned));
        Ok(spawned)
    }

    async fn select_reusable_bridge_locked(&self) -> Option<Arc<ClaudeBridgeHandle>> {
        let bridges = self.inner.bridges.read().await;
        if bridges.is_empty() {
            return None;
        }

        let sessions = self.inner.sessions.read().await;
        let soft_limit = self.inner.config.max_sessions_per_bridge.max(1);

        let eligible = bridges
            .values()
            .filter(|bridge| {
                let assigned_sessions = sessions
                    .values()
                    .filter(|session| session.bridge.instance_id == bridge.instance_id)
                    .count();
                assigned_sessions < soft_limit
            })
            .cloned()
            .collect::<Vec<_>>();
        drop(sessions);
        drop(bridges);

        if eligible.is_empty() {
            return None;
        }

        let selection = self
            .inner
            .next_bridge_selection
            .fetch_add(1, Ordering::SeqCst) as usize;
        let index = selection % eligible.len();
        eligible.get(index).cloned()
    }

    async fn select_existing_bridge_locked(&self) -> Option<Arc<ClaudeBridgeHandle>> {
        let bridges = self.inner.bridges.read().await;
        if bridges.is_empty() {
            return None;
        }

        let selection = self
            .inner
            .next_bridge_selection
            .fetch_add(1, Ordering::SeqCst) as usize;
        let index = selection % bridges.len();
        bridges.values().nth(index).cloned()
    }

    async fn spawn_bridge_handle(&self) -> Result<Arc<ClaudeBridgeHandle>, RuntimeError> {
        let instance_id = self
            .inner
            .next_bridge_instance_id
            .fetch_add(1, Ordering::SeqCst);

        let bridge_auth_env = self.resolve_bridge_auth_environment()?;
        self.validate_bridge_auth_environment(&bridge_auth_env)?;

        let mut command = Command::new(&self.inner.config.bridge_command);
        command
            .args(&self.inner.config.bridge_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("HOME", bridge_auth_env.home_dir.as_path());
        if let Some(claude_config_dir) = bridge_auth_env.claude_config_dir.as_ref() {
            command.env("CLAUDE_CONFIG_DIR", claude_config_dir);
        } else {
            command.env_remove("CLAUDE_CONFIG_DIR");
        }
        if claude_smoke_debug_enabled() {
            let effective_config_dir = self
                .inner
                .config
                .bridge_env
                .get("CLAUDE_CONFIG_DIR")
                .cloned()
                .or_else(|| {
                    bridge_auth_env
                        .claude_config_dir
                        .as_ref()
                        .map(|path| path.display().to_string())
                })
                .unwrap_or_else(|| "<unset>".to_string());
            let effective_home = self
                .inner
                .config
                .bridge_env
                .get("HOME")
                .cloned()
                .unwrap_or_else(|| bridge_auth_env.home_dir.display().to_string());
            eprintln!(
                "[claude-provider] spawning bridge command={} args={:?} CLAUDE_CONFIG_DIR={} HOME={} credentials_path={} config_path={} config_source={}",
                self.inner.config.bridge_command,
                self.inner.config.bridge_args,
                effective_config_dir,
                effective_home,
                bridge_auth_env.auth_paths.credentials_path.display(),
                bridge_auth_env.auth_paths.config_path.display(),
                bridge_auth_env.auth_paths.config_source.as_str(),
            );
        }
        for (key, value) in &self.inner.config.bridge_env {
            command.env(key, value);
        }

        if let Some(api_key) = self.read_api_key().await? {
            command.env("ANTHROPIC_API_KEY", api_key);
        }
        let export_oauth_token = self
            .inner
            .config
            .bridge_env
            .get("GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN")
            .map(|value| value.trim() == "1")
            .or_else(|| {
                std::env::var("GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN")
                    .ok()
                    .map(|value| value.trim() == "1")
            })
            .unwrap_or(false);
        if export_oauth_token {
            let credentials_path = std::env::var_os("GG_CLAUDE_BRIDGE_CREDENTIALS_FILE")
                .map(PathBuf::from)
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| bridge_auth_env.auth_paths.credentials_path.clone());
            if let Some(oauth_token) = read_claude_oauth_access_token(credentials_path.as_path()) {
                if claude_smoke_debug_enabled() {
                    eprintln!(
                        "[claude-provider] setting CLAUDE_CODE_OAUTH_TOKEN from {}",
                        credentials_path.display()
                    );
                }
                command.env("CLAUDE_CODE_OAUTH_TOKEN", oauth_token);
            }
        }

        let mut child = command.spawn().map_err(|error| {
            RuntimeError::Io(format!(
                "failed to spawn Claude bridge sidecar command {} with args {:?}: {error}",
                self.inner.config.bridge_command, self.inner.config.bridge_args
            ))
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::Io("Claude bridge missing stdin handle".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::Io("Claude bridge missing stdout handle".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::Io("Claude bridge missing stderr handle".to_string()))?;

        let (writer_tx, writer_rx) = mpsc::channel(CLAUDE_BRIDGE_STDIN_QUEUE_CAPACITY);

        let handle = Arc::new(ClaudeBridgeHandle {
            instance_id,
            process: Mutex::new(ClaudeBridgeProcessState {
                child,
                closed: false,
            }),
            pending_requests: Mutex::new(HashMap::new()),
            writer_tx,
            writer_shutdown: Notify::new(),
            closed: AtomicBool::new(false),
            shutdown_requested: AtomicBool::new(false),
            last_event_seq_by_session: Mutex::new(BTreeMap::new()),
        });

        let stdout_worker_lanes =
            spawn_stdout_worker_lanes(Arc::clone(&self.inner), Arc::clone(&handle));
        spawn_stdin_writer_task(
            Arc::clone(&self.inner),
            Arc::clone(&handle),
            stdin,
            writer_rx,
        );
        spawn_stdout_task(
            Arc::clone(&self.inner),
            Arc::clone(&handle),
            stdout,
            stdout_worker_lanes,
        );
        spawn_stderr_task(Arc::clone(&self.inner), Arc::clone(&handle), stderr);
        spawn_heartbeat_task(Arc::clone(&self.inner), Arc::clone(&handle));

        send_bridge_request(
            &self.inner,
            &handle,
            "bridge.ping",
            serde_json::json!({}),
            self.inner.config.request_timeout_ms,
        )
        .await?;

        Ok(handle)
    }

    async fn prune_closed_bridges_locked(&self) {
        let mut bridges = self.inner.bridges.write().await;
        bridges.retain(|_, bridge| !bridge.closed.load(Ordering::SeqCst));
    }

    async fn get_session(
        &self,
        runtime_session_id: &str,
    ) -> Result<Arc<ClaudeSessionHandle>, RuntimeError> {
        let sessions = self.inner.sessions.read().await;
        sessions
            .get(runtime_session_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("claude session {runtime_session_id}")))
    }

    async fn insert_session(
        &self,
        runtime_session_id: &str,
        session: Arc<ClaudeSessionHandle>,
    ) -> Result<(), RuntimeError> {
        let mut sessions = self.inner.sessions.write().await;
        let mut sessions_by_bridge_key = self.inner.sessions_by_bridge_key.write().await;
        if sessions.contains_key(runtime_session_id) {
            return Err(RuntimeError::InvalidState(format!(
                "Claude runtime session already exists: {runtime_session_id}"
            )));
        }
        let bridge_key = bridge_session_key(session.bridge.instance_id, &session.bridge_session_id);
        if sessions_by_bridge_key.contains_key(&bridge_key) {
            return Err(RuntimeError::InvalidState(format!(
                "Claude bridge session already mapped: {bridge_key}"
            )));
        }

        sessions.insert(runtime_session_id.to_string(), Arc::clone(&session));
        sessions_by_bridge_key.insert(bridge_key, session);
        Ok(())
    }

    async fn remove_session(&self, runtime_session_id: &str) -> Option<Arc<ClaudeSessionHandle>> {
        let mut sessions = self.inner.sessions.write().await;
        let removed = sessions.remove(runtime_session_id);
        drop(sessions);

        if let Some(session) = removed.as_ref() {
            let key = bridge_session_key(session.bridge.instance_id, &session.bridge_session_id);
            let mut sessions_by_bridge_key = self.inner.sessions_by_bridge_key.write().await;
            sessions_by_bridge_key.remove(&key);
        }

        removed
    }

    async fn shutdown_bridges_if_idle(&self) {
        let has_sessions = {
            let sessions = self.inner.sessions.read().await;
            !sessions.is_empty()
        };
        if has_sessions {
            return;
        }

        let bridges = {
            let mut bridges = self.inner.bridges.write().await;
            std::mem::take(&mut *bridges)
                .into_values()
                .collect::<Vec<_>>()
        };

        for bridge in bridges {
            fail_bridge(
                &self.inner,
                &bridge,
                "Claude bridge closed after last runtime session cleanup".to_string(),
            )
            .await;
        }
    }

    async fn resolve_bridge_turn_id(
        &self,
        session: &Arc<ClaudeSessionHandle>,
        runtime_turn_id: &str,
    ) -> String {
        let bridge_turn_by_runtime_turn = session.bridge_turn_by_runtime_turn.lock().await;
        bridge_turn_by_runtime_turn
            .get(runtime_turn_id)
            .cloned()
            .unwrap_or_else(|| runtime_turn_id.to_string())
    }

    async fn resolve_runtime_turn_id(
        &self,
        session: &Arc<ClaudeSessionHandle>,
        bridge_turn_id: &str,
    ) -> String {
        let runtime_turn_by_bridge_turn = session.runtime_turn_by_bridge_turn.lock().await;
        runtime_turn_by_bridge_turn
            .get(bridge_turn_id)
            .cloned()
            .unwrap_or_else(|| bridge_turn_id.to_string())
    }
}

fn resolve_claude_auth_paths(
    env_claude_config_dir: Option<PathBuf>,
    home_dir: Option<PathBuf>,
) -> Option<ClaudeAuthPathsResolution> {
    let home_dir = home_dir?;
    let credentials_path = home_dir.join(".claude").join(".credentials.json");

    if let Some(config_dir) = env_claude_config_dir {
        return Some(ClaudeAuthPathsResolution {
            credentials_path,
            config_path: config_dir.join(".claude.json"),
            config_dir: Some(config_dir),
            config_source: ClaudeConfigResolutionSource::EnvOverride,
        });
    }

    let gg_claude_dir = home_dir.join(".gg").join("claude");
    if gg_claude_dir.is_dir() {
        return Some(ClaudeAuthPathsResolution {
            credentials_path,
            config_path: gg_claude_dir.join(".claude.json"),
            config_dir: Some(gg_claude_dir),
            config_source: ClaudeConfigResolutionSource::GgFallback,
        });
    }

    Some(ClaudeAuthPathsResolution {
        credentials_path,
        config_path: home_dir.join(".claude.json"),
        config_dir: None,
        config_source: ClaudeConfigResolutionSource::UpstreamDefault,
    })
}

fn has_claude_oauth_access_token(credentials_path: &Path) -> bool {
    read_claude_oauth_access_token(credentials_path).is_some()
}

fn read_claude_oauth_access_token(credentials_path: &Path) -> Option<String> {
    let Ok(contents) = std::fs::read_to_string(credentials_path) else {
        return None;
    };
    let Ok(parsed) = serde_json::from_str::<Value>(&contents) else {
        return None;
    };

    let token = parsed
        .get("claudeAiOauth")
        .and_then(Value::as_object)
        .and_then(|oauth| oauth.get("accessToken"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(token.to_string())
}

fn as_json_object(value: &Value, label: &str) -> Result<Value, RuntimeError> {
    if value.is_object() {
        return Ok(value.clone());
    }
    Err(RuntimeError::InvalidState(format!(
        "Claude auth import field '{label}' must be a JSON object"
    )))
}

fn parse_claude_auth_import_payload(value: Value) -> Result<ClaudeAuthImportPayload, RuntimeError> {
    let object = value.as_object().ok_or_else(|| {
        RuntimeError::InvalidState("Claude auth import payload must be a JSON object".to_string())
    })?;

    let mut credentials_json = if let Some(raw) = object
        .get("credentials_json")
        .or_else(|| object.get("credentials"))
    {
        Some(as_json_object(raw, "credentials_json")?)
    } else {
        None
    };

    let mut config_json =
        if let Some(raw) = object.get("config_json").or_else(|| object.get("config")) {
            Some(as_json_object(raw, "config_json")?)
        } else {
            None
        };

    if credentials_json.is_none() {
        if let Some(token) = object.get("token").and_then(Value::as_str) {
            let token = token.trim();
            if !token.is_empty() {
                credentials_json = Some(serde_json::json!({
                    "claudeAiOauth": {
                        "accessToken": token,
                        "refreshToken": token,
                    }
                }));
            }
        }
    }

    if credentials_json.is_none() && config_json.is_none() {
        if object.get("claudeAiOauth").is_some() {
            credentials_json = Some(Value::Object(object.clone()));
        } else {
            config_json = Some(Value::Object(object.clone()));
        }
    }

    if credentials_json.is_none() && config_json.is_none() {
        return Err(RuntimeError::InvalidState(
            "Claude auth import payload must include credentials/config data".to_string(),
        ));
    }

    Ok(ClaudeAuthImportPayload {
        credentials_json,
        config_json,
    })
}

#[async_trait]
impl RuntimeProvider for ClaudeProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Claude
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Claude,
            display_name: "Claude".to_string(),
            enabled: self.inner.config.enabled,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        self.ensure_provider_enabled().await
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        if !self.inner.config.enabled {
            return Ok(Vec::new());
        }
        Ok(vec![
            ProviderModel {
                id: "claude-sonnet-5".to_string(),
                display_name: "Claude Sonnet 5".to_string(),
            },
            ProviderModel {
                id: "claude-opus-4-8".to_string(),
                display_name: "Claude Opus 4.8".to_string(),
            },
            ProviderModel {
                id: "claude-haiku-4-5".to_string(),
                display_name: "Claude Haiku 4.5".to_string(),
            },
        ])
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        self.provider_auth_status_internal().await
    }

    async fn auth_set_api_key(&self, api_key: String) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let trimmed = api_key.trim();
        if trimmed.is_empty() {
            return Err(RuntimeError::InvalidState(
                "Claude API key cannot be empty".to_string(),
            ));
        }
        self.write_api_key(trimmed).await?;
        self.recycle_after_live_auth_change().await;
        self.provider_auth_status_internal().await
    }

    async fn auth_import_json(&self, auth_json: Value) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let import_payload = parse_claude_auth_import_payload(auth_json)?;
        if let Some(credentials_json) = import_payload.credentials_json.as_ref() {
            self.write_oauth_credentials_json(credentials_json).await?;
        }
        if let Some(config_json) = import_payload.config_json.as_ref() {
            self.write_claude_config_json(config_json).await?;
        }
        self.recycle_after_live_auth_change().await;
        self.provider_auth_status_internal().await
    }

    async fn auth_import_json_text(
        &self,
        auth_json_text: String,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let parsed = serde_json::from_str::<Value>(auth_json_text.trim()).map_err(|error| {
            RuntimeError::InvalidState(format!("Claude auth_json_text must be valid JSON: {error}"))
        })?;
        self.auth_import_json(parsed).await
    }

    async fn auth_logout(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;

        let credentials_path = self.claude_credentials_path();
        if let Err(error) = tokio::fs::remove_file(credentials_path.as_path()).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(RuntimeError::Io(format!(
                    "failed removing Claude credentials file {}: {error}",
                    credentials_path.display()
                )));
            }
        }
        let config_path = self.claude_config_path();
        if let Err(error) = tokio::fs::remove_file(config_path.as_path()).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(RuntimeError::Io(format!(
                    "failed removing Claude config file {}: {error}",
                    config_path.display()
                )));
            }
        }

        let api_key_path = self.api_key_path();
        if let Err(error) = tokio::fs::remove_file(api_key_path.as_path()).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(RuntimeError::Io(format!(
                    "failed removing Claude API key {}: {error}",
                    api_key_path.display()
                )));
            }
        }

        self.recycle_after_live_auth_change().await;
        self.provider_auth_status_internal().await
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let bridge = self.acquire_bridge_for_new_session().await?;
        let mut create_params = serde_json::json!({
            "cwd": req.cwd,
            "model": req.model,
            "permissionMode": req.permission_mode,
        });
        let configure_gg_mcp_server = self.inner.config.gg_mcp.enabled;
        if configure_gg_mcp_server {
            create_params["ggMcpServer"] =
                self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str());
        }

        let response = match send_bridge_request(
            &self.inner,
            &bridge,
            "session.create",
            create_params.clone(),
            self.inner.config.request_timeout_ms,
        )
        .await
        {
            Ok(response) => response,
            Err(error)
                if !configure_gg_mcp_server && is_missing_gg_mcp_server_bad_request(&error) =>
            {
                create_params["ggMcpServer"] =
                    self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str());
                send_bridge_request(
                    &self.inner,
                    &bridge,
                    "session.create",
                    create_params,
                    self.inner.config.request_timeout_ms,
                )
                .await?
            }
            Err(error) => return Err(error),
        };

        let bridge_session_id = response
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "session.create response missing sessionId".to_string(),
                )
            })?;

        let provider_session_ref = response
            .get("providerSessionRef")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| bridge_session_id.clone());
        let canonical_provider_session_ref = response
            .get("claudeCanonicalSessionRef")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let session = Arc::new(ClaudeSessionHandle {
            runtime_session_id: req.runtime_session_id.clone(),
            bridge_session_id,
            provider_session_ref: RwLock::new(provider_session_ref.clone()),
            canonical_provider_session_ref: RwLock::new(canonical_provider_session_ref.clone()),
            bridge,
            active_turn_id: RwLock::new(None),
            bridge_turn_by_runtime_turn: Mutex::new(BTreeMap::new()),
            runtime_turn_by_bridge_turn: Mutex::new(BTreeMap::new()),
            completed_turns: Mutex::new(BTreeMap::new()),
        });
        self.insert_session(req.runtime_session_id.as_str(), session)
            .await?;

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref,
            canonical_provider_session_ref,
        })
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let _ = self.remove_session(req.runtime_session_id.as_str()).await;
        let bridge = self.acquire_bridge_for_new_session().await?;
        let mut resume_params = serde_json::json!({
            "sessionId": req.provider_session_ref,
            "providerSessionRef": req.provider_session_ref,
            "claudeCanonicalSessionRef": req.canonical_provider_session_ref,
            "cwd": req.cwd,
        });
        let configure_gg_mcp_server = self.inner.config.gg_mcp.enabled;
        if configure_gg_mcp_server {
            if let Some(object) = resume_params.as_object_mut() {
                object.insert(
                    "ggMcpServer".to_string(),
                    self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str()),
                );
            }
        }

        let response = match send_bridge_request(
            &self.inner,
            &bridge,
            "session.resume",
            resume_params.clone(),
            self.inner.config.request_timeout_ms,
        )
        .await
        {
            Ok(response) => response,
            Err(error)
                if !configure_gg_mcp_server && is_missing_gg_mcp_server_bad_request(&error) =>
            {
                if let Some(object) = resume_params.as_object_mut() {
                    object.insert(
                        "ggMcpServer".to_string(),
                        self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str()),
                    );
                }
                send_bridge_request(
                    &self.inner,
                    &bridge,
                    "session.resume",
                    resume_params,
                    self.inner.config.request_timeout_ms,
                )
                .await?
            }
            Err(error) => return Err(error),
        };

        let bridge_session_id = response
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "session.resume response missing sessionId".to_string(),
                )
            })?;

        let provider_session_ref = response
            .get("providerSessionRef")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| bridge_session_id.clone());
        let canonical_provider_session_ref = response
            .get("claudeCanonicalSessionRef")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or(req.canonical_provider_session_ref);

        let session = Arc::new(ClaudeSessionHandle {
            runtime_session_id: req.runtime_session_id.clone(),
            bridge_session_id,
            provider_session_ref: RwLock::new(provider_session_ref.clone()),
            canonical_provider_session_ref: RwLock::new(canonical_provider_session_ref.clone()),
            bridge,
            active_turn_id: RwLock::new(None),
            bridge_turn_by_runtime_turn: Mutex::new(BTreeMap::new()),
            runtime_turn_by_bridge_turn: Mutex::new(BTreeMap::new()),
            completed_turns: Mutex::new(BTreeMap::new()),
        });
        self.insert_session(req.runtime_session_id.as_str(), session)
            .await?;

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref,
            canonical_provider_session_ref,
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let runtime_turn_id = req.turn_id.clone();

        let result = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.send",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "input": req.input,
                "expectedTurnId": req.expected_turn_id,
            }),
            self.inner.config.request_timeout_ms,
        )
        .await?;

        let bridge_turn_id = result
            .get("turnId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| runtime_turn_id.clone());

        {
            let mut bridge_turn_by_runtime_turn = session.bridge_turn_by_runtime_turn.lock().await;
            bridge_turn_by_runtime_turn.insert(runtime_turn_id.clone(), bridge_turn_id.clone());
        }
        {
            let mut runtime_turn_by_bridge_turn = session.runtime_turn_by_bridge_turn.lock().await;
            runtime_turn_by_bridge_turn.insert(bridge_turn_id, runtime_turn_id.clone());
        }

        {
            let mut active_turn_id = session.active_turn_id.write().await;
            *active_turn_id = Some(runtime_turn_id.clone());
        }

        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: runtime_turn_id,
        })
    }

    async fn interrupt_turn(&self, req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let bridge_turn_id = self
            .resolve_bridge_turn_id(&session, req.turn_id.as_str())
            .await;
        let _ = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.interrupt",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "turnId": bridge_turn_id,
            }),
            self.inner.config.request_timeout_ms,
        )
        .await?;
        Ok(())
    }

    async fn respond_approval(
        &self,
        req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let decision = ApprovalDecision::parse(req.decision.as_str())?;
        let decision = match decision {
            ApprovalDecision::Accept => "accept",
            ApprovalDecision::Decline => "decline",
        };
        let bridge_turn_id = self
            .resolve_bridge_turn_id(&session, req.turn_id.as_str())
            .await;

        let mut payload = serde_json::json!({
            "sessionId": session.bridge_session_id,
            "turnId": bridge_turn_id,
            "approvalId": req.approval_id,
            "decision": decision,
        });
        if let Some(updated_input) = req.payload {
            if let Some(object) = payload.as_object_mut() {
                object.insert("updatedInput".to_string(), updated_input);
            }
        }

        let _ = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.approval.respond",
            payload,
            self.inner.config.request_timeout_ms,
        )
        .await?;
        Ok(())
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let runtime_turn_id = req.turn_id.clone();

        if let Some(result) = {
            let completed_turns = session.completed_turns.lock().await;
            completed_turns.get(runtime_turn_id.as_str()).cloned()
        } {
            return Ok(result);
        }

        let timeout_ms = req
            .timeout_ms
            .unwrap_or(self.inner.config.default_wait_timeout_ms as u64)
            .max(1);
        let transport_timeout_ms =
            timeout_ms.saturating_add(self.inner.config.request_timeout_ms.max(1));
        let bridge_turn_id = self
            .resolve_bridge_turn_id(&session, runtime_turn_id.as_str())
            .await;
        if claude_smoke_debug_enabled() {
            eprintln!(
                "[claude-provider] session.wait start runtime_session_id={} runtime_turn_id={} bridge_turn_id={} timeout_ms={} transport_timeout_ms={}",
                req.runtime_session_id,
                runtime_turn_id,
                bridge_turn_id,
                timeout_ms,
                transport_timeout_ms
            );
        }

        let result = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.wait",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "turnId": bridge_turn_id,
                "timeoutMs": timeout_ms,
            }),
            transport_timeout_ms,
        )
        .await?;
        if claude_smoke_debug_enabled() {
            eprintln!(
                "[claude-provider] session.wait returned runtime_session_id={} turn_id={} payload={}",
                req.runtime_session_id, runtime_turn_id, result
            );
        }

        let bridge_turn_id = result
            .get("turnId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation("session.wait response missing turnId".to_string())
            })?;
        let resolved_turn_id = self
            .resolve_runtime_turn_id(&session, bridge_turn_id.as_str())
            .await;
        // Bridge completion events can arrive before session.wait resolves and
        // clear the runtime/bridge turn map. Preserve the runtime turn id from
        // the wait request when the map lookup falls back to the bridge id.
        let turn_id = if resolved_turn_id == bridge_turn_id && runtime_turn_id != bridge_turn_id {
            runtime_turn_id.clone()
        } else {
            resolved_turn_id
        };
        let status = extract_turn_status(result.get("status"));
        let assistant_text = extract_assistant_text(result.get("assistant_text"))
            .or_else(|| extract_assistant_text(result.get("assistantText")));

        let turn_result = ProviderTurnResult {
            runtime_session_id: req.runtime_session_id,
            turn_id: turn_id.clone(),
            status,
            usage: merge_assistant_text_into_usage(result.get("usage").cloned(), assistant_text),
            error: result.get("error").cloned(),
        };

        {
            let mut completed_turns = session.completed_turns.lock().await;
            completed_turns.insert(turn_id.clone(), turn_result.clone());
        }
        {
            let mut active_turn_id = session.active_turn_id.write().await;
            if active_turn_id.as_deref() == Some(turn_id.as_str()) {
                *active_turn_id = None;
            }
        }
        {
            let mut bridge_turn_by_runtime_turn = session.bridge_turn_by_runtime_turn.lock().await;
            if let Some(bridge_turn_id) = bridge_turn_by_runtime_turn.remove(turn_id.as_str()) {
                let mut runtime_turn_by_bridge_turn =
                    session.runtime_turn_by_bridge_turn.lock().await;
                runtime_turn_by_bridge_turn.remove(bridge_turn_id.as_str());
            }
        }

        Ok(turn_result)
    }

    async fn close_session(&self, req: ProviderCloseSessionRequest) -> Result<(), RuntimeError> {
        let Some(session) = self.remove_session(req.runtime_session_id.as_str()).await else {
            return Ok(());
        };

        let _ = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.close",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "reason": req.reason,
            }),
            self.inner.config.request_timeout_ms,
        )
        .await;

        self.shutdown_bridges_if_idle().await;

        Ok(())
    }
}

fn claude_smoke_debug_enabled() -> bool {
    std::env::var("GG_CLAUDE_SMOKE_DEBUG")
        .ok()
        .map(|value| value.trim() == "1")
        .unwrap_or(false)
}

fn extract_turn_status(value: Option<&Value>) -> ProviderTurnStatus {
    match value
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("completed") => ProviderTurnStatus::Completed,
        Some("interrupted") => ProviderTurnStatus::Interrupted,
        Some("failed") => ProviderTurnStatus::Failed,
        _ => ProviderTurnStatus::InProgress,
    }
}

fn extract_assistant_text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn merge_assistant_text_into_usage(
    usage: Option<Value>,
    assistant_text: Option<String>,
) -> Option<Value> {
    let Some(assistant_text) = assistant_text else {
        return usage;
    };
    if assistant_text.trim().is_empty() {
        return usage;
    }

    match usage {
        Some(Value::Object(mut usage_object)) => {
            usage_object.insert(
                "last_message".to_string(),
                Value::String(assistant_text.clone()),
            );
            usage_object.insert("assistant_text".to_string(), Value::String(assistant_text));
            Some(Value::Object(usage_object))
        }
        Some(existing) => Some(serde_json::json!({
            "last_message": assistant_text.clone(),
            "assistant_text": assistant_text,
            "raw_usage": existing,
        })),
        None => Some(serde_json::json!({
            "last_message": assistant_text.clone(),
            "assistant_text": assistant_text,
        })),
    }
}

fn map_bridge_error(error: &Value) -> RuntimeError {
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("INTERNAL_ERROR");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Claude bridge request failed");
    let message = format!("claude bridge {code}: {message}");
    match code {
        "SESSION_NOT_FOUND" | "TURN_NOT_FOUND" | "APPROVAL_NOT_FOUND" => {
            RuntimeError::NotFound(message)
        }
        "BAD_REQUEST" | "TURN_IN_PROGRESS" | "TURN_NOT_IN_PROGRESS" => {
            RuntimeError::InvalidState(message)
        }
        "PROTOCOL_VIOLATION" => RuntimeError::ProtocolViolation(message),
        "UNAUTHORIZED" => RuntimeError::Configuration(message),
        "TIMEOUT" => RuntimeError::InvalidState(message),
        "PROVIDER_PROCESS_EXITED" | "INTERNAL_ERROR" => RuntimeError::Io(message),
        _ => RuntimeError::InvalidState(message),
    }
}

fn is_missing_gg_mcp_server_bad_request(error: &RuntimeError) -> bool {
    match error {
        RuntimeError::InvalidState(message)
        | RuntimeError::ProtocolViolation(message)
        | RuntimeError::NotFound(message)
        | RuntimeError::Io(message)
        | RuntimeError::Configuration(message)
        | RuntimeError::Bootstrap(message)
        | RuntimeError::Unsupported(message)
        | RuntimeError::ProviderAlreadyRegistered(message)
        | RuntimeError::ProviderNotRegistered(message) => {
            message.contains(GG_MCP_MISSING_BAD_REQUEST)
        }
    }
}

fn bridge_session_key(bridge_instance_id: u64, bridge_session_id: &str) -> String {
    format!("{bridge_instance_id}:{bridge_session_id}")
}

fn default_bridge_command() -> String {
    std::env::var("GG_CLAUDE_BRIDGE_COMMAND").unwrap_or_else(|_| {
        standalone_claude_bridge_command_path()
            .display()
            .to_string()
    })
}

fn default_bridge_args() -> Vec<String> {
    if let Ok(raw) = std::env::var("GG_CLAUDE_BRIDGE_ARGS_JSON") {
        if let Ok(parsed) = serde_json::from_str::<Vec<String>>(raw.as_str()) {
            if !parsed.is_empty() {
                return parsed;
            }
        }
    }
    Vec::new()
}

fn runtime_install_root_from_executable(executable_path: &Path) -> PathBuf {
    let executable_dir = executable_path.parent().unwrap_or_else(|| Path::new("."));
    if executable_dir.ends_with("bin") {
        executable_dir
            .parent()
            .unwrap_or(executable_dir)
            .to_path_buf()
    } else {
        executable_dir.to_path_buf()
    }
}

fn sidecar_command_path_from_executable(
    executable_path: &Path,
    sidecar: &str,
    binary: &str,
) -> PathBuf {
    runtime_install_root_from_executable(executable_path)
        .join("sidecars")
        .join(sidecar)
        .join(binary)
}

fn workspace_sidecar_command_path_if_present(
    workspace_root: &Path,
    sidecar: &str,
    binary: &str,
    workspace_launcher: &str,
) -> Option<PathBuf> {
    let workspace_sidecar_binary = workspace_root.join("sidecars").join(sidecar).join(binary);
    if workspace_sidecar_binary.exists() {
        return Some(workspace_sidecar_binary);
    }

    let workspace_sidecar_launcher = workspace_root
        .join("sidecars")
        .join(sidecar)
        .join("bin")
        .join(workspace_launcher);
    if workspace_sidecar_launcher.exists() {
        return Some(workspace_sidecar_launcher);
    }

    None
}

fn workspace_root_from_target_binary_path(executable_path: &Path) -> Option<PathBuf> {
    for ancestor in executable_path.ancestors() {
        if ancestor.file_name().is_some_and(|name| name == "target") {
            return ancestor.parent().map(Path::to_path_buf);
        }
    }
    None
}

fn sidecar_command_path_for_executable(
    executable_path: &Path,
    sidecar: &str,
    binary: &str,
    workspace_launcher: &str,
) -> PathBuf {
    let workspace_roots = std::env::current_dir()
        .map(|cwd| vec![cwd])
        .unwrap_or_default();
    sidecar_command_path_for_executable_with_workspace_roots(
        executable_path,
        sidecar,
        binary,
        workspace_launcher,
        workspace_roots.as_slice(),
    )
}

fn sidecar_command_path_for_executable_with_workspace_roots(
    executable_path: &Path,
    sidecar: &str,
    binary: &str,
    workspace_launcher: &str,
    workspace_roots: &[PathBuf],
) -> PathBuf {
    let install_path = sidecar_command_path_from_executable(executable_path, sidecar, binary);
    if install_path.exists() {
        return install_path;
    }

    if let Some(workspace_root) = workspace_root_from_target_binary_path(executable_path) {
        if let Some(workspace_sidecar_path) = workspace_sidecar_command_path_if_present(
            workspace_root.as_path(),
            sidecar,
            binary,
            workspace_launcher,
        ) {
            return workspace_sidecar_path;
        }
    }

    for workspace_root in workspace_roots {
        for ancestor in workspace_root.ancestors() {
            if let Some(workspace_sidecar_path) = workspace_sidecar_command_path_if_present(
                ancestor,
                sidecar,
                binary,
                workspace_launcher,
            ) {
                return workspace_sidecar_path;
            }
        }
    }

    install_path
}

fn sidecar_command_path_for_current_executable(sidecar: &str, binary: &str) -> PathBuf {
    match std::env::current_exe() {
        Ok(executable_path) => sidecar_command_path_for_executable(
            executable_path.as_path(),
            sidecar,
            binary,
            &format!("{binary}-dev"),
        ),
        Err(_) => PathBuf::from("sidecars").join(sidecar).join(binary),
    }
}

pub fn standalone_claude_bridge_command_path() -> PathBuf {
    sidecar_command_path_for_current_executable("claude-bridge", "claude-bridge")
}

pub fn standalone_gg_mcp_server_command_path() -> PathBuf {
    sidecar_command_path_for_current_executable("gg-mcp-server", "gg-mcp-server")
}

fn default_gg_mcp_server_command() -> String {
    std::env::var("GG_MCP_SERVER_PATH").unwrap_or_else(|_| {
        standalone_gg_mcp_server_command_path()
            .display()
            .to_string()
    })
}

fn set_permissions_if_unix(path: &Path, mode: u32) -> Result<(), RuntimeError> {
    #[cfg(unix)]
    {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).map_err(|error| {
            RuntimeError::Io(format!(
                "failed setting permissions on {}: {error}",
                path.display()
            ))
        })?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
    Ok(())
}

fn stdout_worker_lane_key_for_payload(payload: &Value) -> String {
    if let Some(bridge_session_id) = payload
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return format!("session:{bridge_session_id}");
    }

    let empty_payload = Value::Null;
    let payload_body = payload.get("payload").unwrap_or(&empty_payload);
    if let Some(turn_id) = payload
        .get("turnId")
        .or_else(|| payload_body.get("turnId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return format!("turn:{turn_id}");
    }

    let event_name = payload
        .get("event")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    format!("event:{event_name}")
}

fn stdout_worker_lane_index(key: &str, lane_count: usize) -> usize {
    if lane_count <= 1 {
        return 0;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % lane_count
}

fn spawn_stdout_worker_lanes(
    inner: Arc<ClaudeProviderInner>,
    bridge: Arc<ClaudeBridgeHandle>,
) -> Vec<mpsc::Sender<ClaudeBridgeEventWorkItem>> {
    let lane_count = CLAUDE_STDOUT_WORKER_LANE_COUNT.max(1);
    let lane_capacity = CLAUDE_STDOUT_WORKER_QUEUE_CAPACITY.max(1);
    let mut senders = Vec::with_capacity(lane_count);

    for _ in 0..lane_count {
        let (sender, mut receiver) = mpsc::channel::<ClaudeBridgeEventWorkItem>(lane_capacity);
        senders.push(sender);
        let inner = Arc::clone(&inner);
        let bridge = Arc::clone(&bridge);
        tokio::spawn(async move {
            while let Some(work_item) = receiver.recv().await {
                handle_bridge_event(&inner, &bridge, work_item.payload).await;
            }
        });
    }

    senders
}

fn spawn_stdout_task(
    inner: Arc<ClaudeProviderInner>,
    bridge: Arc<ClaudeBridgeHandle>,
    stdout: ChildStdout,
    worker_lane_senders: Vec<mpsc::Sender<ClaudeBridgeEventWorkItem>>,
) {
    tokio::spawn(async move {
        if worker_lane_senders.is_empty() {
            fail_bridge(
                &inner,
                &bridge,
                "Claude bridge stdout worker lanes were not initialized".to_string(),
            )
            .await;
            return;
        }

        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let payload: Value = match serde_json::from_str(line.as_str()) {
                        Ok(payload) => payload,
                        Err(_) => continue,
                    };

                    if payload.get("event").is_some() {
                        let lane_key = stdout_worker_lane_key_for_payload(&payload);
                        let lane_index =
                            stdout_worker_lane_index(lane_key.as_str(), worker_lane_senders.len());
                        if worker_lane_senders[lane_index]
                            .send(ClaudeBridgeEventWorkItem { payload })
                            .await
                            .is_err()
                        {
                            fail_bridge(
                                &inner,
                                &bridge,
                                format!(
                                    "Claude stdout worker lane {lane_index} closed unexpectedly for bridge {}",
                                    bridge.instance_id
                                ),
                            )
                            .await;
                            break;
                        }
                        continue;
                    }

                    if payload.get("id").is_some() {
                        handle_bridge_response(&bridge, payload).await;
                        continue;
                    }

                    fail_bridge(
                        &inner,
                        &bridge,
                        format!("Unexpected Claude bridge payload shape: {payload}"),
                    )
                    .await;
                    break;
                }
                Ok(None) => {
                    if !bridge.shutdown_requested.load(Ordering::SeqCst) {
                        fail_bridge(
                            &inner,
                            &bridge,
                            "Claude bridge stdout closed unexpectedly".to_string(),
                        )
                        .await;
                    }
                    break;
                }
                Err(error) => {
                    fail_bridge(
                        &inner,
                        &bridge,
                        format!("Failed reading Claude bridge stdout: {error}"),
                    )
                    .await;
                    break;
                }
            }
        }
    });
}

fn spawn_stderr_task(
    _inner: Arc<ClaudeProviderInner>,
    _bridge: Arc<ClaudeBridgeHandle>,
    stderr: ChildStderr,
) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    tracing::debug!("claude bridge stderr: {line}");
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });
}

fn spawn_stdin_writer_task(
    inner: Arc<ClaudeProviderInner>,
    bridge: Arc<ClaudeBridgeHandle>,
    stdin: ChildStdin,
    mut writer_rx: mpsc::Receiver<OutboundJsonLine>,
) {
    tokio::spawn(async move {
        let mut writer = BufWriter::new(stdin);
        loop {
            let outbound_line = tokio::select! {
                _ = bridge.writer_shutdown.notified() => break,
                outbound_line = writer_rx.recv() => outbound_line,
            };

            let Some(outbound_line) = outbound_line else {
                break;
            };

            if let Err(error) =
                write_outbound_batch(&mut writer, &mut writer_rx, outbound_line).await
            {
                fail_bridge(
                    &inner,
                    &bridge,
                    format!(
                        "Failed writing request to Claude bridge stdin for bridge {}: {error}",
                        bridge.instance_id
                    ),
                )
                .await;
                break;
            }
        }
    });
}

async fn write_outbound_batch(
    writer: &mut BufWriter<ChildStdin>,
    writer_rx: &mut mpsc::Receiver<OutboundJsonLine>,
    first_line: OutboundJsonLine,
) -> Result<(), std::io::Error> {
    writer.write_all(first_line.as_slice()).await?;
    for _ in 0..CLAUDE_BRIDGE_STDIN_FLUSH_BATCH_MAX {
        match writer_rx.try_recv() {
            Ok(next_line) => writer.write_all(next_line.as_slice()).await?,
            Err(mpsc::error::TryRecvError::Empty)
            | Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    writer.flush().await
}

fn spawn_heartbeat_task(inner: Arc<ClaudeProviderInner>, bridge: Arc<ClaudeBridgeHandle>) {
    tokio::spawn(async move {
        let mut consecutive_failures = 0_u64;
        let heartbeat_interval = Duration::from_millis(inner.config.heartbeat_interval_ms.max(1));

        loop {
            tokio::time::sleep(heartbeat_interval).await;
            if bridge.shutdown_requested.load(Ordering::SeqCst) {
                break;
            }

            let ping = send_bridge_request(
                &inner,
                &bridge,
                "bridge.ping",
                serde_json::json!({}),
                inner.config.request_timeout_ms,
            )
            .await;

            match ping {
                Ok(_) => {
                    consecutive_failures = 0;
                }
                Err(error) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= inner.config.heartbeat_failure_threshold.max(1) {
                        fail_bridge(
                            &inner,
                            &bridge,
                            format!(
                                "Claude bridge heartbeat failed {} times: {error}",
                                consecutive_failures
                            ),
                        )
                        .await;
                        break;
                    }
                }
            }
        }
    });
}

async fn send_bridge_request(
    inner: &Arc<ClaudeProviderInner>,
    bridge: &Arc<ClaudeBridgeHandle>,
    method: &str,
    params: Value,
    timeout_ms: u64,
) -> Result<Value, RuntimeError> {
    if bridge.closed.load(Ordering::SeqCst) {
        return Err(RuntimeError::Io(
            "Claude bridge process is not running".to_string(),
        ));
    }

    let request_id = inner
        .next_request_id
        .fetch_add(1, Ordering::SeqCst)
        .to_string();

    {
        let process = bridge.process.lock().await;
        if process.closed {
            bridge.closed.store(true, Ordering::SeqCst);
            return Err(RuntimeError::Io(
                "Claude bridge process is not running".to_string(),
            ));
        }
    }

    let (sender, receiver) = oneshot::channel();
    {
        let mut pending_requests = bridge.pending_requests.lock().await;
        pending_requests.insert(request_id.clone(), sender);
    }

    let request_payload = serde_json::json!({
        "id": request_id.clone(),
        "method": method,
        "params": params,
    });
    let mut serialized_request = serde_json::to_vec(&request_payload).map_err(|error| {
        RuntimeError::Io(format!(
            "failed serializing request to Claude bridge: {error}"
        ))
    })?;
    serialized_request.push(b'\n');

    if bridge.writer_tx.send(serialized_request).await.is_err() {
        let mut pending_requests = bridge.pending_requests.lock().await;
        pending_requests.remove(&request_id);
        drop(pending_requests);

        fail_bridge(
            inner,
            bridge,
            "Claude bridge stdin writer task closed unexpectedly".to_string(),
        )
        .await;
        return Err(RuntimeError::Io(format!(
            "failed writing request to Claude bridge for method {method}"
        )));
    }

    let response_result =
        match tokio::time::timeout(Duration::from_millis(timeout_ms.max(1)), receiver).await {
            Ok(response_result) => response_result,
            Err(_) => {
                let mut pending_requests = bridge.pending_requests.lock().await;
                pending_requests.remove(&request_id);
                return Err(RuntimeError::InvalidState(format!(
                    "timed out waiting for Claude bridge response to {method}"
                )));
            }
        };

    match response_result {
        Ok(response) => response,
        Err(_) => Err(RuntimeError::Io(format!(
            "Claude bridge response channel closed for method {method}"
        ))),
    }
}

async fn handle_bridge_response(bridge: &Arc<ClaudeBridgeHandle>, payload: Value) {
    let rpc_id = payload
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string);

    let Some(rpc_id) = rpc_id else {
        return;
    };

    let response = if let Some(result) = payload.get("result") {
        Ok(result.clone())
    } else if let Some(error) = payload.get("error") {
        Err(map_bridge_error(error))
    } else {
        Err(RuntimeError::ProtocolViolation(format!(
            "bridge response missing result/error for id {rpc_id}: {payload}"
        )))
    };

    let sender = {
        let mut pending_requests = bridge.pending_requests.lock().await;
        pending_requests.remove(&rpc_id)
    };
    if let Some(sender) = sender {
        let _ = sender.send(response);
    }
}

async fn handle_bridge_event(
    inner: &Arc<ClaudeProviderInner>,
    bridge: &Arc<ClaudeBridgeHandle>,
    payload: Value,
) {
    let event_name = payload
        .get("event")
        .and_then(Value::as_str)
        .map(str::to_string);
    let seq = payload.get("seq").and_then(Value::as_u64);
    let bridge_session_id = payload
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let payload_body = payload
        .get("payload")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let (Some(event_name), Some(seq), Some(bridge_session_id)) =
        (event_name, seq, bridge_session_id)
    else {
        fail_bridge(
            inner,
            bridge,
            format!("Bridge event missing required fields: {payload}"),
        )
        .await;
        return;
    };

    let non_monotonic_previous = {
        let mut last_event_seq_by_session = bridge.last_event_seq_by_session.lock().await;
        let previous = last_event_seq_by_session
            .get(&bridge_session_id)
            .copied()
            .unwrap_or(0);
        if seq <= previous {
            Some(previous)
        } else {
            last_event_seq_by_session.insert(bridge_session_id.clone(), seq);
            None
        }
    };
    if let Some(previous) = non_monotonic_previous {
        fail_bridge(
            inner,
            bridge,
            format!(
                "non-monotonic bridge event sequence for bridge instance {} session {}: current={seq}, previous={previous}",
                bridge.instance_id, bridge_session_id
            ),
        )
        .await;
        return;
    }

    let key = bridge_session_key(bridge.instance_id, bridge_session_id.as_str());
    let session = {
        let sessions_by_bridge_key = inner.sessions_by_bridge_key.read().await;
        sessions_by_bridge_key.get(&key).cloned()
    };
    let Some(session) = session else {
        return;
    };

    let mut event_turn_id = payload
        .get("turnId")
        .and_then(Value::as_str)
        .map(str::to_string);
    if event_turn_id.is_none() {
        event_turn_id = payload_body
            .get("turnId")
            .and_then(Value::as_str)
            .map(str::to_string);
    }
    let event_turn_id = if let Some(bridge_turn_id) = event_turn_id {
        let runtime_turn_by_bridge_turn = session.runtime_turn_by_bridge_turn.lock().await;
        Some(
            runtime_turn_by_bridge_turn
                .get(bridge_turn_id.as_str())
                .cloned()
                .unwrap_or(bridge_turn_id),
        )
    } else {
        None
    };

    match event_name.as_str() {
        _ if claude_smoke_debug_enabled() => {
            eprintln!(
                "[claude-provider] bridge event bridge_instance_id={} session_id={} seq={} event={} payload={}",
                bridge.instance_id, bridge_session_id, seq, event_name, payload_body
            );
        }
        _ => {}
    }

    match event_name.as_str() {
        "session.updated" => {
            if let Some(provider_session_ref) = payload_body
                .get("providerSessionRef")
                .and_then(Value::as_str)
                .map(str::to_string)
            {
                let mut provider_session = session.provider_session_ref.write().await;
                *provider_session = provider_session_ref;
            }
            let canonical = payload_body
                .get("claudeCanonicalSessionRef")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            if canonical.is_some() {
                let mut canonical_ref = session.canonical_provider_session_ref.write().await;
                *canonical_ref = canonical;
            }
        }
        "turn.started" => {
            if let Some(turn_id) = event_turn_id {
                let mut active_turn_id = session.active_turn_id.write().await;
                *active_turn_id = Some(turn_id);
            }
        }
        "turn.completed" => {
            if let Some(turn_id) = event_turn_id {
                let status = extract_turn_status(payload_body.get("status"));
                let assistant_text = extract_assistant_text(payload_body.get("assistant_text"))
                    .or_else(|| extract_assistant_text(payload_body.get("assistantText")));
                let turn_result = ProviderTurnResult {
                    runtime_session_id: session.runtime_session_id.clone(),
                    turn_id: turn_id.clone(),
                    status,
                    usage: merge_assistant_text_into_usage(
                        payload_body.get("usage").cloned(),
                        assistant_text,
                    ),
                    error: payload_body.get("error").cloned(),
                };
                {
                    let mut completed = session.completed_turns.lock().await;
                    completed.insert(turn_id.clone(), turn_result);
                }
                {
                    let mut active_turn_id = session.active_turn_id.write().await;
                    if active_turn_id.as_deref() == Some(turn_id.as_str()) {
                        *active_turn_id = None;
                    }
                }
                {
                    let mut bridge_turn_by_runtime_turn =
                        session.bridge_turn_by_runtime_turn.lock().await;
                    if let Some(bridge_turn_id) =
                        bridge_turn_by_runtime_turn.remove(turn_id.as_str())
                    {
                        let mut runtime_turn_by_bridge_turn =
                            session.runtime_turn_by_bridge_turn.lock().await;
                        runtime_turn_by_bridge_turn.remove(bridge_turn_id.as_str());
                    }
                }
            }
        }
        "error" => {
            if let Some(turn_id) = event_turn_id {
                let turn_result = ProviderTurnResult {
                    runtime_session_id: session.runtime_session_id.clone(),
                    turn_id: turn_id.clone(),
                    status: ProviderTurnStatus::Failed,
                    usage: None,
                    error: Some(payload_body.clone()),
                };
                {
                    let mut completed = session.completed_turns.lock().await;
                    completed.insert(turn_id.clone(), turn_result);
                }
                {
                    let mut active_turn_id = session.active_turn_id.write().await;
                    if active_turn_id.as_deref() == Some(turn_id.as_str()) {
                        *active_turn_id = None;
                    }
                }
                {
                    let mut bridge_turn_by_runtime_turn =
                        session.bridge_turn_by_runtime_turn.lock().await;
                    if let Some(bridge_turn_id) =
                        bridge_turn_by_runtime_turn.remove(turn_id.as_str())
                    {
                        let mut runtime_turn_by_bridge_turn =
                            session.runtime_turn_by_bridge_turn.lock().await;
                        runtime_turn_by_bridge_turn.remove(bridge_turn_id.as_str());
                    }
                }
            }
        }
        _ => {}
    }
}

async fn fail_bridge(
    inner: &Arc<ClaudeProviderInner>,
    bridge: &Arc<ClaudeBridgeHandle>,
    message: String,
) {
    let already_closed = {
        let mut process = bridge.process.lock().await;
        if process.closed {
            true
        } else {
            process.closed = true;
            bridge.closed.store(true, Ordering::SeqCst);
            bridge.shutdown_requested.store(true, Ordering::SeqCst);
            bridge.writer_shutdown.notify_waiters();
            let _ = process.child.start_kill();
            let _ = process.child.wait().await;
            false
        }
    };
    if already_closed {
        return;
    }

    let pending_requests = {
        let mut pending_requests = bridge.pending_requests.lock().await;
        std::mem::take(&mut *pending_requests)
    };

    for (_, sender) in pending_requests {
        let _ = sender.send(Err(RuntimeError::Io(message.clone())));
    }

    {
        let mut bridges = inner.bridges.write().await;
        bridges.remove(&bridge.instance_id);
    }

    let affected_sessions = {
        let sessions = inner.sessions.read().await;
        sessions
            .values()
            .filter(|session| session.bridge.instance_id == bridge.instance_id)
            .cloned()
            .collect::<Vec<_>>()
    };

    for session in affected_sessions {
        let active_turn_id = session.active_turn_id.read().await.clone();
        if let Some(turn_id) = active_turn_id {
            let mut completed = session.completed_turns.lock().await;
            completed.insert(
                turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: session.runtime_session_id.clone(),
                    turn_id,
                    status: ProviderTurnStatus::Failed,
                    usage: None,
                    error: Some(serde_json::json!({
                        "message": message,
                    })),
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use runtime_core::{
        CreateSessionInput, ProviderRegistry, RuntimeSessionManager, RuntimeStore, SendTurnInput,
    };
    use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};

    const FAKE_BRIDGE_SCRIPT: &str = r#"#!/usr/bin/env python3
import json
import os
import sys

MISSING_GG_MCP = "Missing ggMcpServer config for SDK mode session"

scenario = os.environ.get("FAKE_BRIDGE_SCENARIO", "normal").strip()
log_path = os.environ.get("FAKE_BRIDGE_REQUEST_LOG", "").strip()
env_log_path = os.environ.get("FAKE_BRIDGE_ENV_LOG", "").strip()
state = {
  "next_session": 1,
  "next_turn": 1,
  "send_calls": 0,
}

def log_request(payload):
  if not log_path:
    return
  with open(log_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload, sort_keys=True))
    handle.write("\n")

def write_env_snapshot():
  if not env_log_path:
    return
  with open(env_log_path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps({
      "HOME": os.environ.get("HOME"),
      "CLAUDE_CONFIG_DIR": os.environ.get("CLAUDE_CONFIG_DIR"),
      "CLAUDE_CODE_OAUTH_TOKEN_PRESENT": bool(os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "").strip()),
    }, sort_keys=True))

write_env_snapshot()

def emit(payload):
  sys.stdout.write(json.dumps(payload))
  sys.stdout.write("\n")
  sys.stdout.flush()

def emit_ok(rpc_id, result):
  emit({
    "id": rpc_id,
    "result": result,
  })

def emit_error(rpc_id, code, message, details=None):
  emit({
    "id": rpc_id,
    "error": {
      "code": code,
      "message": message,
      "details": details,
    },
  })

def requires_gg_mcp(params):
  return scenario == "require_gg_mcp" and "ggMcpServer" not in params

for raw_line in sys.stdin:
  line = raw_line.strip()
  if not line:
    continue

  try:
    request = json.loads(line)
  except Exception:
    continue

  log_request(request)
  rpc_id = request.get("id", "")
  method = request.get("method", "")
  params = request.get("params", {})
  if not isinstance(params, dict):
    emit_error(rpc_id, "BAD_REQUEST", "params must be an object")
    continue

  if method == "bridge.ping":
    emit_ok(rpc_id, {"ok": True})
    continue

  if method == "session.create":
    if requires_gg_mcp(params):
      emit_error(
        rpc_id,
        "BAD_REQUEST",
        MISSING_GG_MCP,
        {"reason": "missing_gg_mcp_server"}
      )
      continue
    session_index = state["next_session"]
    state["next_session"] = session_index + 1
    session_id = f"bridge-session-{session_index}"
    emit_ok(
      rpc_id,
      {
        "sessionId": session_id,
        "providerSessionRef": f"provider-session-{session_index}",
        "claudeCanonicalSessionRef": f"canonical-session-{session_index}",
      },
    )
    continue

  if method == "session.resume":
    if requires_gg_mcp(params):
      emit_error(
        rpc_id,
        "BAD_REQUEST",
        MISSING_GG_MCP,
        {"reason": "missing_gg_mcp_server"}
      )
      continue
    session_index = state["next_session"]
    state["next_session"] = session_index + 1
    session_id = f"bridge-session-resume-{session_index}"
    provider_session_ref = params.get("providerSessionRef") or params.get("sessionId") or f"provider-resume-{session_index}"
    canonical_ref = params.get("claudeCanonicalSessionRef")
    emit_ok(
      rpc_id,
      {
        "sessionId": session_id,
        "providerSessionRef": provider_session_ref,
        "claudeCanonicalSessionRef": canonical_ref or f"canonical-resume-{session_index}",
      },
    )
    continue

  if method == "session.send":
    if scenario == "send_not_found_once" and state["send_calls"] == 0:
      state["send_calls"] = 1
      emit_error(
        rpc_id,
        "SESSION_NOT_FOUND",
        "bridge session was recycled",
        {"sessionId": params.get("sessionId")},
      )
      continue
    state["send_calls"] = state["send_calls"] + 1
    turn_index = state["next_turn"]
    state["next_turn"] = turn_index + 1
    emit_ok(
      rpc_id,
      {
        "turnId": f"bridge-turn-{turn_index}",
      },
    )
    continue

  if method == "session.interrupt":
    emit_ok(rpc_id, {"ok": True})
    continue

  if method == "session.approval.respond":
    if params.get("approvalId") == "missing-approval":
      emit_error(
        rpc_id,
        "APPROVAL_NOT_FOUND",
        "approval was not found",
        {"approvalId": "missing-approval"},
      )
      continue
    emit_ok(rpc_id, {"ok": True})
    continue

  if method == "session.wait":
    turn_id = params.get("turnId") or "unknown-turn"
    emit_ok(
      rpc_id,
      {
        "turnId": turn_id,
        "status": "completed",
        "usage": {"output_tokens": 1},
      },
    )
    continue

  if method == "session.close":
    emit_ok(rpc_id, {"ok": True})
    continue

  emit_error(
    rpc_id,
    "BAD_REQUEST",
    f"unsupported fake-bridge method: {method}",
  )
"#;

    struct FakeClaudeBridgeHarness {
        _temp_dir: tempfile::TempDir,
        script_path: PathBuf,
        request_log_path: PathBuf,
        env_log_path: PathBuf,
        home_dir: PathBuf,
        config_dir: PathBuf,
        scenario: String,
    }

    impl FakeClaudeBridgeHarness {
        fn new(scenario: &str) -> Self {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let script_path = temp_dir.path().join("fake_claude_bridge.py");
            let request_log_path = temp_dir.path().join("bridge-requests.jsonl");
            let env_log_path = temp_dir.path().join("bridge-env.json");
            let home_dir = temp_dir.path().join("home");
            let config_dir = temp_dir.path().join("claude-config");
            let credentials_path = home_dir.join(".claude").join(".credentials.json");
            let config_path = config_dir.join(".claude.json");

            let mut script_file =
                std::fs::File::create(script_path.as_path()).expect("create fake bridge script");
            script_file
                .write_all(FAKE_BRIDGE_SCRIPT.as_bytes())
                .expect("write fake bridge script");
            std::fs::create_dir_all(
                credentials_path
                    .parent()
                    .expect("credentials parent should resolve"),
            )
            .expect("create fake bridge credentials dir");
            std::fs::create_dir_all(config_dir.as_path()).expect("create fake bridge config dir");
            std::fs::write(
                credentials_path.as_path(),
                r#"{"claudeAiOauth":{"accessToken":"fixture-token","refreshToken":"fixture-token"}}"#,
            )
            .expect("write fake bridge credentials fixture");
            std::fs::write(
                config_path.as_path(),
                r#"{"oauthAccount":{"emailAddress":"fixture@example.com"}}"#,
            )
            .expect("write fake bridge config fixture");

            Self {
                _temp_dir: temp_dir,
                script_path,
                request_log_path,
                env_log_path,
                home_dir,
                config_dir,
                scenario: scenario.to_string(),
            }
        }

        fn provider_with_bridge_env(
            &self,
            gg_mcp: ClaudeGgMcpConfig,
            extra_bridge_env: BTreeMap<String, String>,
        ) -> ClaudeProvider {
            let mut bridge_env = BTreeMap::new();
            bridge_env.insert("FAKE_BRIDGE_SCENARIO".to_string(), self.scenario.clone());
            bridge_env.insert(
                "FAKE_BRIDGE_REQUEST_LOG".to_string(),
                self.request_log_path.display().to_string(),
            );
            bridge_env.insert(
                "FAKE_BRIDGE_ENV_LOG".to_string(),
                self.env_log_path.display().to_string(),
            );
            bridge_env.insert("HOME".to_string(), self.home_dir.display().to_string());
            bridge_env.insert(
                "CLAUDE_CONFIG_DIR".to_string(),
                self.config_dir.display().to_string(),
            );
            for (key, value) in extra_bridge_env {
                bridge_env.insert(key, value);
            }

            ClaudeProvider::new(ClaudeProviderConfig {
                enabled: true,
                config_dir: self.config_dir.clone(),
                bridge_command: "python3".to_string(),
                bridge_args: vec![self.script_path.display().to_string()],
                max_bridges: 1,
                max_sessions_per_bridge: 8,
                request_timeout_ms: 2_000,
                default_wait_timeout_ms: 5_000,
                heartbeat_interval_ms: 120_000,
                heartbeat_failure_threshold: 3,
                gg_mcp,
                bridge_env,
            })
        }

        fn provider(&self, gg_mcp: ClaudeGgMcpConfig) -> ClaudeProvider {
            self.provider_with_bridge_env(gg_mcp, BTreeMap::new())
        }

        fn read_requests(&self) -> Vec<Value> {
            let content = match std::fs::read_to_string(self.request_log_path.as_path()) {
                Ok(content) => content,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
                Err(error) => panic!("read fake bridge request log: {error}"),
            };
            content
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .collect()
        }

        fn read_spawn_env(&self) -> Value {
            let content = std::fs::read_to_string(self.env_log_path.as_path())
                .expect("read fake bridge env log");
            serde_json::from_str(content.as_str()).expect("parse fake bridge env log")
        }
    }

    fn requests_for_method<'a>(requests: &'a [Value], method: &str) -> Vec<&'a Value> {
        requests
            .iter()
            .filter(|request| {
                request
                    .get("method")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == method)
            })
            .collect()
    }

    fn expected_gg_mcp_config(
        server_name: &str,
        command: &str,
        args: &[&str],
        runtime_session_id: &str,
        enable_process_tools: bool,
        gateway_url: Option<&str>,
        gateway_token: Option<&str>,
    ) -> Value {
        let mut env = serde_json::Map::new();
        env.insert(
            "GG_MCP_ENABLE_PROCESS_TOOLS".to_string(),
            Value::String(if enable_process_tools {
                "1".to_string()
            } else {
                "0".to_string()
            }),
        );
        env.insert(
            "GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID".to_string(),
            Value::String("1".to_string()),
        );
        env.insert(
            "GG_MCP_CALLER_AGENT_ID".to_string(),
            Value::String(runtime_session_id.to_string()),
        );
        if let Some(url) = gateway_url {
            env.insert(
                "GG_MCP_GATEWAY_URL".to_string(),
                Value::String(url.to_string()),
            );
        }
        if let Some(token) = gateway_token {
            env.insert(
                "GG_MCP_GATEWAY_TOKEN".to_string(),
                Value::String(token.to_string()),
            );
        }
        if let Some(home) = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.insert(
                "HOME".to_string(),
                Value::String(home.display().to_string()),
            );
        }
        if let Some(cargo_home) = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.insert(
                "CARGO_HOME".to_string(),
                Value::String(cargo_home.display().to_string()),
            );
        }
        if let Some(rustup_home) = std::env::var_os("RUSTUP_HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.insert(
                "RUSTUP_HOME".to_string(),
                Value::String(rustup_home.display().to_string()),
            );
        }

        serde_json::json!({
            "serverName": server_name,
            "callerAgentId": runtime_session_id,
            "command": command,
            "args": args,
            "env": env,
        })
    }

    async fn wait_for_ready_session(manager: &Arc<RuntimeSessionManager>, session_id: &str) {
        for _ in 0..20 {
            let session = manager
                .get_session(session_id)
                .await
                .expect("runtime session should exist");
            if session.status == "ready" && session.active_turn_id.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let session = manager
            .get_session(session_id)
            .await
            .expect("runtime session should exist");
        panic!(
            "session {session_id} did not become ready in time (status={}, active_turn_id={:?})",
            session.status, session.active_turn_id
        );
    }

    #[tokio::test]
    async fn auth_lifecycle_is_runtime_managed() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let bridge_home = temp_dir.path().join("bridge-home");
        let bridge_config_dir = temp_dir.path().join("bridge-config");
        std::fs::create_dir_all(bridge_home.as_path()).expect("create bridge home dir");
        std::fs::create_dir_all(bridge_config_dir.as_path()).expect("create bridge config dir");
        let mut bridge_env = BTreeMap::new();
        bridge_env.insert("HOME".to_string(), bridge_home.display().to_string());
        bridge_env.insert(
            "CLAUDE_CONFIG_DIR".to_string(),
            bridge_config_dir.display().to_string(),
        );
        let provider = ClaudeProvider::new(ClaudeProviderConfig {
            enabled: true,
            config_dir: temp_dir.path().join("claude"),
            bridge_command: "does-not-matter".to_string(),
            bridge_args: Vec::new(),
            max_bridges: 1,
            max_sessions_per_bridge: 1,
            request_timeout_ms: 100,
            default_wait_timeout_ms: 100,
            heartbeat_interval_ms: 10_000,
            heartbeat_failure_threshold: 3,
            gg_mcp: ClaudeGgMcpConfig::default(),
            bridge_env,
        });

        let initial = provider.auth_status().await.expect("auth status");
        assert!(!initial.authenticated);

        let with_key = provider
            .auth_set_api_key("sk-ant-test".to_string())
            .await
            .expect("set api key");
        assert!(with_key.authenticated);
        assert_eq!(with_key.mode.as_deref(), Some("api_key"));
        assert!(provider.api_key_path().exists());

        let with_auth_bundle = provider
            .auth_import_json(serde_json::json!({
                "credentials_json": {
                    "claudeAiOauth": {
                        "accessToken": "abc",
                        "refreshToken": "abc"
                    }
                },
                "config_json": {
                    "projects": {}
                }
            }))
            .await
            .expect("import auth bundle");
        assert!(with_auth_bundle.authenticated);
        assert_eq!(with_auth_bundle.mode.as_deref(), Some("claude_code_oauth"));
        assert!(provider.claude_credentials_path().exists());
        assert!(provider.claude_config_path().exists());

        let logged_out = provider.auth_logout().await.expect("logout");
        assert!(!logged_out.authenticated);
        assert!(!provider.claude_credentials_path().exists());
        assert!(!provider.claude_config_path().exists());
        assert!(!provider.api_key_path().exists());
    }

    #[tokio::test]
    async fn auth_status_uses_host_machine_bridge_oauth_by_default() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let canonical_home = temp_dir.path().join("canonical-home");
        let canonical_config = temp_dir.path().join("canonical-config");
        let canonical_credentials = canonical_home.join(".claude").join(".credentials.json");
        std::fs::create_dir_all(
            canonical_credentials
                .parent()
                .expect("canonical credentials parent"),
        )
        .expect("create canonical credentials dir");
        std::fs::create_dir_all(canonical_config.as_path()).expect("create canonical config dir");
        std::fs::write(
            canonical_credentials.as_path(),
            r#"{"claudeAiOauth":{"accessToken":"bridge-only","refreshToken":"bridge-only"}}"#,
        )
        .expect("write canonical credentials");
        std::fs::write(canonical_config.join(".claude.json"), r#"{"projects":{}}"#)
            .expect("write canonical config");

        let mut bridge_env = BTreeMap::new();
        bridge_env.insert("HOME".to_string(), canonical_home.display().to_string());
        bridge_env.insert(
            "CLAUDE_CONFIG_DIR".to_string(),
            canonical_config.display().to_string(),
        );

        let provider = ClaudeProvider::new(ClaudeProviderConfig {
            enabled: true,
            config_dir: temp_dir.path().join("runtime-claude-config"),
            bridge_command: "does-not-matter".to_string(),
            bridge_args: Vec::new(),
            max_bridges: 1,
            max_sessions_per_bridge: 1,
            request_timeout_ms: 100,
            default_wait_timeout_ms: 100,
            heartbeat_interval_ms: 10_000,
            heartbeat_failure_threshold: 3,
            gg_mcp: ClaudeGgMcpConfig::default(),
            bridge_env,
        });

        let status = provider.auth_status().await.expect("auth status");
        assert!(status.authenticated, "host-mode auth status should be true");
        assert_eq!(status.mode.as_deref(), Some("claude_code_oauth"));
        let detail = status.detail.unwrap_or_default();
        assert!(
            detail.contains("bridge_credentials_present=true"),
            "expected detail to surface bridge credential visibility"
        );
        assert!(
            detail.contains("auth_mode=host_machine"),
            "expected host-mode detail annotation"
        );
    }

    #[tokio::test]
    async fn runtime_managed_mode_allows_explicit_host_bridge_overrides() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let canonical_home = temp_dir.path().join("canonical-home");
        let canonical_config = temp_dir.path().join("canonical-config");
        let canonical_credentials = canonical_home.join(".claude").join(".credentials.json");
        std::fs::create_dir_all(
            canonical_credentials
                .parent()
                .expect("canonical credentials parent"),
        )
        .expect("create canonical credentials dir");
        std::fs::create_dir_all(canonical_config.as_path()).expect("create canonical config dir");
        std::fs::write(
            canonical_credentials.as_path(),
            r#"{"claudeAiOauth":{"accessToken":"bridge-only","refreshToken":"bridge-only"}}"#,
        )
        .expect("write canonical credentials");
        std::fs::write(canonical_config.join(".claude.json"), r#"{"projects":{}}"#)
            .expect("write canonical config");

        let mut bridge_env = BTreeMap::new();
        bridge_env.insert("HOME".to_string(), canonical_home.display().to_string());
        bridge_env.insert(
            "CLAUDE_CONFIG_DIR".to_string(),
            canonical_config.display().to_string(),
        );
        bridge_env.insert(
            "GG_CLAUDE_AUTH_MODE".to_string(),
            "runtime_managed".to_string(),
        );

        let provider = ClaudeProvider::new(ClaudeProviderConfig {
            enabled: true,
            config_dir: temp_dir.path().join("runtime-claude-config"),
            bridge_command: "does-not-matter".to_string(),
            bridge_args: Vec::new(),
            max_bridges: 1,
            max_sessions_per_bridge: 1,
            request_timeout_ms: 100,
            default_wait_timeout_ms: 100,
            heartbeat_interval_ms: 10_000,
            heartbeat_failure_threshold: 3,
            gg_mcp: ClaudeGgMcpConfig::default(),
            bridge_env,
        });
        let status = provider.auth_status().await.expect("auth status");
        assert!(
            status.authenticated,
            "runtime-managed mode should accept explicit bridge HOME/CLAUDE_CONFIG_DIR overrides"
        );
        let detail = status.detail.unwrap_or_default();
        assert!(
            detail.contains("auth_mode=runtime_managed"),
            "expected runtime-managed mode detail annotation"
        );
        assert!(
            detail.contains("bridge_override_active=true"),
            "expected explicit override detail annotation"
        );
    }

    #[tokio::test]
    async fn bridge_spawn_defaults_to_host_machine_home_and_config_resolution() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let provider = harness.provider(ClaudeGgMcpConfig::default());

        provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-default-env".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-default-env".to_string(),
                reason: None,
            })
            .await
            .expect("close session");

        let spawn_env = harness.read_spawn_env();
        let expected_home_display = harness.home_dir.display().to_string();
        let expected_config_display = harness.config_dir.display().to_string();
        assert_eq!(
            spawn_env.get("HOME").and_then(Value::as_str),
            Some(expected_home_display.as_str())
        );
        assert_eq!(
            spawn_env.get("CLAUDE_CONFIG_DIR").and_then(Value::as_str),
            Some(expected_config_display.as_str())
        );
        assert_eq!(
            spawn_env
                .get("CLAUDE_CODE_OAUTH_TOKEN_PRESENT")
                .and_then(Value::as_bool),
            Some(false)
        );
    }

    #[tokio::test]
    async fn bridge_spawn_allows_explicit_passthrough_home_and_config_overrides() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let passthrough_home = harness
            .config_dir
            .parent()
            .expect("provider dir")
            .join("passthrough-home");
        let passthrough_config = harness
            .config_dir
            .parent()
            .expect("provider dir")
            .join("passthrough-config");
        let passthrough_credentials = passthrough_home.join(".claude").join(".credentials.json");
        std::fs::create_dir_all(
            passthrough_credentials
                .parent()
                .expect("passthrough credentials parent"),
        )
        .expect("create passthrough credentials dir");
        std::fs::create_dir_all(passthrough_config.as_path())
            .expect("create passthrough config dir");
        std::fs::write(
            passthrough_credentials.as_path(),
            r#"{"claudeAiOauth":{"accessToken":"passthrough-token","refreshToken":"passthrough-token"}}"#,
        )
        .expect("write passthrough credentials fixture");
        std::fs::write(
            passthrough_config.join(".claude.json"),
            r#"{"oauthAccount":{"emailAddress":"passthrough@example.com"}}"#,
        )
        .expect("write passthrough config fixture");
        let mut bridge_env = BTreeMap::new();
        bridge_env.insert("HOME".to_string(), passthrough_home.display().to_string());
        bridge_env.insert(
            "CLAUDE_CONFIG_DIR".to_string(),
            passthrough_config.display().to_string(),
        );
        let provider = harness.provider_with_bridge_env(ClaudeGgMcpConfig::default(), bridge_env);

        provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-override-env".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-override-env".to_string(),
                reason: None,
            })
            .await
            .expect("close session");

        let spawn_env = harness.read_spawn_env();
        let passthrough_home_display = passthrough_home.display().to_string();
        let passthrough_config_display = passthrough_config.display().to_string();
        assert_eq!(
            spawn_env.get("HOME").and_then(Value::as_str),
            Some(passthrough_home_display.as_str())
        );
        assert_eq!(
            spawn_env.get("CLAUDE_CONFIG_DIR").and_then(Value::as_str),
            Some(passthrough_config_display.as_str())
        );
    }

    #[tokio::test]
    async fn bridge_spawn_does_not_export_oauth_token_by_default() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let provider = harness.provider(ClaudeGgMcpConfig::default());

        provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-oauth-env".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-oauth-env".to_string(),
                reason: None,
            })
            .await
            .expect("close session");

        let spawn_env = harness.read_spawn_env();
        assert_eq!(
            spawn_env
                .get("CLAUDE_CODE_OAUTH_TOKEN_PRESENT")
                .and_then(Value::as_bool),
            Some(false)
        );
    }

    #[tokio::test]
    async fn bridge_spawn_exports_oauth_token_when_explicitly_forced() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let mut bridge_env = BTreeMap::new();
        bridge_env.insert(
            "GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN".to_string(),
            "1".to_string(),
        );
        let provider = harness.provider_with_bridge_env(ClaudeGgMcpConfig::default(), bridge_env);

        provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-forced-oauth-env".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create session");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-forced-oauth-env".to_string(),
                reason: None,
            })
            .await
            .expect("close session");

        let spawn_env = harness.read_spawn_env();
        assert_eq!(
            spawn_env
                .get("CLAUDE_CODE_OAUTH_TOKEN_PRESENT")
                .and_then(Value::as_bool),
            Some(true)
        );
    }

    #[tokio::test]
    async fn bridge_spawn_fails_fast_when_runtime_credentials_missing() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let credentials_path = harness.home_dir.join(".claude").join(".credentials.json");
        std::fs::remove_file(credentials_path.as_path())
            .expect("remove runtime credentials fixture");
        let provider = harness.provider(ClaudeGgMcpConfig::default());

        let result = provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-missing-credentials".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await;

        let error = result.expect_err("create session should fail when credentials are missing");
        let rendered = format!("{error}");
        assert!(
            rendered.contains("Claude OAuth credentials file missing"),
            "expected missing-runtime-credentials fail-fast message, got: {rendered}"
        );
        assert!(
            rendered.contains("/v1/providers/claude/auth/import-json"),
            "expected import-json route guidance, got: {rendered}"
        );
        assert!(
            rendered.contains("/v1/providers/claude/auth/import-file"),
            "expected import-file route guidance, got: {rendered}"
        );
    }

    #[tokio::test]
    async fn bridge_spawn_fails_fast_when_runtime_config_is_malformed() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let config_path = harness.config_dir.join(".claude.json");
        std::fs::write(config_path.as_path(), "{not-valid-json")
            .expect("write malformed config fixture");
        let provider = harness.provider(ClaudeGgMcpConfig::default());

        let result = provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-malformed-config".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await;

        let error = result.expect_err("create session should fail on malformed config json");
        let rendered = format!("{error}");
        assert!(
            rendered.contains("is not valid JSON"),
            "expected malformed-runtime-config fail-fast message, got: {rendered}"
        );
    }

    #[tokio::test]
    async fn bridge_spawn_explicit_api_key_override_bypasses_oauth_preflight() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let credentials_path = harness.home_dir.join(".claude").join(".credentials.json");
        let config_path = harness.config_dir.join(".claude.json");
        std::fs::remove_file(credentials_path.as_path())
            .expect("remove runtime credentials fixture");
        std::fs::write(config_path.as_path(), "{not-valid-json")
            .expect("write malformed config fixture");

        let mut bridge_env = BTreeMap::new();
        bridge_env.insert(
            "ANTHROPIC_API_KEY".to_string(),
            "sk-ant-test-bypass".to_string(),
        );
        let provider = harness.provider_with_bridge_env(ClaudeGgMcpConfig::default(), bridge_env);

        let session = provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-api-key-bypass".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("api key should bypass oauth preflight");
        assert_eq!(session.runtime_session_id, "sess-api-key-bypass");

        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-api-key-bypass".to_string(),
                reason: None,
            })
            .await
            .expect("close session");
    }

    #[test]
    fn claude_auth_paths_use_split_credentials_and_env_config_override() {
        let home_dir = PathBuf::from("/home/alice");
        let config_dir = PathBuf::from("/runtime/claude-config");
        let resolved = resolve_claude_auth_paths(Some(config_dir.clone()), Some(home_dir.clone()))
            .expect("resolved auth paths");

        assert_eq!(
            resolved.credentials_path,
            home_dir.join(".claude").join(".credentials.json")
        );
        assert_eq!(resolved.config_path, config_dir.join(".claude.json"));
        assert_eq!(resolved.config_dir, Some(config_dir));
        assert_eq!(
            resolved.config_source,
            ClaudeConfigResolutionSource::EnvOverride
        );
    }

    #[test]
    fn claude_auth_paths_use_gg_fallback_when_no_override_exists() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home_dir = temp_dir.path().join("home");
        let gg_claude = home_dir.join(".gg").join("claude");
        std::fs::create_dir_all(&gg_claude).expect("create gg claude dir");
        let resolved =
            resolve_claude_auth_paths(None, Some(home_dir.clone())).expect("resolved auth paths");

        assert_eq!(
            resolved.credentials_path,
            home_dir.join(".claude").join(".credentials.json")
        );
        assert_eq!(resolved.config_path, gg_claude.join(".claude.json"));
        assert_eq!(resolved.config_dir, Some(gg_claude));
        assert_eq!(
            resolved.config_source,
            ClaudeConfigResolutionSource::GgFallback
        );
    }

    #[test]
    fn claude_auth_paths_fall_back_to_upstream_defaults() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home_dir = temp_dir.path().join("home");
        std::fs::create_dir_all(&home_dir).expect("create home dir");
        let resolved =
            resolve_claude_auth_paths(None, Some(home_dir.clone())).expect("resolved auth paths");

        assert_eq!(
            resolved.credentials_path,
            home_dir.join(".claude").join(".credentials.json")
        );
        assert_eq!(resolved.config_path, home_dir.join(".claude.json"));
        assert_eq!(resolved.config_dir, None);
        assert_eq!(
            resolved.config_source,
            ClaudeConfigResolutionSource::UpstreamDefault
        );
    }

    #[test]
    fn lane_index_is_stable_for_same_key() {
        let key = "session:test";
        let lane_count = 16;
        let first = stdout_worker_lane_index(key, lane_count);
        let second = stdout_worker_lane_index(key, lane_count);
        assert_eq!(first, second);
        assert!(first < lane_count);
    }

    #[test]
    fn standalone_gg_mcp_server_command_path_is_branch_owned() {
        let executable = PathBuf::from("/opt/gg-runtime/bin/gg-runtime-server");
        let path = sidecar_command_path_from_executable(
            executable.as_path(),
            "gg-mcp-server",
            "gg-mcp-server",
        );
        assert_eq!(
            path,
            PathBuf::from("/opt/gg-runtime/sidecars/gg-mcp-server/gg-mcp-server")
        );
    }

    #[test]
    fn standalone_claude_bridge_command_path_is_branch_owned() {
        let executable = PathBuf::from("/opt/gg-runtime/bin/gg-runtime-server");
        let path = sidecar_command_path_from_executable(
            executable.as_path(),
            "claude-bridge",
            "claude-bridge",
        );
        assert_eq!(
            path,
            PathBuf::from("/opt/gg-runtime/sidecars/claude-bridge/claude-bridge")
        );
    }

    #[test]
    fn sidecar_paths_resolve_from_runtime_install_layout_not_source_tree() {
        let executable = PathBuf::from("/opt/gg-runtime/bin/gg-runtime-server");
        let claude_path = sidecar_command_path_for_executable_with_workspace_roots(
            executable.as_path(),
            "claude-bridge",
            "claude-bridge",
            "claude-bridge-dev",
            &[],
        );
        let gg_mcp_path = sidecar_command_path_for_executable_with_workspace_roots(
            executable.as_path(),
            "gg-mcp-server",
            "gg-mcp-server",
            "gg-mcp-server-dev",
            &[],
        );
        assert_eq!(
            claude_path,
            PathBuf::from("/opt/gg-runtime/sidecars/claude-bridge/claude-bridge")
        );
        assert_eq!(
            gg_mcp_path,
            PathBuf::from("/opt/gg-runtime/sidecars/gg-mcp-server/gg-mcp-server")
        );
        assert!(!claude_path.ends_with("src/main.ts"));
        assert!(!gg_mcp_path.to_string_lossy().contains("Cargo.toml"));
    }

    #[test]
    fn workspace_root_is_detected_for_cargo_test_binary_paths() {
        let executable =
            PathBuf::from("/repo/worktree/target/debug/deps/runtime_provider_claude-abcdef");
        let workspace_root =
            workspace_root_from_target_binary_path(executable.as_path()).expect("workspace root");
        assert_eq!(workspace_root, PathBuf::from("/repo/worktree"));
    }

    #[test]
    fn sidecar_paths_fallback_to_workspace_sidecars_for_target_binaries() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let repo_root = temp_dir.path();
        let target_bin = repo_root
            .join("target")
            .join("debug")
            .join("deps")
            .join("runtime-provider-claude-test-bin");
        std::fs::create_dir_all(target_bin.parent().expect("parent")).expect("create target dir");
        std::fs::write(target_bin.as_path(), b"").expect("write target bin placeholder");

        let claude_launcher = repo_root
            .join("sidecars")
            .join("claude-bridge")
            .join("bin")
            .join("claude-bridge-dev");
        let gg_mcp_launcher = repo_root
            .join("sidecars")
            .join("gg-mcp-server")
            .join("bin")
            .join("gg-mcp-server-dev");
        std::fs::create_dir_all(claude_launcher.parent().expect("claude parent"))
            .expect("create claude sidecar dir");
        std::fs::create_dir_all(gg_mcp_launcher.parent().expect("gg mcp parent"))
            .expect("create gg mcp sidecar dir");
        std::fs::write(claude_launcher.as_path(), b"").expect("write claude launcher placeholder");
        std::fs::write(gg_mcp_launcher.as_path(), b"").expect("write gg mcp launcher placeholder");

        let resolved_claude = sidecar_command_path_for_executable(
            target_bin.as_path(),
            "claude-bridge",
            "claude-bridge",
            "claude-bridge-dev",
        );
        let resolved_gg_mcp = sidecar_command_path_for_executable(
            target_bin.as_path(),
            "gg-mcp-server",
            "gg-mcp-server",
            "gg-mcp-server-dev",
        );

        assert_eq!(
            resolved_claude,
            repo_root
                .join("sidecars")
                .join("claude-bridge")
                .join("bin")
                .join("claude-bridge-dev")
        );
        assert_eq!(
            resolved_gg_mcp,
            repo_root
                .join("sidecars")
                .join("gg-mcp-server")
                .join("bin")
                .join("gg-mcp-server-dev")
        );
    }

    #[test]
    fn sidecar_paths_fallback_to_workspace_roots_for_cargo_cache_binaries() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let repo_root = temp_dir.path().join("workspace");
        let cargo_cache_bin = temp_dir
            .path()
            .join("cargo-build")
            .join("debug")
            .join("deps")
            .join("runtime-server-test-bin");
        std::fs::create_dir_all(cargo_cache_bin.parent().expect("parent"))
            .expect("create cache bin dir");
        std::fs::write(cargo_cache_bin.as_path(), b"").expect("write cache bin placeholder");

        let claude_launcher = repo_root
            .join("sidecars")
            .join("claude-bridge")
            .join("bin")
            .join("claude-bridge-dev");
        std::fs::create_dir_all(claude_launcher.parent().expect("claude parent"))
            .expect("create claude launcher dir");
        std::fs::write(claude_launcher.as_path(), b"").expect("write claude launcher placeholder");

        let resolved_claude = sidecar_command_path_for_executable_with_workspace_roots(
            cargo_cache_bin.as_path(),
            "claude-bridge",
            "claude-bridge",
            "claude-bridge-dev",
            &[repo_root.clone()],
        );
        assert_eq!(resolved_claude, claude_launcher);
    }

    #[test]
    fn map_bridge_error_preserves_not_found_and_protocol_categories() {
        let session_not_found = map_bridge_error(&serde_json::json!({
            "code": "SESSION_NOT_FOUND",
            "message": "session is gone",
        }));
        assert!(matches!(session_not_found, RuntimeError::NotFound(_)));

        let turn_not_found = map_bridge_error(&serde_json::json!({
            "code": "TURN_NOT_FOUND",
            "message": "turn is gone",
        }));
        assert!(matches!(turn_not_found, RuntimeError::NotFound(_)));

        let approval_not_found = map_bridge_error(&serde_json::json!({
            "code": "APPROVAL_NOT_FOUND",
            "message": "approval is gone",
        }));
        assert!(matches!(approval_not_found, RuntimeError::NotFound(_)));

        let protocol = map_bridge_error(&serde_json::json!({
            "code": "PROTOCOL_VIOLATION",
            "message": "bad payload",
        }));
        assert!(matches!(protocol, RuntimeError::ProtocolViolation(_)));

        let bad_request = map_bridge_error(&serde_json::json!({
            "code": "BAD_REQUEST",
            "message": "invalid field",
        }));
        assert!(matches!(bad_request, RuntimeError::InvalidState(_)));
    }

    #[test]
    fn merge_assistant_text_into_usage_sets_last_message() {
        let merged = merge_assistant_text_into_usage(
            Some(serde_json::json!({
                "inputTokens": 10,
                "outputTokens": 20
            })),
            Some("hello from claude".to_string()),
        )
        .expect("merged usage");
        assert_eq!(merged["last_message"], "hello from claude");
        assert_eq!(merged["assistant_text"], "hello from claude");
    }

    #[test]
    fn merge_assistant_text_into_usage_creates_usage_when_missing() {
        let merged = merge_assistant_text_into_usage(None, Some("terminal text only".to_string()))
            .expect("merged usage");
        assert_eq!(merged["last_message"], "terminal text only");
        assert_eq!(merged["assistant_text"], "terminal text only");
    }

    #[tokio::test]
    async fn real_adapter_contract_covers_create_resume_send_interrupt_approval_wait_and_close() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let provider = harness.provider(ClaudeGgMcpConfig::default());

        let created = provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-create".to_string(),
                model: Some("claude-sonnet-5".to_string()),
                cwd: Some("/tmp/project".to_string()),
                permission_mode: Some("default".to_string()),
                metadata: None,
            })
            .await
            .expect("create session");
        assert_eq!(created.runtime_session_id, "sess-create");
        assert_eq!(created.provider_session_ref, "provider-session-1");

        let ack = provider
            .send_turn(ProviderSendTurnRequest {
                runtime_session_id: "sess-create".to_string(),
                turn_id: "runtime-turn-1".to_string(),
                input: vec![serde_json::json!({
                    "type": "text",
                    "text": "hello"
                })],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send turn");
        assert_eq!(ack.runtime_session_id, "sess-create");
        assert_eq!(ack.turn_id, "runtime-turn-1");

        provider
            .interrupt_turn(ProviderInterruptTurnRequest {
                runtime_session_id: "sess-create".to_string(),
                turn_id: ack.turn_id.clone(),
            })
            .await
            .expect("interrupt turn");

        provider
            .respond_approval(ProviderApprovalResponseRequest {
                runtime_session_id: "sess-create".to_string(),
                turn_id: ack.turn_id.clone(),
                approval_id: "approval-1".to_string(),
                decision: "accept".to_string(),
                payload: Some(serde_json::json!({
                    "type": "text",
                    "text": "updated"
                })),
            })
            .await
            .expect("respond approval");

        let result = provider
            .wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: "sess-create".to_string(),
                turn_id: ack.turn_id.clone(),
                timeout_ms: Some(500),
            })
            .await
            .expect("wait for turn");
        assert_eq!(result.runtime_session_id, "sess-create");
        assert_eq!(result.turn_id, "runtime-turn-1");
        assert_eq!(result.status, ProviderTurnStatus::Completed);

        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-create".to_string(),
                reason: Some("contract_complete".to_string()),
            })
            .await
            .expect("close session");

        let resumed = provider
            .resume_session(ProviderResumeSessionRequest {
                runtime_session_id: "sess-resume".to_string(),
                provider_session_ref: "provider-session-upstream".to_string(),
                canonical_provider_session_ref: Some("canonical-upstream".to_string()),
                cwd: Some("/tmp/resumed".to_string()),
                metadata: None,
            })
            .await
            .expect("resume session");
        assert_eq!(resumed.runtime_session_id, "sess-resume");
        assert_eq!(resumed.provider_session_ref, "provider-session-upstream");
        assert_eq!(
            resumed.canonical_provider_session_ref.as_deref(),
            Some("canonical-upstream")
        );

        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-resume".to_string(),
                reason: Some("contract_complete".to_string()),
            })
            .await
            .expect("close resumed session");

        let requests = harness.read_requests();
        assert_eq!(requests_for_method(&requests, "session.create").len(), 1);
        assert_eq!(requests_for_method(&requests, "session.resume").len(), 1);
        assert_eq!(requests_for_method(&requests, "session.send").len(), 1);
        assert_eq!(requests_for_method(&requests, "session.interrupt").len(), 1);
        assert_eq!(
            requests_for_method(&requests, "session.approval.respond").len(),
            1
        );
        assert_eq!(requests_for_method(&requests, "session.wait").len(), 1);
        assert_eq!(requests_for_method(&requests, "session.close").len(), 2);
    }

    #[tokio::test]
    async fn create_and_resume_include_expected_gg_mcp_server_shape() {
        let harness = FakeClaudeBridgeHarness::new("normal");
        let provider = harness.provider(ClaudeGgMcpConfig {
            enabled: true,
            server_name: "gg".to_string(),
            command: "gg-mcp-server".to_string(),
            args: vec!["--stdio".to_string()],
            enable_process_tools: true,
            gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
            gateway_token: Some("bridge-token".to_string()),
        });

        provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-create".to_string(),
                model: None,
                cwd: Some("/tmp/create".to_string()),
                permission_mode: Some("default".to_string()),
                metadata: None,
            })
            .await
            .expect("create session");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-create".to_string(),
                reason: Some("done".to_string()),
            })
            .await
            .expect("close create session");

        provider
            .resume_session(ProviderResumeSessionRequest {
                runtime_session_id: "sess-resume".to_string(),
                provider_session_ref: "provider-resume".to_string(),
                canonical_provider_session_ref: Some("canonical-resume".to_string()),
                cwd: Some("/tmp/resume".to_string()),
                metadata: None,
            })
            .await
            .expect("resume session");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-resume".to_string(),
                reason: Some("done".to_string()),
            })
            .await
            .expect("close resume session");

        let requests = harness.read_requests();
        let create = requests_for_method(&requests, "session.create");
        let resume = requests_for_method(&requests, "session.resume");
        assert_eq!(create.len(), 1);
        assert_eq!(resume.len(), 1);

        let create_gg_mcp = create[0]
            .get("params")
            .and_then(|params| params.get("ggMcpServer"))
            .expect("create request should include ggMcpServer");
        assert_eq!(
            create_gg_mcp,
            &expected_gg_mcp_config(
                "gg",
                "gg-mcp-server",
                &["--stdio"],
                "sess-create",
                true,
                Some("http://127.0.0.1:8787/v1/mcp"),
                Some("bridge-token"),
            )
        );

        let resume_gg_mcp = resume[0]
            .get("params")
            .and_then(|params| params.get("ggMcpServer"))
            .expect("resume request should include ggMcpServer");
        assert_eq!(
            resume_gg_mcp,
            &expected_gg_mcp_config(
                "gg",
                "gg-mcp-server",
                &["--stdio"],
                "sess-resume",
                true,
                Some("http://127.0.0.1:8787/v1/mcp"),
                Some("bridge-token"),
            )
        );
    }

    #[tokio::test]
    async fn create_and_resume_retry_with_gg_mcp_when_bridge_requires_it() {
        let harness = FakeClaudeBridgeHarness::new("require_gg_mcp");
        let provider = harness.provider(ClaudeGgMcpConfig {
            enabled: false,
            server_name: "gg".to_string(),
            command: "gg-mcp-server".to_string(),
            args: vec!["--stdio".to_string()],
            enable_process_tools: false,
            gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
            gateway_token: Some("bridge-token".to_string()),
        });

        provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "sess-create".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create with compatibility retry");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-create".to_string(),
                reason: None,
            })
            .await
            .expect("close create");

        provider
            .resume_session(ProviderResumeSessionRequest {
                runtime_session_id: "sess-resume".to_string(),
                provider_session_ref: "provider-resume".to_string(),
                canonical_provider_session_ref: Some("canonical-resume".to_string()),
                cwd: None,
                metadata: None,
            })
            .await
            .expect("resume with compatibility retry");
        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: "sess-resume".to_string(),
                reason: None,
            })
            .await
            .expect("close resume");

        let requests = harness.read_requests();
        let create = requests_for_method(&requests, "session.create");
        let resume = requests_for_method(&requests, "session.resume");
        assert_eq!(create.len(), 2, "create should be retried once");
        assert_eq!(resume.len(), 2, "resume should be retried once");

        assert!(
            create[0]
                .get("params")
                .and_then(|params| params.get("ggMcpServer"))
                .is_none(),
            "initial create call should omit ggMcpServer when disabled",
        );
        assert!(
            create[1]
                .get("params")
                .and_then(|params| params.get("ggMcpServer"))
                .is_some(),
            "retry create call should include ggMcpServer",
        );

        assert!(
            resume[0]
                .get("params")
                .and_then(|params| params.get("ggMcpServer"))
                .is_none(),
            "initial resume call should omit ggMcpServer when disabled",
        );
        assert!(
            resume[1]
                .get("params")
                .and_then(|params| params.get("ggMcpServer"))
                .is_some(),
            "retry resume call should include ggMcpServer",
        );
    }

    #[tokio::test]
    async fn runtime_manager_recovers_send_turn_after_bridge_session_not_found() {
        let harness = FakeClaudeBridgeHarness::new("send_not_found_once");
        let provider = Arc::new(harness.provider(ClaudeGgMcpConfig::default()));

        let mut registry = ProviderRegistry::new();
        registry.register(provider).expect("register provider");

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp_dir.path().join("runtime.sqlite3"),
        }));
        store.initialize().await.expect("initialize sqlite store");

        let manager = Arc::new(
            RuntimeSessionManager::new(store, Arc::new(registry), 256)
                .expect("construct runtime session manager"),
        );

        let session = manager
            .create_session(CreateSessionInput {
                provider: ProviderKind::Claude,
                model: Some("claude-sonnet-5".to_string()),
                cwd: Some("/tmp/runtime".to_string()),
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create runtime session");

        let send = manager
            .send_turn(
                session.id.as_str(),
                SendTurnInput {
                    input: vec![serde_json::json!({
                        "type": "text",
                        "text": "recover this turn"
                    })],
                    expected_turn_id: None,
                    permission_mode: None,
                },
            )
            .await
            .expect("send turn should recover after session_not_found");
        assert_eq!(send.status, "in_progress");

        wait_for_ready_session(&manager, session.id.as_str()).await;

        let requests = harness.read_requests();
        assert_eq!(requests_for_method(&requests, "session.send").len(), 2);
        assert_eq!(requests_for_method(&requests, "session.resume").len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires local Claude auth sources at ~/.claude/.credentials.json and ~/.gg/claude/.claude.json (or ~/.claude.json fallback)"]
    async fn ignored_real_claude_smoke_with_standalone_bridge() {
        let home_dir = std::env::var_os("HOME")
            .map(PathBuf::from)
            .expect("HOME must be set for Claude smoke");
        let credentials_source_path = std::env::var("GG_CLAUDE_SMOKE_CREDENTIALS_SOURCE")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| home_dir.join(".claude").join(".credentials.json"));
        let config_source_path = std::env::var("GG_CLAUDE_SMOKE_CONFIG_SOURCE")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let gg_claude_config = home_dir.join(".gg").join("claude").join(".claude.json");
                if gg_claude_config.exists() {
                    gg_claude_config
                } else {
                    home_dir.join(".claude.json")
                }
            });
        assert!(
            credentials_source_path.exists(),
            "Claude smoke credentials source path must exist: {}",
            credentials_source_path.display()
        );
        assert!(
            config_source_path.exists(),
            "Claude smoke config source path must exist: {}",
            config_source_path.display()
        );

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let config_dir = temp_dir.path().join("claude-config");
        std::fs::create_dir_all(config_dir.as_path()).expect("create smoke config dir");
        let runtime_home = temp_dir.path().join("home");
        let runtime_credentials_path = runtime_home.join(".claude").join(".credentials.json");
        std::fs::create_dir_all(
            runtime_credentials_path
                .parent()
                .expect("runtime credentials parent"),
        )
        .expect("create runtime credentials dir");
        std::fs::copy(
            credentials_source_path.as_path(),
            runtime_credentials_path.as_path(),
        )
        .expect("stage Claude credentials into runtime-managed home");
        std::fs::copy(
            config_source_path.as_path(),
            config_dir.join(".claude.json"),
        )
        .expect("stage Claude config into runtime-managed config dir");

        let provider = ClaudeProvider::new(ClaudeProviderConfig {
            enabled: true,
            config_dir,
            bridge_command: standalone_claude_bridge_command_path()
                .display()
                .to_string(),
            bridge_args: Vec::new(),
            max_bridges: 1,
            max_sessions_per_bridge: 1,
            request_timeout_ms: 30_000,
            default_wait_timeout_ms: 120_000,
            heartbeat_interval_ms: 30_000,
            heartbeat_failure_threshold: 3,
            gg_mcp: ClaudeGgMcpConfig {
                enabled: false,
                ..ClaudeGgMcpConfig::default()
            },
            bridge_env: BTreeMap::new(),
        });

        let created = provider
            .create_session(ProviderCreateSessionRequest {
                runtime_session_id: "smoke-claude-runtime-session".to_string(),
                model: Some("claude-sonnet-5".to_string()),
                cwd: Some(
                    std::env::current_dir()
                        .expect("current dir")
                        .display()
                        .to_string(),
                ),
                permission_mode: Some("default".to_string()),
                metadata: None,
            })
            .await
            .expect("create real Claude session");

        let ack = provider
            .send_turn(ProviderSendTurnRequest {
                runtime_session_id: created.runtime_session_id.clone(),
                turn_id: "smoke-turn-1".to_string(),
                input: vec![serde_json::json!({
                    "type": "text",
                    "text": "Return exactly: smoke_ok"
                })],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send real Claude turn");

        let result = provider
            .wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: created.runtime_session_id.clone(),
                turn_id: ack.turn_id,
                timeout_ms: Some(120_000),
            })
            .await
            .expect("wait for real Claude turn");
        assert_eq!(result.status, ProviderTurnStatus::Completed);

        provider
            .close_session(ProviderCloseSessionRequest {
                runtime_session_id: created.runtime_session_id,
                reason: Some("smoke_complete".to_string()),
            })
            .await
            .expect("close real Claude smoke session");
    }
}
