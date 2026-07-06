use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use runtime_core::{ProviderTurnResult, RuntimeError};
use serde_json::Value;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock};

use crate::auth::{bridge_session_key, claude_smoke_debug_enabled, read_claude_oauth_access_token};
use crate::bridge::{
    fail_bridge, send_bridge_request, spawn_heartbeat_task, spawn_stderr_task,
    spawn_stdin_writer_task, spawn_stdout_task, spawn_stdout_worker_lanes,
};
use crate::config::{ClaudeProviderConfig, CLAUDE_BRIDGE_STDIN_QUEUE_CAPACITY};

#[derive(Clone, Debug)]
pub struct ClaudeProvider {
    pub(crate) inner: Arc<ClaudeProviderInner>,
}

#[derive(Debug)]
pub(crate) struct ClaudeProviderInner {
    pub(crate) config: ClaudeProviderConfig,
    pub(crate) next_bridge_instance_id: AtomicU64,
    pub(crate) next_bridge_selection: AtomicU64,
    pub(crate) next_request_id: AtomicU64,
    pub(crate) bridges: RwLock<BTreeMap<u64, Arc<ClaudeBridgeHandle>>>,
    pub(crate) bridge_allocation_lock: Mutex<()>,
    pub(crate) sessions: RwLock<BTreeMap<String, Arc<ClaudeSessionHandle>>>,
    pub(crate) sessions_by_bridge_key: RwLock<BTreeMap<String, Arc<ClaudeSessionHandle>>>,
}

#[derive(Debug)]
pub(crate) struct ClaudeSessionHandle {
    pub(crate) runtime_session_id: String,
    pub(crate) bridge_session_id: String,
    pub(crate) provider_session_ref: RwLock<String>,
    pub(crate) canonical_provider_session_ref: RwLock<Option<String>>,
    pub(crate) bridge: Arc<ClaudeBridgeHandle>,
    pub(crate) active_turn_id: RwLock<Option<String>>,
    pub(crate) bridge_turn_by_runtime_turn: Mutex<BTreeMap<String, String>>,
    pub(crate) runtime_turn_by_bridge_turn: Mutex<BTreeMap<String, String>>,
    pub(crate) completed_turns: Mutex<BTreeMap<String, ProviderTurnResult>>,
}

#[derive(Debug)]
pub(crate) struct ClaudeBridgeHandle {
    pub(crate) instance_id: u64,
    pub(crate) process: Mutex<ClaudeBridgeProcessState>,
    pub(crate) pending_requests: Mutex<HashMap<String, oneshot::Sender<RpcResponseResult>>>,
    pub(crate) writer_tx: mpsc::Sender<OutboundJsonLine>,
    pub(crate) writer_shutdown: Notify,
    pub(crate) closed: AtomicBool,
    pub(crate) shutdown_requested: AtomicBool,
    pub(crate) last_event_seq_by_session: Mutex<BTreeMap<String, u64>>,
}

#[derive(Debug)]
pub(crate) struct ClaudeBridgeProcessState {
    pub(crate) child: Child,
    pub(crate) closed: bool,
}

pub(crate) type OutboundJsonLine = Vec<u8>;
pub(crate) type RpcResponseResult = Result<Value, RuntimeError>;

#[derive(Debug)]
pub(crate) struct ClaudeBridgeEventWorkItem {
    pub(crate) payload: Value,
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

    pub(crate) fn build_gg_mcp_server_session_config(&self, runtime_session_id: &str) -> Value {
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

    pub(crate) async fn acquire_bridge_for_new_session(
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

    pub(crate) async fn get_session(
        &self,
        runtime_session_id: &str,
    ) -> Result<Arc<ClaudeSessionHandle>, RuntimeError> {
        let sessions = self.inner.sessions.read().await;
        sessions
            .get(runtime_session_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("claude session {runtime_session_id}")))
    }

    pub(crate) async fn insert_session(
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

    pub(crate) async fn remove_session(
        &self,
        runtime_session_id: &str,
    ) -> Option<Arc<ClaudeSessionHandle>> {
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

    pub(crate) async fn shutdown_bridges_if_idle(&self) {
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

    pub(crate) async fn resolve_bridge_turn_id(
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

    pub(crate) async fn resolve_runtime_turn_id(
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
