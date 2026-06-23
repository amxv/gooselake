use std::collections::{BTreeMap, HashMap};
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
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex, RwLock};

const DEFAULT_PROVIDER_DIR: &str = ".gg-runtime/providers/acp";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 300;
const DEFAULT_PROTOCOL_VERSION: u64 = 1;
const STDERR_TAIL_MAX_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpProviderConfig {
    pub enabled: bool,
    pub provider_dir: PathBuf,
    pub max_instances: usize,
    pub max_sessions_per_instance: usize,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub transport: String,
    pub request_timeout_secs: u64,
    pub wait_timeout_secs: u64,
    pub gg_mcp_enabled: bool,
    pub gg_mcp_server_name: String,
    pub gg_mcp_command: String,
    pub gg_mcp_args: Vec<String>,
    pub gg_mcp_enable_process_tools: bool,
    pub gg_mcp_gateway_url: Option<String>,
    pub gg_mcp_gateway_token: Option<String>,
}

impl Default for AcpProviderConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider_dir: PathBuf::from(DEFAULT_PROVIDER_DIR),
            max_instances: 4,
            max_sessions_per_instance: 4,
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            transport: "stdio".to_string(),
            request_timeout_secs: DEFAULT_REQUEST_TIMEOUT_SECS,
            wait_timeout_secs: DEFAULT_WAIT_TIMEOUT_SECS,
            gg_mcp_enabled: true,
            gg_mcp_server_name: "gg".to_string(),
            gg_mcp_command: "gg-mcp-server".to_string(),
            gg_mcp_args: Vec::new(),
            gg_mcp_enable_process_tools: true,
            gg_mcp_gateway_url: None,
            gg_mcp_gateway_token: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingApprovalTurn {
    turn_id: String,
    input: Vec<Value>,
    expected_turn_id: Option<String>,
    permission_mode: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct AcpAgentCapabilities {
    load_session: bool,
    resume_session: bool,
    close_session: bool,
}

#[derive(Debug, Clone)]
struct AcpActiveTurnState {
    runtime_turn_id: String,
    cancelled: Arc<AtomicBool>,
    assistant_chunks: Arc<Mutex<Vec<String>>>,
    last_message_id: Arc<Mutex<Option<String>>>,
    usage_update: Arc<Mutex<Option<Value>>>,
    tool_calls: Arc<Mutex<BTreeMap<String, Value>>>,
}

impl AcpActiveTurnState {
    fn new(runtime_turn_id: String) -> Self {
        Self {
            runtime_turn_id,
            cancelled: Arc::new(AtomicBool::new(false)),
            assistant_chunks: Arc::new(Mutex::new(Vec::new())),
            last_message_id: Arc::new(Mutex::new(None)),
            usage_update: Arc::new(Mutex::new(None)),
            tool_calls: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[derive(Debug, Default)]
struct AcpSessionState {
    provider_session_ref: String,
    active_turn: Option<AcpActiveTurnState>,
    pending_approvals: HashMap<String, PendingApprovalTurn>,
    completed_turns: HashMap<String, ProviderTurnResult>,
    waiters: HashMap<String, Vec<oneshot::Sender<ProviderTurnResult>>>,
}

#[derive(Debug)]
struct AcpConnection {
    child: Mutex<Child>,
    stdin: Mutex<BufWriter<ChildStdin>>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<Result<Value, RuntimeError>>>>,
    next_request_id: AtomicU64,
    closed: AtomicBool,
    capabilities: RwLock<AcpAgentCapabilities>,
    stderr_tail: Mutex<String>,
}

#[derive(Debug)]
struct AcpProviderInner {
    config: AcpProviderConfig,
    connection: Mutex<Option<Arc<AcpConnection>>>,
    sessions: RwLock<HashMap<String, AcpSessionState>>,
}

impl Drop for AcpProviderInner {
    fn drop(&mut self) {
        let Ok(mut slot) = self.connection.try_lock() else {
            return;
        };
        let Some(connection) = slot.take() else {
            return;
        };
        let Ok(mut child) = connection.child.try_lock() else {
            return;
        };
        let _ = child.start_kill();
    }
}

#[derive(Clone, Debug)]
pub struct AcpProvider {
    inner: Arc<AcpProviderInner>,
}

impl AcpProvider {
    pub fn new(config: AcpProviderConfig) -> Self {
        Self {
            inner: Arc::new(AcpProviderInner {
                config: AcpProviderConfig {
                    provider_dir: absolutize_path(config.provider_dir.as_path()),
                    ..config
                },
                connection: Mutex::new(None),
                sessions: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub fn provider_dir(&self) -> &Path {
        self.inner.config.provider_dir.as_path()
    }

    pub fn config(&self) -> &AcpProviderConfig {
        &self.inner.config
    }

    fn runtime_subdirs(&self) -> [PathBuf; 3] {
        [
            self.provider_dir().to_path_buf(),
            self.provider_dir().join("instances"),
            self.provider_dir().join("sessions"),
        ]
    }

    fn validate_base_config(&self) -> Result<(), RuntimeError> {
        if self.inner.config.transport.trim() != "stdio" {
            return Err(RuntimeError::Configuration(format!(
                "acp transport '{}' is unsupported; expected stdio",
                self.inner.config.transport
            )));
        }
        if self.inner.config.max_instances == 0 {
            return Err(RuntimeError::Configuration(
                "acp max_instances must be greater than zero".to_string(),
            ));
        }
        if self.inner.config.max_sessions_per_instance == 0 {
            return Err(RuntimeError::Configuration(
                "acp max_sessions_per_instance must be greater than zero".to_string(),
            ));
        }
        if self.inner.config.request_timeout_secs == 0 {
            return Err(RuntimeError::Configuration(
                "acp request_timeout_secs must be greater than zero".to_string(),
            ));
        }
        if self.inner.config.wait_timeout_secs == 0 {
            return Err(RuntimeError::Configuration(
                "acp wait_timeout_secs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    fn configured_command(&self) -> Result<String, RuntimeError> {
        let command = self
            .inner
            .config
            .command
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RuntimeError::Configuration("acp command is not configured".to_string())
            })?;
        Ok(command.to_string())
    }

    async fn ensure_provider_enabled(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap("acp provider disabled".to_string()));
        }
        self.validate_base_config()?;
        self.configured_command()?;
        self.ensure_runtime_dirs().await?;
        Ok(())
    }

    async fn ensure_runtime_dirs(&self) -> Result<(), RuntimeError> {
        for dir in self.runtime_subdirs() {
            tokio::fs::create_dir_all(&dir).await.map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create acp provider directory {}: {error}",
                    dir.display()
                ))
            })?;
        }
        Ok(())
    }

    fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.inner.config.request_timeout_secs.max(1))
    }

    fn wait_timeout(&self, timeout_ms: Option<u64>) -> Duration {
        match timeout_ms {
            Some(value) => Duration::from_millis(value.max(1)),
            None => Duration::from_secs(self.inner.config.wait_timeout_secs.max(1)),
        }
    }

    fn max_session_capacity(&self) -> usize {
        self.inner
            .config
            .max_instances
            .saturating_mul(self.inner.config.max_sessions_per_instance)
    }

    async fn reserve_session_slot(&self, runtime_session_id: &str) -> Result<(), RuntimeError> {
        let mut sessions = self.inner.sessions.write().await;
        if sessions.contains_key(runtime_session_id) {
            return Ok(());
        }

        let capacity = self.max_session_capacity();
        if sessions.len() >= capacity {
            return Err(RuntimeError::InvalidState(format!(
                "acp session capacity exceeded ({capacity} total sessions from max_instances={} * max_sessions_per_instance={})",
                self.inner.config.max_instances, self.inner.config.max_sessions_per_instance
            )));
        }

        sessions.insert(runtime_session_id.to_string(), AcpSessionState::default());
        Ok(())
    }

    async fn release_session_slot(&self, runtime_session_id: &str) {
        let mut sessions = self.inner.sessions.write().await;
        sessions.remove(runtime_session_id);
    }

    async fn activate_reserved_session(
        &self,
        runtime_session_id: &str,
        provider_session_ref: String,
    ) -> Result<(), RuntimeError> {
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions.get_mut(runtime_session_id).ok_or_else(|| {
            RuntimeError::InvalidState(format!(
                "reserved acp session {} disappeared before activation",
                runtime_session_id
            ))
        })?;
        session.provider_session_ref = provider_session_ref;
        Ok(())
    }

    async fn shutdown_connection(&self, kill_if_running: bool) {
        let connection = {
            let mut slot = self.inner.connection.lock().await;
            slot.take()
        };
        if let Some(connection) = connection {
            connection.shutdown(kill_if_running).await;
        }
    }

    async fn shutdown_connection_if_idle(&self) {
        let is_idle = {
            let sessions = self.inner.sessions.read().await;
            sessions.is_empty()
        };
        if is_idle {
            self.shutdown_connection(true).await;
        }
    }

    async fn reap_connection_if_current_and_closed(&self, current: &Arc<AcpConnection>) {
        let should_reap = {
            let slot = self.inner.connection.lock().await;
            slot.as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, current))
                && current.closed.load(Ordering::SeqCst)
        };
        if should_reap {
            self.shutdown_connection(false).await;
        }
    }

    fn build_gg_mcp_server_config(&self, runtime_session_id: &str) -> Option<Value> {
        if !self.inner.config.gg_mcp_enabled {
            return None;
        }

        let mut env = Vec::new();
        env.push(json!({
            "name": "GG_MCP_ENABLE_PROCESS_TOOLS",
            "value": if self.inner.config.gg_mcp_enable_process_tools {
                "1"
            } else {
                "0"
            },
        }));
        env.push(json!({
            "name": "GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID",
            "value": "1",
        }));
        env.push(json!({
            "name": "GG_MCP_CALLER_AGENT_ID",
            "value": runtime_session_id,
        }));
        if let Some(gateway_url) = self
            .inner
            .config
            .gg_mcp_gateway_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.push(json!({
                "name": "GG_MCP_GATEWAY_URL",
                "value": gateway_url,
            }));
        }
        if let Some(gateway_token) = self
            .inner
            .config
            .gg_mcp_gateway_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            env.push(json!({
                "name": "GG_MCP_GATEWAY_TOKEN",
                "value": gateway_token,
            }));
        }
        if let Some(home) = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.push(json!({
                "name": "HOME",
                "value": home.display().to_string(),
            }));
        }
        if let Some(cargo_home) = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.push(json!({
                "name": "CARGO_HOME",
                "value": cargo_home.display().to_string(),
            }));
        }

        Some(json!({
            "name": self.inner.config.gg_mcp_server_name,
            "command": self.inner.config.gg_mcp_command,
            "args": self.inner.config.gg_mcp_args,
            "env": env,
        }))
    }

    fn build_mcp_servers(&self, runtime_session_id: &str) -> Value {
        match self.build_gg_mcp_server_config(runtime_session_id) {
            Some(server) => Value::Array(vec![server]),
            None => Value::Array(Vec::new()),
        }
    }

    async fn ensure_connection(&self) -> Result<Arc<AcpConnection>, RuntimeError> {
        self.ensure_provider_enabled().await?;

        let mut slot = self.inner.connection.lock().await;
        if let Some(existing) = slot.as_ref() {
            if !existing.closed.load(Ordering::SeqCst) {
                return Ok(Arc::clone(existing));
            }
        }

        let connection = AcpConnection::spawn(self.clone()).await?;
        *slot = Some(Arc::clone(&connection));
        Ok(connection)
    }

    fn build_prompt_blocks(input: &[Value]) -> Vec<Value> {
        let mut blocks = Vec::new();
        for item in input {
            if let Some(text) = item
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                blocks.push(json!({
                    "type": "text",
                    "text": text,
                }));
                continue;
            }
            if let Some(raw) = item
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                blocks.push(json!({
                    "type": "text",
                    "text": raw,
                }));
                continue;
            }
            if let Some(kind) = item.get("type").and_then(Value::as_str) {
                blocks.push(json!({
                    "type": "text",
                    "text": format!("[{kind}] {item}"),
                }));
                continue;
            }
            blocks.push(json!({
                "type": "text",
                "text": item.to_string(),
            }));
        }

        if blocks.is_empty() {
            blocks.push(json!({
                "type": "text",
                "text": "Continue with the latest task context.",
            }));
        }

        blocks
    }

    fn resolve_session_cwd(cwd: Option<&str>) -> Result<String, RuntimeError> {
        match cwd.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => {
                let path = PathBuf::from(value);
                if path.is_absolute() {
                    Ok(path.display().to_string())
                } else {
                    let absolute = std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .join(path);
                    Ok(absolute.display().to_string())
                }
            }
            None => {
                let cwd = std::env::current_dir().map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to resolve current dir for acp session: {error}"
                    ))
                })?;
                Ok(cwd.display().to_string())
            }
        }
    }

    async fn execute_turn(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        input: Vec<Value>,
    ) -> Result<(), RuntimeError> {
        let active_turn = AcpActiveTurnState::new(turn_id.to_string());
        let provider_session_ref = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions.get_mut(runtime_session_id).ok_or_else(|| {
                RuntimeError::NotFound(format!("acp session {runtime_session_id}"))
            })?;

            if session.active_turn.is_some() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::InvalidState(format!(
                    "acp session {} already has an active turn",
                    runtime_session_id
                )));
            }

            session.active_turn = Some(active_turn.clone());
            session.provider_session_ref.clone()
        };

        let provider = self.clone();
        let runtime_session_id = runtime_session_id.to_string();
        let turn_id = turn_id.to_string();
        let prompt_blocks = Self::build_prompt_blocks(input.as_slice());

        tokio::spawn(async move {
            let connection = match provider.ensure_connection().await {
                Ok(connection) => connection,
                Err(error) => {
                    let result = ProviderTurnResult {
                        runtime_session_id: runtime_session_id.clone(),
                        turn_id: turn_id.clone(),
                        status: ProviderTurnStatus::Failed,
                        usage: None,
                        error: Some(json!({ "message": error.to_string() })),
                    };
                    provider
                        .complete_turn(runtime_session_id.as_str(), turn_id.as_str(), result)
                        .await;
                    return;
                }
            };

            let response = connection
                .send_request(
                    "session/prompt",
                    json!({
                        "sessionId": provider_session_ref,
                        "prompt": prompt_blocks,
                    }),
                    None,
                )
                .await;

            let result = match response {
                Ok(payload) => {
                    provider
                        .build_prompt_result(runtime_session_id.as_str(), turn_id.as_str(), payload)
                        .await
                }
                Err(error) => ProviderTurnResult {
                    runtime_session_id: runtime_session_id.clone(),
                    turn_id: turn_id.clone(),
                    status: if active_turn.cancelled.load(Ordering::SeqCst) {
                        ProviderTurnStatus::Interrupted
                    } else {
                        ProviderTurnStatus::Failed
                    },
                    usage: None,
                    error: Some(json!({ "message": error.to_string() })),
                },
            };

            provider
                .complete_turn(runtime_session_id.as_str(), turn_id.as_str(), result)
                .await;
        });

        Ok(())
    }

    async fn build_prompt_result(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        payload: Value,
    ) -> ProviderTurnResult {
        let stop_reason = payload
            .get("stopReason")
            .and_then(Value::as_str)
            .map(str::to_string);
        let active_turn = {
            let sessions = self.inner.sessions.read().await;
            sessions
                .get(runtime_session_id)
                .and_then(|session| session.active_turn.clone())
        };

        let (assistant_text, usage_update, tool_calls, cancelled) = match active_turn {
            Some(active_turn) => {
                let assistant_text = {
                    let chunks = active_turn.assistant_chunks.lock().await;
                    let combined = chunks.join("");
                    let trimmed = combined.trim().to_string();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed)
                    }
                };
                let usage_update = active_turn.usage_update.lock().await.clone();
                let tool_calls = active_turn
                    .tool_calls
                    .lock()
                    .await
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                (
                    assistant_text,
                    usage_update,
                    tool_calls,
                    active_turn.cancelled.load(Ordering::SeqCst),
                )
            }
            None => (None, None, Vec::new(), false),
        };

        let status = match stop_reason.as_deref() {
            Some("cancelled") => ProviderTurnStatus::Interrupted,
            Some("end_turn") => ProviderTurnStatus::Completed,
            Some("max_tokens") | Some("max_turn_requests") | Some("refusal") => {
                ProviderTurnStatus::Failed
            }
            Some(_other) => ProviderTurnStatus::Failed,
            None => ProviderTurnStatus::Failed,
        };

        let mut usage = serde_json::Map::new();
        if let Some(stop_reason) = stop_reason.clone() {
            usage.insert("stop_reason".to_string(), Value::String(stop_reason));
        }
        if let Some(assistant_text) = assistant_text.clone() {
            usage.insert(
                "assistant_text".to_string(),
                Value::String(assistant_text.clone()),
            );
            usage.insert("last_message".to_string(), Value::String(assistant_text));
        }
        if let Some(usage_update) = usage_update {
            usage.insert("usage_update".to_string(), usage_update);
        }
        if !tool_calls.is_empty() {
            usage.insert("tool_calls".to_string(), Value::Array(tool_calls));
        }

        let error = match (status, stop_reason, cancelled) {
            (ProviderTurnStatus::Failed, Some(reason), _) if reason != "cancelled" => Some(json!({
                "message": format!("acp turn stopped with unsupported or failed stop reason '{reason}'"),
            })),
            (ProviderTurnStatus::Failed, None, _) => Some(json!({
                "message": "acp prompt response missing stopReason",
                "raw": payload,
            })),
            (ProviderTurnStatus::Interrupted, _, true) => None,
            _ => None,
        };

        ProviderTurnResult {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: turn_id.to_string(),
            status,
            usage: if usage.is_empty() {
                None
            } else {
                Some(Value::Object(usage))
            },
            error,
        }
    }

    async fn complete_turn(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        result: ProviderTurnResult,
    ) {
        let waiters = {
            let mut sessions = self.inner.sessions.write().await;
            let Some(session) = sessions.get_mut(runtime_session_id) else {
                return;
            };

            if let Some(existing) = session.completed_turns.get(turn_id) {
                if existing.status == result.status {
                    return;
                }
                return;
            }

            if session
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.runtime_turn_id == turn_id)
            {
                session.active_turn = None;
            }
            session
                .pending_approvals
                .retain(|_, pending| pending.turn_id != turn_id);
            session
                .completed_turns
                .insert(turn_id.to_string(), result.clone());
            session.waiters.remove(turn_id).unwrap_or_default()
        };

        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    async fn fail_permission_request(
        &self,
        provider_session_ref: &str,
    ) -> Result<(), RuntimeError> {
        let target = {
            let sessions = self.inner.sessions.read().await;
            sessions.iter().find_map(|(runtime_session_id, session)| {
                if session.provider_session_ref == provider_session_ref {
                    session
                        .active_turn
                        .as_ref()
                        .map(|turn| (runtime_session_id.clone(), turn.runtime_turn_id.clone()))
                } else {
                    None
                }
            })
        };

        if let Some((runtime_session_id, turn_id)) = target {
            let result = ProviderTurnResult {
                runtime_session_id: runtime_session_id.clone(),
                turn_id: turn_id.clone(),
                status: ProviderTurnStatus::Failed,
                usage: None,
                error: Some(json!({
                    "message": "ACP session/request_permission is unsupported in v1",
                })),
            };
            self.complete_turn(runtime_session_id.as_str(), turn_id.as_str(), result)
                .await;
        }

        Ok(())
    }

    async fn apply_session_update(
        &self,
        provider_session_ref: &str,
        update: Value,
    ) -> Result<(), RuntimeError> {
        let active_turn = {
            let sessions = self.inner.sessions.read().await;
            sessions
                .values()
                .find(|session| session.provider_session_ref == provider_session_ref)
                .and_then(|session| session.active_turn.clone())
        };

        let Some(active_turn) = active_turn else {
            return Ok(());
        };

        match update.get("sessionUpdate").and_then(Value::as_str) {
            Some("agent_message_chunk") => {
                let text = update
                    .get("content")
                    .and_then(Value::as_object)
                    .filter(|content| content.get("type").and_then(Value::as_str) == Some("text"))
                    .and_then(|content| content.get("text"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if let Some(text) = text {
                    let message_id = update
                        .get("messageId")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    let mut chunks = active_turn.assistant_chunks.lock().await;
                    let mut last_message_id = active_turn.last_message_id.lock().await;
                    if let Some(message_id) = message_id {
                        if last_message_id
                            .as_deref()
                            .is_some_and(|current| current != message_id.as_str())
                            && !chunks.is_empty()
                        {
                            chunks.push("\n\n".to_string());
                        }
                        *last_message_id = Some(message_id);
                    }
                    chunks.push(text);
                }
            }
            Some("usage_update") => {
                *active_turn.usage_update.lock().await = Some(update);
            }
            Some("tool_call") | Some("tool_call_update") => {
                if let Some(tool_call_id) = update.get("toolCallId").and_then(Value::as_str) {
                    active_turn
                        .tool_calls
                        .lock()
                        .await
                        .insert(tool_call_id.to_string(), update);
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl AcpConnection {
    async fn spawn(provider: AcpProvider) -> Result<Arc<Self>, RuntimeError> {
        let command = provider.configured_command()?;

        let mut child = Command::new(command.as_str());
        child.args(provider.inner.config.args.iter());
        child.stdin(Stdio::piped());
        child.stdout(Stdio::piped());
        child.stderr(Stdio::piped());
        for (key, value) in &provider.inner.config.env {
            child.env(key, value);
        }

        let mut child = child
            .spawn()
            .map_err(|error| RuntimeError::Io(format!("failed to spawn acp agent: {error}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::Io("acp agent did not expose stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::Io("acp agent did not expose stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::Io("acp agent did not expose stderr".to_string()))?;

        let connection = Arc::new(Self {
            child: Mutex::new(child),
            stdin: Mutex::new(BufWriter::new(stdin)),
            pending_requests: Mutex::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            capabilities: RwLock::new(AcpAgentCapabilities::default()),
            stderr_tail: Mutex::new(String::new()),
        });

        connection.spawn_reader(provider.clone(), stdout);
        connection.spawn_stderr_reader(stderr);

        let init_result = connection
            .send_request(
                "initialize",
                json!({
                    "protocolVersion": DEFAULT_PROTOCOL_VERSION,
                    "clientCapabilities": {
                        "fs": {
                            "readTextFile": false,
                            "writeTextFile": false
                        },
                        "terminal": false
                    },
                    "clientInfo": {
                        "name": "gg-runtime",
                        "title": "GG Runtime",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
                Some(provider.request_timeout()),
            )
            .await?;
        let capabilities = parse_initialize_capabilities(&init_result)?;
        *connection.capabilities.write().await = capabilities;

        Ok(connection)
    }

    fn spawn_reader(self: &Arc<Self>, provider: AcpProvider, stdout: ChildStdout) {
        let connection = Arc::clone(self);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let parsed = serde_json::from_str::<Value>(line.as_str());
                        let message = match parsed {
                            Ok(message) => message,
                            Err(error) => {
                                connection
                                    .mark_closed_protocol(format!(
                                        "acp agent emitted malformed JSON-RPC line: {error}"
                                    ))
                                    .await;
                                break;
                            }
                        };

                        match message.get("method").and_then(Value::as_str) {
                            Some("session/update") => {
                                let session_id = message
                                    .get("params")
                                    .and_then(|params| params.get("sessionId"))
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                let update = message
                                    .get("params")
                                    .and_then(|params| params.get("update"))
                                    .cloned();
                                if let (Some(session_id), Some(update)) = (session_id, update) {
                                    let _ = provider
                                        .apply_session_update(session_id.as_str(), update)
                                        .await;
                                }
                            }
                            Some("session/request_permission") => {
                                let session_id = message
                                    .get("params")
                                    .and_then(|params| params.get("sessionId"))
                                    .and_then(Value::as_str)
                                    .map(str::to_string);
                                let request_id = message.get("id").cloned();
                                if let Some(request_id) = request_id {
                                    let _ = connection
                                        .write_message(&json!({
                                            "jsonrpc": "2.0",
                                            "id": request_id,
                                            "result": {
                                                "outcome": {
                                                    "outcome": "cancelled"
                                                }
                                            }
                                        }))
                                        .await;
                                }
                                if let Some(session_id) = session_id {
                                    let _ =
                                        provider.fail_permission_request(session_id.as_str()).await;
                                }
                                continue;
                            }
                            Some(_) => continue,
                            None => {}
                        }

                        if let Some(id_key) = message_id_key(&message) {
                            let responder = {
                                let mut pending = connection.pending_requests.lock().await;
                                pending.remove(id_key.as_str())
                            };
                            if let Some(responder) = responder {
                                if let Some(error) = message.get("error") {
                                    let _ = responder.send(Err(RuntimeError::ProtocolViolation(
                                        format!(
                                            "acp request {} failed: {}",
                                            id_key,
                                            jsonrpc_error_message(error)
                                        ),
                                    )));
                                } else if let Some(result) = message.get("result") {
                                    let _ = responder.send(Ok(result.clone()));
                                } else {
                                    let _ = responder.send(Err(RuntimeError::ProtocolViolation(
                                        format!("acp response {} missing result and error", id_key),
                                    )));
                                }
                                continue;
                            }
                        }
                    }
                    Ok(None) => {
                        let stderr = connection.stderr_tail.lock().await.clone();
                        let detail = if stderr.trim().is_empty() {
                            "acp agent connection closed".to_string()
                        } else {
                            format!("acp agent connection closed: {}", stderr.trim())
                        };
                        connection.mark_closed_io(detail).await;
                        break;
                    }
                    Err(error) => {
                        connection
                            .mark_closed_io(format!("failed reading acp agent stdout: {error}"))
                            .await;
                        break;
                    }
                }
            }
            provider
                .reap_connection_if_current_and_closed(&connection)
                .await;
        });
    }

    fn spawn_stderr_reader(self: &Arc<Self>, stderr: ChildStderr) {
        let connection = Arc::clone(self);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut tail = connection.stderr_tail.lock().await;
                if !tail.is_empty() {
                    tail.push('\n');
                }
                tail.push_str(line.as_str());
                if tail.len() > STDERR_TAIL_MAX_BYTES {
                    let split_at = tail.len().saturating_sub(STDERR_TAIL_MAX_BYTES);
                    let trimmed = tail.split_off(split_at);
                    *tail = trimmed;
                }
            }
        });
    }

    async fn write_message(&self, message: &Value) -> Result<(), RuntimeError> {
        let mut stdin = self.stdin.lock().await;
        let bytes = serde_json::to_vec(message).map_err(|error| {
            RuntimeError::ProtocolViolation(format!(
                "failed serializing acp json-rpc message: {error}"
            ))
        })?;
        stdin.write_all(bytes.as_slice()).await.map_err(|error| {
            RuntimeError::Io(format!("failed writing to acp agent stdin: {error}"))
        })?;
        stdin.write_all(b"\n").await.map_err(|error| {
            RuntimeError::Io(format!(
                "failed writing newline to acp agent stdin: {error}"
            ))
        })?;
        stdin.flush().await.map_err(|error| {
            RuntimeError::Io(format!("failed flushing acp agent stdin: {error}"))
        })?;
        Ok(())
    }

    async fn send_notification(&self, method: &str, params: Value) -> Result<(), RuntimeError> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn send_request(
        &self,
        method: &str,
        params: Value,
        timeout: Option<Duration>,
    ) -> Result<Value, RuntimeError> {
        if self.closed.load(Ordering::SeqCst) {
            let stderr = self.stderr_tail.lock().await.clone();
            return Err(RuntimeError::Io(if stderr.trim().is_empty() {
                "acp agent connection is closed".to_string()
            } else {
                format!("acp agent connection is closed: {}", stderr.trim())
            }));
        }

        let id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let id_key = id.to_string();
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(id_key.clone(), sender);
        }

        let message = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        if let Err(error) = self.write_message(&message).await {
            let mut pending = self.pending_requests.lock().await;
            pending.remove(id_key.as_str());
            return Err(error);
        }

        let result = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, receiver).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(RuntimeError::InvalidState(format!(
                    "acp response channel closed for request {}",
                    id
                ))),
                Err(_) => {
                    let mut pending = self.pending_requests.lock().await;
                    pending.remove(id_key.as_str());
                    Err(RuntimeError::InvalidState(format!(
                        "timed out waiting for acp response to {}",
                        method
                    )))
                }
            },
            None => match receiver.await {
                Ok(result) => result,
                Err(_) => Err(RuntimeError::InvalidState(format!(
                    "acp response channel closed for request {}",
                    id
                ))),
            },
        }?;

        Ok(result)
    }

    async fn mark_closed_io(&self, message: String) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut pending = self.pending_requests.lock().await;
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(RuntimeError::Io(message.clone())));
        }
    }

    async fn mark_closed_protocol(&self, message: String) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut pending = self.pending_requests.lock().await;
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(RuntimeError::ProtocolViolation(message.clone())));
        }
    }

    async fn shutdown(&self, kill_if_running: bool) {
        self.closed.store(true, Ordering::SeqCst);
        {
            let mut pending = self.pending_requests.lock().await;
            for (_, sender) in pending.drain() {
                let _ = sender.send(Err(RuntimeError::Io(
                    "acp connection shutting down".to_string(),
                )));
            }
        }
        {
            let mut stdin = self.stdin.lock().await;
            let _ = stdin.flush().await;
        }
        let mut child = self.child.lock().await;
        if kill_if_running {
            let _ = child.start_kill();
        }
        let _ = child.wait().await;
    }
}

fn message_id_key(message: &Value) -> Option<String> {
    match message.get("id") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Number(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn parse_initialize_capabilities(result: &Value) -> Result<AcpAgentCapabilities, RuntimeError> {
    let protocol_version = result
        .get("protocolVersion")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "acp initialize response missing protocolVersion".to_string(),
            )
        })?;
    if protocol_version != DEFAULT_PROTOCOL_VERSION {
        return Err(RuntimeError::ProtocolViolation(format!(
            "acp protocol version mismatch (expected={}, actual={})",
            DEFAULT_PROTOCOL_VERSION, protocol_version
        )));
    }

    let agent_capabilities = result
        .get("agentCapabilities")
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));

    Ok(AcpAgentCapabilities {
        load_session: agent_capabilities
            .get("loadSession")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        resume_session: agent_capabilities
            .get("sessionCapabilities")
            .and_then(|value| value.get("resume"))
            .is_some(),
        close_session: agent_capabilities
            .get("sessionCapabilities")
            .and_then(|value| value.get("close"))
            .is_some(),
    })
}

fn jsonrpc_error_message(error: &Value) -> String {
    match error.get("message").and_then(Value::as_str) {
        Some(message) => message.to_string(),
        None => error.to_string(),
    }
}

fn absolutize_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

#[async_trait]
impl RuntimeProvider for AcpProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Acp
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Acp,
            display_name: "ACP".to_string(),
            enabled: self.inner.config.enabled,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap("acp provider disabled".to_string()));
        }
        self.validate_base_config()?;
        self.configured_command()?;
        self.ensure_runtime_dirs().await?;
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(Vec::new())
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        if !self.inner.config.enabled {
            return Ok(ProviderAuthStatus {
                authenticated: false,
                mode: Some("disabled".to_string()),
                detail: Some("ACP provider is disabled".to_string()),
            });
        }

        if let Err(error) = self.validate_base_config() {
            return Ok(ProviderAuthStatus {
                authenticated: false,
                mode: Some("invalid_config".to_string()),
                detail: Some(error.to_string()),
            });
        }

        match self.configured_command() {
            Ok(command) => Ok(ProviderAuthStatus {
                authenticated: false,
                mode: Some("agent_managed".to_string()),
                detail: Some(format!(
                    "ACP stdio agent '{}' is configured; auth negotiation remains agent-managed and lazy",
                    command
                )),
            }),
            Err(error) => Ok(ProviderAuthStatus {
                authenticated: false,
                mode: Some("not_configured".to_string()),
                detail: Some(error.to_string()),
            }),
        }
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.reserve_session_slot(req.runtime_session_id.as_str())
            .await?;
        let connection = match self.ensure_connection().await {
            Ok(connection) => connection,
            Err(error) => {
                self.release_session_slot(req.runtime_session_id.as_str())
                    .await;
                return Err(error);
            }
        };
        let cwd = match Self::resolve_session_cwd(req.cwd.as_deref()) {
            Ok(cwd) => cwd,
            Err(error) => {
                self.release_session_slot(req.runtime_session_id.as_str())
                    .await;
                return Err(error);
            }
        };
        let response = connection
            .send_request(
                "session/new",
                json!({
                    "cwd": cwd,
                    "mcpServers": self.build_mcp_servers(req.runtime_session_id.as_str()),
                }),
                Some(self.request_timeout()),
            )
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.release_session_slot(req.runtime_session_id.as_str())
                    .await;
                return Err(error);
            }
        };
        let provider_session_ref = response
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "acp session/new response missing sessionId".to_string(),
                )
            });
        let provider_session_ref = match provider_session_ref {
            Ok(provider_session_ref) => provider_session_ref,
            Err(error) => {
                self.release_session_slot(req.runtime_session_id.as_str())
                    .await;
                return Err(error);
            }
        };
        if let Err(error) = self
            .activate_reserved_session(
                req.runtime_session_id.as_str(),
                provider_session_ref.clone(),
            )
            .await
        {
            self.release_session_slot(req.runtime_session_id.as_str())
                .await;
            return Err(error);
        }

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: provider_session_ref.clone(),
            canonical_provider_session_ref: Some(provider_session_ref),
        })
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.reserve_session_slot(req.runtime_session_id.as_str())
            .await?;
        let connection = match self.ensure_connection().await {
            Ok(connection) => connection,
            Err(error) => {
                self.release_session_slot(req.runtime_session_id.as_str())
                    .await;
                return Err(error);
            }
        };
        let capabilities = connection.capabilities.read().await.clone();
        let cwd = match Self::resolve_session_cwd(req.cwd.as_deref()) {
            Ok(cwd) => cwd,
            Err(error) => {
                self.release_session_slot(req.runtime_session_id.as_str())
                    .await;
                return Err(error);
            }
        };
        let method = if capabilities.resume_session {
            "session/resume"
        } else if capabilities.load_session {
            "session/load"
        } else {
            self.release_session_slot(req.runtime_session_id.as_str())
                .await;
            return Err(RuntimeError::Unsupported(
                "acp agent does not advertise session resume or load support".to_string(),
            ));
        };
        if let Err(error) = self
            .activate_reserved_session(
                req.runtime_session_id.as_str(),
                req.provider_session_ref.clone(),
            )
            .await
        {
            self.release_session_slot(req.runtime_session_id.as_str())
                .await;
            return Err(error);
        }

        let response = connection
            .send_request(
                method,
                json!({
                    "sessionId": req.provider_session_ref,
                    "cwd": cwd,
                    "mcpServers": self.build_mcp_servers(req.runtime_session_id.as_str()),
                }),
                Some(self.request_timeout()),
            )
            .await;

        if let Err(error) = response {
            self.release_session_slot(req.runtime_session_id.as_str())
                .await;
            return Err(error);
        }

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: req.provider_session_ref.clone(),
            canonical_provider_session_ref: req
                .canonical_provider_session_ref
                .or_else(|| Some(req.provider_session_ref)),
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;

            if session.active_turn.is_some() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::InvalidState(format!(
                    "acp session {} already has an active turn",
                    req.runtime_session_id
                )));
            }

            if let Some(approval_id) = req.approval_id.clone() {
                session.pending_approvals.insert(
                    approval_id,
                    PendingApprovalTurn {
                        turn_id: req.turn_id.clone(),
                        input: req.input.clone(),
                        expected_turn_id: req.expected_turn_id,
                        permission_mode: req.permission_mode,
                    },
                );
            }
        }

        if req.approval_id.is_none() {
            self.execute_turn(
                req.runtime_session_id.as_str(),
                req.turn_id.as_str(),
                req.input,
            )
            .await?;
        }

        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
        })
    }

    async fn interrupt_turn(&self, req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        let provider_session_ref = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;
            let active_turn = session.active_turn.as_ref().ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "turn {} is not active for session {}",
                    req.turn_id, req.runtime_session_id
                ))
            })?;
            if active_turn.runtime_turn_id != req.turn_id {
                return Err(RuntimeError::InvalidState(format!(
                    "turn {} is not active for session {}",
                    req.turn_id, req.runtime_session_id
                )));
            }
            active_turn.cancelled.store(true, Ordering::SeqCst);
            session.provider_session_ref.clone()
        };

        let connection = self.ensure_connection().await?;
        connection
            .send_notification(
                "session/cancel",
                json!({
                    "sessionId": provider_session_ref,
                }),
            )
            .await?;
        Ok(())
    }

    async fn respond_approval(
        &self,
        req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        let decision = ApprovalDecision::parse(req.decision.as_str())?;
        let pending = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;
            let pending = session
                .pending_approvals
                .get(req.approval_id.as_str())
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound(format!("approval {}", req.approval_id)))?;
            if pending.turn_id != req.turn_id {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "approval {} turn mismatch (expected={}, actual={})",
                    req.approval_id, pending.turn_id, req.turn_id
                )));
            }
            session.pending_approvals.remove(req.approval_id.as_str());
            pending
        };

        if decision == ApprovalDecision::Decline {
            let result = ProviderTurnResult {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: req.turn_id.clone(),
                status: ProviderTurnStatus::Interrupted,
                usage: None,
                error: Some(json!({
                    "message": "approval declined",
                })),
            };
            self.complete_turn(
                req.runtime_session_id.as_str(),
                req.turn_id.as_str(),
                result,
            )
            .await;
            return Ok(());
        }

        let mut input = pending.input;
        let mut _expected_turn_id = pending.expected_turn_id;
        let mut _permission_mode = pending.permission_mode;
        if let Some(payload) = req.payload.as_ref() {
            if let Some(updated_input) = payload.get("input").and_then(Value::as_array) {
                input = updated_input.clone();
            }
            if let Some(permission_mode) = payload
                .get("permission_mode")
                .and_then(Value::as_str)
                .map(str::to_string)
            {
                _permission_mode = Some(permission_mode);
            }
        }

        self.execute_turn(req.runtime_session_id.as_str(), req.turn_id.as_str(), input)
            .await
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;
            if let Some(result) = session.completed_turns.get(req.turn_id.as_str()) {
                return Ok(result.clone());
            }
            if session
                .active_turn
                .as_ref()
                .is_none_or(|turn| turn.runtime_turn_id != req.turn_id)
                && !session
                    .pending_approvals
                    .values()
                    .any(|pending| pending.turn_id == req.turn_id)
            {
                return Err(RuntimeError::NotFound(format!(
                    "turn {} in session {}",
                    req.turn_id, req.runtime_session_id
                )));
            }
        }

        let (sender, receiver) = oneshot::channel();
        {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;
            if let Some(result) = session.completed_turns.get(req.turn_id.as_str()) {
                return Ok(result.clone());
            }
            session
                .waiters
                .entry(req.turn_id.clone())
                .or_default()
                .push(sender);
        }

        match tokio::time::timeout(self.wait_timeout(req.timeout_ms), receiver).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(RuntimeError::InvalidState(format!(
                "turn result channel closed for {}",
                req.turn_id
            ))),
            Err(_) => Err(RuntimeError::InvalidState(format!(
                "timed out waiting for turn {}",
                req.turn_id
            ))),
        }
    }

    async fn close_session(&self, req: ProviderCloseSessionRequest) -> Result<(), RuntimeError> {
        let (provider_session_ref, active_turn_id) = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;
            (
                session.provider_session_ref.clone(),
                session
                    .active_turn
                    .as_ref()
                    .map(|turn| turn.runtime_turn_id.clone()),
            )
        };

        if let Some(turn_id) = active_turn_id {
            let _ = self
                .interrupt_turn(ProviderInterruptTurnRequest {
                    runtime_session_id: req.runtime_session_id.clone(),
                    turn_id: turn_id.clone(),
                })
                .await;
            self.complete_turn(
                req.runtime_session_id.as_str(),
                turn_id.as_str(),
                ProviderTurnResult {
                    runtime_session_id: req.runtime_session_id.clone(),
                    turn_id: turn_id.clone(),
                    status: ProviderTurnStatus::Interrupted,
                    usage: None,
                    error: Some(json!({
                        "message": req
                            .reason
                            .unwrap_or_else(|| "session closed before turn completion".to_string()),
                    })),
                },
            )
            .await;
        }

        let connection = self.ensure_connection().await?;
        let capabilities = connection.capabilities.read().await.clone();
        if capabilities.close_session {
            let _ = connection
                .send_request(
                    "session/close",
                    json!({
                        "sessionId": provider_session_ref,
                    }),
                    Some(self.request_timeout()),
                )
                .await?;
        }

        let mut sessions = self.inner.sessions.write().await;
        sessions.remove(req.runtime_session_id.as_str());
        drop(sessions);
        self.shutdown_connection_if_idle().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use runtime_core::RuntimeProvider;
    use serde_json::{json, Value};

    use super::{AcpProvider, AcpProviderConfig};

    struct FakeAgentHarness {
        _temp_dir: tempfile::TempDir,
        provider_dir: PathBuf,
        script_path: PathBuf,
    }

    impl FakeAgentHarness {
        fn new(mode: &str) -> Self {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let script_path = temp_dir.path().join("fake_acp_agent.py");
            fs::write(script_path.as_path(), fake_agent_script(mode)).expect("write script");
            Self {
                provider_dir: temp_dir.path().join("provider"),
                script_path,
                _temp_dir: temp_dir,
            }
        }

        fn provider(&self) -> AcpProvider {
            AcpProvider::new(AcpProviderConfig {
                enabled: true,
                provider_dir: self.provider_dir.clone(),
                command: Some("python3".to_string()),
                args: vec![self.script_path.display().to_string()],
                ..AcpProviderConfig::default()
            })
        }
    }

    fn fake_agent_script(mode: &str) -> String {
        format!(
            r#"#!/usr/bin/env python3
import json
import os
import sys
import time

MODE = {mode:?}
SESSIONS = {{}}
PENDING_PROMPTS = {{}}
PENDING_PERMISSIONS = {{}}
NEXT_SESSION_ID = 1

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def prompt_text(prompt):
    parts = []
    for block in prompt:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
        else:
            parts.append(json.dumps(block))
    return " ".join(part.strip() for part in parts if part).strip()

def finish_prompt(session_id, request_id, stop_reason, texts):
    for index, text in enumerate(texts):
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "msg_shared" if index < 2 else f"msg_{{index}}",
                    "content": {{
                        "type": "text",
                        "text": text
                    }}
                }}
            }}
        }})
    send({{
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {{
            "sessionId": session_id,
            "update": {{
                "sessionUpdate": "usage_update",
                "used": 7,
                "size": 64,
                "cost": {{
                    "amount": 0.01,
                    "currency": "USD"
                }}
            }}
        }}
    }})
    send({{
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {{
            "stopReason": stop_reason
        }}
    }})

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    msg = json.loads(line)

    if "method" not in msg and "id" in msg:
        permission = None
        for session_id, value in list(PENDING_PERMISSIONS.items()):
            if value["permission_id"] == msg["id"]:
                permission = (session_id, value)
                break
        if permission is not None:
            session_id, value = permission
            PENDING_PERMISSIONS.pop(session_id, None)
            finish_prompt(session_id, value["prompt_request_id"], "cancelled", ["Permission request was cancelled."])
        continue

    method = msg.get("method")
    if method == "initialize":
        result = {{
            "protocolVersion": 1,
            "agentCapabilities": {{
                "loadSession": True,
                "sessionCapabilities": {{
                    "close": {{}}
                }}
            }},
            "authMethods": []
        }}
        if MODE != "load_only":
            result["agentCapabilities"]["sessionCapabilities"]["resume"] = {{}}
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": result}})
    elif method == "session/new":
        if MODE == "slow_create":
            time.sleep(0.2)
        session_id = f"sess_{{NEXT_SESSION_ID}}"
        NEXT_SESSION_ID += 1
        SESSIONS[session_id] = {{}}
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{"sessionId": session_id}}}})
    elif method == "session/resume":
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
    elif method == "session/load":
        session_id = msg["params"]["sessionId"]
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "history_1",
                    "content": {{
                        "type": "text",
                        "text": "Loaded history."
                    }}
                }}
            }}
        }})
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": None}})
    elif method == "session/prompt":
        session_id = msg["params"]["sessionId"]
        request_id = msg["id"]
        text = prompt_text(msg["params"].get("prompt", []))
        if "malformed" in text:
            sys.stdout.write("{{not-json\n")
            sys.stdout.flush()
            continue
        if "crash" in text:
            os._exit(9)
        if "permission collision" in text:
            permission_id = request_id
            PENDING_PERMISSIONS[session_id] = {{
                "permission_id": permission_id,
                "prompt_request_id": request_id
            }}
            send({{
                "jsonrpc": "2.0",
                "id": permission_id,
                "method": "session/request_permission",
                "params": {{
                    "sessionId": session_id,
                    "toolCall": {{
                        "toolCallId": "call_permission_collision"
                    }},
                    "options": [
                        {{
                            "optionId": "allow-once",
                            "name": "Allow once",
                            "kind": "allow_once"
                        }}
                    ]
                }}
            }})
            continue
        if "permission" in text:
            permission_id = request_id + 1000 if isinstance(request_id, int) else 1000
            PENDING_PERMISSIONS[session_id] = {{
                "permission_id": permission_id,
                "prompt_request_id": request_id
            }}
            send({{
                "jsonrpc": "2.0",
                "id": permission_id,
                "method": "session/request_permission",
                "params": {{
                    "sessionId": session_id,
                    "toolCall": {{
                        "toolCallId": "call_permission"
                    }},
                    "options": [
                        {{
                            "optionId": "allow-once",
                            "name": "Allow once",
                            "kind": "allow_once"
                        }},
                        {{
                            "optionId": "reject-once",
                            "name": "Reject",
                            "kind": "reject_once"
                        }}
                    ]
                }}
            }})
            continue
        if "sleep" in text:
            PENDING_PROMPTS[session_id] = {{
                "request_id": request_id
            }}
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "sleep_1",
                        "content": {{
                            "type": "text",
                            "text": "Starting long task..."
                        }}
                    }}
                }}
            }})
            continue
        if "split" in text:
            finish_prompt(session_id, request_id, "end_turn", ["Hello ", "world"])
            continue
        if "refusal" in text:
            finish_prompt(session_id, request_id, "refusal", ["Refused."])
            continue
        if "max tokens" in text:
            finish_prompt(session_id, request_id, "max_tokens", ["Stopped for token limit."])
            continue
        if "max turns" in text:
            finish_prompt(session_id, request_id, "max_turn_requests", ["Stopped for turn limit."])
            continue
        if "tooling" in text:
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "msg_tool_1",
                        "content": {{
                            "type": "text",
                            "text": "First line."
                        }}
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "tool_call",
                        "toolCallId": "tool_1",
                        "toolName": "gg_ping"
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "tool_1",
                        "status": "completed"
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "msg_tool_2",
                        "content": {{
                            "type": "text",
                            "text": "Second line."
                        }}
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "usage_update",
                        "used": 11,
                        "size": 128,
                        "cost": {{
                            "amount": 0.02,
                            "currency": "USD"
                        }}
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {{
                    "stopReason": "end_turn"
                }}
            }})
            continue
        finish_prompt(session_id, request_id, "end_turn", [f"Echo: {{text}}"])
    elif method == "session/cancel":
        session_id = msg["params"]["sessionId"]
        pending = PENDING_PROMPTS.pop(session_id, None)
        if pending is not None:
            finish_prompt(session_id, pending["request_id"], "cancelled", ["Cancelled by client."])
    elif method == "session/close":
        session_id = msg["params"]["sessionId"]
        PENDING_PROMPTS.pop(session_id, None)
        PENDING_PERMISSIONS.pop(session_id, None)
        SESSIONS.pop(session_id, None)
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
"#
        )
    }

    fn expected_gg_mcp_server(
        runtime_session_id: &str,
        enable_process_tools: bool,
        gateway_url: Option<&str>,
        gateway_token: Option<&str>,
    ) -> Value {
        let mut env = vec![
            json!({
                "name": "GG_MCP_ENABLE_PROCESS_TOOLS",
                "value": if enable_process_tools { "1" } else { "0" },
            }),
            json!({
                "name": "GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID",
                "value": "1",
            }),
            json!({
                "name": "GG_MCP_CALLER_AGENT_ID",
                "value": runtime_session_id,
            }),
        ];
        if let Some(url) = gateway_url {
            env.push(json!({
                "name": "GG_MCP_GATEWAY_URL",
                "value": url,
            }));
        }
        if let Some(token) = gateway_token {
            env.push(json!({
                "name": "GG_MCP_GATEWAY_TOKEN",
                "value": token,
            }));
        }
        if let Some(home) = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.push(json!({
                "name": "HOME",
                "value": home.display().to_string(),
            }));
        }
        if let Some(cargo_home) = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
        {
            env.push(json!({
                "name": "CARGO_HOME",
                "value": cargo_home.display().to_string(),
            }));
        }

        json!({
            "name": "gg",
            "command": "gg-mcp-server",
            "args": ["--stdio"],
            "env": env,
        })
    }

    #[tokio::test]
    async fn metadata_reports_acp_provider_identity() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            command: Some("python3".to_string()),
            ..AcpProviderConfig::default()
        });

        let metadata = provider.metadata();
        assert_eq!(metadata.kind.as_str(), "acp");
        assert_eq!(metadata.display_name, "ACP");
        assert!(metadata.enabled);
    }

    #[tokio::test]
    async fn healthcheck_creates_provider_runtime_directories() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();

        provider.healthcheck().await.expect("healthcheck");

        assert!(harness.provider_dir.is_dir());
        assert!(harness.provider_dir.join("instances").is_dir());
        assert!(harness.provider_dir.join("sessions").is_dir());
    }

    #[tokio::test]
    async fn healthcheck_rejects_disabled_provider() {
        let provider = AcpProvider::new(AcpProviderConfig::default());
        let error = provider.healthcheck().await.expect_err("disabled");
        assert_eq!(error.to_string(), "bootstrap error: acp provider disabled");
    }

    #[tokio::test]
    async fn list_models_is_empty_for_session_scoped_acp_selection() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();

        let models = provider.list_models().await.expect("models");
        assert!(models.is_empty());
    }

    #[tokio::test]
    async fn auth_status_is_clear_about_unconfigured_state() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            ..AcpProviderConfig::default()
        });

        let status = provider.auth_status().await.expect("auth status");
        assert!(!status.authenticated);
        assert_eq!(status.mode.as_deref(), Some("not_configured"));
        assert!(status
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("command is not configured")));
    }

    #[test]
    fn default_config_matches_phase_two_server_contract() {
        let config = AcpProviderConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_instances, 4);
        assert_eq!(config.max_sessions_per_instance, 4);
        assert!(config.command.is_none());
        assert_eq!(config.transport, "stdio");
        assert_eq!(config.request_timeout_secs, 30);
        assert_eq!(config.wait_timeout_secs, 300);
        assert!(config.gg_mcp_enabled);
        assert_eq!(config.gg_mcp_server_name, "gg");
        assert_eq!(config.gg_mcp_command, "gg-mcp-server");
        assert!(config.gg_mcp_args.is_empty());
        assert!(config.gg_mcp_enable_process_tools);
    }

    #[tokio::test]
    async fn healthcheck_rejects_non_stdio_transport() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            command: Some("python3".to_string()),
            transport: "http".to_string(),
            ..AcpProviderConfig::default()
        });

        let error = provider
            .healthcheck()
            .await
            .expect_err("unsupported transport");
        assert_eq!(
            error.to_string(),
            "configuration error: acp transport 'http' is unsupported; expected stdio"
        );
    }

    #[tokio::test]
    async fn lifecycle_methods_require_configured_command() {
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            ..AcpProviderConfig::default()
        });

        let error = provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_acp_test".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect_err("missing command");
        assert_eq!(
            error.to_string(),
            "configuration error: acp command is not configured"
        );
    }

    #[tokio::test]
    async fn real_adapter_contract_create_send_wait_resume_and_close() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();

        let created = provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_real_1".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");
        assert!(created.provider_session_ref.starts_with("sess_"));

        let ack = provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_real_1".to_string(),
                turn_id: "turn_real_1".to_string(),
                input: vec![json!({"type":"text","text":"split please"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");
        assert_eq!(ack.turn_id, "turn_real_1");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_real_1".to_string(),
                turn_id: "turn_real_1".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Completed);
        let usage = result.usage.expect("usage");
        assert_eq!(
            usage.get("last_message").and_then(Value::as_str),
            Some("Hello world")
        );

        let resumed = provider
            .resume_session(runtime_core::ProviderResumeSessionRequest {
                runtime_session_id: "sess_real_2".to_string(),
                provider_session_ref: created.provider_session_ref.clone(),
                canonical_provider_session_ref: created.canonical_provider_session_ref.clone(),
                cwd: None,
                metadata: None,
            })
            .await
            .expect("resume");
        assert_eq!(resumed.provider_session_ref, created.provider_session_ref);

        provider
            .close_session(runtime_core::ProviderCloseSessionRequest {
                runtime_session_id: "sess_real_2".to_string(),
                reason: Some("test close".to_string()),
            })
            .await
            .expect("close");
    }

    #[tokio::test]
    async fn create_and_resume_include_expected_gg_mcp_server_shape() {
        let harness = FakeAgentHarness::new("normal");
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            provider_dir: harness.provider_dir.clone(),
            command: Some("python3".to_string()),
            args: vec![harness.script_path.display().to_string()],
            gg_mcp_enabled: true,
            gg_mcp_server_name: "gg".to_string(),
            gg_mcp_command: "gg-mcp-server".to_string(),
            gg_mcp_args: vec!["--stdio".to_string()],
            gg_mcp_enable_process_tools: true,
            gg_mcp_gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
            gg_mcp_gateway_token: Some("acp-token".to_string()),
            ..AcpProviderConfig::default()
        });

        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_create".to_string(),
                model: None,
                cwd: Some("/tmp/create".to_string()),
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");
        provider
            .resume_session(runtime_core::ProviderResumeSessionRequest {
                runtime_session_id: "sess_resume".to_string(),
                provider_session_ref: "sess_1".to_string(),
                canonical_provider_session_ref: Some("sess_1".to_string()),
                cwd: Some("/tmp/resume".to_string()),
                metadata: None,
            })
            .await
            .expect("resume");

        let server = provider
            .build_gg_mcp_server_config("sess_create")
            .expect("gg mcp server config");
        assert_eq!(
            server,
            expected_gg_mcp_server(
                "sess_create",
                true,
                Some("http://127.0.0.1:8787/v1/mcp"),
                Some("acp-token")
            )
        );
    }

    #[tokio::test]
    async fn real_adapter_contract_load_based_resume_is_supported() {
        let harness = FakeAgentHarness::new("load_only");
        let provider = harness.provider();

        let created = provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_load_1".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        let resumed = provider
            .resume_session(runtime_core::ProviderResumeSessionRequest {
                runtime_session_id: "sess_load_2".to_string(),
                provider_session_ref: created.provider_session_ref.clone(),
                canonical_provider_session_ref: created.canonical_provider_session_ref.clone(),
                cwd: None,
                metadata: None,
            })
            .await
            .expect("load resume");
        assert_eq!(resumed.provider_session_ref, created.provider_session_ref);
    }

    #[tokio::test]
    async fn real_adapter_contract_interrupts_active_prompt() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_interrupt".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_interrupt".to_string(),
                turn_id: "turn_interrupt".to_string(),
                input: vec![json!({"type":"text","text":"sleep now"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        provider
            .interrupt_turn(runtime_core::ProviderInterruptTurnRequest {
                runtime_session_id: "sess_interrupt".to_string(),
                turn_id: "turn_interrupt".to_string(),
            })
            .await
            .expect("interrupt");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_interrupt".to_string(),
                turn_id: "turn_interrupt".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Interrupted);
    }

    #[tokio::test]
    async fn real_adapter_contract_fails_permission_requests_clearly() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_permission".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_permission".to_string(),
                turn_id: "turn_permission".to_string(),
                input: vec![json!({"type":"text","text":"permission please"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_permission".to_string(),
                turn_id: "turn_permission".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
        assert!(result
            .error
            .as_ref()
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("request_permission")));
    }

    #[tokio::test]
    async fn real_adapter_contract_fails_permission_request_id_collisions_clearly() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_permission_collision".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_permission_collision".to_string(),
                turn_id: "turn_permission_collision".to_string(),
                input: vec![json!({"type":"text","text":"permission collision please"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_permission_collision".to_string(),
                turn_id: "turn_permission_collision".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
        assert!(result
            .error
            .as_ref()
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("request_permission")));
    }

    #[tokio::test]
    async fn real_adapter_contract_maps_non_happy_stop_reasons_to_failed() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_stop_reasons".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        for (turn_id, prompt_text, stop_reason) in [
            ("turn_refusal", "refusal please", "refusal"),
            ("turn_max_tokens", "max tokens please", "max_tokens"),
            ("turn_max_turns", "max turns please", "max_turn_requests"),
        ] {
            provider
                .send_turn(runtime_core::ProviderSendTurnRequest {
                    runtime_session_id: "sess_stop_reasons".to_string(),
                    turn_id: turn_id.to_string(),
                    input: vec![json!({"type":"text","text":prompt_text})],
                    expected_turn_id: None,
                    permission_mode: None,
                    approval_id: None,
                })
                .await
                .expect("send");

            let result = provider
                .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                    runtime_session_id: "sess_stop_reasons".to_string(),
                    turn_id: turn_id.to_string(),
                    timeout_ms: Some(5_000),
                })
                .await
                .expect("wait");
            assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
            let usage = result.usage.expect("usage");
            assert_eq!(
                usage.get("stop_reason").and_then(Value::as_str),
                Some(stop_reason)
            );
        }
    }

    #[tokio::test]
    async fn create_session_enforces_configured_capacity() {
        let harness = FakeAgentHarness::new("normal");
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            provider_dir: harness.provider_dir.clone(),
            command: Some("python3".to_string()),
            args: vec![harness.script_path.display().to_string()],
            max_instances: 1,
            max_sessions_per_instance: 1,
            ..AcpProviderConfig::default()
        });

        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_capacity_1".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create first");

        let error = provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_capacity_2".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect_err("capacity exceeded");
        assert!(error
            .to_string()
            .contains("acp session capacity exceeded (1 total sessions"));
    }

    #[tokio::test]
    async fn concurrent_create_session_enforces_configured_capacity() {
        let harness = FakeAgentHarness::new("slow_create");
        let provider = AcpProvider::new(AcpProviderConfig {
            enabled: true,
            provider_dir: harness.provider_dir.clone(),
            command: Some("python3".to_string()),
            args: vec![harness.script_path.display().to_string()],
            max_instances: 1,
            max_sessions_per_instance: 1,
            ..AcpProviderConfig::default()
        });

        let provider_a = provider.clone();
        let provider_b = provider.clone();

        let create_a = tokio::spawn(async move {
            provider_a
                .create_session(runtime_core::ProviderCreateSessionRequest {
                    runtime_session_id: "sess_capacity_a".to_string(),
                    model: None,
                    cwd: None,
                    permission_mode: None,
                    metadata: None,
                })
                .await
        });
        let create_b = tokio::spawn(async move {
            provider_b
                .create_session(runtime_core::ProviderCreateSessionRequest {
                    runtime_session_id: "sess_capacity_b".to_string(),
                    model: None,
                    cwd: None,
                    permission_mode: None,
                    metadata: None,
                })
                .await
        });

        let result_a = create_a.await.expect("task a");
        let result_b = create_b.await.expect("task b");
        let successes = [result_a.is_ok(), result_b.is_ok()]
            .into_iter()
            .filter(|value| *value)
            .count();
        let failures = [result_a, result_b]
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();

        assert_eq!(successes, 1, "exactly one concurrent create should succeed");
        assert_eq!(
            failures.len(),
            1,
            "exactly one concurrent create should fail"
        );
        assert!(failures[0]
            .to_string()
            .contains("acp session capacity exceeded (1 total sessions"));
    }

    #[tokio::test]
    async fn close_session_shuts_down_idle_connection() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();

        let created = provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_idle_close".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");
        assert!(created.provider_session_ref.starts_with("sess_"));
        assert!(provider.inner.connection.lock().await.is_some());

        provider
            .close_session(runtime_core::ProviderCloseSessionRequest {
                runtime_session_id: "sess_idle_close".to_string(),
                reason: Some("idle close".to_string()),
            })
            .await
            .expect("close");

        assert!(provider.inner.connection.lock().await.is_none());
    }

    #[tokio::test]
    async fn real_adapter_contract_handles_malformed_agent_output() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_malformed".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_malformed".to_string(),
                turn_id: "turn_malformed".to_string(),
                input: vec![json!({"type":"text","text":"malformed response"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_malformed".to_string(),
                turn_id: "turn_malformed".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
        assert!(result
            .error
            .as_ref()
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("malformed JSON-RPC")));
    }

    #[tokio::test]
    async fn real_adapter_contract_handles_process_death() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_crash".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_crash".to_string(),
                turn_id: "turn_crash".to_string(),
                input: vec![json!({"type":"text","text":"crash now"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_crash".to_string(),
                turn_id: "turn_crash".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
        assert!(result
            .error
            .as_ref()
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("connection closed")));
    }

    #[tokio::test]
    async fn real_adapter_contract_preserves_ordered_updates_in_terminal_usage() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_tooling".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_tooling".to_string(),
                turn_id: "turn_tooling".to_string(),
                input: vec![json!({"type":"text","text":"tooling please"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_tooling".to_string(),
                turn_id: "turn_tooling".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Completed);
        let usage = result.usage.expect("usage");
        assert_eq!(
            usage.get("last_message").and_then(Value::as_str),
            Some("First line.\n\nSecond line.")
        );
        assert_eq!(
            usage.get("assistant_text").and_then(Value::as_str),
            Some("First line.\n\nSecond line.")
        );
        assert_eq!(
            usage.pointer("/usage_update/used").and_then(Value::as_i64),
            Some(11)
        );
        let tool_calls = usage
            .get("tool_calls")
            .and_then(Value::as_array)
            .expect("tool calls");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(
            tool_calls[0].get("toolCallId").and_then(Value::as_str),
            Some("tool_1")
        );
        assert_eq!(
            tool_calls[0].get("sessionUpdate").and_then(Value::as_str),
            Some("tool_call_update")
        );
    }

    #[tokio::test]
    async fn real_adapter_contract_stages_runtime_approval_before_execution() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_approval".to_string(),
                model: None,
                cwd: None,
                permission_mode: Some("require_approval".to_string()),
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_approval".to_string(),
                turn_id: "turn_approval".to_string(),
                input: vec![json!({"type":"text","text":"split please"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                approval_id: Some("apr_1".to_string()),
            })
            .await
            .expect("send");

        provider
            .respond_approval(runtime_core::ProviderApprovalResponseRequest {
                runtime_session_id: "sess_approval".to_string(),
                turn_id: "turn_approval".to_string(),
                approval_id: "apr_1".to_string(),
                decision: "accept".to_string(),
                payload: None,
            })
            .await
            .expect("respond");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_approval".to_string(),
                turn_id: "turn_approval".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Completed);
    }

    #[tokio::test]
    async fn real_adapter_contract_declined_runtime_approval_interrupts_turn() {
        let harness = FakeAgentHarness::new("normal");
        let provider = harness.provider();
        provider
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_decline".to_string(),
                model: None,
                cwd: None,
                permission_mode: Some("require_approval".to_string()),
                metadata: None,
            })
            .await
            .expect("create");

        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_decline".to_string(),
                turn_id: "turn_decline".to_string(),
                input: vec![json!({"type":"text","text":"will not run"})],
                expected_turn_id: None,
                permission_mode: Some("require_approval".to_string()),
                approval_id: Some("apr_decline".to_string()),
            })
            .await
            .expect("send");

        provider
            .respond_approval(runtime_core::ProviderApprovalResponseRequest {
                runtime_session_id: "sess_decline".to_string(),
                turn_id: "turn_decline".to_string(),
                approval_id: "apr_decline".to_string(),
                decision: "decline".to_string(),
                payload: None,
            })
            .await
            .expect("respond");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_decline".to_string(),
                turn_id: "turn_decline".to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Interrupted);
    }
}
