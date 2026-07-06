use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use runtime_core::{ProviderTurnResult, ProviderTurnStatus, RuntimeError};
use serde_json::{json, Value};
use tokio::sync::{Mutex, RwLock};

use crate::config::AcpProviderConfig;
use crate::connection::AcpConnection;
use crate::protocol::absolutize_path;
use crate::state::{AcpActiveTurnState, AcpSessionState};

#[derive(Debug)]
pub(super) struct AcpProviderInner {
    pub(super) config: AcpProviderConfig,
    pub(super) connection: Mutex<Option<Arc<AcpConnection>>>,
    pub(super) sessions: RwLock<HashMap<String, AcpSessionState>>,
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
    pub(crate) inner: Arc<AcpProviderInner>,
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

    pub(super) fn runtime_subdirs(&self) -> [PathBuf; 3] {
        [
            self.provider_dir().to_path_buf(),
            self.provider_dir().join("instances"),
            self.provider_dir().join("sessions"),
        ]
    }

    pub(super) fn validate_base_config(&self) -> Result<(), RuntimeError> {
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

    pub(super) fn configured_command(&self) -> Result<String, RuntimeError> {
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

    pub(super) async fn ensure_provider_enabled(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap("acp provider disabled".to_string()));
        }
        self.validate_base_config()?;
        self.configured_command()?;
        self.ensure_runtime_dirs().await?;
        Ok(())
    }

    pub(super) async fn ensure_runtime_dirs(&self) -> Result<(), RuntimeError> {
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

    pub(super) fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.inner.config.request_timeout_secs.max(1))
    }

    pub(super) fn wait_timeout(&self, timeout_ms: Option<u64>) -> Duration {
        match timeout_ms {
            Some(value) => Duration::from_millis(value.max(1)),
            None => Duration::from_secs(self.inner.config.wait_timeout_secs.max(1)),
        }
    }

    pub(super) fn max_session_capacity(&self) -> usize {
        self.inner
            .config
            .max_instances
            .saturating_mul(self.inner.config.max_sessions_per_instance)
    }

    pub(super) async fn reserve_session_slot(
        &self,
        runtime_session_id: &str,
    ) -> Result<(), RuntimeError> {
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

    pub(super) async fn release_session_slot(&self, runtime_session_id: &str) {
        let mut sessions = self.inner.sessions.write().await;
        sessions.remove(runtime_session_id);
    }

    pub(super) async fn activate_reserved_session(
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

    pub(super) async fn shutdown_connection(&self, kill_if_running: bool) {
        let connection = {
            let mut slot = self.inner.connection.lock().await;
            slot.take()
        };
        if let Some(connection) = connection {
            connection.shutdown(kill_if_running).await;
        }
    }

    pub(super) async fn shutdown_connection_if_idle(&self) {
        let is_idle = {
            let sessions = self.inner.sessions.read().await;
            sessions.is_empty()
        };
        if is_idle {
            self.shutdown_connection(true).await;
        }
    }

    pub(super) async fn current_connection(&self) -> Option<Arc<AcpConnection>> {
        let slot = self.inner.connection.lock().await;
        slot.clone()
            .filter(|connection| !connection.closed.load(Ordering::SeqCst))
    }

    pub(super) async fn reap_connection_if_current_and_closed(&self, current: &Arc<AcpConnection>) {
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

    pub(super) fn build_gg_mcp_server_config(&self, runtime_session_id: &str) -> Option<Value> {
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

    pub(super) fn build_mcp_servers(&self, runtime_session_id: &str) -> Value {
        match self.build_gg_mcp_server_config(runtime_session_id) {
            Some(server) => Value::Array(vec![server]),
            None => Value::Array(Vec::new()),
        }
    }

    pub(super) async fn ensure_connection(&self) -> Result<Arc<AcpConnection>, RuntimeError> {
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

    pub(super) fn build_prompt_blocks(input: &[Value]) -> Vec<Value> {
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

    pub(super) fn resolve_session_cwd(cwd: Option<&str>) -> Result<String, RuntimeError> {
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

    pub(super) async fn execute_turn(
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

    pub(super) async fn build_prompt_result(
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

    pub(super) async fn complete_turn(
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

    pub(super) async fn fail_permission_request(
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

    pub(super) async fn apply_session_update(
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
