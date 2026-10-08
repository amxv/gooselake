use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use runtime_core::{ProviderTurnResult, ProviderTurnStatus, RuntimeError};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::config::AcpProviderConfig;
use crate::connection::AcpConnection;
use crate::protocol::absolutize_path;
use crate::state::{AcpActiveTurnState, AcpSessionState};

#[derive(Debug)]
pub(super) struct AcpProviderInner {
    pub(super) config: AcpProviderConfig,
    pub(super) connection: Mutex<Option<Arc<AcpConnection>>>,
    pub(super) next_connection_id: std::sync::atomic::AtomicU64,
    pub(super) provider_events: broadcast::Sender<runtime_core::ProviderRuntimeEvent>,
    pub(super) permission_response_gate: Mutex<()>,
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
        let (provider_events, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(AcpProviderInner {
                config: AcpProviderConfig {
                    provider_dir: absolutize_path(config.provider_dir.as_path()),
                    ..config
                },
                connection: Mutex::new(None),
                next_connection_id: std::sync::atomic::AtomicU64::new(1),
                provider_events,
                permission_response_gate: Mutex::new(()),
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
            return Err(RuntimeError::Conflict(format!(
                "ACP runtime session {runtime_session_id} is already reserved or active"
            )));
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
        drop(sessions);
        self.shutdown_connection_if_idle().await;
    }

    /// Reserve either a fresh runtime attachment or a provably idle stale
    /// attachment. A restarted child never inherits ownership of native IDs
    /// merely because it happens to reuse the same sessionId string.
    pub(super) async fn prepare_resume_slot(
        &self,
        runtime_session_id: &str,
        provider_session_ref: &str,
        connection_id: u64,
    ) -> Result<bool, RuntimeError> {
        let mut sessions = self.inner.sessions.write().await;
        if sessions.iter().any(|(id, session)| {
            id != runtime_session_id
                && session.provider_session_ref == provider_session_ref
                && session.connection_id == Some(connection_id)
        }) {
            return Err(RuntimeError::Conflict(
                "ACP native session is already bound to another runtime agent".into(),
            ));
        }
        if let Some(session) = sessions.get_mut(runtime_session_id) {
            if session.provider_session_ref != provider_session_ref {
                return Err(RuntimeError::ProtocolViolation(
                    "ACP resume native session identity differs from persisted attachment".into(),
                ));
            }
            if session.resuming
                || session.connection_id == Some(connection_id)
                || session.active_turn.is_some()
                || !session.pending_approvals.is_empty()
                || !session.pending_native_permissions.is_empty()
            {
                return Err(RuntimeError::Conflict(
                    "ACP session cannot be rebound while active or already attached".into(),
                ));
            }
            if session.completed_turns.values().any(|result| {
                result
                    .error
                    .as_ref()
                    .and_then(|value| value.get("code"))
                    .and_then(Value::as_str)
                    == Some("acp_prompt_dispatch_unknown")
            }) {
                return Err(RuntimeError::InvalidState(
                    "ACP dispatch outcome is unresolved; reconcile or close before resuming".into(),
                ));
            }
            session.resuming = true;
            return Ok(false);
        }

        let capacity = self.max_session_capacity();
        if sessions.len() >= capacity {
            return Err(RuntimeError::InvalidState(format!(
                "acp session capacity exceeded ({capacity} total sessions from max_instances={} * max_sessions_per_instance={})",
                self.inner.config.max_instances, self.inner.config.max_sessions_per_instance
            )));
        }
        sessions.insert(
            runtime_session_id.to_string(),
            AcpSessionState {
                resuming: true,
                ..Default::default()
            },
        );
        Ok(true)
    }

    pub(super) async fn abort_resume_slot(&self, runtime_session_id: &str, is_new: bool) {
        let mut sessions = self.inner.sessions.write().await;
        if is_new {
            sessions.remove(runtime_session_id);
        } else if let Some(session) = sessions.get_mut(runtime_session_id) {
            session.resuming = false;
        }
        drop(sessions);
        self.shutdown_connection_if_idle().await;
    }

    pub(super) async fn activate_reserved_session(
        &self,
        runtime_session_id: &str,
        provider_session_ref: String,
        connection_id: u64,
    ) -> Result<(), RuntimeError> {
        let mut sessions = self.inner.sessions.write().await;
        if provider_session_ref.trim().is_empty() {
            return Err(RuntimeError::ProtocolViolation(
                "ACP provider session identity is empty".into(),
            ));
        }
        if sessions.iter().any(|(runtime_id, existing)| {
            runtime_id != runtime_session_id
                && existing.provider_session_ref == provider_session_ref
                && existing.connection_id == Some(connection_id)
        }) {
            return Err(RuntimeError::Conflict(
                "ACP provider session is already bound to another runtime agent".into(),
            ));
        }
        let session = sessions.get_mut(runtime_session_id).ok_or_else(|| {
            RuntimeError::InvalidState(format!(
                "reserved acp session {} disappeared before activation",
                runtime_session_id
            ))
        })?;
        session.provider_session_ref = provider_session_ref;
        session.connection_id = Some(connection_id);
        session.resuming = false;
        Ok(())
    }

    pub(super) async fn shutdown_connection_if_idle(&self) {
        // Keep the session read guard while detaching the child. An in-flight
        // create must not reserve a slot between the idle check and shutdown.
        let sessions = self.inner.sessions.read().await;
        if !sessions.is_empty() {
            return;
        }
        let connection = self.inner.connection.lock().await.take();
        drop(sessions);
        if let Some(connection) = connection {
            connection.shutdown(true).await;
        }
    }

    pub(super) async fn current_connection(&self) -> Option<Arc<AcpConnection>> {
        let slot = self.inner.connection.lock().await;
        slot.clone()
            .filter(|connection| !connection.closed.load(Ordering::SeqCst))
    }

    pub(super) async fn reap_connection_if_current_and_closed(&self, current: &Arc<AcpConnection>) {
        let connection = {
            let mut slot = self.inner.connection.lock().await;
            if slot.as_ref().is_some_and(|active| {
                Arc::ptr_eq(active, current) && current.closed.load(Ordering::SeqCst)
            }) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(connection) = connection {
            // A protocol error can close our reader while the child is still
            // running. Terminate that child instead of waiting on its stdin.
            connection.shutdown(true).await;
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
        let connection = self.current_connection().await.ok_or_else(|| {
            RuntimeError::provider_not_dispatched(
                "session_not_found",
                "ACP session transport is unavailable; resume must prove the original session before another turn",
            )
        })?;
        let caps = connection.capabilities.read().await.clone();
        let prompt_blocks =
            crate::prompt::build_prompt_blocks(input.as_slice(), &caps).map_err(|error| {
                RuntimeError::provider_not_dispatched("unsupported_acp_input", error.to_string())
            })?;
        let active_turn = AcpActiveTurnState::new(turn_id.to_string());
        let provider_session_ref = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions.get_mut(runtime_session_id).ok_or_else(|| {
                RuntimeError::NotFound(format!("acp session {runtime_session_id}"))
            })?;

            if session.resuming {
                return Err(RuntimeError::provider_not_dispatched(
                    "acp_session_resuming",
                    "ACP session resume/load is in progress",
                ));
            }
            if session.connection_id != Some(connection.instance_id) {
                return Err(RuntimeError::provider_not_dispatched(
                    "session_not_found",
                    "ACP native session belongs to a previous subprocess; resume must prove its native identity before dispatch",
                ));
            }

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

        tokio::spawn(async move {
            let response = connection
                .send_request(
                    "session/prompt",
                    json!({
                        "sessionId": provider_session_ref,
                        "prompt": prompt_blocks,
                    }),
                    Some(provider.wait_timeout(None)),
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
                    status: ProviderTurnStatus::Failed,
                    usage: None,
                    // Once session/prompt crosses the RPC send boundary, a
                    // timeout/disconnect does not prove the agent did no work.
                    // The manager must quarantine rather than blindly replay.
                    error: Some(json!({
                        "code": "acp_prompt_dispatch_unknown",
                        "message": error.to_string(),
                    })),
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
                    .iter()
                    .map(|(_, call)| call.clone())
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
        // A terminal turn cannot race a native permission decision into a
        // stale subprocess request after its approval state was discarded.
        let _permission_guard = self.inner.permission_response_gate.lock().await;
        let is_dispatch_unknown = result
            .error
            .as_ref()
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str)
            == Some("acp_prompt_dispatch_unknown");
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
                .pending_native_permissions
                .retain(|_, pending| pending.turn_id != turn_id);
            session
                .completed_turns
                .insert(turn_id.to_string(), result.clone());
            session.waiters.remove(turn_id).unwrap_or_default()
        };

        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
        if is_dispatch_unknown {
            let _ = self.inner.provider_events.send(
                runtime_core::ProviderRuntimeEvent::TurnOutcomeUnknown {
                    runtime_session_id: runtime_session_id.to_string(),
                    turn_id: turn_id.to_string(),
                    code: "acp_prompt_dispatch_unknown".into(),
                    message: result
                        .error
                        .as_ref()
                        .and_then(|error| error.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("ACP prompt outcome is unknown")
                        .to_string(),
                },
            );
        }
    }

    pub(super) async fn fail_permission_request(
        &self,
        provider_session_ref: &str,
        connection_id: u64,
    ) -> Result<(), RuntimeError> {
        let target = {
            let sessions = self.inner.sessions.read().await;
            sessions.iter().find_map(|(runtime_session_id, session)| {
                if session.provider_session_ref == provider_session_ref
                    && session.connection_id == Some(connection_id)
                {
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
                    "message": "ACP session/request_permission could not be safely routed to an admitted turn",
                })),
            };
            self.complete_turn(runtime_session_id.as_str(), turn_id.as_str(), result)
                .await;
        }

        Ok(())
    }

    pub(super) async fn apply_session_update(
        &self,
        connection_id: u64,
        provider_session_ref: &str,
        update: Value,
    ) -> Result<(), RuntimeError> {
        let active_turn = {
            let sessions = self.inner.sessions.read().await;
            sessions
                .values()
                .find(|session| {
                    session.provider_session_ref == provider_session_ref
                        && session.connection_id == Some(connection_id)
                })
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
                    let mut calls = active_turn.tool_calls.lock().await;
                    if let Some((_, existing)) = calls.iter_mut().find(|(id, _)| id == tool_call_id)
                    {
                        if let (Some(original), Some(delta)) =
                            (existing.as_object_mut(), update.as_object())
                        {
                            for (key, value) in delta {
                                original.insert(key.clone(), value.clone());
                            }
                        }
                    } else {
                        calls.push((tool_call_id.to_string(), update));
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}
