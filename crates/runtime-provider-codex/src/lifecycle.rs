use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use runtime_core::{
    provider_harness_text, ProviderCreateSessionPolicyRequest, ProviderKind,
    ProviderResumeSessionPolicyRequest, ProviderRuntimeEvent, ProviderSession, ProviderTurnResult,
    ProviderTurnStatus, RuntimeError,
};
use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::mcp_config::format_codex_gg_mcp_config;
use crate::protocol::{apply_thread_permission_mode, parse_turn_status, path_component};
use crate::provider::CodexProvider;
use crate::state::{
    CodexProviderInner, CodexSessionState, PendingProviderApproval, PendingTerminalTurn,
};
use crate::transport::CodexTransport;

fn merge_assistant_text_into_usage(
    usage: Option<Value>,
    assistant_text: Option<String>,
) -> Option<Value> {
    let Some(assistant_text) = assistant_text
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return usage;
    };

    match usage {
        Some(Value::Object(mut usage_object)) => {
            usage_object.insert(
                "last_message".to_string(),
                Value::String(assistant_text.clone()),
            );
            usage_object.insert("assistant_text".to_string(), Value::String(assistant_text));
            Some(Value::Object(usage_object))
        }
        Some(existing) => Some(json!({
            "last_message": assistant_text.clone(),
            "assistant_text": assistant_text,
            "raw_usage": existing,
        })),
        None => Some(json!({
            "last_message": assistant_text.clone(),
            "assistant_text": assistant_text,
        })),
    }
}

fn merge_staged_config(
    base_config: Option<&str>,
    gg_mcp_config: Option<&str>,
) -> Result<Option<String>, RuntimeError> {
    if base_config.is_none() && gg_mcp_config.is_none() {
        return Ok(None);
    }

    let mut base = match base_config {
        Some(config) => toml::from_str::<toml::Table>(config).map_err(|error| {
            RuntimeError::ProtocolViolation(format!(
                "failed to parse base Codex config.toml before per-session staging: {error}"
            ))
        })?,
        None => toml::Table::new(),
    };

    if let Some(gg_mcp_config) = gg_mcp_config {
        let mut overlay = toml::from_str::<toml::Table>(gg_mcp_config).map_err(|error| {
            RuntimeError::ProtocolViolation(format!(
                "failed to parse generated Codex GG MCP config: {error}"
            ))
        })?;
        if let Some(overlay_servers) = overlay.remove("mcp_servers") {
            let overlay_servers = overlay_servers.as_table().ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "generated Codex GG MCP config mcp_servers must be a table".to_string(),
                )
            })?;
            let servers = base
                .entry("mcp_servers")
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation(
                        "base Codex config mcp_servers must be a table".to_string(),
                    )
                })?;
            for (name, value) in overlay_servers {
                servers.insert(name.clone(), value.clone());
            }
        }
    }

    toml::to_string(&base).map(Some).map_err(|error| {
        RuntimeError::ProtocolViolation(format!(
            "failed to serialize staged Codex config.toml: {error}"
        ))
    })
}
use crate::CodexProviderConfig;

impl CodexProvider {
    pub(super) fn codex_auth_path(&self) -> PathBuf {
        self.inner.config.home_dir.join("auth.json")
    }

    fn aggregate_session_capacity(&self) -> usize {
        self.inner
            .config
            .max_transports
            .max(1)
            .saturating_mul(self.inner.config.max_sessions_per_transport.max(1))
    }

    fn session_home(&self, runtime_session_id: &str) -> PathBuf {
        self.inner
            .config
            .home_dir
            .join("runtime-sessions")
            .join(path_component(runtime_session_id))
    }

    fn auxiliary_home(&self, name: &str) -> PathBuf {
        self.inner
            .config
            .home_dir
            .join("aux")
            .join(path_component(name))
    }

    fn transport_config_for_home(&self, home_dir: PathBuf) -> CodexProviderConfig {
        CodexProviderConfig {
            home_dir,
            ..self.inner.config.clone()
        }
    }

    fn prepare_home(
        &self,
        home: &Path,
        runtime_session_id: Option<&str>,
    ) -> Result<(), RuntimeError> {
        std::fs::create_dir_all(home).map_err(|error| {
            RuntimeError::Io(format!(
                "failed to create Codex home {}: {error}",
                home.display()
            ))
        })?;

        let base_auth = self.codex_auth_path();
        let session_auth = home.join("auth.json");
        if base_auth != session_auth {
            if base_auth.exists() {
                std::fs::copy(&base_auth, &session_auth).map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to stage Codex auth {} -> {}: {error}",
                        base_auth.display(),
                        session_auth.display()
                    ))
                })?;
            } else if session_auth.exists() {
                std::fs::remove_file(&session_auth).map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to remove stale staged Codex auth {}: {error}",
                        session_auth.display()
                    ))
                })?;
            }
        }

        let base_config_path = self.inner.config.home_dir.join("config.toml");
        let staged_config_path = home.join("config.toml");
        if base_config_path != staged_config_path {
            let base_config = if base_config_path.exists() {
                Some(std::fs::read_to_string(&base_config_path).map_err(|error| {
                    RuntimeError::Io(format!(
                        "failed to read base Codex config {}: {error}",
                        base_config_path.display()
                    ))
                })?)
            } else {
                None
            };
            let gg_mcp = runtime_session_id
                .filter(|_| self.inner.config.gg_mcp.enabled)
                .map(|runtime_session_id| {
                    format_codex_gg_mcp_config(&self.inner.config.gg_mcp, runtime_session_id)
                });
            match merge_staged_config(base_config.as_deref(), gg_mcp.as_deref())? {
                Some(rendered) => {
                    std::fs::write(&staged_config_path, rendered).map_err(|error| {
                        RuntimeError::Io(format!(
                            "failed to write staged Codex config {}: {error}",
                            staged_config_path.display()
                        ))
                    })?;
                }
                None if staged_config_path.exists() => {
                    std::fs::remove_file(&staged_config_path).map_err(|error| {
                        RuntimeError::Io(format!(
                            "failed to remove stale staged Codex config {}: {error}",
                            staged_config_path.display()
                        ))
                    })?;
                }
                None => {}
            }
        }

        Ok(())
    }

    pub(super) async fn spawn_transport_for_session(
        &self,
        runtime_session_id: &str,
    ) -> Result<(Arc<CodexTransport>, PathBuf), RuntimeError> {
        let home = self.session_home(runtime_session_id);
        self.prepare_home(&home, Some(runtime_session_id))?;
        let transport =
            CodexTransport::spawn(&self.transport_config_for_home(home.clone())).await?;
        Ok((transport, home))
    }

    pub(super) async fn spawn_auxiliary_transport(
        &self,
        name: &str,
    ) -> Result<Arc<CodexTransport>, RuntimeError> {
        let home = self.auxiliary_home(name);
        self.prepare_home(&home, None)?;
        CodexTransport::spawn(&self.transport_config_for_home(home)).await
    }

    pub(super) fn developer_instructions(
        system_prompt: Option<&str>,
    ) -> Result<String, RuntimeError> {
        let harness = provider_harness_text(ProviderKind::Codex)?.unwrap_or_default();
        let system_prompt = system_prompt
            .map(str::trim)
            .filter(|value| !value.is_empty());
        Ok(match system_prompt {
            Some(system_prompt) if harness.is_empty() => system_prompt.to_string(),
            Some(system_prompt) => format!("{harness}\n\n{system_prompt}"),
            None => harness,
        })
    }

    pub(super) fn thread_start_params(
        req: &ProviderCreateSessionPolicyRequest,
    ) -> Result<Value, RuntimeError> {
        req.launch_policy
            .resolved_setting_sources(req.cwd.as_deref())?;
        let mut params = json!({
            "developerInstructions": Self::developer_instructions(
                req.launch_policy.system_prompt.as_deref()
            )?,
            "ephemeral": false,
        });
        if let Some(cwd) = req
            .cwd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params["cwd"] = json!(cwd);
        }
        if let Some(model) = req
            .model
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params["model"] = json!(model);
        }
        apply_thread_permission_mode(
            &mut params,
            req.launch_policy
                .permission_intent
                .resolved_mode()
                .as_deref(),
        );
        Ok(params)
    }

    pub(super) fn thread_resume_params(
        req: &ProviderResumeSessionPolicyRequest,
    ) -> Result<Value, RuntimeError> {
        req.launch_policy
            .resolved_setting_sources(req.cwd.as_deref())?;
        let mut params = json!({
            "threadId": req.provider_session_ref,
            "developerInstructions": Self::developer_instructions(
                req.launch_policy.system_prompt.as_deref()
            )?,
        });
        if let Some(cwd) = req
            .cwd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params["cwd"] = json!(cwd);
        }
        if let Some(model) = req
            .model
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params["model"] = json!(model);
        }
        apply_thread_permission_mode(
            &mut params,
            req.launch_policy
                .permission_intent
                .resolved_mode()
                .as_deref(),
        );
        Ok(params)
    }

    fn spawn_session_router(
        inner: Weak<CodexProviderInner>,
        runtime_session_id: String,
        transport: Arc<CodexTransport>,
    ) {
        let mut receiver = transport.subscribe();
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(message) => {
                        let Some(inner) = inner.upgrade() else {
                            break;
                        };
                        Self::route_transport_message(&inner, &runtime_session_id, message).await;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let Some(inner) = inner.upgrade() else {
                            break;
                        };
                        Self::emit_turn_outcome_unknown(
                            &inner,
                            &runtime_session_id,
                            "codex_transport_event_lagged",
                            format!("Codex transport event router skipped {skipped} messages"),
                        )
                        .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    async fn emit_turn_outcome_unknown(
        inner: &Arc<CodexProviderInner>,
        runtime_session_id: &str,
        code: &str,
        message: String,
    ) {
        let active_turn_id = {
            let sessions = inner.sessions.read().await;
            sessions
                .get(runtime_session_id)
                .and_then(|session| session.active_turn_id.clone())
        };
        if let Some(turn_id) = active_turn_id {
            let _ = inner.events.send(ProviderRuntimeEvent::TurnOutcomeUnknown {
                runtime_session_id: runtime_session_id.to_string(),
                turn_id,
                code: code.to_string(),
                message,
            });
        }
    }

    async fn route_transport_message(
        inner: &Arc<CodexProviderInner>,
        runtime_session_id: &str,
        message: Value,
    ) {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return;
        };

        match method {
            "item/completed" => {
                let Some(params) = message.get("params") else {
                    return;
                };
                let Some(native_turn_id) = params.get("turnId").and_then(Value::as_str) else {
                    return;
                };
                let Some(item) = params.get("item") else {
                    return;
                };
                if item.get("type").and_then(Value::as_str) != Some("agentMessage") {
                    return;
                }
                let Some(text) = item.get("text").and_then(Value::as_str) else {
                    return;
                };
                let mut sessions = inner.sessions.write().await;
                if let Some(session) = sessions.get_mut(runtime_session_id) {
                    session
                        .last_messages
                        .insert(native_turn_id.to_string(), text.to_string());
                }
            }
            "thread/tokenUsage/updated" => {
                let Some(params) = message.get("params") else {
                    return;
                };
                let Some(token_usage) = params.get("tokenUsage") else {
                    return;
                };
                let native_turn_id = params.get("turnId").and_then(Value::as_str);
                let mut sessions = inner.sessions.write().await;
                let Some(session) = sessions.get_mut(runtime_session_id) else {
                    return;
                };
                if let Some(native_turn_id) = native_turn_id {
                    session
                        .usage_by_native_turn
                        .insert(native_turn_id.to_string(), token_usage.clone());
                }
                session.model_context_window = token_usage
                    .get("modelContextWindow")
                    .and_then(Value::as_u64)
                    .or(session.model_context_window);
                session.last_total_tokens = token_usage
                    .get("total")
                    .and_then(|total| total.get("totalTokens"))
                    .and_then(Value::as_u64)
                    .or(session.last_total_tokens);
            }
            "thread/compacted" => {
                let mut sessions = inner.sessions.write().await;
                if let Some(session) = sessions.get_mut(runtime_session_id) {
                    for waiter in session.compaction_waiters.drain(..) {
                        let _ = waiter.send(());
                    }
                }
            }
            "turn/completed" => {
                let Some(turn) = message.get("params").and_then(|params| params.get("turn")) else {
                    return;
                };
                let Some(native_turn_id) = turn.get("id").and_then(Value::as_str) else {
                    return;
                };
                let status = parse_turn_status(turn.get("status").and_then(Value::as_str));
                let error = turn.get("error").filter(|value| !value.is_null()).cloned();
                Self::complete_native_turn(
                    inner,
                    runtime_session_id,
                    native_turn_id,
                    status,
                    error,
                )
                .await;
            }
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => {
                let Some(rpc_id) = message.get("id").cloned() else {
                    return;
                };
                let request = message.get("params").cloned().unwrap_or(Value::Null);
                let Some(native_turn_id) = request.get("turnId").and_then(Value::as_str) else {
                    return;
                };
                let tool_call_id = request
                    .get("itemId")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let provider_approval_ref = request
                    .get("approvalId")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        format!(
                            "{}:{}:{}",
                            method,
                            tool_call_id.as_deref().unwrap_or("item"),
                            rpc_id
                        )
                    });

                let mut sessions = inner.sessions.write().await;
                let Some(session) = sessions.get_mut(runtime_session_id) else {
                    return;
                };
                let logical_turn_id = session.native_to_logical_turns.get(native_turn_id).cloned();
                session.pending_approvals.insert(
                    provider_approval_ref.clone(),
                    PendingProviderApproval {
                        rpc_id,
                        method: method.to_string(),
                        native_turn_id: native_turn_id.to_string(),
                        tool_call_id: tool_call_id.clone(),
                        request: request.clone(),
                        emitted: logical_turn_id.is_some(),
                    },
                );
                if let Some(logical_turn_id) = logical_turn_id {
                    let _ = inner.events.send(ProviderRuntimeEvent::ApprovalRequested {
                        runtime_session_id: runtime_session_id.to_string(),
                        turn_id: logical_turn_id,
                        provider_approval_ref,
                        tool_call_id,
                        request,
                    });
                }
            }
            "transport/closed" | "transport/error" => {
                let message = message
                    .get("params")
                    .and_then(|params| params.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("Codex app-server transport closed")
                    .to_string();
                Self::emit_turn_outcome_unknown(
                    inner,
                    runtime_session_id,
                    "codex_transport_outcome_unknown",
                    message,
                )
                .await;
            }
            _ => {}
        }
    }

    pub(super) async fn complete_native_turn(
        inner: &Arc<CodexProviderInner>,
        runtime_session_id: &str,
        native_turn_id: &str,
        status: ProviderTurnStatus,
        error: Option<Value>,
    ) {
        let mut sessions = inner.sessions.write().await;
        let Some(session) = sessions.get_mut(runtime_session_id) else {
            return;
        };
        let usage = merge_assistant_text_into_usage(
            session.usage_by_native_turn.get(native_turn_id).cloned(),
            session.last_messages.get(native_turn_id).cloned(),
        );
        let Some(logical_turn_id) = session.native_to_logical_turns.get(native_turn_id).cloned()
        else {
            if let Some(existing) = session.pending_terminal_by_native.get(native_turn_id) {
                if existing.status != status || existing.error != error {
                    let turn_id = session.active_turn_id.clone();
                    drop(sessions);
                    if let Some(turn_id) = turn_id {
                        let _ = inner.events.send(ProviderRuntimeEvent::TurnOutcomeUnknown {
                            runtime_session_id: runtime_session_id.to_string(),
                            turn_id,
                            code: "codex_conflicting_terminal_observation".to_string(),
                            message: format!(
                                "Codex emitted conflicting terminal observations for native turn {native_turn_id} before logical mapping was established"
                            ),
                        });
                    }
                    return;
                }
            } else {
                session.pending_terminal_by_native.insert(
                    native_turn_id.to_string(),
                    PendingTerminalTurn { status, error },
                );
            }
            return;
        };

        if session
            .completed_turns
            .contains_key(logical_turn_id.as_str())
        {
            return;
        }
        let result = ProviderTurnResult {
            runtime_session_id: runtime_session_id.to_string(),
            turn_id: logical_turn_id.clone(),
            status,
            usage,
            error,
        };
        if session.active_turn_id.as_deref() == Some(logical_turn_id.as_str()) {
            session.active_turn_id = None;
        }
        session
            .completed_turns
            .insert(logical_turn_id.clone(), result.clone());
        if let Some(waiters) = session.waiters.remove(logical_turn_id.as_str()) {
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }
        }
    }

    pub(super) async fn bind_native_turn(
        &self,
        runtime_session_id: &str,
        logical_turn_id: &str,
        native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let pending_terminal = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions.get_mut(runtime_session_id).ok_or_else(|| {
                RuntimeError::NotFound(format!("codex session {runtime_session_id}"))
            })?;
            if let Some(existing) = session.logical_to_native_turns.get(logical_turn_id) {
                if existing != native_turn_id {
                    return Err(RuntimeError::ProtocolViolation(format!(
                        "logical turn {logical_turn_id} already maps to native turn {existing}, not {native_turn_id}"
                    )));
                }
            }
            if let Some(existing) = session.native_to_logical_turns.get(native_turn_id) {
                if existing != logical_turn_id {
                    return Err(RuntimeError::ProtocolViolation(format!(
                        "native turn {native_turn_id} already maps to logical turn {existing}, not {logical_turn_id}"
                    )));
                }
            }
            session
                .logical_to_native_turns
                .insert(logical_turn_id.to_string(), native_turn_id.to_string());
            session
                .native_to_logical_turns
                .insert(native_turn_id.to_string(), logical_turn_id.to_string());

            for (approval_ref, approval) in session.pending_approvals.iter_mut() {
                if approval.native_turn_id == native_turn_id && !approval.emitted {
                    approval.emitted = true;
                    let _ = self
                        .inner
                        .events
                        .send(ProviderRuntimeEvent::ApprovalRequested {
                            runtime_session_id: runtime_session_id.to_string(),
                            turn_id: logical_turn_id.to_string(),
                            provider_approval_ref: approval_ref.clone(),
                            tool_call_id: approval.tool_call_id.clone(),
                            request: approval.request.clone(),
                        });
                }
            }
            session.pending_terminal_by_native.remove(native_turn_id)
        };

        if let Some(pending_terminal) = pending_terminal {
            Self::complete_native_turn(
                &self.inner,
                runtime_session_id,
                native_turn_id,
                pending_terminal.status,
                pending_terminal.error,
            )
            .await;
        }
        Ok(())
    }

    pub(super) async fn install_session(
        &self,
        runtime_session_id: String,
        state: CodexSessionState,
    ) -> Result<ProviderSession, RuntimeError> {
        let result = ProviderSession {
            runtime_session_id: runtime_session_id.clone(),
            provider_session_ref: state.provider_session_ref.clone(),
            canonical_provider_session_ref: state.canonical_provider_session_ref.clone(),
        };
        let transport = Arc::clone(&state.transport);
        self.inner
            .sessions
            .write()
            .await
            .insert(runtime_session_id.clone(), state);
        Self::spawn_session_router(Arc::downgrade(&self.inner), runtime_session_id, transport);
        Ok(result)
    }

    pub(super) async fn ensure_capacity_for_new_session(
        &self,
        runtime_session_id: &str,
    ) -> Result<(), RuntimeError> {
        let sessions = self.inner.sessions.read().await;
        if sessions.contains_key(runtime_session_id) {
            return Err(RuntimeError::Conflict(format!(
                "codex session {runtime_session_id} is already active"
            )));
        }
        let capacity = self.aggregate_session_capacity();
        if sessions.len() >= capacity {
            return Err(RuntimeError::provider_not_dispatched(
                "codex_capacity_exhausted",
                format!(
                    "Codex session capacity exhausted (active={}, capacity={capacity})",
                    sessions.len()
                ),
            ));
        }
        Ok(())
    }
}
