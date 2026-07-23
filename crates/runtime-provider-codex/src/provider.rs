use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCloseSessionRequest, ProviderCreateSessionRequest, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderSession, ProviderTurnAck, ProviderTurnResult,
    ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError, RuntimeProvider,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::mcp_config::format_codex_gg_mcp_config;
use crate::state::{CodexProviderInner, CodexSessionState, PendingApprovalTurn, RunningTurn};
use crate::CodexProviderConfig;

#[derive(Clone, Debug)]
pub struct CodexProvider {
    pub(super) inner: Arc<CodexProviderInner>,
}

impl CodexProvider {
    pub fn new(config: CodexProviderConfig) -> Self {
        let config = CodexProviderConfig {
            home_dir: absolutize_path(config.home_dir.as_path()),
            ..config
        };
        Self {
            inner: Arc::new(CodexProviderInner {
                config,
                sessions: RwLock::new(std::collections::HashMap::new()),
            }),
        }
    }

    fn codex_auth_path(&self) -> PathBuf {
        self.inner.config.home_dir.join("auth.json")
    }

    fn codex_config_path(&self) -> PathBuf {
        self.inner.config.home_dir.join("config.toml")
    }

    fn write_gg_mcp_config_for_session(
        &self,
        runtime_session_id: &str,
    ) -> Result<(), RuntimeError> {
        if !self.inner.config.gg_mcp.enabled {
            return Ok(());
        }
        std::fs::create_dir_all(&self.inner.config.home_dir).map_err(|error| {
            RuntimeError::Io(format!(
                "failed to create codex home {}: {error}",
                self.inner.config.home_dir.display()
            ))
        })?;
        let config = format_codex_gg_mcp_config(&self.inner.config.gg_mcp, runtime_session_id);
        let config_path = self.codex_config_path();
        std::fs::write(config_path.as_path(), config).map_err(|error| {
            RuntimeError::Io(format!(
                "failed to write codex MCP config {}: {error}",
                config_path.display()
            ))
        })
    }

    pub(crate) fn build_turn_prompt(input: &[Value]) -> String {
        let mut lines = Vec::new();
        for item in input {
            if let Some(text) = item
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                lines.push(text.to_string());
                continue;
            }
            if let Some(kind) = item.get("type").and_then(Value::as_str) {
                lines.push(format!("[{kind}] {item}"));
                continue;
            }
            if let Some(raw) = item
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                lines.push(raw.to_string());
                continue;
            }
            lines.push(item.to_string());
        }

        if lines.is_empty() {
            return "Continue with the latest task context.".to_string();
        }
        lines.join("\n\n")
    }

    fn is_placeholder_provider_ref(provider_session_ref: &str) -> bool {
        provider_session_ref.starts_with("runtime:")
    }

    pub(crate) fn build_turn_command_args(
        last_message_path: &Path,
        provider_session_ref: &str,
        model: Option<&str>,
        permission_mode: Option<&str>,
        prompt: &str,
    ) -> Vec<OsString> {
        let mut args = Vec::new();
        args.push(OsString::from("exec"));

        if !Self::is_placeholder_provider_ref(provider_session_ref) {
            args.push(OsString::from("resume"));
        }

        args.push(OsString::from("--json"));
        args.push(OsString::from("--skip-git-repo-check"));
        args.push(OsString::from("-o"));
        args.push(last_message_path.as_os_str().to_os_string());

        if let Some(model) = model {
            args.push(OsString::from("-m"));
            args.push(OsString::from(model));
        }

        match permission_mode {
            Some("full_auto") => args.push(OsString::from("--full-auto")),
            Some("danger_full_access") | Some("danger-full-access") => {
                args.push(OsString::from("--dangerously-bypass-approvals-and-sandbox"))
            }
            _ => {}
        }

        if !Self::is_placeholder_provider_ref(provider_session_ref) {
            args.push(OsString::from(provider_session_ref));
        }
        args.push(OsString::from(prompt));
        args
    }

    fn spawn_turn(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        input: &[Value],
        session: &CodexSessionState,
    ) -> Result<RunningTurn, RuntimeError> {
        let prompt = Self::build_turn_prompt(input);
        let last_message_path = self
            .inner
            .config
            .home_dir
            .join("turn-last-message")
            .join(format!("{runtime_session_id}-{turn_id}.txt"));
        if let Some(parent) = last_message_path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create codex last-message dir {}: {error}",
                    parent.display()
                ))
            })?;
        }
        self.write_gg_mcp_config_for_session(runtime_session_id)?;

        let mut command = Command::new("codex");
        command.env("CODEX_HOME", self.inner.config.home_dir.as_os_str());
        let provider_ref = session.provider_session_ref.clone();
        for arg in Self::build_turn_command_args(
            last_message_path.as_path(),
            provider_ref.as_str(),
            session.model.as_deref(),
            session.permission_mode.as_deref(),
            prompt.as_str(),
        ) {
            command.arg(arg);
        }

        if let Some(cwd) = session.cwd.as_ref() {
            command.current_dir(cwd);
        }

        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());

        let mut child = command
            .spawn()
            .map_err(|error| RuntimeError::Io(format!("failed to spawn codex: {error}")))?;

        let stdout = child.stdout.take().ok_or_else(|| {
            RuntimeError::Io("codex process did not expose stdout pipe".to_string())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            RuntimeError::Io("codex process did not expose stderr pipe".to_string())
        })?;

        let child = Arc::new(Mutex::new(child));
        let interrupt_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let provider = self.clone();
        let runtime_session_id = runtime_session_id.to_string();
        let turn_id = turn_id.to_string();
        let interrupt_state = Arc::clone(&interrupt_requested);
        let child_for_task = Arc::clone(&child);
        let last_message_path_for_task = last_message_path.clone();

        tokio::spawn(async move {
            let stderr_task = tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut bytes = Vec::new();
                if reader.read_to_end(&mut bytes).await.is_err() {
                    return String::new();
                }
                String::from_utf8_lossy(&bytes).to_string()
            });

            let mut line_reader = BufReader::new(stdout).lines();
            let mut thread_id: Option<String> = None;
            let mut terminal_status: Option<ProviderTurnStatus> = None;
            let mut usage_payload: Option<Value> = None;
            let mut error_payload: Option<Value> = None;
            let mut last_message: Option<String> = None;

            while let Ok(Some(line)) = line_reader.next_line().await {
                let Ok(event) = serde_json::from_str::<Value>(line.as_str()) else {
                    continue;
                };
                let event_type = event.get("type").and_then(Value::as_str);
                match event_type {
                    Some("thread.started") => {
                        thread_id = event
                            .get("thread_id")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                    }
                    Some("item.completed") => {
                        if let Some(item) = event.get("item") {
                            let is_agent_message = item
                                .get("type")
                                .and_then(Value::as_str)
                                .is_some_and(|kind| kind == "agent_message");
                            if is_agent_message {
                                last_message = item
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .map(str::to_string);
                            }
                        }
                    }
                    Some("turn.completed") => {
                        terminal_status = Some(ProviderTurnStatus::Completed);
                        usage_payload = event.get("usage").cloned();
                    }
                    Some("turn.interrupted") => {
                        terminal_status = Some(ProviderTurnStatus::Interrupted);
                        usage_payload = event.get("usage").cloned();
                        error_payload = event.get("error").cloned();
                    }
                    Some("turn.failed") => {
                        terminal_status = Some(ProviderTurnStatus::Failed);
                        usage_payload = event.get("usage").cloned();
                        error_payload = event.get("error").cloned();
                    }
                    _ => {}
                }
            }

            let exit_status = {
                let mut child = child_for_task.lock().await;
                child.wait().await
            };
            let stderr_text = stderr_task.await.unwrap_or_default();

            let status = match terminal_status {
                Some(status) => status,
                None => match exit_status {
                    Ok(status) if status.success() => ProviderTurnStatus::Completed,
                    Ok(_) => {
                        if interrupt_state.load(Ordering::SeqCst) {
                            ProviderTurnStatus::Interrupted
                        } else {
                            ProviderTurnStatus::Failed
                        }
                    }
                    Err(_) => {
                        if interrupt_state.load(Ordering::SeqCst) {
                            ProviderTurnStatus::Interrupted
                        } else {
                            ProviderTurnStatus::Failed
                        }
                    }
                },
            };

            if last_message.is_none() {
                if let Ok(file_last_message) =
                    tokio::fs::read_to_string(&last_message_path_for_task).await
                {
                    let trimmed = file_last_message.trim();
                    if !trimmed.is_empty() {
                        last_message = Some(trimmed.to_string());
                    }
                }
            }
            let _ = tokio::fs::remove_file(&last_message_path_for_task).await;

            if let Some(last_message) = last_message {
                match usage_payload.as_mut() {
                    Some(Value::Object(object)) => {
                        object.insert("last_message".to_string(), Value::String(last_message));
                    }
                    Some(value) => {
                        usage_payload = Some(serde_json::json!({
                            "raw_usage": value,
                            "last_message": last_message,
                        }));
                    }
                    None => {
                        usage_payload = Some(serde_json::json!({
                            "last_message": last_message,
                        }));
                    }
                }
            }

            if error_payload.is_none() && status == ProviderTurnStatus::Failed {
                let error_message = match exit_status {
                    Ok(exit) => format!(
                        "codex turn process exited unsuccessfully for {} (exit_status={exit})",
                        turn_id
                    ),
                    Err(error) => format!(
                        "failed waiting for codex turn process for {}: {}",
                        turn_id, error
                    ),
                };
                error_payload = Some(serde_json::json!({
                    "message": error_message,
                    "stderr": stderr_text,
                }));
            }

            let result = ProviderTurnResult {
                runtime_session_id: runtime_session_id.clone(),
                turn_id: turn_id.clone(),
                status,
                usage: usage_payload,
                error: error_payload,
            };

            provider
                .complete_turn(
                    runtime_session_id.as_str(),
                    turn_id.as_str(),
                    result,
                    thread_id,
                )
                .await;
        });

        Ok(RunningTurn {
            child,
            interrupt_requested,
        })
    }

    async fn complete_turn(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        result: ProviderTurnResult,
        provider_session_ref: Option<String>,
    ) {
        let waiters = {
            let mut sessions = self.inner.sessions.write().await;
            let Some(session) = sessions.get_mut(runtime_session_id) else {
                return;
            };
            session.active_turns.remove(turn_id);
            session
                .pending_approvals
                .retain(|_, pending| pending.turn_id != turn_id);
            if let Some(provider_session_ref) = provider_session_ref {
                session.provider_session_ref = provider_session_ref;
                session.canonical_provider_session_ref = None;
            }
            session
                .completed_turns
                .insert(turn_id.to_string(), result.clone());
            session.waiters.remove(turn_id).unwrap_or_default()
        };

        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }
}

fn absolutize_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path.to_path_buf(),
    }
}

fn reasoning_levels(levels: &[&str]) -> Vec<String> {
    levels.iter().map(|level| (*level).to_string()).collect()
}

#[async_trait]
impl RuntimeProvider for CodexProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "Codex".to_string(),
            enabled: self.inner.config.enabled,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap(
                "codex provider disabled".to_string(),
            ));
        }
        tokio::fs::create_dir_all(&self.inner.config.home_dir)
            .await
            .map_err(|error| {
                RuntimeError::Io(format!(
                    "failed to create codex home {}: {error}",
                    self.inner.config.home_dir.display()
                ))
            })?;
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(vec![
            ProviderModel {
                id: "gpt-5.6-sol".to_string(),
                display_name: "GPT-5.6-Sol".to_string(),
                reasoning_levels: reasoning_levels(&[
                    "low", "medium", "high", "xhigh", "max", "ultra",
                ]),
            },
            ProviderModel {
                id: "gpt-5.6-terra".to_string(),
                display_name: "GPT-5.6-Terra".to_string(),
                reasoning_levels: reasoning_levels(&[
                    "low", "medium", "high", "xhigh", "max", "ultra",
                ]),
            },
            ProviderModel {
                id: "gpt-5.6-luna".to_string(),
                display_name: "GPT-5.6-Luna".to_string(),
                reasoning_levels: reasoning_levels(&["low", "medium", "high", "xhigh", "max"]),
            },
            ProviderModel {
                id: "gpt-5.5".to_string(),
                display_name: "GPT 5.5".to_string(),
                reasoning_levels: reasoning_levels(&["low", "medium", "high", "xhigh"]),
            },
            ProviderModel {
                id: "gpt-5.4".to_string(),
                display_name: "GPT 5.4".to_string(),
                reasoning_levels: reasoning_levels(&["low", "medium", "high", "xhigh"]),
            },
            ProviderModel {
                id: "gpt-5.4-mini".to_string(),
                display_name: "GPT 5.4 Mini".to_string(),
                reasoning_levels: reasoning_levels(&["low", "medium", "high", "xhigh"]),
            },
            ProviderModel {
                id: "gpt-5.3-codex-spark".to_string(),
                display_name: "GPT-5.3-Codex-Spark".to_string(),
                reasoning_levels: reasoning_levels(&["low", "medium", "high", "xhigh"]),
            },
        ])
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        let auth_exists = self.codex_auth_path().exists();
        Ok(ProviderAuthStatus {
            authenticated: auth_exists,
            mode: if auth_exists {
                Some("auth_json".to_string())
            } else {
                None
            },
            detail: if auth_exists {
                Some(format!(
                    "using CODEX_HOME at {}",
                    self.inner.config.home_dir.display()
                ))
            } else {
                Some(format!("missing {}", self.codex_auth_path().display()))
            },
        })
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let state = CodexSessionState {
            provider_session_ref: format!("runtime:{}", req.runtime_session_id),
            canonical_provider_session_ref: None,
            cwd: req.cwd,
            model: req.model,
            permission_mode: req.permission_mode,
            ..Default::default()
        };

        let provider_session_ref = state.provider_session_ref.clone();
        let canonical_provider_session_ref = state.canonical_provider_session_ref.clone();

        let mut sessions = self.inner.sessions.write().await;
        sessions.insert(req.runtime_session_id.clone(), state);

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
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .entry(req.runtime_session_id.clone())
            .or_insert_with(CodexSessionState::default);
        session.provider_session_ref = req.provider_session_ref.clone();
        session.canonical_provider_session_ref = req.canonical_provider_session_ref.clone();
        session.cwd = req.cwd;

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: session.provider_session_ref.clone(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let running_turn = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;

            if !session.active_turns.is_empty() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::InvalidState(format!(
                    "codex session {} already has an active turn",
                    req.runtime_session_id
                )));
            }

            if let Some(approval_id) = req.approval_id {
                session.pending_approvals.insert(
                    approval_id,
                    PendingApprovalTurn {
                        turn_id: req.turn_id.clone(),
                        input: req.input,
                        expected_turn_id: req.expected_turn_id,
                        permission_mode: req.permission_mode,
                    },
                );
                None
            } else {
                let running_turn = self.spawn_turn(
                    req.runtime_session_id.as_str(),
                    req.turn_id.as_str(),
                    req.input.as_slice(),
                    session,
                )?;
                session
                    .active_turns
                    .insert(req.turn_id.clone(), running_turn);
                Some(())
            }
        };

        let _ = running_turn;

        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
        })
    }

    async fn interrupt_turn(&self, req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        let child = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            let running_turn = session
                .active_turns
                .get(req.turn_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::InvalidState(format!(
                        "turn {} is not active for session {}",
                        req.turn_id, req.runtime_session_id
                    ))
                })?;
            running_turn
                .interrupt_requested
                .store(true, Ordering::SeqCst);
            Arc::clone(&running_turn.child)
        };

        let mut child = child.lock().await;
        let _ = child.kill().await;
        Ok(())
    }

    async fn respond_approval(
        &self,
        req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        let decision = ApprovalDecision::parse(req.decision.as_str())?;

        if decision == ApprovalDecision::Decline {
            let result = ProviderTurnResult {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: req.turn_id.clone(),
                status: ProviderTurnStatus::Interrupted,
                usage: None,
                error: Some(serde_json::json!({
                    "message": "approval declined",
                })),
            };
            self.complete_turn(
                req.runtime_session_id.as_str(),
                req.turn_id.as_str(),
                result,
                None,
            )
            .await;
            return Ok(());
        }

        {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
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

            let mut execute_request = ProviderSendTurnRequest {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: pending.turn_id,
                input: pending.input,
                expected_turn_id: pending.expected_turn_id,
                permission_mode: pending.permission_mode,
                approval_id: None,
            };

            if let Some(payload) = req.payload.as_ref() {
                if let Some(mode) = payload
                    .get("permission_mode")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                {
                    execute_request.permission_mode = Some(mode);
                }
            }

            let running_turn = self.spawn_turn(
                execute_request.runtime_session_id.as_str(),
                execute_request.turn_id.as_str(),
                execute_request.input.as_slice(),
                session,
            )?;
            session.pending_approvals.remove(req.approval_id.as_str());
            session
                .active_turns
                .insert(execute_request.turn_id, running_turn);
        }
        Ok(())
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
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            if let Some(result) = session.completed_turns.get(req.turn_id.as_str()) {
                return Ok(result.clone());
            }
            if !session.active_turns.contains_key(req.turn_id.as_str())
                && !session
                    .pending_approvals
                    .values()
                    .any(|approval| approval.turn_id == req.turn_id)
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
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
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

        if let Some(timeout_ms) = req.timeout_ms {
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), receiver).await
            {
                Ok(result) => result.map_err(|_| {
                    RuntimeError::InvalidState(format!(
                        "turn result channel closed for {}",
                        req.turn_id
                    ))
                }),
                Err(_) => Err(RuntimeError::InvalidState(format!(
                    "timed out waiting for turn {}",
                    req.turn_id
                ))),
            }
        } else {
            receiver.await.map_err(|_| {
                RuntimeError::InvalidState(format!(
                    "turn result channel closed for {}",
                    req.turn_id
                ))
            })
        }
    }

    async fn close_session(&self, req: ProviderCloseSessionRequest) -> Result<(), RuntimeError> {
        let session = {
            let mut sessions = self.inner.sessions.write().await;
            sessions.remove(req.runtime_session_id.as_str())
        };

        let Some(session) = session else {
            return Ok(());
        };

        for (_turn_id, running_turn) in session.active_turns {
            running_turn
                .interrupt_requested
                .store(true, Ordering::SeqCst);
            let mut child = running_turn.child.lock().await;
            let _ = child.kill().await;
        }

        Ok(())
    }
}
