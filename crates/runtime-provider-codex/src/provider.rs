use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runtime_core::{
    codex_model_catalog, ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCapabilities, ProviderCapabilitySupport, ProviderCloseSessionRequest,
    ProviderCompactSessionOutcome, ProviderCompactSessionRequest, ProviderContextLimitObservation,
    ProviderCreateSessionPolicyRequest, ProviderCreateSessionRequest, ProviderDiscoveryMode,
    ProviderDispatchOutcome, ProviderHardForkEditRerunRequest, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderResumeSessionPolicyRequest,
    ProviderResumeSessionRequest, ProviderRuntimeEvent, ProviderSendTurnRequest, ProviderSession,
    ProviderSkillDescriptor, ProviderSkillDiscoveryRequest, ProviderTurnAck, ProviderTurnResult,
    ProviderWaitTurnRequest, ProviderWorkspaceRebindEvidence, ProviderWorkspaceRebindRequest,
    RuntimeError, RuntimeProvider,
};
use serde_json::{json, Map, Value};
use tokio::sync::{broadcast, oneshot};

use crate::protocol::{
    absolutize_path, apply_turn_permission_mode, build_native_input, extract_thread_id,
    extract_turn_id, terminal_turn_from_thread_read,
};
use crate::rebind::canonical_paths_equal;
use crate::state::{CodexProviderInner, CodexSessionState};
use crate::CodexProviderConfig;

#[derive(Clone, Debug)]
pub struct CodexProvider {
    pub(super) inner: Arc<CodexProviderInner>,
}

impl CodexProvider {
    pub fn new(config: CodexProviderConfig) -> Self {
        let (events, _) = broadcast::channel(1024);
        let config = CodexProviderConfig {
            home_dir: absolutize_path(config.home_dir.as_path()),
            ..config
        };
        Self {
            inner: Arc::new(CodexProviderInner {
                config,
                sessions: tokio::sync::RwLock::new(std::collections::HashMap::new()),
                events,
                admission_lock: tokio::sync::Mutex::new(()),
            }),
        }
    }
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

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            model_discovery: ProviderDiscoveryMode::Catalog,
            skill_discovery: ProviderDiscoveryMode::Catalog,
            session_resume: ProviderCapabilitySupport::Supported,
            streaming: ProviderCapabilitySupport::Unsupported,
            approvals: ProviderCapabilitySupport::Supported,
            permission_mutation: ProviderCapabilitySupport::Supported,
            session_preferences: ProviderCapabilitySupport::Supported,
            interrupt: ProviderCapabilitySupport::Supported,
            tools: ProviderCapabilitySupport::Supported,
            images: ProviderCapabilitySupport::Supported,
            structured_output: ProviderCapabilitySupport::Unsupported,
            setting_sources: ProviderCapabilitySupport::Unsupported,
            context_limit_observation: ProviderCapabilitySupport::Supported,
            workspace_rebind: ProviderCapabilitySupport::Supported,
            manual_compact: ProviderCapabilitySupport::Supported,
            hard_fork_edit_rerun: ProviderCapabilitySupport::Supported,
        }
    }

    fn subscribe_events(&self) -> Option<broadcast::Receiver<ProviderRuntimeEvent>> {
        Some(self.inner.events.subscribe())
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::Bootstrap(
                "codex provider disabled".to_string(),
            ));
        }
        let transport = self.spawn_auxiliary_transport("healthcheck").await?;
        transport.shutdown().await;
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(codex_model_catalog())
    }

    async fn list_skills(
        &self,
        req: ProviderSkillDiscoveryRequest,
    ) -> Result<Vec<ProviderSkillDescriptor>, RuntimeError> {
        let transport = self.spawn_auxiliary_transport("skill-discovery").await?;
        let mut params = Map::new();
        let cwds = req
            .cwd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|cwd| vec![cwd])
            .unwrap_or_default();
        params.insert("cwds".to_string(), json!(cwds));
        params.insert("forceReload".to_string(), json!(req.force_refresh));
        let result = transport
            .request("skills/list", Value::Object(params))
            .await;
        transport.shutdown().await;
        let result = result?;
        let mut skills = Vec::new();
        let mut seen = HashSet::new();
        let entries = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "Codex skills/list response missing data array".to_string(),
                )
            })?;
        for entry in entries {
            let Some(entry_skills) = entry.get("skills").and_then(Value::as_array) else {
                continue;
            };
            for skill in entry_skills {
                if skill.get("enabled").and_then(Value::as_bool) == Some(false) {
                    continue;
                }
                let Some(name) = skill
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    continue;
                };
                let path = skill
                    .get("path")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);
                if !seen.insert(format!("{name}:{}", path.as_deref().unwrap_or_default())) {
                    continue;
                }
                let interface = skill.get("interface");
                let description = skill
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .or_else(|| {
                        interface
                            .and_then(|value| value.get("shortDescription"))
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .map(str::to_string)
                    })
                    .unwrap_or_else(|| "No description provided.".to_string());
                skills.push(ProviderSkillDescriptor {
                    provider: ProviderKind::Codex,
                    name: name.to_string(),
                    description,
                    display_name: interface
                        .and_then(|interface| {
                            interface
                                .get("displayName")
                                .or_else(|| interface.get("display_name"))
                        })
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    path,
                    argument_hint: interface
                        .and_then(|interface| {
                            interface
                                .get("argumentHint")
                                .or_else(|| interface.get("argument_hint"))
                        })
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                });
            }
        }
        Ok(skills)
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        let auth_exists = self.codex_auth_path().exists();
        Ok(ProviderAuthStatus {
            authenticated: auth_exists,
            mode: auth_exists.then(|| "auth_json".to_string()),
            detail: Some(if auth_exists {
                format!(
                    "using staged Codex auth at {}",
                    self.codex_auth_path().display()
                )
            } else {
                format!("missing {}", self.codex_auth_path().display())
            }),
        })
    }

    async fn auth_set_api_key(&self, api_key: String) -> Result<ProviderAuthStatus, RuntimeError> {
        if api_key.trim().is_empty() {
            return Err(RuntimeError::InvalidState(
                "Codex API key must not be empty".to_string(),
            ));
        }
        self.auth_import_json(json!({"OPENAI_API_KEY": api_key}))
            .await
    }

    async fn auth_import_json(&self, auth_json: Value) -> Result<ProviderAuthStatus, RuntimeError> {
        std::fs::create_dir_all(&self.inner.config.home_dir)?;
        let encoded = serde_json::to_vec_pretty(&auth_json)
            .map_err(|error| RuntimeError::InvalidState(error.to_string()))?;
        std::fs::write(self.codex_auth_path(), encoded)?;
        self.auth_status().await
    }

    async fn auth_import_json_text(
        &self,
        auth_json_text: String,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let auth_json = serde_json::from_str::<Value>(&auth_json_text).map_err(|error| {
            RuntimeError::InvalidState(format!("invalid Codex auth JSON: {error}"))
        })?;
        self.auth_import_json(auth_json).await
    }

    async fn auth_logout(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        match std::fs::remove_file(self.codex_auth_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(RuntimeError::Io(error.to_string())),
        }
        self.auth_status().await
    }

    async fn create_session_with_policy(
        &self,
        req: ProviderCreateSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::provider_not_dispatched(
                "codex_disabled",
                "Codex provider is disabled",
            ));
        }
        let thread_start_params = Self::thread_start_params(&req)?;
        let developer_instructions =
            Self::developer_instructions(req.launch_policy.system_prompt.as_deref())?;
        let _admission = self.inner.admission_lock.lock().await;
        self.ensure_capacity_for_new_session(req.runtime_session_id.as_str())
            .await?;
        let (transport, _session_home) = self
            .spawn_transport_for_session(req.runtime_session_id.as_str())
            .await?;
        let result = transport.request("thread/start", thread_start_params).await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                transport.shutdown().await;
                return Err(error);
            }
        };
        let Some(provider_session_ref) = extract_thread_id(&result) else {
            transport.shutdown().await;
            return Err(RuntimeError::ProtocolViolation(
                "Codex thread/start response missing thread.id".to_string(),
            ));
        };
        let state = CodexSessionState::new(
            transport,
            provider_session_ref.clone(),
            Some(provider_session_ref.clone()),
            req.cwd,
            req.model,
            developer_instructions,
            req.launch_policy.permission_intent.resolved_mode(),
            req.current_preferences,
        );
        self.install_session(req.runtime_session_id, state).await
    }

    async fn resume_session_with_policy(
        &self,
        req: ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        if !self.inner.config.enabled {
            return Err(RuntimeError::provider_not_dispatched(
                "codex_disabled",
                "Codex provider is disabled",
            ));
        }
        if let Some(canonical_provider_session_ref) = req
            .canonical_provider_session_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if canonical_provider_session_ref != req.provider_session_ref {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "Codex canonical provider session ref {canonical_provider_session_ref} does not match thread id {}",
                    req.provider_session_ref
                )));
            }
        }
        let thread_resume_params = Self::thread_resume_params(&req)?;
        let developer_instructions =
            Self::developer_instructions(req.launch_policy.system_prompt.as_deref())?;
        let _admission = self.inner.admission_lock.lock().await;
        if let Some(existing) = self
            .inner
            .sessions
            .write()
            .await
            .remove(req.runtime_session_id.as_str())
        {
            existing.transport.shutdown().await;
        }
        self.ensure_capacity_for_new_session(req.runtime_session_id.as_str())
            .await?;
        let (transport, _session_home) = self
            .spawn_transport_for_session(req.runtime_session_id.as_str())
            .await?;
        let result = transport
            .request("thread/resume", thread_resume_params)
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                transport.shutdown().await;
                return Err(error);
            }
        };
        let Some(provider_session_ref) = extract_thread_id(&result) else {
            transport.shutdown().await;
            return Err(RuntimeError::ProtocolViolation(
                "Codex thread/resume response missing thread.id".to_string(),
            ));
        };
        if provider_session_ref != req.provider_session_ref {
            transport.shutdown().await;
            return Err(RuntimeError::ProtocolViolation(format!(
                "Codex thread/resume returned {}, expected {}",
                provider_session_ref, req.provider_session_ref
            )));
        }
        let effective_cwd = if let Some(expected_cwd) = req
            .cwd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let returned_cwd = result
                .get("cwd")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let Some(returned_cwd) = returned_cwd else {
                transport.shutdown().await;
                return Err(RuntimeError::ProtocolViolation(
                    "Codex thread/resume response missing required top-level cwd evidence"
                        .to_string(),
                ));
            };
            if !canonical_paths_equal(expected_cwd, returned_cwd) {
                transport.shutdown().await;
                return Err(RuntimeError::ProtocolViolation(format!(
                    "Codex thread/resume returned mismatched cwd evidence (expected {expected_cwd}, actual {returned_cwd})"
                )));
            }
            Some(returned_cwd.to_string())
        } else {
            req.cwd.clone()
        };
        let state = CodexSessionState::new(
            transport,
            provider_session_ref.clone(),
            Some(provider_session_ref.clone()),
            effective_cwd,
            req.model,
            developer_instructions,
            req.launch_policy.permission_intent.resolved_mode(),
            req.current_preferences,
        );
        self.install_session(req.runtime_session_id, state).await
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.create_session_with_policy(ProviderCreateSessionPolicyRequest::legacy_compatible(
            req.runtime_session_id,
            req.model,
            req.cwd,
            req.permission_mode,
            req.metadata,
        )?)
        .await
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.resume_session_with_policy(ProviderResumeSessionPolicyRequest::legacy_compatible(
            req.runtime_session_id,
            req.provider_session_ref,
            req.canonical_provider_session_ref,
            req.model,
            req.cwd,
            req.permission_mode,
            req.system_prompt,
            req.metadata,
        )?)
        .await
    }

    async fn restore_turn_identity_mapping(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        provider_native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        self.bind_native_turn(runtime_session_id, turn_id, provider_native_turn_id)
            .await
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let native_input = build_native_input(req.input.as_slice())?;
        let (transport, thread_id, model, cwd, thinking_effort, permission_mode) = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::provider_not_dispatched(
                        "session_not_found",
                        format!("codex session {}", req.runtime_session_id),
                    )
                })?;
            if session.active_turn_id.is_some() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::provider_not_dispatched(
                    "turn_in_progress",
                    format!(
                        "codex session {} already has active work",
                        req.runtime_session_id
                    ),
                ));
            }
            session.active_turn_id = Some(req.turn_id.clone());
            (
                Arc::clone(&session.transport),
                session.provider_session_ref.clone(),
                session.model.clone(),
                session.cwd.clone(),
                session.current_preferences.thinking_effort,
                req.permission_mode
                    .clone()
                    .or_else(|| session.permission_mode.clone()),
            )
        };

        let mut params = json!({
            "threadId": thread_id,
            "input": native_input,
            "summary": "concise",
        });
        if let Some(model) = model
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params["model"] = json!(model);
        }
        if let Some(cwd) = cwd
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            params["cwd"] = json!(cwd);
        }
        if let Some(effort) = thinking_effort {
            params["effort"] = json!(effort.as_str());
        }
        apply_turn_permission_mode(&mut params, permission_mode.as_deref());

        let result = transport.request("turn/start", params).await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if error.provider_dispatch_outcome() == ProviderDispatchOutcome::NotDispatched {
                    if let Some(session) = self
                        .inner
                        .sessions
                        .write()
                        .await
                        .get_mut(req.runtime_session_id.as_str())
                    {
                        if session.active_turn_id.as_deref() == Some(req.turn_id.as_str()) {
                            session.active_turn_id = None;
                        }
                    }
                }
                return Err(error);
            }
        };
        let native_turn_id = extract_turn_id(&result).ok_or_else(|| {
            RuntimeError::provider_dispatch_unknown(
                "codex_turn_id_missing",
                "Codex turn/start response missing turn.id",
            )
        })?;
        self.bind_native_turn(
            req.runtime_session_id.as_str(),
            req.turn_id.as_str(),
            native_turn_id.as_str(),
        )
        .await?;
        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            provider_native_turn_id: Some(native_turn_id),
        })
    }

    async fn interrupt_turn(&self, req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        let (transport, thread_id, native_turn_id) = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            let native_turn_id = session
                .logical_to_native_turns
                .get(req.turn_id.as_str())
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "native turn for {} in session {}",
                        req.turn_id, req.runtime_session_id
                    ))
                })?;
            (
                Arc::clone(&session.transport),
                session.provider_session_ref.clone(),
                native_turn_id,
            )
        };
        transport
            .request(
                "turn/interrupt",
                json!({"threadId": thread_id, "turnId": native_turn_id}),
            )
            .await?;
        Ok(())
    }

    async fn respond_approval(
        &self,
        req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        let decision = ApprovalDecision::parse(req.decision.as_str())?;
        let (transport, pending) = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            let pending = session
                .pending_approvals
                .remove(req.approval_id.as_str())
                .ok_or_else(|| RuntimeError::NotFound(format!("approval {}", req.approval_id)))?;
            let mapped = session
                .native_to_logical_turns
                .get(pending.native_turn_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation(format!(
                        "approval {} has no logical turn mapping",
                        req.approval_id
                    ))
                })?;
            if mapped != &req.turn_id {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "approval {} turn mismatch (expected={}, actual={})",
                    req.approval_id, mapped, req.turn_id
                )));
            }
            (Arc::clone(&session.transport), pending)
        };

        match pending.method.as_str() {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                let response_decision = req
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("decision"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| decision.as_str().to_string());
                transport
                    .respond(pending.rpc_id, json!({"decision": response_decision}))
                    .await
            }
            "item/permissions/requestApproval" => {
                if decision == ApprovalDecision::Decline {
                    transport
                        .respond(pending.rpc_id, json!({"permissions": {}, "scope": "turn"}))
                        .await
                } else if let Some(provider_response) = req
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("provider_response"))
                    .cloned()
                {
                    transport.respond(pending.rpc_id, provider_response).await
                } else {
                    let requested = pending
                        .request
                        .get("permissions")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    let mut granted = Map::new();
                    if let Some(network) = requested.get("network").filter(|value| !value.is_null())
                    {
                        granted.insert("network".to_string(), network.clone());
                    }
                    if let Some(file_system) =
                        requested.get("fileSystem").filter(|value| !value.is_null())
                    {
                        granted.insert("fileSystem".to_string(), file_system.clone());
                    }
                    transport
                        .respond(
                            pending.rpc_id,
                            json!({"permissions": Value::Object(granted), "scope": "turn"}),
                        )
                        .await
                }
            }
            other => Err(RuntimeError::ProtocolViolation(format!(
                "unsupported Codex approval request method {other}"
            ))),
        }
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        let (transport, thread_id, native_turn_id) = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            if let Some(result) = session.completed_turns.get(req.turn_id.as_str()) {
                return Ok(result.clone());
            }
            if session.active_turn_id.as_deref() != Some(req.turn_id.as_str())
                && !session
                    .logical_to_native_turns
                    .contains_key(req.turn_id.as_str())
            {
                return Err(RuntimeError::NotFound(format!(
                    "turn {} in session {}",
                    req.turn_id, req.runtime_session_id
                )));
            }
            let native_turn_id = session
                .logical_to_native_turns
                .get(req.turn_id.as_str())
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "native turn for {} in session {}",
                        req.turn_id, req.runtime_session_id
                    ))
                })?;
            (
                Arc::clone(&session.transport),
                session.provider_session_ref.clone(),
                native_turn_id,
            )
        };

        let observation = transport
            .request(
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": true}),
            )
            .await
            .map_err(|error| {
                if error.provider_dispatch_code().is_some() {
                    error
                } else {
                    RuntimeError::provider_dispatch_unknown(
                        "codex_turn_observation_failed",
                        format!(
                            "failed to reconcile dispatched Codex turn {} via thread/read: {error}",
                            req.turn_id
                        ),
                    )
                }
            })?;
        if let Some((status, usage, error)) =
            terminal_turn_from_thread_read(&observation, native_turn_id.as_str())
        {
            if let Some(usage) = usage {
                if let Some(session) = self
                    .inner
                    .sessions
                    .write()
                    .await
                    .get_mut(req.runtime_session_id.as_str())
                {
                    session
                        .usage_by_native_turn
                        .insert(native_turn_id.clone(), usage);
                }
            }
            Self::complete_native_turn(
                &self.inner,
                req.runtime_session_id.as_str(),
                native_turn_id.as_str(),
                status,
                error,
            )
            .await;
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
            match tokio::time::timeout(Duration::from_millis(timeout_ms), receiver).await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(_)) => Err(RuntimeError::provider_dispatch_unknown(
                    "codex_turn_result_channel_closed",
                    format!(
                        "turn result channel closed for dispatched turn {}",
                        req.turn_id
                    ),
                )),
                Err(_) => Err(RuntimeError::provider_dispatch_unknown(
                    "codex_turn_wait_timeout",
                    format!("timed out waiting for dispatched turn {}", req.turn_id),
                )),
            }
        } else {
            receiver.await.map_err(|_| {
                RuntimeError::provider_dispatch_unknown(
                    "codex_turn_result_channel_closed",
                    format!(
                        "turn result channel closed for dispatched turn {}",
                        req.turn_id
                    ),
                )
            })
        }
    }

    async fn observe_context_limit(
        &self,
        runtime_session_id: &str,
    ) -> Result<ProviderContextLimitObservation, RuntimeError> {
        let sessions = self.inner.sessions.read().await;
        let session = sessions
            .get(runtime_session_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("codex session {runtime_session_id}")))?;
        let model_context_window = session.model_context_window.ok_or_else(|| {
            RuntimeError::InvalidState(format!(
                "Codex session {runtime_session_id} has no context-limit observation yet"
            ))
        })?;
        let last_total_tokens = session.last_total_tokens.unwrap_or(0);
        let remaining = model_context_window.saturating_sub(last_total_tokens);
        let remaining_percentage = if model_context_window == 0 {
            0
        } else {
            ((remaining.saturating_mul(100) / model_context_window).min(100)) as u8
        };
        Ok(ProviderContextLimitObservation {
            model_context_window,
            last_total_tokens,
            remaining_percentage,
        })
    }

    async fn rebind_workspace(
        &self,
        req: ProviderWorkspaceRebindRequest,
    ) -> Result<ProviderWorkspaceRebindEvidence, RuntimeError> {
        self.rebind_workspace_with_evidence(req).await
    }

    async fn compact_session(
        &self,
        req: ProviderCompactSessionRequest,
    ) -> Result<ProviderCompactSessionOutcome, RuntimeError> {
        let (transport, thread_id, receiver) = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            if session.active_turn_id.is_some() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::Conflict(format!(
                    "cannot compact busy Codex session {}",
                    req.runtime_session_id
                )));
            }
            let (sender, receiver) = oneshot::channel();
            session.compaction_waiters.push(sender);
            (
                Arc::clone(&session.transport),
                session.provider_session_ref.clone(),
                receiver,
            )
        };
        transport
            .request("thread/compact/start", json!({"threadId": thread_id}))
            .await?;
        match tokio::time::timeout(
            Duration::from_millis(self.inner.config.request_timeout_ms.max(1)),
            receiver,
        )
        .await
        {
            Ok(Ok(())) => Ok(ProviderCompactSessionOutcome::Accepted),
            Ok(Err(_)) => Err(RuntimeError::InvalidState(
                "Codex compaction observation channel closed".to_string(),
            )),
            Err(_) => Err(RuntimeError::provider_dispatch_unknown(
                "codex_compaction_timeout",
                "Codex compaction request was accepted but no completion observation arrived",
            )),
        }
    }

    async fn hard_fork_edit_rerun(
        &self,
        req: ProviderHardForkEditRerunRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.hard_fork_edit_rerun_verified(req).await
    }

    async fn close_session(&self, req: ProviderCloseSessionRequest) -> Result<(), RuntimeError> {
        let session = self
            .inner
            .sessions
            .write()
            .await
            .remove(req.runtime_session_id.as_str());
        if let Some(session) = session {
            session.transport.shutdown().await;
        }
        Ok(())
    }
}
