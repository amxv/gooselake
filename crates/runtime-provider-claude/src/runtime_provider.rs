use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    claude_model_catalog, ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCapabilities, ProviderCapabilitySupport, ProviderCloseSessionRequest,
    ProviderCreateSessionRequest, ProviderDiscoveryMode, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderModelDescriptor,
    ProviderModelDiscoveryRequest, ProviderModelDiscoveryResponse, ProviderResumeSessionRequest,
    ProviderRuntimeEvent, ProviderSendTurnRequest, ProviderSession, ProviderTurnAck,
    ProviderTurnResult, ProviderWaitTurnRequest, RuntimeError, RuntimeProvider,
};
use runtime_core::{ProviderCreateSessionPolicyRequest, ProviderResumeSessionPolicyRequest};
use serde_json::Value;
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::auth::{
    claude_smoke_debug_enabled, extract_assistant_text, extract_turn_status,
    is_missing_gg_mcp_server_bad_request, merge_assistant_text_into_usage,
    parse_claude_auth_import_payload,
};
use crate::bridge::send_bridge_request;
use crate::provider::{ClaudeProvider, ClaudeSessionHandle};

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

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            model_discovery: ProviderDiscoveryMode::Catalog,
            skill_discovery: ProviderDiscoveryMode::Catalog,
            session_resume: ProviderCapabilitySupport::Supported,
            streaming: ProviderCapabilitySupport::Supported,
            approvals: ProviderCapabilitySupport::Supported,
            permission_mutation: ProviderCapabilitySupport::Supported,
            session_preferences: ProviderCapabilitySupport::Supported,
            interrupt: ProviderCapabilitySupport::Supported,
            tools: ProviderCapabilitySupport::Supported,
            images: ProviderCapabilitySupport::Supported,
            structured_output: ProviderCapabilitySupport::Unsupported,
            setting_sources: ProviderCapabilitySupport::Supported,
            context_limit_observation: ProviderCapabilitySupport::Supported,
            workspace_rebind: ProviderCapabilitySupport::Supported,
            manual_compact: ProviderCapabilitySupport::Supported,
            hard_fork_edit_rerun: ProviderCapabilitySupport::Supported,
        }
    }

    fn subscribe_events(&self) -> Option<broadcast::Receiver<ProviderRuntimeEvent>> {
        Some(self.inner.provider_events.subscribe())
    }

    async fn observe_session_identity(
        &self,
        runtime_session_id: &str,
    ) -> Result<Option<ProviderSession>, RuntimeError> {
        let session = self.get_session(runtime_session_id).await?;
        if session.quarantined.load(Ordering::SeqCst) {
            return Err(RuntimeError::InvalidState(
                "Claude session identity cannot be observed while quarantined".into(),
            ));
        }
        // Session-updated events use a bridge worker lane, whereas the wait
        // RPC response is resolved separately. Register before checking so a
        // queued identity event cannot be missed by the final observation.
        let ready = session.native_identity_ready.notified();
        tokio::pin!(ready);
        ready.as_mut().enable();
        if session
            .canonical_provider_session_ref
            .read()
            .await
            .is_none()
        {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), ready).await;
        }
        let canonical = session.canonical_provider_session_ref.read().await.clone();
        if canonical.as_deref().is_none_or(str::is_empty) {
            return Err(RuntimeError::ProtocolViolation(
                "Claude turn did not establish a canonical SDK session identity".into(),
            ));
        }
        let provider_session_ref = session.provider_session_ref.read().await.clone();
        Ok(Some(ProviderSession {
            runtime_session_id: runtime_session_id.to_string(),
            provider_session_ref,
            canonical_provider_session_ref: canonical,
        }))
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        self.ensure_provider_enabled().await
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        if !self.inner.config.enabled {
            return Ok(Vec::new());
        }
        Ok(claude_model_catalog())
    }

    async fn discover_models(
        &self,
        req: ProviderModelDiscoveryRequest,
    ) -> Result<ProviderModelDiscoveryResponse, RuntimeError> {
        let discovered = if req.startup_mode
            == runtime_core::ProviderModelDiscoveryStartupMode::StartRuntime
        {
            req.setting_sources_intent
                .resolved_wire_values(req.cwd.as_deref())?;
            self.ensure_provider_enabled().await?;
            let bridge = self.acquire_bridge_for_new_session().await?;
            let response = send_bridge_request(
                &self.inner,
                &bridge,
                "session.supported_models",
                serde_json::json!({}),
                self.inner.config.request_timeout_ms,
            )
            .await?;
            let models = response
                .get("models")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation("supported_models missing models".into())
                })?;
            models
                .iter()
                .map(|model| {
                    let id = model
                        .get("value")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            RuntimeError::ProtocolViolation("supported model missing value".into())
                        })?;
                    Ok(ProviderModel {
                        id: id.into(),
                        display_name: model
                            .get("displayName")
                            .and_then(Value::as_str)
                            .unwrap_or(id)
                            .into(),
                        reasoning_levels: model
                            .get("supportedEffortLevels")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .filter(|s| runtime_core::ProviderThinkingEffort::parse(s).is_ok())
                            .map(str::to_string)
                            .collect(),
                    })
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?
        } else {
            self.list_models().await?
        };
        let models = discovered
            .into_iter()
            .map(|model| {
                let mut descriptor =
                    ProviderModelDescriptor::from_legacy(ProviderKind::Claude, model);
                descriptor.capabilities.supports_tool_calling = true;
                descriptor.capabilities.supports_vision = true;
                descriptor
            })
            .collect();
        Ok(ProviderModelDiscoveryResponse {
            provider: ProviderKind::Claude,
            mode: ProviderDiscoveryMode::Catalog,
            models,
        })
    }

    async fn list_skills(
        &self,
        req: runtime_core::ProviderSkillDiscoveryRequest,
    ) -> Result<Vec<runtime_core::ProviderSkillDescriptor>, RuntimeError> {
        self.claude_skills(req).await
    }
    async fn observe_context_limit(
        &self,
        runtime_session_id: &str,
    ) -> Result<runtime_core::ProviderContextLimitObservation, RuntimeError> {
        self.claude_context(runtime_session_id).await
    }

    async fn mutate_session_permission(
        &self,
        req: runtime_core::ProviderPermissionMutationRequest,
    ) -> Result<runtime_core::ProviderPermissionMutationResult, RuntimeError> {
        self.claude_mutate_permission(req).await
    }
    async fn mutate_session_preferences(
        &self,
        req: runtime_core::ProviderSessionPreferencesMutationRequest,
    ) -> Result<runtime_core::ProviderSessionPreferencesMutationResult, RuntimeError> {
        self.claude_mutate_preferences(req).await
    }
    async fn rebind_workspace(
        &self,
        req: runtime_core::ProviderWorkspaceRebindRequest,
    ) -> Result<runtime_core::ProviderWorkspaceRebindEvidence, RuntimeError> {
        self.claude_rebind(req).await
    }
    async fn compact_session(
        &self,
        req: runtime_core::ProviderCompactSessionRequest,
    ) -> Result<runtime_core::ProviderCompactSessionOutcome, RuntimeError> {
        self.claude_compact(req).await
    }

    async fn hard_fork_edit_rerun(
        &self,
        req: runtime_core::ProviderHardForkEditRerunRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        if req.edited_input.is_empty() {
            return Err(RuntimeError::ProtocolViolation(
                "Claude edit-and-rerun input must not be empty".to_string(),
            ));
        }
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let bridge_target_turn_id = session
            .bridge_turn_by_runtime_turn
            .lock()
            .await
            .get(req.target_turn_id.as_str())
            .cloned()
            .ok_or_else(|| {
                RuntimeError::NotFound(format!(
                    "Claude provider turn for historical logical turn {}",
                    req.target_turn_id
                ))
            })?;
        self.hard_fork_at_boundary(
            req.runtime_session_id.as_str(),
            bridge_target_turn_id.as_str(),
        )
        .await
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
        let launch_policy = crate::policy::legacy_policy(
            req.permission_mode,
            req.setting_sources,
            req.system_prompt,
            req.allowed_tools,
            req.disallowed_tools,
            req.harness_version_slot,
        )?;
        self.create_session_with_policy(ProviderCreateSessionPolicyRequest {
            runtime_session_id: req.runtime_session_id,
            model: req.model,
            cwd: req.cwd,
            launch_policy,
            current_preferences: Default::default(),
            metadata: req.metadata,
        })
        .await
    }
    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let launch_policy = crate::policy::legacy_policy(
            req.permission_mode,
            req.setting_sources,
            req.system_prompt,
            req.allowed_tools,
            req.disallowed_tools,
            req.harness_version_slot,
        )?;
        self.resume_session_with_policy(ProviderResumeSessionPolicyRequest {
            runtime_session_id: req.runtime_session_id,
            model: req.model,
            cwd: req.cwd,
            provider_session_ref: req.provider_session_ref,
            canonical_provider_session_ref: req.canonical_provider_session_ref,
            launch_policy,
            current_preferences: Default::default(),
            metadata: req.metadata,
        })
        .await
    }
    async fn create_session_with_policy(
        &self,
        req: ProviderCreateSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.ensure_provider_enabled().await?;
        crate::policy::validate_policy(
            &req.launch_policy,
            req.cwd.as_deref(),
            &req.current_preferences,
            req.model.as_deref(),
        )?;
        let bridge = self.acquire_bridge_for_new_session().await?;
        let mut create_params = serde_json::json!({
            "cwd": req.cwd,
            "model": req.model,
            "settingSourcesIntent": req.launch_policy.setting_sources_intent,
            "systemPrompt": req.launch_policy.system_prompt,
            "harnessInstructions": runtime_core::provider_harness_text(ProviderKind::Claude)?,
            "harnessVersion": runtime_core::HARNESS_VERSION,
            "allowedTools": req.launch_policy.allowed_tools,
            "disallowedTools": req.launch_policy.disallowed_tools,
            "thinkingEffort": req.current_preferences.thinking_effort,
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
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation("missing Claude provider identity".into())
            })?;
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
            native_identity_ready: tokio::sync::Notify::new(),
            bridge,
            operation_lock: Mutex::new(()),
            quarantined: AtomicBool::new(false),
            effective_cwd: RwLock::new(req.cwd.clone()),
            binding_generation: RwLock::new(0),
            model: req.model.clone(),
            launch_policy: tokio::sync::RwLock::new(req.launch_policy.clone()),
            permission_revision: tokio::sync::RwLock::new(0),
            current_preferences: tokio::sync::RwLock::new(req.current_preferences.clone()),
            preferences_revision: tokio::sync::RwLock::new(0),
            context_observation: RwLock::new(None),
            active_turn_id: RwLock::new(None),
            pending_runtime_turn_id: RwLock::new(None),
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

    async fn resume_session_with_policy(
        &self,
        req: ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.ensure_provider_enabled().await?;
        crate::policy::validate_policy(
            &req.launch_policy,
            req.cwd.as_deref(),
            &req.current_preferences,
            req.model.as_deref(),
        )?;
        crate::policy::validate_resume_identity(
            &req.provider_session_ref,
            req.canonical_provider_session_ref.as_deref(),
        )?;
        let existing = self.get_session(&req.runtime_session_id).await.ok();
        let _replacement_operation = if let Some(existing) = existing.as_ref() {
            let operation = existing.operation_lock.try_lock().map_err(|_| {
                RuntimeError::InvalidState(
                    "Claude session cannot resume while an existing operation is in progress"
                        .into(),
                )
            })?;
            crate::advanced::ensure_idle(existing).await?;
            Some(operation)
        } else {
            None
        };
        let _ = self.remove_session(req.runtime_session_id.as_str()).await;
        let bridge = self.acquire_bridge_for_new_session().await?;
        let mut resume_params = serde_json::json!({
            "sessionId": req.provider_session_ref,
            "providerSessionRef": req.provider_session_ref,
            "claudeCanonicalSessionRef": req.canonical_provider_session_ref,
            "cwd": req.cwd,
            "model": req.model,
            "settingSourcesIntent": req.launch_policy.setting_sources_intent,
            "systemPrompt": req.launch_policy.system_prompt,
            "harnessInstructions": runtime_core::provider_harness_text(ProviderKind::Claude)?,
            "harnessVersion": runtime_core::HARNESS_VERSION,
            "allowedTools": req.launch_policy.allowed_tools,
            "disallowedTools": req.launch_policy.disallowed_tools,
            "thinkingEffort": req.current_preferences.thinking_effort,
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
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation("missing Claude provider identity".into())
            })?;
        let canonical_provider_session_ref = response
            .get("claudeCanonicalSessionRef")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if canonical_provider_session_ref != req.canonical_provider_session_ref {
            return Err(RuntimeError::ProtocolViolation(
                "Claude resume canonical identity mismatch".into(),
            ));
        }

        let session = Arc::new(ClaudeSessionHandle {
            runtime_session_id: req.runtime_session_id.clone(),
            bridge_session_id,
            provider_session_ref: RwLock::new(provider_session_ref.clone()),
            canonical_provider_session_ref: RwLock::new(canonical_provider_session_ref.clone()),
            native_identity_ready: tokio::sync::Notify::new(),
            bridge,
            operation_lock: Mutex::new(()),
            quarantined: AtomicBool::new(false),
            effective_cwd: RwLock::new(req.cwd.clone()),
            binding_generation: RwLock::new(0),
            model: req.model.clone(),
            launch_policy: tokio::sync::RwLock::new(req.launch_policy.clone()),
            permission_revision: tokio::sync::RwLock::new(0),
            current_preferences: tokio::sync::RwLock::new(req.current_preferences.clone()),
            preferences_revision: tokio::sync::RwLock::new(0),
            context_observation: RwLock::new(None),
            active_turn_id: RwLock::new(None),
            pending_runtime_turn_id: RwLock::new(None),
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

    async fn restore_turn_identity_mapping(
        &self,
        runtime_session_id: &str,
        turn_id: &str,
        provider_native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let session = self.get_session(runtime_session_id).await?;
        let mut bridge_turn_by_runtime_turn = session.bridge_turn_by_runtime_turn.lock().await;
        let mut runtime_turn_by_bridge_turn = session.runtime_turn_by_bridge_turn.lock().await;

        if let Some(existing) = bridge_turn_by_runtime_turn.get(turn_id) {
            if existing != provider_native_turn_id {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "runtime turn {turn_id} is already mapped to Claude turn {existing}, not {provider_native_turn_id}"
                )));
            }
        }
        if let Some(existing) = runtime_turn_by_bridge_turn.get(provider_native_turn_id) {
            if existing != turn_id {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "Claude turn {provider_native_turn_id} is already mapped to runtime turn {existing}, not {turn_id}"
                )));
            }
        }

        bridge_turn_by_runtime_turn
            .insert(turn_id.to_string(), provider_native_turn_id.to_string());
        runtime_turn_by_bridge_turn
            .insert(provider_native_turn_id.to_string(), turn_id.to_string());
        Ok(())
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let session = self
            .get_session(req.runtime_session_id.as_str())
            .await
            .map_err(|error| match error {
                RuntimeError::NotFound(message) => {
                    RuntimeError::provider_not_dispatched("session_not_found", message)
                }
                other => other,
            })?;
        let _operation = session.operation_lock.lock().await;
        if session.quarantined.load(Ordering::SeqCst) {
            return Err(RuntimeError::provider_not_dispatched(
                "session_quarantined",
                "Claude session was detached after an unverified provider operation",
            ));
        }
        let mut send_policy = session.launch_policy.read().await.clone();
        if let Some(mode) = req.permission_mode.as_ref() {
            send_policy.permission_intent =
                runtime_core::ProviderPermissionIntent::explicit(mode.clone())?;
            crate::policy::validate_policy(
                &send_policy,
                session.effective_cwd.read().await.as_deref(),
                &Default::default(),
                None,
            )?;
        }
        let runtime_turn_id = req.turn_id.clone();

        {
            let mut pending_runtime_turn_id = session.pending_runtime_turn_id.write().await;
            *pending_runtime_turn_id = Some(runtime_turn_id.clone());
        }

        let result = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.send",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "input": req.input,
                "expectedTurnId": req.expected_turn_id,
                "permissionIntent": send_policy.permission_intent,
                "thinkingEffort": session.current_preferences.read().await.thinking_effort,
            }),
            self.inner.config.request_timeout_ms,
        )
        .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let mut pending_runtime_turn_id = session.pending_runtime_turn_id.write().await;
                if pending_runtime_turn_id.as_deref() == Some(runtime_turn_id.as_str()) {
                    *pending_runtime_turn_id = None;
                }
                return Err(match error {
                    RuntimeError::NotFound(message) if message.contains("SESSION_NOT_FOUND") => {
                        RuntimeError::provider_not_dispatched("session_not_found", message)
                    }
                    other => other,
                });
            }
        };

        let bridge_turn_id = result
            .get("turnId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| runtime_turn_id.clone());

        let provider_native_turn_id = bridge_turn_id.clone();
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
            *active_turn_id = if session
                .completed_turns
                .lock()
                .await
                .contains_key(&runtime_turn_id)
            {
                None
            } else {
                Some(runtime_turn_id.clone())
            };
        }
        {
            let mut pending_runtime_turn_id = session.pending_runtime_turn_id.write().await;
            if pending_runtime_turn_id.as_deref() == Some(runtime_turn_id.as_str()) {
                *pending_runtime_turn_id = None;
            }
        }

        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: runtime_turn_id,
            provider_native_turn_id: Some(provider_native_turn_id),
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
        let _operation = session.operation_lock.lock().await;
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

        if let Some(observation) = turn_result
            .usage
            .as_ref()
            .and_then(crate::advanced::context_from_usage)
        {
            *session.context_observation.write().await = Some(observation);
        }
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
