use std::sync::atomic::Ordering;

use async_trait::async_trait;
use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus, ProviderCapabilities,
    ProviderCapabilitySupport, ProviderCloseSessionRequest, ProviderCreateSessionRequest,
    ProviderDiscoveryMode, ProviderInterruptTurnRequest, ProviderKind, ProviderMetadata,
    ProviderModel, ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderSession,
    ProviderTurnAck, ProviderTurnResult, ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError,
    RuntimeProvider,
};
use serde_json::{json, Value};
use tokio::sync::{broadcast, oneshot};

use crate::provider::AcpProvider;
use crate::state::PendingApprovalTurn;

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

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            model_discovery: ProviderDiscoveryMode::AgentManaged,
            skill_discovery: ProviderDiscoveryMode::Unsupported,
            session_resume: ProviderCapabilitySupport::AgentManaged,
            streaming: ProviderCapabilitySupport::Supported,
            approvals: ProviderCapabilitySupport::Supported,
            permission_mutation: ProviderCapabilitySupport::Unsupported,
            session_preferences: ProviderCapabilitySupport::Unsupported,
            interrupt: ProviderCapabilitySupport::Supported,
            tools: ProviderCapabilitySupport::AgentManaged,
            images: ProviderCapabilitySupport::AgentManaged,
            structured_output: ProviderCapabilitySupport::Unsupported,
            setting_sources: ProviderCapabilitySupport::Unsupported,
            context_limit_observation: ProviderCapabilitySupport::Unsupported,
            workspace_rebind: ProviderCapabilitySupport::Unsupported,
            manual_compact: ProviderCapabilitySupport::Unsupported,
            hard_fork_edit_rerun: ProviderCapabilitySupport::Unsupported,
        }
    }

    fn subscribe_events(&self) -> Option<broadcast::Receiver<runtime_core::ProviderRuntimeEvent>> {
        Some(self.inner.provider_events.subscribe())
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
        validate_agent_managed_policy(
            &req.model,
            &req.permission_mode,
            &req.setting_sources,
            &req.system_prompt,
            &req.allowed_tools,
            &req.disallowed_tools,
            &req.harness_version_slot,
        )?;
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
            .map(str::trim)
            .filter(|value| !value.is_empty())
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
                connection.instance_id,
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
        validate_agent_managed_policy(
            &req.model,
            &req.permission_mode,
            &req.setting_sources,
            &req.system_prompt,
            &req.allowed_tools,
            &req.disallowed_tools,
            &req.harness_version_slot,
        )?;
        if req.provider_session_ref.trim().is_empty()
            || req
                .canonical_provider_session_ref
                .as_deref()
                .is_some_and(|canonical| canonical != req.provider_session_ref)
        {
            return Err(RuntimeError::ProtocolViolation(
                "ACP resume requires the same nonblank canonical provider session reference".into(),
            ));
        }
        let connection = self.ensure_connection().await?;
        let capabilities = connection.capabilities.read().await.clone();
        let cwd = Self::resolve_session_cwd(req.cwd.as_deref())?;
        let method = if capabilities.resume_session {
            "session/resume"
        } else if capabilities.load_session {
            "session/load"
        } else {
            return Err(RuntimeError::Unsupported(
                "acp agent does not advertise session resume or load support".to_string(),
            ));
        };
        let is_new = self
            .prepare_resume_slot(
                req.runtime_session_id.as_str(),
                req.provider_session_ref.as_str(),
                connection.instance_id,
            )
            .await?;
        let resume = async {
            connection
                .send_request(
                    method,
                    json!({
                        "sessionId": req.provider_session_ref,
                        "cwd": cwd,
                        "mcpServers": self.build_mcp_servers(req.runtime_session_id.as_str()),
                    }),
                    Some(self.request_timeout()),
                )
                .await?;
            // Commit the new child binding only after the advertised RPC
            // accepted this canonical native session identity.
            self.activate_reserved_session(
                req.runtime_session_id.as_str(),
                req.provider_session_ref.clone(),
                connection.instance_id,
            )
            .await
        }
        .await;
        if let Err(error) = resume {
            self.abort_resume_slot(req.runtime_session_id.as_str(), is_new)
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
        if req
            .permission_mode
            .as_deref()
            .is_some_and(|value| value != "require_approval")
        {
            return Err(RuntimeError::provider_not_dispatched(
                "unsupported_acp_permission_mode",
                "ACP agent owns native turn permissions; only an explicit runtime approval gate is supported",
            ));
        }
        {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::provider_not_dispatched(
                        "session_not_found",
                        format!("acp session {}", req.runtime_session_id),
                    )
                })?;

            if session.resuming {
                return Err(RuntimeError::provider_not_dispatched(
                    "acp_session_resuming",
                    "ACP session resume/load is in progress",
                ));
            }
            if session.active_turn.is_some() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::provider_not_dispatched(
                    "turn_in_progress",
                    format!(
                        "acp session {} already has an active turn",
                        req.runtime_session_id
                    ),
                ));
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
            provider_native_turn_id: None,
        })
    }

    async fn interrupt_turn(&self, req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        let connection = self.current_connection().await.ok_or_else(|| {
            RuntimeError::provider_dispatch_unknown(
                "acp_interrupt_connection_lost",
                "ACP interrupt cannot be proven after its stdio transport was lost",
            )
        })?;
        let provider_session_ref = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;
            if session.resuming {
                return Err(RuntimeError::Conflict(
                    "ACP session cannot be interrupted while native resume/load is in progress"
                        .into(),
                ));
            }
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
            if session.connection_id != Some(connection.instance_id) {
                return Err(RuntimeError::provider_dispatch_unknown(
                    "acp_interrupt_stale_session",
                    "ACP native turn belongs to a previous subprocess; cannot signal an unverified replacement",
                ));
            }
            active_turn.cancelled.store(true, Ordering::SeqCst);
            session.provider_session_ref.clone()
        };
        self.cancel_native_permissions(req.runtime_session_id.as_str(), Some(req.turn_id.as_str()))
            .await;
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
        if self.resolve_native_permission(&req).await? {
            return Ok(());
        }
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
                return classify_turn_result(result.clone());
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
                return classify_turn_result(result.clone());
            }
            session
                .waiters
                .entry(req.turn_id.clone())
                .or_default()
                .push(sender);
        }

        match tokio::time::timeout(self.wait_timeout(req.timeout_ms), receiver).await {
            Ok(Ok(result)) => classify_turn_result(result),
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
        let (provider_session_ref, active_turn_id, connection_id) = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("acp session {}", req.runtime_session_id))
                })?;
            if session.resuming {
                return Err(RuntimeError::Conflict(
                    "ACP session cannot be closed while native resume/load is in progress".into(),
                ));
            }
            (
                session.provider_session_ref.clone(),
                session
                    .active_turn
                    .as_ref()
                    .map(|turn| turn.runtime_turn_id.clone()),
                session.connection_id,
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

        self.cancel_native_permissions(req.runtime_session_id.as_str(), None)
            .await;
        let mut sessions = self.inner.sessions.write().await;
        sessions.remove(req.runtime_session_id.as_str());
        drop(sessions);

        if let Some(connection) = self.current_connection().await {
            let capabilities = connection.capabilities.read().await.clone();
            if connection_id == Some(connection.instance_id) && capabilities.close_session {
                let _ = connection
                    .send_request(
                        "session/close",
                        json!({
                            "sessionId": provider_session_ref,
                        }),
                        Some(self.request_timeout()),
                    )
                    .await;
            }
        }

        self.shutdown_connection_if_idle().await;
        Ok(())
    }
}

fn classify_turn_result(result: ProviderTurnResult) -> Result<ProviderTurnResult, RuntimeError> {
    if result
        .error
        .as_ref()
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        == Some("acp_prompt_dispatch_unknown")
    {
        return Err(RuntimeError::provider_dispatch_unknown(
            "acp_prompt_dispatch_unknown",
            result
                .error
                .as_ref()
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("ACP prompt outcome is unknown"),
        ));
    }
    Ok(result)
}

fn validate_agent_managed_policy(
    model: &Option<String>,
    permission: &Option<String>,
    setting_sources: &[String],
    system_prompt: &Option<String>,
    allowed_tools: &[String],
    disallowed_tools: &[String],
    harness_version_slot: &Option<String>,
) -> Result<(), RuntimeError> {
    if model.is_some()
        || permission
            .as_deref()
            .is_some_and(|mode| mode != "require_approval")
        || !setting_sources.is_empty()
        || system_prompt.as_deref().is_some_and(|s| !s.is_empty())
        || !allowed_tools.is_empty()
        || !disallowed_tools.is_empty()
        || harness_version_slot.is_some()
    {
        return Err(RuntimeError::Unsupported(
            "ACP owns model/system/tool/permission/setting-source/harness policy; Gooselake cannot silently apply these direct-provider settings".into(),
        ));
    }
    Ok(())
}
