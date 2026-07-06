use std::sync::atomic::Ordering;

use async_trait::async_trait;
use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCloseSessionRequest, ProviderCreateSessionRequest, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderSession, ProviderTurnAck, ProviderTurnResult,
    ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError, RuntimeProvider,
};
use serde_json::{json, Value};
use tokio::sync::oneshot;

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

        let mut sessions = self.inner.sessions.write().await;
        sessions.remove(req.runtime_session_id.as_str());
        drop(sessions);

        if let Some(connection) = self.current_connection().await {
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
                    .await;
            }
        }

        self.shutdown_connection_if_idle().await;
        Ok(())
    }
}
