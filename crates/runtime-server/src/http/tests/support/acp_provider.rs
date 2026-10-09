use super::*;

#[async_trait::async_trait]
impl RuntimeProvider for TestAcpProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Acp
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Acp,
            display_name: "Test ACP".to_string(),
            enabled: true,
        }
    }

    fn capabilities(&self) -> runtime_core::ProviderCapabilities {
        runtime_core::ProviderCapabilities {
            model_discovery: runtime_core::ProviderDiscoveryMode::AgentManaged,
            session_resume: runtime_core::ProviderCapabilitySupport::AgentManaged,
            streaming: runtime_core::ProviderCapabilitySupport::Supported,
            approvals: runtime_core::ProviderCapabilitySupport::Supported,
            interrupt: runtime_core::ProviderCapabilitySupport::Supported,
            tools: runtime_core::ProviderCapabilitySupport::AgentManaged,
            images: runtime_core::ProviderCapabilitySupport::AgentManaged,
            setting_sources: runtime_core::ProviderCapabilitySupport::Unsupported,
            ..Default::default()
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(Vec::new())
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        Ok(ProviderAuthStatus {
            authenticated: false,
            mode: Some("agent_managed".to_string()),
            detail: Some("Test ACP provider".to_string()),
        })
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let provider_session_ref = format!("test-acp-thread-{}", req.runtime_session_id);
        let canonical_provider_session_ref =
            Some(format!("test-acp-canonical-{}", req.runtime_session_id));
        let mut state = self.state.lock().await;
        state.sessions.insert(
            req.runtime_session_id.clone(),
            TestProviderSession {
                provider_session_ref: provider_session_ref.clone(),
                ..Default::default()
            },
        );
        drop(state);
        self.created_sessions
            .lock()
            .await
            .push(CapturedProviderSessionOpen {
                runtime_session_id: req.runtime_session_id.clone(),
                cwd: req.cwd.clone(),
                provider_session_ref: provider_session_ref.clone(),
                canonical_provider_session_ref: canonical_provider_session_ref.clone(),
            });
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
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .entry(req.runtime_session_id.clone())
            .or_default();
        session.provider_session_ref = req.provider_session_ref.clone();
        drop(state);
        self.resumed_sessions
            .lock()
            .await
            .push(CapturedProviderSessionOpen {
                runtime_session_id: req.runtime_session_id.clone(),
                cwd: req.cwd.clone(),
                provider_session_ref: req.provider_session_ref.clone(),
                canonical_provider_session_ref: req.canonical_provider_session_ref.clone(),
            });
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: req.provider_session_ref,
            canonical_provider_session_ref: req.canonical_provider_session_ref,
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(req.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
            })?;

        let user_text = TestProvider::extract_text(req.input.as_slice());
        session.completed.insert(
            req.turn_id.clone(),
            ProviderTurnResult {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: req.turn_id.clone(),
                status: ProviderTurnStatus::Completed,
                usage: Some(serde_json::json!({ "last_message": format!("acp:{user_text}") })),
                error: None,
            },
        );

        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            provider_native_turn_id: None,
        })
    }

    async fn interrupt_turn(&self, _req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn respond_approval(
        &self,
        _req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        let state = self.state.lock().await;
        let session = state
            .sessions
            .get(req.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
            })?;
        session
            .completed
            .get(req.turn_id.as_str())
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("test turn {}", req.turn_id)))
    }
}
