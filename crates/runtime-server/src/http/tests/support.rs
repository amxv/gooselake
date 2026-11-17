use super::*;
#[path = "support/session_create.rs"]
mod session_create;
#[path = "support/session_history.rs"]
mod session_history;
#[path = "support/session_lifecycle_close.rs"]
mod session_lifecycle_close;
pub(super) use session_history::{
    build_history_records, build_test_router_with_history, historical_session_json,
    persist_historical_session,
};

pub(super) fn test_team_model_presets() -> Vec<TeamModelPreset> {
    vec![
        TeamModelPreset {
            name: "fast".to_string(),
            provider: Some("codex".to_string()),
            model: "gpt-6-luna".to_string(),
            thinking_effort: Some("low".to_string()),
        },
        TeamModelPreset {
            name: "deep".to_string(),
            provider: Some("claude".to_string()),
            model: "claude-opus-5-5".to_string(),
            thinking_effort: Some("high".to_string()),
        },
    ]
}

#[derive(Default)]
struct TestProviderState {
    sessions: HashMap<String, TestProviderSession>,
}

#[derive(Default)]
struct TestProviderSession {
    provider_session_ref: String,
    cwd: Option<String>,
    history: Vec<String>,
    current_preferences: runtime_core::ProviderSessionPreferences,
    current_permission_intent: runtime_core::ProviderPermissionIntent,
    seen_thinking_efforts: Vec<Option<runtime_core::ProviderThinkingEffort>>,
    completed: HashMap<String, ProviderTurnResult>,
    pending: HashMap<String, ProviderSendTurnRequest>,
}

#[derive(Default)]
pub(super) struct TestProvider {
    state: Mutex<TestProviderState>,
    resumed_requests: Mutex<Vec<ProviderResumeSessionRequest>>,
    rebind_mode: Mutex<Option<String>>,
    rebind_calls: Mutex<Vec<runtime_core::ProviderWorkspaceRebindRequest>>,
    compaction_mode: std::sync::atomic::AtomicU8,
    compaction_calls: std::sync::atomic::AtomicUsize,
    edit_mode: std::sync::atomic::AtomicU8,
    edit_calls: std::sync::atomic::AtomicUsize,
    edit_inputs: Mutex<Vec<Vec<serde_json::Value>>>,
    pub(super) send_failure_mode: std::sync::atomic::AtomicU8,
    pub(super) send_invocations: std::sync::atomic::AtomicUsize,
    pub(super) interrupt_failure_mode: std::sync::atomic::AtomicU8,
    pub(super) interrupt_invocations: std::sync::atomic::AtomicUsize,
    allow_lifecycle_close: std::sync::atomic::AtomicBool,
    supports_thinking: std::sync::atomic::AtomicBool,
    preferences_mutation_calls: std::sync::atomic::AtomicUsize,
    preferences_mutation_mode: std::sync::atomic::AtomicU8,
    restore_failure_mode: std::sync::atomic::AtomicU8,
    create_failure_mode: std::sync::atomic::AtomicU8,
    create_calls: std::sync::atomic::AtomicUsize,
    create_barrier: std::sync::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    archive_close_mode: std::sync::atomic::AtomicU8,
    archive_close_calls: std::sync::atomic::AtomicUsize,
    disabled: std::sync::atomic::AtomicBool,
    compaction_events: std::sync::Mutex<
        Option<tokio::sync::broadcast::Sender<runtime_core::ProviderRuntimeEvent>>,
    >,
}

impl TestProvider {
    pub(super) fn set_create_failure_mode(&self, mode: u8) {
        self.create_failure_mode
            .store(mode, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn create_call_count(&self) -> usize {
        self.create_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn set_archive_close_mode(&self, mode: u8) {
        self.archive_close_mode
            .store(mode, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn archive_close_count(&self) -> usize {
        self.archive_close_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn set_restore_failure_mode(&self, mode: u8) {
        self.restore_failure_mode
            .store(mode, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) async fn restore_call_count(&self) -> usize {
        self.resumed_requests.lock().await.len()
    }

    pub(super) fn set_enabled(&self, enabled: bool) {
        self.disabled
            .store(!enabled, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Default)]
pub(super) struct TestClaudeProvider {
    state: Mutex<TestProviderState>,
    permission_mutation_calls: std::sync::atomic::AtomicUsize,
    permission_mutation_mode: std::sync::atomic::AtomicU8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CapturedProviderSessionOpen {
    pub(super) runtime_session_id: String,
    pub(super) cwd: Option<String>,
    provider_session_ref: String,
    canonical_provider_session_ref: Option<String>,
}

#[derive(Default)]
pub(super) struct TestAcpProvider {
    state: Mutex<TestProviderState>,
    created_sessions: Mutex<Vec<CapturedProviderSessionOpen>>,
    resumed_sessions: Mutex<Vec<CapturedProviderSessionOpen>>,
}

impl TestAcpProvider {
    pub(super) async fn created_sessions(&self) -> Vec<CapturedProviderSessionOpen> {
        self.created_sessions.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl RuntimeProvider for TestProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "Test Codex".to_string(),
            enabled: !self.disabled.load(std::sync::atomic::Ordering::SeqCst),
        }
    }

    fn capabilities(&self) -> runtime_core::ProviderCapabilities {
        runtime_core::ProviderCapabilities {
            hard_fork_edit_rerun: if self.edit_mode.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                runtime_core::ProviderCapabilitySupport::Unsupported
            } else {
                runtime_core::ProviderCapabilitySupport::Supported
            },
            images: if self.edit_mode.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                runtime_core::ProviderCapabilitySupport::Unsupported
            } else {
                runtime_core::ProviderCapabilitySupport::Supported
            },
            manual_compact: if self
                .compaction_mode
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                runtime_core::ProviderCapabilitySupport::Unsupported
            } else {
                runtime_core::ProviderCapabilitySupport::Supported
            },
            model_discovery: runtime_core::ProviderDiscoveryMode::Catalog,
            session_resume: runtime_core::ProviderCapabilitySupport::Supported,
            workspace_rebind: runtime_core::ProviderCapabilitySupport::Supported,
            interrupt: runtime_core::ProviderCapabilitySupport::Supported,
            tools: runtime_core::ProviderCapabilitySupport::Supported,
            session_preferences: if self
                .supports_thinking
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                runtime_core::ProviderCapabilitySupport::Supported
            } else {
                runtime_core::ProviderCapabilitySupport::Unsupported
            },
            ..Default::default()
        }
    }

    async fn mutate_session_preferences(
        &self,
        req: runtime_core::ProviderSessionPreferencesMutationRequest,
    ) -> Result<runtime_core::ProviderSessionPreferencesMutationResult, RuntimeError> {
        if !self
            .supports_thinking
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(RuntimeError::Unsupported(
                "thinking mutation disabled".into(),
            ));
        }
        self.preferences_mutation_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(&req.runtime_session_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("session {}", req.runtime_session_id)))?;
        session.current_preferences = req.current_preferences.clone();
        if self
            .preferences_mutation_mode
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
        {
            return Err(RuntimeError::provider_dispatch_unknown(
                "test_policy_ack_lost",
                "test provider applied its native policy before losing the acknowledgement",
            ));
        }
        Ok(runtime_core::ProviderSessionPreferencesMutationResult {
            revision: 0,
            current_preferences: req.current_preferences,
        })
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn subscribe_events(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<runtime_core::ProviderRuntimeEvent>> {
        Some(self.compaction_event_receiver())
    }

    async fn compact_session(
        &self,
        req: runtime_core::ProviderCompactSessionRequest,
    ) -> Result<runtime_core::ProviderCompactSessionOutcome, RuntimeError> {
        self.run_test_compaction(req)
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(vec![ProviderModel {
            id: "test-model".to_string(),
            display_name: "Test Model".to_string(),
            reasoning_levels: Vec::new(),
        }])
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        Ok(ProviderAuthStatus {
            authenticated: true,
            mode: Some("test".to_string()),
            detail: None,
        })
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.perform_test_session_create(req).await
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.resumed_requests.lock().await.push(req.clone());
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .entry(req.runtime_session_id.clone())
            .or_default();
        session.provider_session_ref = req.provider_session_ref.clone();
        session.cwd = req.cwd.clone();
        if self
            .restore_failure_mode
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
        {
            return Err(RuntimeError::provider_dispatch_unknown(
                "test_restore_ack_lost",
                "provider resumed its native session but acknowledgement was lost",
            ));
        }
        let observed_ref = if self
            .restore_failure_mode
            .load(std::sync::atomic::Ordering::SeqCst)
            == 3
        {
            format!("{}-unexpected-new-attachment", req.provider_session_ref)
        } else {
            req.provider_session_ref
        };
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: observed_ref,
            canonical_provider_session_ref: req.canonical_provider_session_ref,
        })
    }

    async fn close_session(
        &self,
        req: runtime_core::ProviderCloseSessionRequest,
    ) -> Result<(), RuntimeError> {
        self.perform_test_session_close(req).await
    }

    async fn close_session_for_retirement(
        &self,
        req: runtime_core::ProviderCloseSessionRequest,
    ) -> Result<runtime_core::ProviderRetirementCloseEvidence, RuntimeError> {
        use std::sync::atomic::Ordering;
        self.archive_close_calls.fetch_add(1, Ordering::SeqCst);
        let mode = self.archive_close_mode.load(Ordering::SeqCst);
        if mode == 4 {
            return Err(RuntimeError::Unsupported(
                "test provider has no verifiable native close".into(),
            ));
        }
        let native = self
            .state
            .lock()
            .await
            .sessions
            .remove(&req.runtime_session_id)
            .ok_or_else(|| RuntimeError::NotFound("native test attachment already closed".into()))?
            .provider_session_ref;
        if mode == 2 {
            return Err(RuntimeError::provider_dispatch_unknown(
                "test_archive_close_ack_lost",
                "test provider closed native session, but its acknowledgement was lost",
            ));
        }
        Ok(runtime_core::ProviderRetirementCloseEvidence {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: if mode == 3 {
                format!("{native}-unexpected-close-reference")
            } else {
                native
            },
            proof_kind:
                runtime_core::ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
        })
    }

    async fn resume_session_with_policy(
        &self,
        req: runtime_core::ProviderResumeSessionPolicyRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let transport = ProviderResumeSessionRequest {
            runtime_session_id: req.runtime_session_id.clone(),
            provider_session_ref: req.provider_session_ref,
            canonical_provider_session_ref: req.canonical_provider_session_ref,
            cwd: req.cwd.clone(),
            model: req.model,
            permission_mode: req.launch_policy.permission_intent.resolved_mode(),
            setting_sources: req
                .launch_policy
                .resolved_setting_sources(req.cwd.as_deref())?,
            system_prompt: req.launch_policy.system_prompt,
            allowed_tools: req.launch_policy.allowed_tools,
            disallowed_tools: req.launch_policy.disallowed_tools,
            harness_version_slot: req.launch_policy.harness_version_slot,
            metadata: req.metadata,
        };
        let resumed = self.resume_session(transport).await?;
        let mut state = self.state.lock().await;
        state
            .sessions
            .entry(req.runtime_session_id)
            .or_default()
            .current_preferences = req.current_preferences;
        Ok(resumed)
    }

    async fn rebind_workspace(
        &self,
        req: runtime_core::ProviderWorkspaceRebindRequest,
    ) -> Result<runtime_core::ProviderWorkspaceRebindEvidence, RuntimeError> {
        self.rebind_for_test(req).await
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        self.send_invocations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inject_missing_session_send()?;
        match self
            .send_failure_mode
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            1 => {
                return Err(RuntimeError::provider_not_dispatched(
                    "capacity_rejected",
                    "test provider explicitly rejected capacity before native turn dispatch",
                ));
            }
            2 => {
                return Err(RuntimeError::provider_dispatch_unknown(
                    "unknown_native_turn",
                    "test provider may have dispatched the native model turn",
                ));
            }
            _ => {}
        }
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(req.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
            })?;

        session.seen_thinking_efforts.push(
            req.turn_settings
                .thinking_effort
                .or(session.current_preferences.thinking_effort),
        );

        if let Some(approval_id) = req.approval_id.clone() {
            session.pending.insert(approval_id, req.clone());
            return Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
                provider_native_turn_id: None,
            });
        }

        let user_text = TestProvider::extract_text(req.input.as_slice());
        if user_text == "model_assign_wait_for_interrupt" {
            session.history.push(user_text);
            return Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
                provider_native_turn_id: None,
            });
        }
        let first_prompt = session
            .history
            .first()
            .cloned()
            .unwrap_or_else(|| "none".to_string());
        let reply = if user_text.contains("first prompt") {
            first_prompt
        } else {
            format!("ack:{user_text}")
        };
        session.history.push(user_text);
        session.completed.insert(
            req.turn_id.clone(),
            ProviderTurnResult {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: req.turn_id.clone(),
                status: ProviderTurnStatus::Completed,
                usage: Some(serde_json::json!({ "last_message": reply })),
                error: None,
            },
        );

        let native_turn = (self.edit_mode.load(std::sync::atomic::Ordering::SeqCst) != 0)
            .then(|| format!("native-{}", req.turn_id));
        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            provider_native_turn_id: native_turn,
        })
    }

    async fn hard_fork_edit_rerun(
        &self,
        req: runtime_core::ProviderHardForkEditRerunRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.fake_hard_fork(req).await
    }

    async fn restore_turn_identity_mapping(
        &self,
        agent_id: &str,
        logical_turn_id: &str,
        native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        self.fake_restore_mapping(agent_id, logical_turn_id, native_turn_id)
            .await
    }

    async fn observe_session_identity(
        &self,
        agent_id: &str,
    ) -> Result<Option<ProviderSession>, RuntimeError> {
        self.fake_observe_session(agent_id).await
    }

    async fn interrupt_turn(&self, req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        self.interrupt_invocations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .interrupt_failure_mode
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            let mut state = self.state.lock().await;
            let session = state.sessions.get_mut(&req.runtime_session_id).unwrap();
            assert!(
                session
                    .history
                    .last()
                    .is_some_and(|text| text == "model_assign_wait_for_interrupt"),
                "lost-ACK fixture requires an actual dispatched native turn"
            );
            session.pending.clear();
            session.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: req.runtime_session_id,
                    turn_id: req.turn_id,
                    status: ProviderTurnStatus::Interrupted,
                    usage: None,
                    error: None,
                },
            );
            return Err(RuntimeError::InvalidState(
                "fake native turn was interrupted but its acknowledgement timed out".into(),
            ));
        }
        Ok(())
    }

    async fn respond_approval(
        &self,
        req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        let decision = ApprovalDecision::parse(req.decision.as_str())?;
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(req.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
            })?;

        let pending = session
            .pending
            .remove(req.approval_id.as_str())
            .ok_or_else(|| RuntimeError::NotFound(format!("approval {}", req.approval_id)))?;
        if decision == ApprovalDecision::Decline {
            session.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: req.runtime_session_id,
                    turn_id: req.turn_id,
                    status: ProviderTurnStatus::Interrupted,
                    usage: None,
                    error: Some(serde_json::json!({ "message": "declined" })),
                },
            );
        } else {
            let user_text = TestProvider::extract_text(pending.input.as_slice());
            session.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: pending.runtime_session_id,
                    turn_id: pending.turn_id,
                    status: ProviderTurnStatus::Completed,
                    usage: Some(serde_json::json!({ "last_message": user_text })),
                    error: None,
                },
            );
        }
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
        if !session.completed.contains_key(&req.turn_id)
            && session
                .history
                .last()
                .is_some_and(|text| text == "model_assign_wait_for_interrupt")
        {
            // Keep the real runtime turn active until this isolated test's
            // runtime shuts down; never hold the native fixture state lock.
            drop(state);
            return std::future::pending().await;
        }
        session
            .completed
            .get(req.turn_id.as_str())
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("test turn {}", req.turn_id)))
    }
}

#[async_trait::async_trait]
impl RuntimeProvider for TestClaudeProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Claude
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Claude,
            display_name: "Test Claude".to_string(),
            enabled: true,
        }
    }

    fn capabilities(&self) -> runtime_core::ProviderCapabilities {
        runtime_core::ProviderCapabilities {
            model_discovery: runtime_core::ProviderDiscoveryMode::Catalog,
            session_resume: runtime_core::ProviderCapabilitySupport::Supported,
            permission_mutation: runtime_core::ProviderCapabilitySupport::Supported,
            interrupt: runtime_core::ProviderCapabilitySupport::Supported,
            tools: runtime_core::ProviderCapabilitySupport::Supported,
            setting_sources: runtime_core::ProviderCapabilitySupport::Supported,
            ..Default::default()
        }
    }

    async fn mutate_session_permission(
        &self,
        request: runtime_core::ProviderPermissionMutationRequest,
    ) -> Result<runtime_core::ProviderPermissionMutationResult, RuntimeError> {
        self.fake_claude_mutate_permission(request).await
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(vec![ProviderModel {
            id: "test-claude-model".to_string(),
            display_name: "Test Claude Model".to_string(),
            reasoning_levels: Vec::new(),
        }])
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        Ok(ProviderAuthStatus {
            authenticated: true,
            mode: Some("test".to_string()),
            detail: None,
        })
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let mut state = self.state.lock().await;
        state.sessions.insert(
            req.runtime_session_id.clone(),
            TestProviderSession {
                provider_session_ref: format!("test-claude-thread-{}", req.runtime_session_id),
                ..Default::default()
            },
        );
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id.clone(),
            provider_session_ref: format!("test-claude-thread-{}", req.runtime_session_id),
            canonical_provider_session_ref: Some(format!(
                "claude-canonical-{}",
                req.runtime_session_id
            )),
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

        if let Some(approval_id) = req.approval_id.clone() {
            session.pending.insert(approval_id, req.clone());
            return Ok(ProviderTurnAck {
                runtime_session_id: req.runtime_session_id,
                turn_id: req.turn_id,
                provider_native_turn_id: None,
            });
        }

        let user_text = TestProvider::extract_text(req.input.as_slice());
        session.completed.insert(
            req.turn_id.clone(),
            ProviderTurnResult {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: req.turn_id.clone(),
                status: ProviderTurnStatus::Completed,
                usage: Some(serde_json::json!({ "last_message": format!("claude:{user_text}") })),
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
        req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        let decision = ApprovalDecision::parse(req.decision.as_str())?;
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(req.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("test session {}", req.runtime_session_id))
            })?;

        let pending = session
            .pending
            .remove(req.approval_id.as_str())
            .ok_or_else(|| RuntimeError::NotFound(format!("approval {}", req.approval_id)))?;
        if decision == ApprovalDecision::Decline {
            session.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: req.runtime_session_id,
                    turn_id: req.turn_id,
                    status: ProviderTurnStatus::Interrupted,
                    usage: None,
                    error: Some(serde_json::json!({ "message": "declined" })),
                },
            );
        } else {
            let user_text = TestProvider::extract_text(pending.input.as_slice());
            session.completed.insert(
                req.turn_id.clone(),
                ProviderTurnResult {
                    runtime_session_id: pending.runtime_session_id,
                    turn_id: pending.turn_id,
                    status: ProviderTurnStatus::Completed,
                    usage: Some(
                        serde_json::json!({ "last_message": format!("claude:{user_text}") }),
                    ),
                    error: None,
                },
            );
        }
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

mod acp_provider;
#[path = "support_claude_policy.rs"]
mod support_claude_policy;
#[path = "support_compaction.rs"]
mod support_compaction;
#[path = "support_edit_rerun.rs"]
mod support_edit_rerun;

pub(super) async fn build_test_router() -> (Router, String, tempfile::TempDir) {
    build_test_router_with_team_policy(TeamMcpPolicy::default()).await
}

pub(super) async fn build_test_router_with_team_policy(
    team_policy: TeamMcpPolicy,
) -> (Router, String, tempfile::TempDir) {
    let (router, token, temp, _) = build_test_router_with_provider(team_policy).await;
    (router, token, temp)
}

pub(super) async fn build_test_router_with_provider(
    team_policy: TeamMcpPolicy,
) -> (Router, String, tempfile::TempDir, Arc<TestProvider>) {
    let (router, token, temp, provider, _) =
        build_test_router_with_provider_and_runtime(team_policy).await;
    (router, token, temp, provider)
}

pub(super) async fn build_test_router_with_runtime() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<RuntimeSessionManager>,
) {
    let (router, token, temp, _, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    (router, token, temp, runtime)
}

pub(super) async fn build_test_router_with_provider_and_runtime(
    team_policy: TeamMcpPolicy,
) -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    Arc<RuntimeSessionManager>,
) {
    let (router, token, temp, provider, runtime, _app) =
        build_test_router_with_services(team_policy).await;
    (router, token, temp, provider, runtime)
}

pub(super) async fn build_test_router_with_services(
    team_policy: TeamMcpPolicy,
) -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    Arc<RuntimeSessionManager>,
    Arc<RuntimeApp>,
) {
    let (router, token, temp, provider, runtime, app, _history) =
        build_test_router_with_history(team_policy, |_| Vec::new()).await;
    (router, token, temp, provider, runtime, app)
}

pub(super) async fn build_mixed_provider_test_router(
) -> (Router, String, tempfile::TempDir, Arc<TestAcpProvider>) {
    let (router, token, temp, provider, _) = build_mixed_provider_test_router_with_runtime().await;
    (router, token, temp, provider)
}

pub(super) async fn build_mixed_provider_test_router_with_runtime() -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestAcpProvider>,
    Arc<RuntimeSessionManager>,
) {
    let (router, token, temp, acp, _claude, runtime) =
        build_mixed_provider_test_router_with_claude().await;
    (router, token, temp, acp, runtime)
}
