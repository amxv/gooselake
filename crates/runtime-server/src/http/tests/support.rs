use super::*;

fn test_team_model_presets() -> Vec<TeamModelPreset> {
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
    supports_thinking: std::sync::atomic::AtomicBool,
    compaction_events: std::sync::Mutex<
        Option<tokio::sync::broadcast::Sender<runtime_core::ProviderRuntimeEvent>>,
    >,
}

#[derive(Default)]
struct TestClaudeProvider {
    state: Mutex<TestProviderState>,
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
            enabled: true,
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
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(&req.runtime_session_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("session {}", req.runtime_session_id)))?;
        session.current_preferences = req.current_preferences.clone();
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
        let native_ref = format!("test-thread-{}", req.runtime_session_id);
        let canonical_ref = (self.edit_mode.load(std::sync::atomic::Ordering::SeqCst) != 0)
            .then(|| native_ref.clone());
        let mut state = self.state.lock().await;
        state.sessions.insert(
            req.runtime_session_id.clone(),
            TestProviderSession {
                provider_session_ref: native_ref.clone(),
                cwd: req.cwd,
                ..Default::default()
            },
        );
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id.clone(),
            provider_session_ref: native_ref,
            canonical_provider_session_ref: canonical_ref,
        })
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
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: req.provider_session_ref,
            canonical_provider_session_ref: req.canonical_provider_session_ref,
        })
    }

    async fn rebind_workspace(
        &self,
        req: runtime_core::ProviderWorkspaceRebindRequest,
    ) -> Result<runtime_core::ProviderWorkspaceRebindEvidence, RuntimeError> {
        self.rebind_calls.lock().await.push(req.clone());
        match self.rebind_mode.lock().await.as_deref() {
            Some("unknown") => {
                return Err(RuntimeError::provider_dispatch_unknown(
                    "reassignment_recovery_required",
                    "native provider route is unresolved",
                ))
            }
            Some("unsupported") => {
                return Err(RuntimeError::Unsupported(
                    "native provider rebind unavailable".into(),
                ))
            }
            _ => {}
        }
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(&req.runtime_session_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("session {}", req.runtime_session_id)))?;
        session.cwd = Some(req.cwd.clone());
        let bad_evidence = self.rebind_mode.lock().await.as_deref() == Some("bad_evidence");
        Ok(runtime_core::ProviderWorkspaceRebindEvidence {
            runtime_session_id: req.runtime_session_id,
            cwd: if bad_evidence {
                "/wrong-route".into()
            } else {
                req.cwd
            },
            provider_session_ref: Some(session.provider_session_ref.clone()),
            canonical_provider_session_ref: None,
            binding_generation: None,
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
            interrupt: runtime_core::ProviderCapabilitySupport::Supported,
            tools: runtime_core::ProviderCapabilitySupport::Supported,
            setting_sources: runtime_core::ProviderCapabilitySupport::Supported,
            ..Default::default()
        }
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
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");

    let test_provider = Arc::new(TestProvider::default());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry
        .register(test_provider.clone())
        .expect("register test provider");
    let provider_registry = Arc::new(registry);
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), provider_registry.clone(), 512)
            .expect("build runtime"),
    );
    let process_manager = RuntimeProcessManager::new_with_runtime(
        store.clone(),
        runtime.clone(),
        ProcessManagerConfig {
            enabled: true,
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
            allow_shell: false,
            completed_retention_ms: 600_000,
            output_event_sample_bytes: 8 * 1024,
            log_dir: temp_dir.path().join("process-logs"),
        },
    )
    .await
    .expect("process manager");
    let team_comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("team comms");

    let worktrees = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        team_comms.clone(),
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("worktree service");
    let tool_gateway = Arc::new(RuntimeToolGateway::new(RuntimeToolGatewayDeps {
        process_manager: process_manager.clone(),
        runtime: Some(runtime.clone()),
        team_comms: team_comms.clone(),
        worktrees: worktrees.clone(),
        team_policy,
        team_model_presets: test_team_model_presets(),
    }));

    let app = runtime_core::RuntimeApp::new(
        provider_registry.clone(),
        runtime_core::RuntimeServices {
            store: store.clone(),
            tool_gateway,
            process_manager,
            team_comms,
            worktrees,
        },
        runtime_core::EventQueueLimits {
            live_queue_capacity: 512,
            critical_queue_capacity: 512,
            team_queue_capacity: 512,
        },
        runtime_core::ProcessLimits {
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
        },
        runtime_core::WorktreeSettings {
            enabled: true,
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("build app");
    app.initialize().await.expect("initialize app");
    let bearer_token = "test-token".to_string();

    let router = build_router(AppState {
        app: Arc::new(app),
        runtime: runtime.clone(),
        bearer_token: bearer_token.clone(),
        public_base_url: "http://localhost:8080".to_string(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    (router, bearer_token, temp_dir, test_provider, runtime)
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
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");

    let mut registry = runtime_core::ProviderRegistry::new();
    let acp_provider = Arc::new(TestAcpProvider::default());
    registry
        .register(Arc::new(TestProvider::default()))
        .expect("register codex test provider");
    registry
        .register(Arc::new(TestClaudeProvider::default()))
        .expect("register claude test provider");
    registry
        .register(acp_provider.clone())
        .expect("register acp test provider");
    let provider_registry = Arc::new(registry);
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), provider_registry.clone(), 512)
            .expect("build runtime"),
    );
    let process_manager = RuntimeProcessManager::new_with_runtime(
        store.clone(),
        runtime.clone(),
        ProcessManagerConfig {
            enabled: true,
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
            allow_shell: false,
            completed_retention_ms: 600_000,
            output_event_sample_bytes: 8 * 1024,
            log_dir: temp_dir.path().join("process-logs"),
        },
    )
    .await
    .expect("process manager");
    let team_comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("team comms");
    let worktrees = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        team_comms.clone(),
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("worktree service");
    let tool_gateway = Arc::new(RuntimeToolGateway::new(RuntimeToolGatewayDeps {
        process_manager: process_manager.clone(),
        runtime: Some(runtime.clone()),
        team_comms: team_comms.clone(),
        worktrees: worktrees.clone(),
        team_policy: TeamMcpPolicy::default(),
        team_model_presets: test_team_model_presets(),
    }));

    let app = runtime_core::RuntimeApp::new(
        provider_registry.clone(),
        runtime_core::RuntimeServices {
            store: store.clone(),
            tool_gateway,
            process_manager,
            team_comms,
            worktrees,
        },
        runtime_core::EventQueueLimits {
            live_queue_capacity: 512,
            critical_queue_capacity: 512,
            team_queue_capacity: 512,
        },
        runtime_core::ProcessLimits {
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 100_000,
        },
        runtime_core::WorktreeSettings {
            enabled: true,
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        },
    )
    .expect("build app");
    app.initialize().await.expect("initialize app");
    let bearer_token = "mixed-provider-token".to_string();

    let router = build_router(AppState {
        app: Arc::new(app),
        runtime: runtime.clone(),
        bearer_token: bearer_token.clone(),
        public_base_url: "http://localhost:8080".to_string(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    (router, bearer_token, temp_dir, acp_provider, runtime)
}

/// Test fixtures do not start a background worker, by design: some tests need
/// to inspect pending notice authority. Tests exercising ordinary user turns
/// must explicitly establish the required first provider turn via this helper.
pub(super) async fn finish_test_onboarding(
    runtime: &Arc<RuntimeSessionManager>,
    workspace_id: &str,
    agent_id: &str,
) {
    runtime
        .dispatch_workspace_system_notices()
        .await
        .expect("deliver pending system onboarding");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let notice = runtime
                .get_workspace_onboarding_notice(workspace_id, agent_id)
                .expect("read onboarding notice")
                .expect("onboarding intent exists");
            let session = runtime.get_session(agent_id).await.expect("test session");
            if notice.state == "delivered" && session.active_turn_id.is_none() {
                break;
            }
            assert!(
                matches!(notice.state.as_str(), "pending" | "injecting" | "delivered"),
                "onboarding cannot safely finish: {notice:?}"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mandatory onboarding must finish before user turns");
}
