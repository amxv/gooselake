use super::*;

fn test_team_model_presets() -> Vec<TeamModelPreset> {
    vec![
        TeamModelPreset {
            name: "fast".to_string(),
            provider: Some("codex".to_string()),
            model: "gpt-5.4-mini".to_string(),
            thinking_effort: Some("low".to_string()),
        },
        TeamModelPreset {
            name: "deep".to_string(),
            provider: Some("claude".to_string()),
            model: "claude-opus-4-8".to_string(),
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
    history: Vec<String>,
    completed: HashMap<String, ProviderTurnResult>,
    pending: HashMap<String, ProviderSendTurnRequest>,
}

#[derive(Default)]
struct TestProvider {
    state: Mutex<TestProviderState>,
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

impl TestProvider {
    pub(super) fn extract_text(input: &[serde_json::Value]) -> String {
        for item in input {
            if let Some(text) = item.get("text").and_then(serde_json::Value::as_str) {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }
        "empty".to_string()
    }
}

impl TestClaudeProvider {
    pub(super) fn extract_text(input: &[serde_json::Value]) -> String {
        TestProvider::extract_text(input)
    }
}

impl TestAcpProvider {
    pub(super) fn extract_text(input: &[serde_json::Value]) -> String {
        TestProvider::extract_text(input)
    }

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

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(vec![ProviderModel {
            id: "test-model".to_string(),
            display_name: "Test Model".to_string(),
            reasoning_levels: vec!["test".to_string()],
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
                provider_session_ref: format!("test-thread-{}", req.runtime_session_id),
                ..Default::default()
            },
        );
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id.clone(),
            provider_session_ref: format!("test-thread-{}", req.runtime_session_id),
            canonical_provider_session_ref: None,
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
            });
        }

        let user_text = Self::extract_text(req.input.as_slice());
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

        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
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
            let user_text = Self::extract_text(pending.input.as_slice());
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

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        Ok(vec![ProviderModel {
            id: "test-claude-model".to_string(),
            display_name: "Test Claude Model".to_string(),
            reasoning_levels: vec!["test".to_string()],
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
            });
        }

        let user_text = Self::extract_text(req.input.as_slice());
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
            let user_text = Self::extract_text(pending.input.as_slice());
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

        let user_text = Self::extract_text(req.input.as_slice());
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

pub(super) async fn build_test_router() -> (Router, String, tempfile::TempDir) {
    build_test_router_with_team_policy(TeamMcpPolicy::default()).await
}

pub(super) async fn build_test_router_with_team_policy(
    team_policy: TeamMcpPolicy,
) -> (Router, String, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");

    let mut registry = runtime_core::ProviderRegistry::new();
    registry
        .register(Arc::new(TestProvider::default()))
        .expect("register test provider");
    let provider_registry = Arc::new(registry);
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), provider_registry.clone(), 512)
            .expect("build runtime"),
    );
    let process_manager = RuntimeProcessManager::new(
        store.clone(),
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
        runtime,
        bearer_token: bearer_token.clone(),
        public_base_url: "http://localhost:8080".to_string(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    (router, bearer_token, temp_dir)
}

pub(super) async fn build_mixed_provider_test_router(
) -> (Router, String, tempfile::TempDir, Arc<TestAcpProvider>) {
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
    let process_manager = RuntimeProcessManager::new(
        store.clone(),
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
        runtime,
        bearer_token: bearer_token.clone(),
        public_base_url: "http://localhost:8080".to_string(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    (router, bearer_token, temp_dir, acp_provider)
}

pub(super) struct FakeAcpAgentScript {
    _temp_dir: tempfile::TempDir,
    pub(super) script_path: PathBuf,
    pub(super) log_path: PathBuf,
}

pub(super) fn fake_acp_agent_with_request_log() -> FakeAcpAgentScript {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let script_path = temp_dir.path().join("fake_acp_http_agent.py");
    let log_path = temp_dir.path().join("acp-agent-requests.jsonl");
    fs::write(
        script_path.as_path(),
        format!(
            r#"#!/usr/bin/env python3
import json
import os
import sys

LOG_PATH = {log_path:?}
NEXT_SESSION_ID = 1

def write_log(obj):
    with open(LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj) + "\n")

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    msg = json.loads(line)
    write_log(msg)
    method = msg.get("method")
    if method == "initialize":
        send({{
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {{
                "protocolVersion": 1,
                "agentCapabilities": {{
                    "loadSession": True,
                    "sessionCapabilities": {{
                        "close": {{}},
                        "resume": {{}}
                    }}
                }},
                "authMethods": []
            }}
        }})
    elif method == "session/new":
        session_id = f"sess_{{NEXT_SESSION_ID}}"
        NEXT_SESSION_ID += 1
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{"sessionId": session_id}}}})
    elif method == "session/resume":
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
    elif method == "session/prompt":
        session_id = msg["params"]["sessionId"]
        prompt = msg["params"].get("prompt", [])
        prompt_text = " ".join(
            block.get("text", "")
            for block in prompt
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "msg_http_1",
                    "content": {{
                        "type": "text",
                        "text": "Echo: "
                    }}
                }}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "msg_http_1",
                    "content": {{
                        "type": "text",
                        "text": prompt_text
                    }}
                }}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "usage_update",
                    "used": 3,
                    "size": 16
                }}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {{
                "stopReason": "end_turn"
            }}
        }})
    elif method == "session/close":
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
"#
        ),
    )
    .expect("write fake acp agent");
    FakeAcpAgentScript {
        _temp_dir: temp_dir,
        script_path,
        log_path,
    }
}

pub(super) fn read_logged_jsonl(path: &Path) -> Vec<serde_json::Value> {
    let contents = fs::read_to_string(path).expect("read jsonl");
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl line"))
        .collect()
}
