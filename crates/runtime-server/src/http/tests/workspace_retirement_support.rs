use super::*;
use runtime_core::{
    ProviderCloseSessionRequest, ProviderRetirementCloseEvidence, ProviderRetirementCloseProofKind,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

/// A controllable native boundary. It never claims a close while a native turn
/// is still running, and it records every close/resume so replay is observable.
pub(super) struct RetirementProvider {
    kind: ProviderKind,
    sessions: Mutex<HashMap<String, String>>,
    turns: Mutex<HashMap<(String, String), ProviderTurnStatus>>,
    sent_inputs: Mutex<Vec<Vec<serde_json::Value>>>,
    interrupt_gate: Mutex<Option<Arc<RetirementInterruptGate>>>,
    changed: Notify,
    pub(super) closes: AtomicUsize,
    pub(super) resumes: AtomicUsize,
    pub(super) interrupts: AtomicUsize,
    pub(super) approval_responses: AtomicUsize,
    pub(super) fail_close: AtomicBool,
}

pub(super) struct RetirementInterruptGate {
    entered: Notify,
    release: Notify,
}

impl RetirementInterruptGate {
    pub(super) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

impl RetirementProvider {
    pub(super) fn new(kind: ProviderKind) -> Arc<Self> {
        Arc::new(Self {
            kind,
            sessions: Mutex::new(HashMap::new()),
            turns: Mutex::new(HashMap::new()),
            sent_inputs: Mutex::new(Vec::new()),
            interrupt_gate: Mutex::new(None),
            changed: Notify::new(),
            closes: AtomicUsize::new(0),
            resumes: AtomicUsize::new(0),
            interrupts: AtomicUsize::new(0),
            approval_responses: AtomicUsize::new(0),
            fail_close: AtomicBool::new(false),
        })
    }

    pub(super) async fn externally_close_for_operator_review(&self, agent_id: &str) {
        assert!(!self.turns.lock().await.iter().any(|((agent, _), status)| {
            agent == agent_id && *status == ProviderTurnStatus::InProgress
        }));
        assert!(self.sessions.lock().await.remove(agent_id).is_some());
    }

    pub(super) async fn turns_containing(&self, text: &str) -> usize {
        self.sent_inputs
            .lock()
            .await
            .iter()
            .filter(|items| items.iter().any(|item| item.to_string().contains(text)))
            .count()
    }

    pub(super) async fn pause_next_interrupt(&self) -> Arc<RetirementInterruptGate> {
        let gate = Arc::new(RetirementInterruptGate {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *self.interrupt_gate.lock().await = Some(gate.clone());
        gate
    }
}

#[async_trait::async_trait]
impl RuntimeProvider for RetirementProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: self.kind,
            display_name: "Retirement controlled provider".into(),
            enabled: true,
        }
    }

    fn capabilities(&self) -> runtime_core::ProviderCapabilities {
        runtime_core::ProviderCapabilities {
            session_resume: runtime_core::ProviderCapabilitySupport::Supported,
            interrupt: runtime_core::ProviderCapabilitySupport::Supported,
            ..Default::default()
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn create_session(
        &self,
        request: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let native = format!(
            "retirement-{}-{}",
            self.kind.as_str(),
            request.runtime_session_id
        );
        self.sessions
            .lock()
            .await
            .insert(request.runtime_session_id.clone(), native.clone());
        Ok(ProviderSession {
            runtime_session_id: request.runtime_session_id,
            provider_session_ref: native.clone(),
            canonical_provider_session_ref: Some(native),
        })
    }

    async fn resume_session(
        &self,
        request: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.resumes.fetch_add(1, Ordering::SeqCst);
        self.sessions.lock().await.insert(
            request.runtime_session_id.clone(),
            request.provider_session_ref.clone(),
        );
        Ok(ProviderSession {
            runtime_session_id: request.runtime_session_id,
            provider_session_ref: request.provider_session_ref,
            canonical_provider_session_ref: request.canonical_provider_session_ref,
        })
    }

    async fn send_turn(
        &self,
        request: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        self.sent_inputs.lock().await.push(request.input.clone());
        assert!(self
            .sessions
            .lock()
            .await
            .contains_key(&request.runtime_session_id));
        let held = request.input.iter().any(|input| {
            input
                .get("text")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|text| text == "retirement_wait_for_interrupt")
        });
        self.turns.lock().await.insert(
            (request.runtime_session_id.clone(), request.turn_id.clone()),
            if held {
                ProviderTurnStatus::InProgress
            } else {
                ProviderTurnStatus::Completed
            },
        );
        self.changed.notify_waiters();
        Ok(ProviderTurnAck {
            runtime_session_id: request.runtime_session_id,
            turn_id: request.turn_id,
            provider_native_turn_id: None,
        })
    }

    async fn interrupt_turn(
        &self,
        request: ProviderInterruptTurnRequest,
    ) -> Result<(), RuntimeError> {
        self.interrupts.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.interrupt_gate.lock().await.take() {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let mut turns = self.turns.lock().await;
        let turn = turns
            .get_mut(&(request.runtime_session_id, request.turn_id))
            .ok_or_else(|| RuntimeError::NotFound("native turn was never dispatched".into()))?;
        if *turn == ProviderTurnStatus::InProgress {
            *turn = ProviderTurnStatus::Interrupted;
        }
        self.changed.notify_waiters();
        Ok(())
    }

    async fn respond_approval(
        &self,
        _request: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        self.approval_responses.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn wait_for_turn(
        &self,
        request: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        loop {
            let changed = self.changed.notified();
            let status = self
                .turns
                .lock()
                .await
                .get(&(request.runtime_session_id.clone(), request.turn_id.clone()))
                .copied()
                .ok_or_else(|| RuntimeError::NotFound("native turn missing".into()))?;
            if status != ProviderTurnStatus::InProgress {
                return Ok(ProviderTurnResult {
                    runtime_session_id: request.runtime_session_id,
                    turn_id: request.turn_id,
                    status,
                    usage: None,
                    error: None,
                });
            }
            changed.await;
        }
    }

    async fn close_session_for_retirement(
        &self,
        request: ProviderCloseSessionRequest,
    ) -> Result<ProviderRetirementCloseEvidence, RuntimeError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        if self.fail_close.load(Ordering::SeqCst) {
            return Err(RuntimeError::provider_dispatch_unknown(
                "retirement_close_unknown",
                "native acknowledgement unavailable",
            ));
        }
        assert!(
            !self.turns.lock().await.iter().any(|((agent, _), status)| {
                agent == &request.runtime_session_id && *status == ProviderTurnStatus::InProgress
            }),
            "native closure attempted before active turn settlement"
        );
        let native = self
            .sessions
            .lock()
            .await
            .remove(&request.runtime_session_id)
            .ok_or_else(|| RuntimeError::NotFound("native attachment already closed".into()))?;
        Ok(ProviderRetirementCloseEvidence {
            runtime_session_id: request.runtime_session_id,
            provider_session_ref: native,
            proof_kind: ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
        })
    }
}

pub(super) struct RetirementFixture {
    pub(super) router: Router,
    pub(super) token: String,
    pub(super) runtime: Arc<RuntimeSessionManager>,
    pub(super) store: Arc<SqliteRuntimeStore>,
}

pub(super) async fn retirement_fixture(
    root: &Path,
    providers: &[Arc<RetirementProvider>],
) -> RetirementFixture {
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: root.join("runtime.sqlite3"),
    }));
    store
        .initialize()
        .await
        .expect("initialize retirement database");
    let mut registry = runtime_core::ProviderRegistry::new();
    for provider in providers {
        registry
            .register(provider.clone())
            .expect("register controlled provider");
    }
    let registry = Arc::new(registry);
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), registry.clone(), 512)
            .expect("retirement runtime"),
    );
    let processes = RuntimeProcessManager::new_with_runtime(
        store.clone(),
        runtime.clone(),
        ProcessManagerConfig {
            enabled: true,
            max_concurrent: 1,
            default_timeout_ms: 30_000,
            max_output_bytes_per_process: 100_000,
            allow_shell: false,
            completed_retention_ms: 600_000,
            output_event_sample_bytes: 1024,
            log_dir: root.join("process-logs"),
        },
    )
    .await
    .expect("native process manager");
    let team_comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 100,
        },
    )
    .expect("retirement communications");
    let worktrees = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        team_comms.clone(),
        WorktreeServiceConfig {
            enabled: true,
            root_dir: root.join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "retain_on_last_claim".into(),
        },
    )
    .expect("retirement worktrees");
    let gateway = Arc::new(RuntimeToolGateway::new(RuntimeToolGatewayDeps {
        process_manager: processes.clone(),
        runtime: Some(runtime.clone()),
        team_comms: team_comms.clone(),
        worktrees: worktrees.clone(),
        team_policy: TeamMcpPolicy::default(),
        team_model_presets: Vec::new(),
    }));
    let app = runtime_core::RuntimeApp::new(
        registry,
        runtime_core::RuntimeServices {
            store: store.clone(),
            tool_gateway: gateway,
            process_manager: processes.clone(),
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
            default_timeout_ms: 30_000,
            max_output_bytes_per_process: 100_000,
        },
        runtime_core::WorktreeSettings {
            enabled: true,
            root_dir: root.join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "retain_on_last_claim".into(),
        },
    )
    .expect("retirement application");
    app.initialize()
        .await
        .expect("initialize retirement application");
    let token = "retirement-test-operator".to_string();
    let router = build_router(AppState {
        app: Arc::new(app),
        runtime: runtime.clone(),
        bearer_token: token.clone(),
        public_base_url: "http://127.0.0.1:1".into(),
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });
    RetirementFixture {
        router,
        token,
        runtime,
        store,
    }
}

impl RetirementFixture {
    pub(super) async fn call(
        &self,
        method: &str,
        uri: &str,
        input: Option<serde_json::Value>,
        key: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        super::workspace_worktree_routes::call(&self.router, &self.token, method, uri, input, key)
            .await
    }

    pub(super) async fn register(&self, root: &Path) -> runtime_core::WorkspaceRecord {
        let (status, value) = self
            .call(
                "POST",
                "/v2/workspaces",
                Some(serde_json::json!({
                    "canonical_root":root,"display_name":"Retirement test",
                })),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        serde_json::from_value::<runtime_core::WorkspaceRegisterResponse>(value)
            .unwrap()
            .workspace
    }

    pub(super) async fn workspace(&self, id: &str) -> runtime_core::WorkspaceRecord {
        let (status, value) = self
            .call("GET", &format!("/v2/workspaces/{id}"), None, None)
            .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        serde_json::from_value(value).unwrap()
    }

    pub(super) async fn agent(
        &self,
        workspace_id: &str,
        provider: ProviderKind,
    ) -> runtime_core::WorkspaceAgentRecord {
        let (status, value) = self
            .call(
                "POST",
                &format!("/v2/workspaces/{workspace_id}/agents"),
                Some(serde_json::json!({"provider":provider.as_str()})),
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let agent: runtime_core::WorkspaceAgentRecord = serde_json::from_value(value).unwrap();
        finish_test_onboarding(&self.runtime, workspace_id, &agent.agent_id).await;
        agent
    }

    pub(super) async fn advance(
        &self,
        workspace_id: &str,
        revision: u64,
        key: &str,
    ) -> runtime_core::OperationDetails {
        let (status, value) = self
            .call(
                "POST",
                &format!("/v2/workspaces/{workspace_id}/retirement/advance"),
                Some(serde_json::json!({"expected_revision":revision})),
                Some(key),
            )
            .await;
        assert!(
            matches!(status, StatusCode::OK | StatusCode::ACCEPTED),
            "{status}: {value}"
        );
        serde_json::from_value(value).unwrap()
    }

    pub(super) async fn finish_retirement(
        &self,
        workspace_id: &str,
        revision: u64,
        key: &str,
    ) -> runtime_core::OperationDetails {
        timeout(Duration::from_secs(15), async {
            loop {
                let operation = self.advance(workspace_id, revision, key).await;
                match operation.operation.phase {
                    runtime_core::OperationPhase::Completed => return operation,
                    runtime_core::OperationPhase::Requested => tokio::task::yield_now().await,
                    other => panic!("retirement unexpectedly stopped at {other:?}: {operation:?}"),
                }
            }
        })
        .await
        .expect("retirement failed to settle")
    }
}
