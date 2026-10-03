use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    CreateSessionInput, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, ProviderAuthStatus,
    ProviderCreateSessionRequest, ProviderInterruptTurnRequest, ProviderKind, ProviderMetadata,
    ProviderModel, ProviderRegistry, ProviderResumeSessionRequest, ProviderSendTurnRequest,
    ProviderSession, ProviderTurnAck, ProviderTurnResult, ProviderTurnStatus,
    ProviderWaitTurnRequest, RuntimeError, RuntimeProvider, RuntimeSessionManager, RuntimeStore,
    RuntimeTeamCommsConfig, RuntimeTeamCommsService, SessionRecord, TeamCommsService,
    TeamCreateRequest, ToolGateway,
};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use crate::*;

mod gateway_basics;
mod process;
mod team_manage;
mod worktree;

#[derive(Default)]
struct WorktreeTestProviderState {
    sessions: HashMap<String, String>,
}

#[derive(Default)]
struct WorktreeTestProvider {
    state: AsyncMutex<WorktreeTestProviderState>,
}

#[async_trait]
impl RuntimeProvider for WorktreeTestProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Codex,
            display_name: "Worktree Test Provider".to_string(),
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
        let provider_ref = format!("test-thread-{}", req.runtime_session_id);
        state
            .sessions
            .insert(req.runtime_session_id.clone(), provider_ref.clone());
        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: provider_ref,
            canonical_provider_session_ref: None,
        })
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        let mut state = self.state.lock().await;
        state.sessions.insert(
            req.runtime_session_id.clone(),
            req.provider_session_ref.clone(),
        );
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
        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            provider_native_turn_id: None,
        })
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        Ok(ProviderTurnResult {
            runtime_session_id: req.runtime_session_id,
            turn_id: req.turn_id,
            status: ProviderTurnStatus::Completed,
            usage: Some(serde_json::json!({
                "last_message": "ok",
                "contextWindowSize": 1000,
                "inputTokens": 300,
                "outputTokens": 200,
            })),
            error: None,
        })
    }

    async fn interrupt_turn(&self, _req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        Ok(())
    }
}

async fn build_runtime_and_team_comms(
    store: Arc<SqliteRuntimeStore>,
) -> (Arc<RuntimeSessionManager>, Arc<RuntimeTeamCommsService>) {
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(WorktreeTestProvider::default()))
        .expect("register test provider");
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512).expect("runtime"),
    );
    let team_comms = RuntimeTeamCommsService::new(
        store,
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("team comms");
    (runtime, team_comms)
}

async fn create_test_session(runtime: &RuntimeSessionManager, cwd: &str) -> SessionRecord {
    runtime
        .create_session(CreateSessionInput {
            provider: ProviderKind::Codex,
            model: Some("test-model".to_string()),
            cwd: Some(cwd.to_string()),
            permission_mode: None,
            metadata: Some(serde_json::json!({ "suite": "runtime_tools_phase6" })),
        })
        .await
        .expect("create session")
}

async fn build_test_tool_gateway(policy: TeamMcpPolicy) -> RuntimeToolGateway {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let process_manager = RuntimeProcessManager::new(
        store,
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

    RuntimeToolGateway::new(RuntimeToolGatewayDeps {
        process_manager,
        runtime: None,
        team_comms: Arc::new(StubTeamCommsService::new(TeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        })),
        worktrees: Arc::new(StubWorktreeService::new(WorktreeServiceConfig {
            enabled: true,
            root_dir: temp_dir.path().join("worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".to_string(),
            deletion_policy_default: "delete_on_last_claim".to_string(),
        })),
        team_policy: policy,
        team_model_presets: crate::gateway::test_default_team_model_presets(),
    })
}

async fn build_team_gateway_fixture() -> (
    RuntimeToolGateway,
    Arc<RuntimeSessionManager>,
    Arc<RuntimeTeamCommsService>,
    tempfile::TempDir,
) {
    build_team_gateway_fixture_with_policy(TeamMcpPolicy::default()).await
}

async fn build_team_gateway_fixture_with_policy(
    team_policy: TeamMcpPolicy,
) -> (
    RuntimeToolGateway,
    Arc<RuntimeSessionManager>,
    Arc<RuntimeTeamCommsService>,
    tempfile::TempDir,
) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let (runtime, team_comms) = build_runtime_and_team_comms(store.clone()).await;
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
    let worktrees = RuntimeWorktreeService::new(
        store,
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
    let gateway = RuntimeToolGateway::new(RuntimeToolGatewayDeps {
        process_manager,
        runtime: Some(runtime.clone()),
        team_comms: team_comms.clone(),
        worktrees,
        team_policy,
        team_model_presets: crate::gateway::test_default_team_model_presets(),
    });
    (gateway, runtime, team_comms, temp_dir)
}

async fn create_team_gateway_sessions(
    runtime: &RuntimeSessionManager,
    team_comms: &RuntimeTeamCommsService,
    temp_dir: &tempfile::TempDir,
    member_count: usize,
) -> (Vec<SessionRecord>, String) {
    let mut sessions = Vec::new();
    for idx in 0..member_count {
        let cwd = temp_dir.path().join(format!("session-{idx}"));
        std::fs::create_dir_all(&cwd).expect("create session cwd");
        sessions.push(create_test_session(runtime, cwd.to_string_lossy().as_ref()).await);
    }
    let lead_id = sessions.first().expect("lead session").id.clone();
    let member_ids = sessions
        .iter()
        .skip(1)
        .map(|session| session.id.clone())
        .collect::<Vec<_>>();
    let team_id = team_comms
        .create_team(TeamCreateRequest {
            name: "MCP Team".to_string(),
            lead_agent_id: lead_id,
            member_agent_ids: member_ids,
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team")
        .team
        .id;
    (sessions, team_id)
}
