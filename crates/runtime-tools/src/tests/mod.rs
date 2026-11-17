use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    CreateSessionInput, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, ProviderAuthStatus,
    ProviderCloseSessionRequest, ProviderCreateSessionRequest, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderRegistry, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderSession, ProviderTurnAck, ProviderTurnResult,
    ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError, RuntimeProvider,
    RuntimeSessionManager, RuntimeStore, RuntimeTeamCommsConfig, RuntimeTeamCommsService,
    SessionRecord, TeamCommsService, TeamCreateRequest, ToolGateway,
};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use crate::*;

#[async_trait::async_trait]
trait JournaledWorkspaceFixture {
    async fn create_workspace_agent_journaled_fixture(
        &self,
        workspace_id: &str,
        request: runtime_core::WorkspaceAgentCreateRequest,
        actor_id: &str,
    ) -> Result<runtime_core::WorkspaceAgentRecord, RuntimeError>;
}

#[async_trait::async_trait]
impl JournaledWorkspaceFixture for Arc<RuntimeSessionManager> {
    async fn create_workspace_agent_journaled_fixture(
        &self,
        workspace_id: &str,
        request: runtime_core::WorkspaceAgentCreateRequest,
        actor_id: &str,
    ) -> Result<runtime_core::WorkspaceAgentRecord, RuntimeError> {
        // Each setup call owns a distinct durable first-attempt key. This
        // fixture never substitutes a synthetic native receipt or DB roster.
        static NEXT_KEY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let key = format!(
            "fixture-workspace-create-{}",
            NEXT_KEY.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        );
        let (admission, command) = self
            .admit_workspace_agent_create_operator(
                workspace_id,
                &request,
                runtime_core::OperationActor::operator(actor_id),
                key,
            )
            .await?;
        match command {
            Some(command) => self
                .execute_workspace_agent_create_operator(command, request)
                .await
                .map(|(agent, _)| agent),
            None => self
                .workspace_agent_create_replay_result(admission)
                .map(|(agent, _)| agent),
        }
    }
}

mod canonical_team_preflight;
mod gateway_basics;
mod legacy_spawn_history_validation;
mod process;
mod team_hook_execution;
mod team_manage;
mod worktree;
mod worktree_cleanup_recovery;
mod worktree_member_removed_legacy;
mod worktree_retirement;
mod worktree_spawn_admission;
mod worktree_spawn_history_git;
mod worktree_spawn_inspection;
mod worktree_spawn_recovery;
mod worktree_spawn_refusal;

#[path = "../../../test-support/legacy_spawn.rs"]
mod legacy_spawn_history;

#[derive(Default)]
struct WorktreeTestProviderState {
    sessions: HashMap<String, String>,
}

#[derive(Default)]
struct WorktreeTestProvider {
    state: AsyncMutex<WorktreeTestProviderState>,
    fail_close: std::sync::atomic::AtomicBool,
    effect_calls: std::sync::atomic::AtomicUsize,
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
        self.effect_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        self.effect_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        self.effect_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        self.effect_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn close_session(&self, req: ProviderCloseSessionRequest) -> Result<(), RuntimeError> {
        self.effect_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail_close.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(RuntimeError::Io(
                "injected ambiguous native provider close".into(),
            ));
        }
        self.state
            .lock()
            .await
            .sessions
            .remove(&req.runtime_session_id);
        Ok(())
    }
}

/// Explicit stored history for tool fixtures. No operation receipt, event or
/// native provider call is synthesized by this constructor.
fn historical_test_session(id: &str, cwd: &str) -> SessionRecord {
    SessionRecord {
        id: id.into(),
        provider: "codex".into(),
        status: "ready".into(),
        cwd: Some(cwd.into()),
        model: Some("test-model".into()),
        permission_mode: None,
        system_prompt: None,
        metadata: json!({"suite":"runtime_tools_phase6"}),
        provider_session_ref: Some(format!("test-thread-{id}")),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 100,
        updated_at: 100,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

async fn persist_test_history(
    store: &SqliteRuntimeStore,
    provider: &WorktreeTestProvider,
    session: &SessionRecord,
) {
    assert_eq!(session.provider, "codex");
    assert!(store.get_persisted_session(&session.id).unwrap().is_none());
    store.upsert_session(session).unwrap();
    let previous = provider.state.lock().await.sessions.insert(
        session.id.clone(),
        session.provider_session_ref.clone().unwrap(),
    );
    assert!(previous.is_none());
}

async fn build_runtime_and_team_comms(
    store: Arc<SqliteRuntimeStore>,
) -> (Arc<RuntimeSessionManager>, Arc<RuntimeTeamCommsService>) {
    build_runtime_and_team_comms_with_history(store, &[]).await
}

async fn build_runtime_and_team_comms_with_history(
    store: Arc<SqliteRuntimeStore>,
    history: &[SessionRecord],
) -> (Arc<RuntimeSessionManager>, Arc<RuntimeTeamCommsService>) {
    let provider = Arc::new(WorktreeTestProvider::default());
    for session in history {
        persist_test_history(store.as_ref(), provider.as_ref(), session).await;
    }
    let mut registry = ProviderRegistry::new();
    registry.register(provider).expect("register test provider");
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

async fn build_team_gateway_fixture_with_policy(
    team_policy: TeamMcpPolicy,
) -> (
    RuntimeToolGateway,
    Arc<RuntimeSessionManager>,
    Arc<RuntimeTeamCommsService>,
    tempfile::TempDir,
) {
    let (gateway, runtime, comms, temp, _history) =
        build_team_gateway_fixture_with_history(team_policy, 0).await;
    (gateway, runtime, comms, temp)
}

async fn build_team_gateway_fixture_with_history(
    team_policy: TeamMcpPolicy,
    historical_count: usize,
) -> (
    RuntimeToolGateway,
    Arc<RuntimeSessionManager>,
    Arc<RuntimeTeamCommsService>,
    tempfile::TempDir,
    Vec<SessionRecord>,
) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let history: Vec<_> = (0..historical_count)
        .map(|index| {
            let cwd = temp_dir.path().join(format!("session-{index}"));
            std::fs::create_dir_all(&cwd).expect("create session cwd");
            historical_test_session(&format!("sess_tool_history_{index}"), cwd.to_str().unwrap())
        })
        .collect();
    let (runtime, team_comms) =
        build_runtime_and_team_comms_with_history(store.clone(), &history).await;
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
    (gateway, runtime, team_comms, temp_dir, history)
}

async fn create_team_gateway_from_history(
    team_comms: &RuntimeTeamCommsService,
    sessions: &[SessionRecord],
) -> String {
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
    team_id
}
