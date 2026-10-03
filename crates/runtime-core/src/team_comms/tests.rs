use super::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::{
    AgentBroadcastMessageRequest, AgentDeliveryListRequest, AgentDeliveryRecord,
    AgentDirectMessageRequest, AgentMessageContextKind, AgentMessageRecord, ApprovalRecord,
    CreateSessionInput, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent,
    ProcessRecord, ProviderAuthStatus, ProviderCreateSessionRequest, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderRegistry, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderSession, ProviderTurnAck, ProviderTurnResult,
    ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError, RuntimeEventRecord,
    RuntimeEventScope, RuntimeProvider, RuntimeStore, SessionRecord, TeamCommsService,
    TeamCreateRequest, TeamDeliveryRecord, TeamGetDeliveriesRequest, TeamMemberRecord,
    TeamMessageRecord, TeamOperationDiagnosticRecord, TeamOperationJournalRecord, TeamRecord,
    TeamRemoveMemberRequest, TeamSendDirectRequest, TeamSetLeadRequest, TurnAdmissionRecord,
    TurnRecord, WorkspaceAgentLifecycleState, WorkspaceAgentProfile, WorkspaceAgentRecord,
    WorkspaceAgentRecreationPolicy, WorkspaceLifecycleState, WorkspaceRecord,
};

#[derive(Default)]
struct TestStore {
    hydrated: std::sync::Mutex<crate::RuntimeHydratedState>,
    turn_admissions: std::sync::Mutex<Vec<TurnAdmissionRecord>>,
    events: std::sync::Mutex<Vec<RuntimeEventRecord>>,
    workspace_agents: std::sync::Mutex<Vec<WorkspaceAgentRecord>>,
}

impl TestStore {
    fn upsert_with_key<T, F>(rows: &mut Vec<T>, value: T, key: F)
    where
        T: Clone,
        F: Fn(&T) -> String,
    {
        let value_key = key(&value);
        if let Some(existing) = rows.iter_mut().find(|row| key(row) == value_key) {
            *existing = value;
            return;
        }
        rows.push(value);
    }
}

#[async_trait]
impl RuntimeStore for TestStore {
    async fn initialize(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        let mut events = self.events.lock().expect("events lock");
        if let Some(existing) = events.iter().find(|row| row.event_id == event.event_id) {
            return Ok(existing.clone());
        }
        let row_id = i64::try_from(events.len()).unwrap_or(0) + 1;
        let seq = events
            .iter()
            .filter(|row| row.scope == event.scope && row.scope_id == event.scope_id)
            .map(|row| row.seq)
            .max()
            .unwrap_or(0)
            + 1;
        let record = RuntimeEventRecord {
            row_id,
            event_id: event.event_id.clone(),
            scope: event.scope,
            scope_id: event.scope_id.clone(),
            session_id: event.session_id.clone(),
            team_id: event.team_id.clone(),
            turn_id: event.turn_id.clone(),
            seq,
            kind: event.kind.clone(),
            criticality: event.criticality,
            payload: event.payload.clone(),
            provider: event.provider.clone(),
            provider_seq: event.provider_seq,
            created_at: event.created_at,
        };
        events.push(record.clone());
        Ok(record)
    }

    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        let events = self.events.lock().expect("events lock");
        let mut rows = events.clone();
        if let Some((scope_value, scope_id)) = scope {
            rows.retain(|row| row.scope == scope_value && row.scope_id == scope_id);
            if let Some(after) = after_seq {
                rows.retain(|row| row.seq > after);
            }
        } else if let Some(after) = after_seq {
            rows.retain(|row| row.row_id > after);
        }
        rows.sort_by_key(|row| row.row_id);
        rows.truncate(limit);
        Ok(rows)
    }

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.sessions, record.clone(), |row| row.id.clone());
        Ok(())
    }

    fn admit_turn(
        &self,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        if hydrated
            .sessions
            .iter()
            .find(|row| row.id == session.id)
            .and_then(|row| row.active_turn_id.as_ref())
            .is_some()
        {
            return Err(RuntimeError::Conflict(format!(
                "session {} already has an active turn",
                session.id
            )));
        }
        Self::upsert_with_key(&mut hydrated.turns, turn.clone(), |row| row.id.clone());
        Self::upsert_with_key(&mut hydrated.sessions, session.clone(), |row| {
            row.id.clone()
        });
        if let Some(approval) = approval {
            Self::upsert_with_key(&mut hydrated.approvals, approval.clone(), |row| {
                row.id.clone()
            });
        }
        drop(hydrated);
        let mut turn_admissions = self.turn_admissions.lock().expect("turn admissions lock");
        Self::upsert_with_key(&mut turn_admissions, admission.clone(), |row| {
            row.turn_id.clone()
        });
        Ok(())
    }

    fn upsert_turn_admission(&self, record: &TurnAdmissionRecord) -> Result<(), RuntimeError> {
        let mut turn_admissions = self.turn_admissions.lock().expect("turn admissions lock");
        Self::upsert_with_key(&mut turn_admissions, record.clone(), |row| {
            row.turn_id.clone()
        });
        Ok(())
    }

    fn list_turn_admissions(&self) -> Result<Vec<TurnAdmissionRecord>, RuntimeError> {
        Ok(self
            .turn_admissions
            .lock()
            .expect("turn admissions lock")
            .clone())
    }

    fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.turns, record.clone(), |row| row.id.clone());
        Ok(())
    }

    fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.approvals, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.teams, record.clone(), |row| row.id.clone());
        Ok(())
    }

    fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.team_members, record.clone(), |row| {
            format!("{}|{}", row.team_id, row.agent_id)
        });
        Ok(())
    }

    fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        hydrated
            .team_members
            .retain(|row| !(row.team_id == team_id && row.agent_id == agent_id));
        Ok(())
    }

    fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.team_messages, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.team_deliveries, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn insert_agent_message_with_deliveries(
        &self,
        message: &AgentMessageRecord,
        deliveries: &[AgentDeliveryRecord],
    ) -> Result<(), RuntimeError> {
        if deliveries
            .iter()
            .any(|delivery| delivery.message_id != message.id)
        {
            return Err(RuntimeError::InvalidState(
                "agent delivery references a different message".to_string(),
            ));
        }
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.agent_messages, message.clone(), |row| {
            row.id.clone()
        });
        for delivery in deliveries {
            Self::upsert_with_key(&mut hydrated.agent_deliveries, delivery.clone(), |row| {
                row.id.clone()
            });
        }
        Ok(())
    }

    fn upsert_agent_delivery(&self, record: &AgentDeliveryRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.agent_deliveries, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn create_workspace_agent(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
    ) -> Result<(), RuntimeError> {
        self.upsert_session(session)?;
        let mut agents = self.workspace_agents.lock().expect("workspace agents lock");
        Self::upsert_with_key(&mut agents, agent.clone(), |row| row.agent_id.clone());
        Ok(())
    }

    fn list_workspace_agents(
        &self,
        workspace_id: &str,
        lifecycle: Option<WorkspaceAgentLifecycleState>,
    ) -> Result<Vec<WorkspaceAgentRecord>, RuntimeError> {
        let mut rows = self
            .workspace_agents
            .lock()
            .expect("workspace agents lock")
            .iter()
            .filter(|row| row.workspace_id == workspace_id)
            .filter(|row| {
                lifecycle
                    .map(|value| row.lifecycle_state == value)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        Ok(rows)
    }

    fn get_workspace_agent(
        &self,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        Ok(self
            .workspace_agents
            .lock()
            .expect("workspace agents lock")
            .iter()
            .find(|row| row.workspace_id == workspace_id && row.agent_id == agent_id)
            .cloned())
    }

    fn get_workspace_agent_by_id(
        &self,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        Ok(self
            .workspace_agents
            .lock()
            .expect("workspace agents lock")
            .iter()
            .find(|row| row.agent_id == agent_id)
            .cloned())
    }

    fn get_workspace(&self, workspace_id: &str) -> Result<Option<WorkspaceRecord>, RuntimeError> {
        let exists = self
            .workspace_agents
            .lock()
            .expect("workspace agents lock")
            .iter()
            .any(|row| row.workspace_id == workspace_id);
        Ok(exists.then(|| WorkspaceRecord {
            workspace_id: workspace_id.to_string(),
            canonical_root: format!("/tmp/{workspace_id}"),
            display_name: workspace_id.to_string(),
            lifecycle_state: WorkspaceLifecycleState::Active,
            lead_agent_id: None,
            revision: 0,
            created_at: 1,
            updated_at: 1,
        }))
    }

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.managed_worktrees, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(
            &mut hydrated.managed_worktree_claims,
            record.clone(),
            |row| format!("{}|{}", row.worktree_id, row.session_id),
        );
        Ok(())
    }

    fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(&mut hydrated.processes, record.clone(), |row| {
            row.id.clone()
        });
        Ok(())
    }

    fn upsert_team_operation_journal(
        &self,
        record: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        Self::upsert_with_key(
            &mut hydrated.team_operation_journal,
            record.clone(),
            |row| row.operation_id.clone(),
        );
        Ok(())
    }

    fn append_team_operation_diagnostic(
        &self,
        operation_id: Option<&str>,
        team_id: Option<&str>,
        code: &str,
        message: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        let id = i64::try_from(hydrated.team_operation_diagnostics.len()).unwrap_or(0) + 1;
        let record = TeamOperationDiagnosticRecord {
            id,
            operation_id: operation_id.map(str::to_string),
            team_id: team_id.map(str::to_string),
            code: code.to_string(),
            message: message.to_string(),
            payload: payload.clone(),
            created_at,
        };
        hydrated.team_operation_diagnostics.push(record.clone());
        Ok(record)
    }

    fn list_team_operation_journal(
        &self,
        team_id: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
        let hydrated = self.hydrated.lock().expect("hydrated lock");
        let mut rows = hydrated.team_operation_journal.clone();
        if let Some(team_id) = team_id {
            rows.retain(|row| row.team_id == team_id);
        }
        Ok(rows)
    }

    fn list_team_operation_diagnostics(
        &self,
        team_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
        let hydrated = self.hydrated.lock().expect("hydrated lock");
        let mut rows = hydrated.team_operation_diagnostics.clone();
        if let Some(team_id) = team_id {
            rows.retain(|row| row.team_id.as_deref() == Some(team_id));
        }
        if let Some(operation_id) = operation_id {
            rows.retain(|row| row.operation_id.as_deref() == Some(operation_id));
        }
        Ok(rows)
    }

    fn hydrate_runtime_state(&self) -> Result<crate::RuntimeHydratedState, RuntimeError> {
        Ok(self.hydrated.lock().expect("hydrated lock").clone())
    }
}

#[derive(Default)]
struct TestProviderState {
    sessions: HashMap<String, String>,
    completed: HashMap<String, ProviderTurnResult>,
}

struct TestProvider {
    kind: ProviderKind,
    wait_ms: u64,
    state: Mutex<TestProviderState>,
}

impl TestProvider {
    fn new(wait_ms: u64) -> Self {
        Self::new_for(ProviderKind::Codex, wait_ms)
    }

    fn new_for(kind: ProviderKind, wait_ms: u64) -> Self {
        Self {
            kind,
            wait_ms,
            state: Mutex::new(TestProviderState::default()),
        }
    }
}

#[async_trait]
impl RuntimeProvider for TestProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: self.kind,
            display_name: format!("Test {}", self.kind.as_str()),
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
        state.sessions.insert(
            req.runtime_session_id.clone(),
            format!("test-thread-{}", req.runtime_session_id),
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
        let mut state = self.state.lock().await;
        state.completed.insert(
            req.turn_id.clone(),
            ProviderTurnResult {
                runtime_session_id: req.runtime_session_id.clone(),
                turn_id: req.turn_id.clone(),
                status: ProviderTurnStatus::Completed,
                usage: Some(serde_json::json!({ "last_message": "ok" })),
                error: None,
            },
        );
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
        if self.wait_ms > 0 {
            sleep(Duration::from_millis(self.wait_ms)).await;
        }
        let state = self.state.lock().await;
        state
            .completed
            .get(req.turn_id.as_str())
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("test turn {}", req.turn_id)))
    }

    async fn interrupt_turn(&self, _req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        Ok(())
    }
}

fn build_runtime_and_service(
    store: Arc<TestStore>,
    wait_ms: u64,
) -> (Arc<RuntimeSessionManager>, Arc<RuntimeTeamCommsService>) {
    let mut registry = ProviderRegistry::new();
    registry
        .register(Arc::new(TestProvider::new(wait_ms)))
        .expect("register provider");
    registry
        .register(Arc::new(TestProvider::new_for(
            ProviderKind::Claude,
            wait_ms,
        )))
        .expect("register claude provider");
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512).expect("build runtime"),
    );
    let team_comms = RuntimeTeamCommsService::new(
        store,
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .expect("build team comms");
    (runtime, team_comms)
}

async fn create_test_session(runtime: &RuntimeSessionManager) -> String {
    create_test_session_for(runtime, ProviderKind::Codex).await
}

async fn create_test_session_for(
    runtime: &RuntimeSessionManager,
    provider: ProviderKind,
) -> String {
    runtime
        .create_session(CreateSessionInput {
            provider,
            model: Some("test-model".to_string()),
            cwd: None,
            permission_mode: None,
            metadata: Some(serde_json::json!({ "suite": "team_comms" })),
        })
        .await
        .expect("create session")
        .id
}

fn workspace_agent_record(
    agent_id: &str,
    workspace_id: &str,
    alias: &str,
    provider: ProviderKind,
    lifecycle_state: WorkspaceAgentLifecycleState,
) -> WorkspaceAgentRecord {
    WorkspaceAgentRecord {
        agent_id: agent_id.to_string(),
        workspace_id: workspace_id.to_string(),
        alias: alias.to_string(),
        lifecycle_state,
        profile: WorkspaceAgentProfile {
            title: None,
            title_provenance: "test".to_string(),
            added_by: "test".to_string(),
            creator_session_id: None,
            creator_compaction_subscription: "auto".to_string(),
            joined_at: 1,
        },
        recreation_policy: WorkspaceAgentRecreationPolicy {
            provider,
            model: Some("test-model".to_string()),
            permission_intent: None,
            setting_sources_intent: Vec::new(),
            system_prompt: None,
            allowed_tools: Vec::new(),
            disallowed_tools: Vec::new(),
            authoritative_cwd: "/tmp".to_string(),
            harness_version_slot: None,
        },
        provider_session_ref: None,
        canonical_provider_session_ref: None,
        metadata: serde_json::json!({}),
        archived_at: (lifecycle_state == WorkspaceAgentLifecycleState::Archived).then_some(1),
        archive_reason: None,
        revision: 0,
        created_at: 1,
        updated_at: 1,
    }
}

async fn add_workspace_agent(
    store: &TestStore,
    runtime: &RuntimeSessionManager,
    agent_id: &str,
    workspace_id: &str,
    alias: &str,
    provider: ProviderKind,
    lifecycle_state: WorkspaceAgentLifecycleState,
) {
    let session = runtime
        .get_session(agent_id)
        .await
        .expect("workspace session");
    store
        .create_workspace_agent(
            &session,
            &workspace_agent_record(agent_id, workspace_id, alias, provider, lifecycle_state),
        )
        .expect("workspace agent");
}

mod agent_messages;
mod legacy_delivery;
