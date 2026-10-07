use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::{
    AgentBroadcastMessageRequest, AgentCancelMessageRequest, AgentDeliveryListRequest,
    AgentDeliveryRecord, AgentDirectMessageRequest, AgentMessageAck, AgentMessageListRequest,
    AgentMessageListResponse, AgentMessageRecord, AgentRetryDeliveryRequest, ApprovalRecord,
    LegacyWorkspaceMigrationApplyCommand, LegacyWorkspaceMigrationApplyResponse,
    LegacyWorkspaceMigrationResolutionCommand, LegacyWorkspaceMigrationResolutionResponse,
    LegacyWorkspaceMigrationStatus, ManagedProcessAdmission, ManagedProcessRecord,
    ManagedProcessTerminalUpdate, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
    NewRuntimeEvent, OperationDetails, ProcessCompletionUpdate, ProcessRecord,
    ProcessSchedulerSettings, RuntimeError, RuntimeEventRecord, RuntimeEventScope,
    RuntimeHydratedState, SessionRecord, TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord,
    TeamOperationDiagnosticRecord, TeamOperationJournalRecord, TeamRecord, TurnAdmissionRecord,
    TurnRecord, WorkspaceAgentLifecycleState, WorkspaceAgentRecord, WorkspaceInterruptAdmission,
    WorkspaceInterruptCommand, WorkspaceInterruptResponse, WorkspaceLeadTransitionCommand,
    WorkspaceLeadTransitionResponse, WorkspaceRecord, WorkspaceRegisterCommand,
    WorkspaceRegisterResponse,
};

#[async_trait]
pub trait RuntimeStore: Send + Sync {
    async fn initialize(&self) -> Result<(), RuntimeError>;

    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError>;

    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError>;

    fn register_workspace(
        &self,
        _command: &WorkspaceRegisterCommand,
    ) -> Result<WorkspaceRegisterResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_workspace(&self, _workspace_id: &str) -> Result<Option<WorkspaceRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn transition_workspace_lead(
        &self,
        _command: &WorkspaceLeadTransitionCommand,
    ) -> Result<WorkspaceLeadTransitionResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace lead authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn begin_workspace_interrupt(
        &self,
        _command: &WorkspaceInterruptCommand,
    ) -> Result<WorkspaceInterruptAdmission, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn mark_workspace_interrupt_started(
        &self,
        _operation_id: &str,
        _agent_id: &str,
        _turn_id: &str,
        _changed_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn finalize_workspace_interrupt_effect(
        &self,
        _operation_id: &str,
        _agent_id: &str,
        _turn_id: &str,
        _interrupted: bool,
        _reason: &str,
        _changed_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn mark_workspace_interrupt_uncertain(
        &self,
        _operation_id: &str,
        _agent_id: &str,
        _turn_id: &str,
        _error: &Value,
        _changed_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn complete_workspace_interrupt(
        &self,
        _operation_id: &str,
        _completed_at: i64,
    ) -> Result<WorkspaceInterruptResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn create_workspace_agent(
        &self,
        _session: &SessionRecord,
        _agent: &WorkspaceAgentRecord,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn list_workspace_agents(
        &self,
        _workspace_id: &str,
        _lifecycle: Option<WorkspaceAgentLifecycleState>,
    ) -> Result<Vec<WorkspaceAgentRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_workspace_agent(
        &self,
        _workspace_id: &str,
        _agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_workspace_agent_by_id(
        &self,
        _agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        Ok(None)
    }

    fn set_workspace_agent_lifecycle(
        &self,
        _session: &SessionRecord,
        _agent_id: &str,
        _lifecycle: WorkspaceAgentLifecycleState,
        _archive_reason: Option<&str>,
        _changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn compare_and_set_workspace_agent_recreation_policy(
        &self,
        _session: &SessionRecord,
        _agent_id: &str,
        _expected_revision: u64,
        _recreation_policy: &crate::WorkspaceAgentRecreationPolicy,
        _changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent recreation-policy mutation is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn get_operation(&self, _operation_id: &str) -> Result<Option<OperationDetails>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operation authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn persist_workspace_migration_preview(
        &self,
        _status: &LegacyWorkspaceMigrationStatus,
    ) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn workspace_migration_status(&self) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn apply_workspace_migration(
        &self,
        _command: &LegacyWorkspaceMigrationApplyCommand,
    ) -> Result<LegacyWorkspaceMigrationApplyResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn resolve_workspace_migration_subject(
        &self,
        _command: &LegacyWorkspaceMigrationResolutionCommand,
    ) -> Result<LegacyWorkspaceMigrationResolutionResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError>;

    fn admit_turn(
        &self,
        _admission: &TurnAdmissionRecord,
        _turn: &TurnRecord,
        _session: &SessionRecord,
        _approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "atomic turn admission is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_turn_admission(&self, _record: &TurnAdmissionRecord) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn list_turn_admissions(&self) -> Result<Vec<TurnAdmissionRecord>, RuntimeError> {
        Ok(Vec::new())
    }

    fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError>;

    fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError>;

    fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError>;

    fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError>;

    fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError>;

    fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError>;

    fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError>;

    fn insert_agent_message_with_deliveries(
        &self,
        _message: &AgentMessageRecord,
        _deliveries: &[AgentDeliveryRecord],
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent message persistence is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_agent_delivery(&self, _record: &AgentDeliveryRecord) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent delivery persistence is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError>;

    fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError>;

    fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError>;

    fn admit_managed_process(
        &self,
        _admission: &ManagedProcessAdmission,
    ) -> Result<ManagedProcessRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process admission authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn list_managed_processes(&self) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_managed_process(
        &self,
        _process_id: &str,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn claim_managed_processes(
        &self,
        _settings: &ProcessSchedulerSettings,
        _claimed_at: i64,
    ) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process scheduler authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn mark_managed_process_running(
        &self,
        _process_id: &str,
        _claim_generation: i64,
        _pid: i64,
        _os_start_identity: &str,
        _started_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process launch authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn requeue_managed_process_claim(
        &self,
        _process_id: &str,
        _claim_generation: i64,
        _updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process requeue authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn terminalize_managed_process(
        &self,
        _process_id: &str,
        _update: &ManagedProcessTerminalUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process terminal authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn update_managed_process_capture_progress(
        &self,
        _process_id: &str,
        _stream: &str,
        _captured_bytes: i64,
        _truncated: bool,
        _updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process capture progress is not implemented by this runtime store".to_string(),
        ))
    }

    fn cancel_queued_managed_process(
        &self,
        _process_id: &str,
        _owner_session_id: Option<&str>,
        _canceled_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process cancellation authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn request_managed_process_cancel(
        &self,
        _process_id: &str,
        _owner_session_id: Option<&str>,
        _updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process cancellation authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn update_managed_process_completion(
        &self,
        _process_id: &str,
        _update: &ProcessCompletionUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process completion authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn load_or_initialize_process_scheduler_settings(
        &self,
        _defaults: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process scheduler settings are not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn replace_process_scheduler_settings(
        &self,
        _settings: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process scheduler settings are not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn reorder_queued_managed_process(
        &self,
        _process_id: &str,
        _before_process_id: Option<&str>,
        _after_process_id: Option<&str>,
        _updated_at: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process queue reorder is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_team_operation_journal(
        &self,
        record: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError>;

    fn append_team_operation_diagnostic(
        &self,
        operation_id: Option<&str>,
        team_id: Option<&str>,
        code: &str,
        message: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError>;

    fn list_team_operation_journal(
        &self,
        team_id: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError>;

    fn list_team_operation_diagnostics(
        &self,
        team_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError>;

    fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvokeRequest {
    pub namespace: Option<String>,
    pub tool_name: String,
    pub caller_session_id: String,
    pub invocation_id: Option<String>,
    pub args: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamWithMembers {
    pub team: TeamRecord,
    pub members: Vec<TeamMemberRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamCreateRequest {
    pub name: String,
    pub lead_agent_id: String,
    #[serde(default)]
    pub member_agent_ids: Vec<String>,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamJoinRequest {
    pub team_id: String,
    pub agent_id: String,
    pub title: Option<String>,
    pub added_by: Option<String>,
    pub creator_agent_id: Option<String>,
    pub creator_compaction_subscription: Option<String>,
    pub worktree_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamSetLeadRequest {
    pub team_id: String,
    pub lead_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamRemoveMemberRequest {
    pub team_id: String,
    pub agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamInterruptAllRequest {
    pub team_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamInterruptAllResponse {
    pub team_id: String,
    pub interrupted_session_ids: Vec<String>,
    pub skipped_session_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamSendDirectRequest {
    pub team_id: String,
    pub sender_agent_id: String,
    pub recipient_agent_id: String,
    pub input: Value,
    #[serde(default)]
    pub image_paths: Vec<String>,
    pub priority: String,
    pub policy: String,
    pub correlation_id: Option<String>,
    pub reply_to_message_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamBroadcastRequest {
    pub team_id: String,
    pub sender_agent_id: String,
    pub input: Value,
    #[serde(default)]
    pub image_paths: Vec<String>,
    pub priority: String,
    pub policy: String,
    pub include_sender: bool,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMessageAck {
    pub message: TeamMessageRecord,
    pub deliveries: Vec<TeamDeliveryRecord>,
    pub disposition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamListMessagesRequest {
    pub team_id: String,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamListMessagesResponse {
    pub messages: Vec<TeamMessageRecord>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamGetDeliveriesRequest {
    pub team_id: String,
    pub message_id: Option<String>,
    pub recipient_agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamRetryDeliveryRequest {
    pub team_id: String,
    pub delivery_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamCancelMessageRequest {
    pub team_id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamViewSnapshotRequest {
    pub team_id: String,
    pub message_cursor: Option<String>,
    pub message_limit: Option<usize>,
    pub include_delivery_map: Option<bool>,
    pub delivery_recipient_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamViewSnapshotResponse {
    pub team: TeamWithMembers,
    pub messages: Vec<TeamMessageRecord>,
    pub deliveries_by_message_id: BTreeMap<String, Vec<TeamDeliveryRecord>>,
    pub next_message_cursor: Option<String>,
    pub snapshot_at: i64,
}

#[async_trait]
pub trait ToolGateway: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn invoke_tool(&self, request: ToolInvokeRequest) -> Result<Value, RuntimeError>;

    async fn capabilities(&self) -> Result<Value, RuntimeError>;
}

#[async_trait]
pub trait TeamCommsService: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn create_team(
        &self,
        request: TeamCreateRequest,
    ) -> Result<TeamWithMembers, RuntimeError>;

    async fn list_teams(&self) -> Result<Vec<TeamWithMembers>, RuntimeError>;

    async fn get_team(&self, team_id: &str) -> Result<TeamWithMembers, RuntimeError>;

    async fn join_team(&self, request: TeamJoinRequest) -> Result<TeamWithMembers, RuntimeError>;

    async fn remove_team_member(
        &self,
        request: TeamRemoveMemberRequest,
    ) -> Result<TeamWithMembers, RuntimeError>;

    async fn set_team_lead(
        &self,
        request: TeamSetLeadRequest,
    ) -> Result<TeamWithMembers, RuntimeError>;

    async fn delete_team(&self, team_id: &str) -> Result<(), RuntimeError>;

    async fn interrupt_all_team_turns(
        &self,
        request: TeamInterruptAllRequest,
    ) -> Result<TeamInterruptAllResponse, RuntimeError>;

    async fn send_direct(
        &self,
        request: TeamSendDirectRequest,
    ) -> Result<TeamMessageAck, RuntimeError>;

    async fn broadcast(
        &self,
        request: TeamBroadcastRequest,
    ) -> Result<TeamMessageAck, RuntimeError>;

    async fn send_agent_direct(
        &self,
        _request: AgentDirectMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first direct messaging is not implemented by this comms service".to_string(),
        ))
    }

    async fn broadcast_workspace(
        &self,
        _request: AgentBroadcastMessageRequest,
    ) -> Result<AgentMessageAck, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first broadcast messaging is not implemented by this comms service".to_string(),
        ))
    }

    async fn list_agent_messages(
        &self,
        _request: AgentMessageListRequest,
    ) -> Result<AgentMessageListResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first message listing is not implemented by this comms service".to_string(),
        ))
    }

    async fn get_agent_deliveries(
        &self,
        _request: AgentDeliveryListRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first delivery listing is not implemented by this comms service".to_string(),
        ))
    }

    async fn retry_agent_delivery(
        &self,
        _request: AgentRetryDeliveryRequest,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first delivery retry is not implemented by this comms service".to_string(),
        ))
    }

    async fn cancel_agent_message(
        &self,
        _request: AgentCancelMessageRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent-first message cancellation is not implemented by this comms service".to_string(),
        ))
    }

    async fn list_messages(
        &self,
        request: TeamListMessagesRequest,
    ) -> Result<TeamListMessagesResponse, RuntimeError>;

    async fn get_deliveries(
        &self,
        request: TeamGetDeliveriesRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError>;

    async fn retry_delivery(
        &self,
        request: TeamRetryDeliveryRequest,
    ) -> Result<TeamDeliveryRecord, RuntimeError>;

    async fn cancel_message(
        &self,
        request: TeamCancelMessageRequest,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError>;

    async fn get_view_snapshot(
        &self,
        request: TeamViewSnapshotRequest,
    ) -> Result<TeamViewSnapshotResponse, RuntimeError>;

    fn replay_team_events(
        &self,
        team_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError>;
}

#[async_trait]
pub trait WorktreeService: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeError>;

    async fn get_worktree(&self, worktree_id: &str) -> Result<ManagedWorktreeRecord, RuntimeError>;

    async fn create_worktree(
        &self,
        request: WorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError>;

    async fn claim_worktree(
        &self,
        request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError>;

    async fn release_worktree(
        &self,
        request: WorktreeReleaseRequest,
    ) -> Result<WorktreeReleaseResponse, RuntimeError>;

    async fn cleanup_worktree(
        &self,
        request: WorktreeCleanupRequest,
    ) -> Result<WorktreeCleanupResponse, RuntimeError>;

    async fn spawn_team_member(
        &self,
        request: TeamMemberSpawnRequest,
    ) -> Result<TeamMemberSpawnResponse, RuntimeError>;

    async fn on_member_removed(
        &self,
        request: WorktreeMemberRemovedRequest,
    ) -> Result<WorktreeMemberRemovedResponse, RuntimeError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCreateRequest {
    pub team_id: Option<String>,
    pub source_session_id: String,
    pub repo_root: Option<String>,
    pub worktree_name: String,
    pub branch_prefix: Option<String>,
    pub base_ref: Option<String>,
    pub deletion_policy: Option<String>,
    pub run_init_script: Option<bool>,
    pub created_by_session_id: Option<String>,
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCreateResponse {
    pub worktree: ManagedWorktreeRecord,
    pub created: bool,
    pub init_script_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeClaimRequest {
    pub worktree_id: String,
    pub session_id: String,
    pub claim_role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeClaimResponse {
    pub worktree: ManagedWorktreeRecord,
    pub claim: ManagedWorktreeClaimRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeReleaseRequest {
    pub worktree_id: String,
    pub session_id: String,
    pub cleanup_if_last_claim: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeReleaseResponse {
    pub worktree: ManagedWorktreeRecord,
    pub released_claim: ManagedWorktreeClaimRecord,
    pub active_claim_count: usize,
    pub cleanup: Option<WorktreeCleanupResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCleanupRequest {
    pub worktree_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeCleanupResponse {
    pub worktree_id: String,
    pub status: String,
    pub deletion_policy: String,
    pub active_claim_count: usize,
    pub worktree_path_deleted: bool,
    pub branch_deleted: bool,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberSpawnWorktreeInput {
    pub mode: Option<String>,
    pub name: Option<String>,
    pub branch_prefix: Option<String>,
    pub base_ref: Option<String>,
    pub run_init_script: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberSpawnRequest {
    pub team_id: String,
    pub source_session_id: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub title: Option<String>,
    pub prompt: Option<String>,
    pub permission_mode: Option<String>,
    pub metadata: Option<Value>,
    pub worktree: Option<TeamMemberSpawnWorktreeInput>,
    pub creator_agent_id: Option<String>,
    pub creator_compaction_subscription: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamMemberSpawnResponse {
    pub operation_id: String,
    pub team: TeamWithMembers,
    pub spawned_session: SessionRecord,
    pub spawned_member: TeamMemberRecord,
    pub worktree: Option<ManagedWorktreeRecord>,
    pub worktree_assignment_mode: String,
    pub worktree_created_by_operation: bool,
    pub onboarding: Value,
    pub journal_stage: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeMemberRemovedRequest {
    pub team_id: String,
    pub agent_id: String,
    pub removed_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeMemberRemovedResponse {
    pub released_claims: Vec<ManagedWorktreeClaimRecord>,
    pub cleanup_results: Vec<WorktreeCleanupResponse>,
    pub diagnostics: Vec<TeamOperationDiagnosticRecord>,
}
