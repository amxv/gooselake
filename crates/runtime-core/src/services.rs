use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::sync::broadcast;

use crate::{
    ApprovalRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent,
    ProcessRecord, RuntimeError, RuntimeEventRecord, RuntimeEventScope, RuntimeHydratedState,
    SessionRecord, TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord,
    TeamOperationDiagnosticRecord, TeamOperationJournalRecord, TeamRecord, TurnRecord,
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

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError>;

    fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError>;

    fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError>;

    fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError>;

    fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError>;

    fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError>;

    fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError>;

    fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError>;

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError>;

    fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError>;

    fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError>;

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
pub struct ProcessRunRequest {
    pub caller_session_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub command: String,
    pub cwd: Option<String>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessListRequest {
    pub caller_session_id: Option<String>,
    pub include_completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessGetRequest {
    pub process_id: String,
    pub caller_session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessLogReadRequest {
    pub process_id: String,
    pub caller_session_id: Option<String>,
    pub stream: Option<String>,
    pub head_lines: Option<usize>,
    pub tail_lines: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessKillRequest {
    pub process_id: String,
    pub caller_session_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessSummary {
    pub process_id: String,
    pub session_id: Option<String>,
    pub pid: Option<i64>,
    pub status: String,
    pub command: Value,
    pub cwd: Option<String>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessDetails {
    pub process: ProcessSummary,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub timeout_ms: Option<i64>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessLogsChunk {
    pub process_id: String,
    pub stream: String,
    pub content: String,
    pub head_lines: usize,
    pub tail_lines: usize,
    pub truncated: bool,
    pub bytes: usize,
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
pub trait ProcessManager: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn run_process(&self, request: ProcessRunRequest)
        -> Result<ProcessDetails, RuntimeError>;

    async fn list_processes(
        &self,
        request: ProcessListRequest,
    ) -> Result<Vec<ProcessSummary>, RuntimeError>;

    async fn get_process(&self, request: ProcessGetRequest)
        -> Result<ProcessDetails, RuntimeError>;

    async fn read_process_logs(
        &self,
        request: ProcessLogReadRequest,
    ) -> Result<Vec<ProcessLogsChunk>, RuntimeError>;

    async fn kill_process(
        &self,
        request: ProcessKillRequest,
    ) -> Result<ProcessDetails, RuntimeError>;

    async fn replay_events(
        &self,
        process_id: String,
        caller_session_id: Option<String>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError>;

    fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEventRecord>;
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
