use runtime_core::{ProviderMetadata, ProviderModel};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeHealthResponse {
    pub status: String,
    pub providers: Option<usize>,
    pub public_base_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeVersionResponse {
    pub version: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderListResponse {
    pub providers: Vec<ProviderMetadata>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderModelsResponse {
    pub provider: String,
    pub models: Vec<ProviderModel>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeDiagnosticsResponse {
    pub providers: Value,
    pub comms: Value,
    pub processes: Value,
    pub worktrees: Value,
    pub recovery: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseSessionRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamCreateInput {
    pub name: String,
    pub lead_agent_id: String,
    pub member_agent_ids: Option<Vec<String>>,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamJoinInput {
    pub agent_id: String,
    pub title: Option<String>,
    pub added_by: Option<String>,
    pub creator_agent_id: Option<String>,
    pub creator_compaction_subscription: Option<String>,
    pub worktree_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamMemberSpawnInput {
    pub source_session_id: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub title: Option<String>,
    pub prompt: Option<String>,
    pub permission_mode: Option<String>,
    pub metadata: Option<Value>,
    pub worktree: Option<runtime_core::TeamMemberSpawnWorktreeInput>,
    pub creator_agent_id: Option<String>,
    pub creator_compaction_subscription: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamSetLeadInput {
    pub lead_agent_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamDirectInput {
    pub sender_agent_id: String,
    pub recipient_agent_id: String,
    pub input: Value,
    pub image_paths: Option<Vec<String>>,
    pub priority: Option<String>,
    pub policy: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to_message_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamBroadcastInput {
    pub sender_agent_id: String,
    pub input: Value,
    pub image_paths: Option<Vec<String>>,
    pub priority: Option<String>,
    pub policy: Option<String>,
    pub include_sender: Option<bool>,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ProcessLogsQuery {
    pub session_id: Option<String>,
    pub stream: Option<String>,
    pub head_lines: Option<usize>,
    pub tail_lines: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessStartInput {
    pub command: String,
    pub cwd: Option<String>,
    pub timeout_ms: Option<u64>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessKillInput {
    pub session_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorktreeCreateInput {
    pub source_session_id: String,
    pub repo_root: Option<String>,
    pub worktree_name: String,
    pub branch_prefix: Option<String>,
    pub base_ref: Option<String>,
    pub deletion_policy: Option<String>,
    pub run_init_script: Option<bool>,
    pub created_by_session_id: Option<String>,
    pub operation_id: Option<String>,
    pub team_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorktreeClaimInput {
    pub session_id: String,
    pub claim_role: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorktreeReleaseInput {
    pub session_id: String,
    pub cleanup_if_last_claim: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorktreeCleanupInput {
    pub reason: Option<String>,
}
