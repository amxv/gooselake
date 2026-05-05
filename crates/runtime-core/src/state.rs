use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventScope {
    Session,
    Team,
    Process,
    Worktree,
    System,
}

impl RuntimeEventScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Team => "team",
            Self::Process => "process",
            Self::Worktree => "worktree",
            Self::System => "system",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "session" => Some(Self::Session),
            "team" => Some(Self::Team),
            "process" => Some(Self::Process),
            "worktree" => Some(Self::Worktree),
            "system" => Some(Self::System),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventCriticality {
    Critical,
    Droppable,
}

impl RuntimeEventCriticality {
    pub const fn as_i64(self) -> i64 {
        match self {
            Self::Critical => 1,
            Self::Droppable => 0,
        }
    }

    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(Self::Critical),
            0 => Some(Self::Droppable),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: String,
    pub provider: String,
    pub status: String,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub permission_mode: Option<String>,
    pub system_prompt: Option<String>,
    pub metadata: Value,
    pub provider_session_ref: Option<String>,
    pub canonical_provider_session_ref: Option<String>,
    pub active_turn_id: Option<String>,
    pub worktree_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub closed_at: Option<i64>,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnRecord {
    pub id: String,
    pub session_id: String,
    pub provider_turn_ref: Option<String>,
    pub status: String,
    pub input: Value,
    pub source: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub usage: Option<Value>,
    pub error: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub id: String,
    pub session_id: String,
    pub turn_id: String,
    pub tool_call_id: Option<String>,
    pub provider_approval_ref: Option<String>,
    pub status: String,
    pub request: Value,
    pub response: Option<Value>,
    pub created_at: i64,
    pub resolved_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRecord {
    pub id: String,
    pub name: String,
    pub lead_agent_id: String,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub deleted_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamMemberRecord {
    pub team_id: String,
    pub agent_id: String,
    pub title: Option<String>,
    pub joined_at: i64,
    pub added_by: String,
    pub creator_agent_id: Option<String>,
    pub creator_compaction_subscription: String,
    pub worktree_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamMessageRecord {
    pub id: String,
    pub team_id: String,
    pub scope: String,
    pub sender_agent_id: String,
    pub recipient_agent_ids: Value,
    pub input: Value,
    pub image_paths: Value,
    pub priority: String,
    pub policy: String,
    pub correlation_id: Option<String>,
    pub reply_to_message_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamDeliveryRecord {
    pub id: String,
    pub message_id: String,
    pub team_id: String,
    pub recipient_agent_id: String,
    pub provider: String,
    pub status: String,
    pub effective_policy: Option<String>,
    pub injection_strategy: Option<String>,
    pub injected_turn_id: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedWorktreeRecord {
    pub id: String,
    pub repo_root: String,
    pub worktree_root: String,
    pub worktree_cwd: String,
    pub branch_name: String,
    pub worktree_name: String,
    pub unified_workspace_path: String,
    pub deletion_policy: String,
    pub created_by_session_id: Option<String>,
    pub created_by_operation_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedWorktreeClaimRecord {
    pub worktree_id: String,
    pub session_id: String,
    pub claim_role: String,
    pub created_at: i64,
    pub released_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub id: String,
    pub session_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub pid: Option<i64>,
    pub command: Value,
    pub cwd: Option<String>,
    pub status: String,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRecord {
    pub id: String,
    pub provider: String,
    pub profile: String,
    pub kind: String,
    pub encrypted_secret: String,
    pub metadata: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamOperationJournalRecord {
    pub operation_id: String,
    pub team_id: String,
    pub kind: String,
    pub stage: String,
    pub payload: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamOperationDiagnosticRecord {
    pub id: i64,
    pub operation_id: Option<String>,
    pub team_id: Option<String>,
    pub code: String,
    pub message: String,
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEventRecord {
    pub row_id: i64,
    pub event_id: String,
    pub scope: RuntimeEventScope,
    pub scope_id: String,
    pub session_id: Option<String>,
    pub team_id: Option<String>,
    pub turn_id: Option<String>,
    pub seq: i64,
    pub kind: String,
    pub criticality: RuntimeEventCriticality,
    pub payload: Value,
    pub provider: Option<String>,
    pub provider_seq: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewRuntimeEvent {
    pub event_id: String,
    pub scope: RuntimeEventScope,
    pub scope_id: String,
    pub session_id: Option<String>,
    pub team_id: Option<String>,
    pub turn_id: Option<String>,
    pub kind: String,
    pub criticality: RuntimeEventCriticality,
    pub payload: Value,
    pub provider: Option<String>,
    pub provider_seq: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuntimeHydratedState {
    pub sessions: Vec<SessionRecord>,
    pub turns: Vec<TurnRecord>,
    pub approvals: Vec<ApprovalRecord>,
    pub teams: Vec<TeamRecord>,
    pub team_members: Vec<TeamMemberRecord>,
    pub team_messages: Vec<TeamMessageRecord>,
    pub team_deliveries: Vec<TeamDeliveryRecord>,
    pub managed_worktrees: Vec<ManagedWorktreeRecord>,
    pub managed_worktree_claims: Vec<ManagedWorktreeClaimRecord>,
    pub team_operation_journal: Vec<TeamOperationJournalRecord>,
    pub team_operation_diagnostics: Vec<TeamOperationDiagnosticRecord>,
    pub processes: Vec<ProcessRecord>,
    pub credentials: Vec<CredentialRecord>,
}
