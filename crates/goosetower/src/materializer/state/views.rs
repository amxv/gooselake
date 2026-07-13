use std::collections::BTreeMap;

use runtime_core::{
    ProviderModel, RuntimeEventScope, SessionRecord, TeamDeliveryRecord, TeamMemberRecord,
    TeamMessageRecord, TeamRecord,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{EntityKey, EntityVersion};
use crate::config::{RuntimeSourceCapabilitiesConfig, RuntimeSourceConfig};
use crate::runtime::events::SourceHealthState;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRowView {
    pub source_id: String,
    pub row_id: String,
    pub session_id: String,
    pub team_id: Option<String>,
    pub title: Option<String>,
    pub provider: String,
    pub model: Option<String>,
    pub status: String,
    pub cwd: Option<String>,
    pub worktree_id: Option<String>,
    pub worktree_path: Option<String>,
    pub active_turn_id: Option<String>,
    pub pending_approval_count: usize,
    pub active_process_count: usize,
    pub delivery_status_counts: BTreeMap<String, usize>,
    pub latest_activity_unix_ms: i64,
    pub source_health: SourceHealthState,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FleetBoardView {
    pub rows: Vec<AgentRowView>,
    pub total_rows: usize,
    pub cursor: Option<SourceCursorView>,
    pub cursors: Vec<SourceCursorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCursorView {
    pub source_id: String,
    pub source_epoch: String,
    pub source_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRowView {
    pub source_id: String,
    pub approval_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub tool_call_id: Option<String>,
    pub status: String,
    pub risk: String,
    pub summary: String,
    pub created_at: i64,
    pub resolved_at: Option<i64>,
    pub source_health: SourceHealthState,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ApprovalInboxView {
    pub approvals: Vec<ApprovalRowView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionDetailView {
    pub source_id: String,
    pub session: SessionRecord,
    pub team_ids: Vec<String>,
    pub pending_approvals: Vec<ApprovalRowView>,
    pub active_processes: Vec<ProcessView>,
    pub recent_processes: Vec<ProcessView>,
    pub transcript: Vec<TranscriptEntryView>,
    pub appended_text: String,
    pub latest_activity_unix_ms: i64,
    pub source_health: SourceHealthState,
    pub discontinuities: Vec<DiscontinuityView>,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptEntryView {
    pub role: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamWorkspaceView {
    pub source_id: String,
    pub team: TeamRecord,
    pub members: Vec<TeamMemberView>,
    pub messages: Vec<TeamMessageRecord>,
    pub deliveries: Vec<TeamDeliveryRecord>,
    pub delivery_status_counts: BTreeMap<String, usize>,
    pub latest_activity_unix_ms: i64,
    pub source_health: SourceHealthState,
    pub discontinuities: Vec<DiscontinuityView>,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamSummaryView {
    pub source_id: String,
    pub team_id: String,
    pub name: String,
    pub lead_member_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TeamSummaryListView {
    pub teams: Vec<TeamSummaryView>,
    pub total_rows: usize,
    pub cursors: Vec<SourceCursorView>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamMemberView {
    pub member: TeamMemberRecord,
    pub session: Option<SessionRecord>,
    pub worktree: Option<WorktreeView>,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessView {
    pub source_id: String,
    pub process_id: String,
    pub session_id: Option<String>,
    pub pid: Option<i64>,
    pub status: String,
    pub command: Value,
    pub cwd: Option<String>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub exit_code: Option<i64>,
    pub signal: Option<i64>,
    pub stdout_bytes: Option<usize>,
    pub stderr_bytes: Option<usize>,
    pub stdout_truncated: Option<bool>,
    pub stderr_truncated: Option<bool>,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessTailView {
    pub source_id: String,
    pub process: Option<ProcessView>,
    pub stdout: Vec<LogLineView>,
    pub stderr: Vec<LogLineView>,
    pub samples: Vec<ProcessOutputSampleView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLineView {
    pub stream: String,
    pub content: String,
    pub bytes: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessOutputSampleView {
    pub source_seq: i64,
    pub stream: String,
    pub bytes_seen: usize,
    pub bytes_written: usize,
    pub truncated: bool,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEventView {
    pub source_id: String,
    pub source_epoch: String,
    pub source_seq: i64,
    pub upstream_row_id: i64,
    pub upstream_scoped_seq: i64,
    pub scope: RuntimeEventScope,
    pub scope_id: String,
    pub session_id: Option<String>,
    pub team_id: Option<String>,
    pub turn_id: Option<String>,
    pub kind: String,
    pub criticality: String,
    pub lane: String,
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LedgerView {
    pub events: Vec<LedgerEventView>,
    pub discontinuities: Vec<DiscontinuityView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscontinuityView {
    pub source_id: String,
    pub source_epoch: String,
    pub source_seq: Option<i64>,
    pub reason: String,
    pub observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceHealthView {
    pub source_id: String,
    pub source_epoch: String,
    pub display_name: String,
    pub source_kind: String,
    pub provisioner_kind: String,
    pub state: SourceHealthState,
    pub last_source_seq: Option<i64>,
    pub last_error: Option<String>,
    pub observed_at_unix_ms: i64,
    pub active_session_count: usize,
    pub active_process_count: usize,
    pub provider_kinds: Vec<String>,
    pub models: Vec<String>,
    pub model_capabilities: Vec<ModelCapabilityView>,
    pub process_capacity: Option<u32>,
    pub supports_worktrees: bool,
    pub supports_teams: bool,
    pub replay_window_events: Option<u64>,
    pub replay_window_ms: Option<u64>,
    pub region: Option<String>,
    pub cost_hint: Option<String>,
    pub provider_status: Value,
    pub diagnostics_summary: Value,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilityView {
    pub provider: String,
    pub model: String,
    pub display_name: String,
    pub reasoning_levels: Vec<String>,
}

impl ModelCapabilityView {
    pub fn from_provider_model(provider: impl Into<String>, model: &ProviderModel) -> Self {
        Self {
            provider: provider.into(),
            model: model.id.clone(),
            display_name: model.display_name.clone(),
            reasoning_levels: model.reasoning_levels.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadataView {
    pub display_name: String,
    pub source_kind: String,
    pub provisioner_kind: String,
    pub provider_kinds: Vec<String>,
    pub models: Vec<String>,
    pub model_capabilities: Vec<ModelCapabilityView>,
    pub process_capacity: Option<u32>,
    pub supports_worktrees: bool,
    pub supports_teams: bool,
    pub replay_window_events: Option<u64>,
    pub replay_window_ms: Option<u64>,
    pub region: Option<String>,
    pub cost_hint: Option<String>,
}

impl SourceMetadataView {
    pub fn from_source_config(source: &RuntimeSourceConfig) -> Self {
        Self {
            display_name: source.display_name.clone(),
            source_kind: source.source_kind.clone(),
            provisioner_kind: serde_json::to_value(source.provisioner_kind)
                .ok()
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "static".to_string()),
            provider_kinds: source.capabilities.provider_kinds.clone(),
            models: source.capabilities.models.clone(),
            model_capabilities: Vec::new(),
            process_capacity: source.capabilities.process_capacity,
            supports_worktrees: source.capabilities.supports_worktrees,
            supports_teams: source.capabilities.supports_teams,
            replay_window_events: source.capabilities.replay_window_events,
            replay_window_ms: source.capabilities.replay_window_ms,
            region: source.region.clone(),
            cost_hint: source.cost_hint.clone(),
        }
    }
}

impl Default for SourceMetadataView {
    fn default() -> Self {
        let capabilities = RuntimeSourceCapabilitiesConfig::default();
        Self {
            display_name: String::new(),
            source_kind: String::new(),
            provisioner_kind: "static".to_string(),
            provider_kinds: capabilities.provider_kinds,
            models: capabilities.models,
            model_capabilities: Vec::new(),
            process_capacity: capabilities.process_capacity,
            supports_worktrees: capabilities.supports_worktrees,
            supports_teams: capabilities.supports_teams,
            replay_window_events: capabilities.replay_window_events,
            replay_window_ms: capabilities.replay_window_ms,
            region: None,
            cost_hint: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorktreeView {
    pub source_id: String,
    pub worktree_id: String,
    pub repo_root: String,
    pub worktree_root: String,
    pub worktree_cwd: String,
    pub branch_name: String,
    pub worktree_name: String,
    pub status: String,
    pub active_claim_session_ids: Vec<String>,
    pub version: EntityVersion,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MaterializedPatchKind {
    EntityUpsert,
    EntityRemove,
    ListInsert,
    ListRemove,
    ListMove,
    TextAppend,
    LogAppend,
    LogSample,
    SourceHealthTransition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaterializedPatch {
    pub kind: MaterializedPatchKind,
    pub view_kind: String,
    pub entity: Option<EntityKey>,
    pub version: Option<EntityVersion>,
    pub source_cursor: Option<SourceCursorView>,
    pub body: Value,
}
