use std::collections::{BTreeMap, BTreeSet, VecDeque};

use runtime_core::{
    ApprovalRecord, ManagedWorktreeRecord, ProcessDetails, ProcessLogsChunk, ProcessSummary,
    ProviderModel, RuntimeEventScope, SessionRecord, TeamDeliveryRecord, TeamMemberRecord,
    TeamMessageRecord, TeamRecord,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{RuntimeSourceCapabilitiesConfig, RuntimeSourceConfig};
use crate::runtime::events::{SourceEvent, SourceHealth, SourceHealthState};

pub const DEFAULT_LEDGER_LIMIT: usize = 2_000;
pub const DEFAULT_TEXT_LIMIT: usize = 128;
pub const DEFAULT_LOG_LIMIT: usize = 256;
const SESSION_CONTEXT_METADATA_KEY: &str = "context_window";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializerStatus {
    Empty,
    Replaying,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct EntityVersion(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntityKey {
    pub source_id: String,
    pub entity_kind: String,
    pub entity_id: String,
}

impl EntityKey {
    pub fn new(
        source_id: impl Into<String>,
        entity_kind: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            entity_kind: entity_kind.into(),
            entity_id: entity_id.into(),
        }
    }
}

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

#[derive(Debug, Clone)]
pub struct MaterializedState {
    pub source_id: String,
    pub source_epoch: String,
    pub status: MaterializerStatus,
    pub source_health: SourceHealth,
    pub source_metadata: SourceMetadataView,
    pub ownership: SourceOwnershipIndexes,
    pub last_gateway_seq: u64,
    pub seen_source_cursors: BTreeSet<(String, String, i64)>,
    versions: BTreeMap<EntityKey, EntityVersion>,
    pub sessions: BTreeMap<String, SessionRecord>,
    pub approvals: BTreeMap<String, ApprovalRecord>,
    pub teams: BTreeMap<String, TeamRecord>,
    pub members_by_team: BTreeMap<String, BTreeMap<String, TeamMemberRecord>>,
    pub messages_by_team: BTreeMap<String, Vec<TeamMessageRecord>>,
    pub deliveries_by_team: BTreeMap<String, Vec<TeamDeliveryRecord>>,
    pub worktrees: BTreeMap<String, ManagedWorktreeRecord>,
    pub active_worktree_claims: BTreeMap<String, BTreeSet<String>>,
    pub processes: BTreeMap<String, ProcessView>,
    pub process_stdout: BTreeMap<String, VecDeque<LogLineView>>,
    pub process_stderr: BTreeMap<String, VecDeque<LogLineView>>,
    pub process_samples: BTreeMap<String, VecDeque<ProcessOutputSampleView>>,
    pub appended_text_by_session: BTreeMap<String, VecDeque<String>>,
    pub ledger: VecDeque<LedgerEventView>,
    pub discontinuities: VecDeque<DiscontinuityView>,
    pub provider_status: Value,
    pub diagnostics_summary: Value,
    pub selected_team_ids: BTreeSet<String>,
    pub default_team_ids: BTreeSet<String>,
    ledger_limit: usize,
    text_limit: usize,
    log_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SourceOwnershipIndexes {
    pub sessions: BTreeSet<String>,
    pub teams: BTreeSet<String>,
    pub processes: BTreeSet<String>,
    pub worktrees: BTreeSet<String>,
    pub deliveries: BTreeSet<String>,
}

impl SourceOwnershipIndexes {
    pub fn owns(&self, entity_kind: &str, entity_id: &str) -> bool {
        match entity_kind {
            "session" => self.sessions.contains(entity_id),
            "team" => self.teams.contains(entity_id),
            "process" => self.processes.contains(entity_id),
            "worktree" => self.worktrees.contains(entity_id),
            "team_delivery" | "delivery" => self.deliveries.contains(entity_id),
            _ => false,
        }
    }
}

impl MaterializedState {
    pub fn new(source_id: impl Into<String>, source_epoch: impl Into<String>) -> Self {
        let source_id = source_id.into();
        let source_epoch = source_epoch.into();
        Self {
            source_health: SourceHealth::new(source_id.clone(), source_epoch.clone()),
            source_metadata: SourceMetadataView {
                display_name: source_id.clone(),
                source_kind: "gooselake-runtime".to_string(),
                ..SourceMetadataView::default()
            },
            ownership: SourceOwnershipIndexes::default(),
            source_id,
            source_epoch,
            status: MaterializerStatus::Empty,
            last_gateway_seq: 0,
            seen_source_cursors: BTreeSet::new(),
            versions: BTreeMap::new(),
            sessions: BTreeMap::new(),
            approvals: BTreeMap::new(),
            teams: BTreeMap::new(),
            members_by_team: BTreeMap::new(),
            messages_by_team: BTreeMap::new(),
            deliveries_by_team: BTreeMap::new(),
            worktrees: BTreeMap::new(),
            active_worktree_claims: BTreeMap::new(),
            processes: BTreeMap::new(),
            process_stdout: BTreeMap::new(),
            process_stderr: BTreeMap::new(),
            process_samples: BTreeMap::new(),
            appended_text_by_session: BTreeMap::new(),
            ledger: VecDeque::new(),
            discontinuities: VecDeque::new(),
            provider_status: Value::Null,
            diagnostics_summary: Value::Null,
            selected_team_ids: BTreeSet::new(),
            default_team_ids: BTreeSet::new(),
            ledger_limit: DEFAULT_LEDGER_LIMIT,
            text_limit: DEFAULT_TEXT_LIMIT,
            log_limit: DEFAULT_LOG_LIMIT,
        }
    }

    pub fn with_limits(mut self, ledger_limit: usize, text_limit: usize, log_limit: usize) -> Self {
        self.ledger_limit = ledger_limit.max(1);
        self.text_limit = text_limit.max(1);
        self.log_limit = log_limit.max(1);
        self
    }

    pub fn apply_source_config(&mut self, source: &RuntimeSourceConfig) {
        let model_capabilities = self.source_metadata.model_capabilities.clone();
        self.source_metadata = SourceMetadataView::from_source_config(source);
        self.source_metadata.model_capabilities = model_capabilities;
        self.source_health.state = source.lifecycle;
        self.bump_source_health_version();
    }

    pub fn cursor(&self) -> Option<SourceCursorView> {
        self.source_health
            .last_source_seq
            .map(|source_seq| SourceCursorView {
                source_id: self.source_id.clone(),
                source_epoch: self.source_epoch.clone(),
                source_seq,
            })
    }

    pub fn mark_replaying(&mut self) {
        self.status = MaterializerStatus::Replaying;
        self.source_health
            .transition(SourceHealthState::Replaying, None, None);
        self.bump_source_health_version();
    }

    pub fn mark_live(&mut self) {
        self.status = MaterializerStatus::Live;
        self.source_health.transition(
            SourceHealthState::Live,
            self.source_health.last_source_seq,
            None,
        );
        self.bump_source_health_version();
    }

    pub fn transition_source_health(
        &mut self,
        state: SourceHealthState,
        error: Option<String>,
    ) -> MaterializedPatch {
        let previous = self.source_health.state;
        self.source_health
            .transition(state, self.source_health.last_source_seq, error);
        let version = self.bump_source_health_version();
        let body = serde_json::to_value(self.source_health_view()).unwrap_or_else(|_| {
            serde_json::json!({
                "source_id": self.source_id,
                "previous": previous,
                "current": self.source_health.state,
                "last_error": self.source_health.last_error,
            })
        });
        MaterializedPatch {
            kind: MaterializedPatchKind::SourceHealthTransition,
            view_kind: "source_health".to_string(),
            entity: Some(EntityKey::new(
                &self.source_id,
                "source",
                self.source_id.as_str(),
            )),
            version: Some(version),
            source_cursor: self.cursor(),
            body,
        }
    }

    pub fn next_gateway_seq(&mut self) -> u64 {
        self.last_gateway_seq = self.last_gateway_seq.saturating_add(1);
        self.last_gateway_seq
    }

    pub fn remember_source_event(&mut self, event: &SourceEvent) -> bool {
        let key = (
            event.source_id.clone(),
            event.source_epoch.clone(),
            event.source_seq,
        );
        if !self.seen_source_cursors.insert(key) {
            return false;
        }
        self.source_health
            .transition(SourceHealthState::Live, Some(event.source_seq), None);
        true
    }

    pub fn bump_version(
        &mut self,
        entity_kind: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> EntityVersion {
        let key = EntityKey::new(&self.source_id, entity_kind, entity_id);
        let next = self
            .versions
            .get(&key)
            .map(|version| version.0.saturating_add(1))
            .unwrap_or(1);
        let version = EntityVersion(next);
        self.versions.insert(key, version);
        version
    }

    pub fn version(
        &self,
        entity_kind: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> EntityVersion {
        let key = EntityKey::new(&self.source_id, entity_kind, entity_id);
        self.versions.get(&key).copied().unwrap_or_default()
    }

    pub fn remove_version(
        &mut self,
        entity_kind: impl Into<String>,
        entity_id: impl Into<String>,
    ) -> Option<EntityVersion> {
        let key = EntityKey::new(&self.source_id, entity_kind, entity_id);
        self.versions.remove(&key)
    }

    pub fn upsert_session(&mut self, mut session: SessionRecord) -> EntityVersion {
        session.updated_at = session.updated_at.max(session.created_at);
        let id = session.id.clone();
        self.ownership.sessions.insert(id.clone());
        self.sessions.insert(id.clone(), session);
        self.bump_version("session", id)
    }

    pub fn update_session_context_usage(
        &mut self,
        session_id: &str,
        usage: &Value,
    ) -> Option<SessionContextUsageView> {
        let context = SessionContextUsageView::from_usage(usage)?;
        let session = self.sessions.get_mut(session_id)?;
        let Some(metadata) = session.metadata.as_object_mut() else {
            session.metadata = serde_json::json!({});
            let metadata = session.metadata.as_object_mut()?;
            metadata.insert(
                SESSION_CONTEXT_METADATA_KEY.to_string(),
                serde_json::to_value(&context).ok()?,
            );
            self.bump_version("session", session_id);
            return Some(context);
        };
        metadata.insert(
            SESSION_CONTEXT_METADATA_KEY.to_string(),
            serde_json::to_value(&context).ok()?,
        );
        self.bump_version("session", session_id);
        Some(context)
    }

    pub fn upsert_approval(&mut self, approval: ApprovalRecord) -> EntityVersion {
        let id = approval.id.clone();
        self.approvals.insert(id.clone(), approval);
        self.bump_version("approval", id)
    }

    pub fn upsert_team(&mut self, team: TeamRecord) -> EntityVersion {
        let id = team.id.clone();
        self.ownership.teams.insert(id.clone());
        self.teams.insert(id.clone(), team);
        self.bump_version("team", id)
    }

    pub fn upsert_team_member(&mut self, member: TeamMemberRecord) -> EntityVersion {
        let team_id = member.team_id.clone();
        let agent_id = member.agent_id.clone();
        self.members_by_team
            .entry(team_id.clone())
            .or_default()
            .insert(agent_id.clone(), member);
        self.bump_version("team_member", format!("{team_id}:{agent_id}"))
    }

    pub fn remove_team_member(&mut self, team_id: &str, agent_id: &str) -> Option<EntityVersion> {
        self.members_by_team
            .get_mut(team_id)
            .and_then(|members| members.remove(agent_id))?;
        self.remove_version("team_member", format!("{team_id}:{agent_id}"))
    }

    pub fn upsert_message(&mut self, message: TeamMessageRecord) -> EntityVersion {
        let id = message.id.clone();
        let team_id = message.team_id.clone();
        let messages = self.messages_by_team.entry(team_id).or_default();
        if let Some(existing) = messages.iter_mut().find(|row| row.id == id) {
            *existing = message;
        } else {
            messages.push(message);
            messages.sort_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
        }
        self.bump_version("team_message", id)
    }

    pub fn upsert_delivery(&mut self, delivery: TeamDeliveryRecord) -> EntityVersion {
        let id = delivery.id.clone();
        let team_id = delivery.team_id.clone();
        self.ownership.deliveries.insert(id.clone());
        let deliveries = self.deliveries_by_team.entry(team_id).or_default();
        if let Some(existing) = deliveries.iter_mut().find(|row| row.id == id) {
            *existing = delivery;
        } else {
            deliveries.push(delivery);
            deliveries.sort_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
        }
        self.bump_version("team_delivery", id)
    }

    pub fn upsert_worktree(&mut self, worktree: ManagedWorktreeRecord) -> EntityVersion {
        let id = worktree.id.clone();
        self.ownership.worktrees.insert(id.clone());
        self.worktrees.insert(id.clone(), worktree);
        self.bump_version("worktree", id)
    }

    pub fn upsert_process_summary(&mut self, process: ProcessSummary) -> EntityVersion {
        let id = process.process_id.clone();
        self.ownership.processes.insert(id.clone());
        let existing = self.processes.get(&id).cloned();
        self.processes.insert(
            id.clone(),
            ProcessView {
                source_id: self.source_id.clone(),
                process_id: id.clone(),
                session_id: process.session_id,
                pid: process.pid,
                status: process.status,
                command: process.command,
                cwd: process.cwd,
                started_at: process.started_at,
                ended_at: process.ended_at,
                exit_code: existing.as_ref().and_then(|row| row.exit_code),
                signal: existing.as_ref().and_then(|row| row.signal),
                stdout_bytes: existing.as_ref().and_then(|row| row.stdout_bytes),
                stderr_bytes: existing.as_ref().and_then(|row| row.stderr_bytes),
                stdout_truncated: existing.as_ref().and_then(|row| row.stdout_truncated),
                stderr_truncated: existing.as_ref().and_then(|row| row.stderr_truncated),
                version: EntityVersion::default(),
            },
        );
        let version = self.bump_version("process", id.clone());
        if let Some(process) = self.processes.get_mut(&id) {
            process.version = version;
        }
        version
    }

    pub fn upsert_process_details(&mut self, details: ProcessDetails) -> EntityVersion {
        let id = details.process.process_id.clone();
        self.ownership.processes.insert(id.clone());
        self.processes.insert(
            id.clone(),
            ProcessView {
                source_id: self.source_id.clone(),
                process_id: id.clone(),
                session_id: details.process.session_id,
                pid: details.process.pid,
                status: details.process.status,
                command: details.process.command,
                cwd: details.process.cwd,
                started_at: details.process.started_at,
                ended_at: details.process.ended_at,
                exit_code: details.exit_code,
                signal: details.signal,
                stdout_bytes: Some(details.stdout_bytes),
                stderr_bytes: Some(details.stderr_bytes),
                stdout_truncated: Some(details.stdout_truncated),
                stderr_truncated: Some(details.stderr_truncated),
                version: EntityVersion::default(),
            },
        );
        let version = self.bump_version("process", id.clone());
        if let Some(process) = self.processes.get_mut(&id) {
            process.version = version;
        }
        version
    }

    pub fn append_process_logs(&mut self, process_id: &str, logs: Vec<ProcessLogsChunk>) {
        for log in logs {
            let lines = if log.stream == "stderr" {
                self.process_stderr
                    .entry(process_id.to_string())
                    .or_default()
            } else {
                self.process_stdout
                    .entry(process_id.to_string())
                    .or_default()
            };
            bounded_push(
                lines,
                LogLineView {
                    stream: log.stream,
                    content: log.content,
                    bytes: log.bytes,
                    truncated: log.truncated,
                },
                self.log_limit,
            );
        }
    }

    pub fn append_text(&mut self, session_id: &str, text: String) {
        let lines = self
            .appended_text_by_session
            .entry(session_id.to_string())
            .or_default();
        bounded_push(lines, text, self.text_limit);
    }

    pub fn append_log_sample(&mut self, process_id: &str, sample: ProcessOutputSampleView) {
        let samples = self
            .process_samples
            .entry(process_id.to_string())
            .or_default();
        bounded_push(samples, sample, self.log_limit);
    }

    pub fn append_ledger_event(&mut self, event: LedgerEventView) {
        bounded_push(&mut self.ledger, event, self.ledger_limit);
    }

    pub fn mark_discontinuity(&mut self, reason: impl Into<String>) {
        let discontinuity = DiscontinuityView {
            source_id: self.source_id.clone(),
            source_epoch: self.source_epoch.clone(),
            source_seq: self.source_health.last_source_seq,
            reason: reason.into(),
            observed_at_unix_ms: now_ms(),
        };
        bounded_push(&mut self.discontinuities, discontinuity, self.ledger_limit);
    }

    pub fn source_health_view(&self) -> SourceHealthView {
        SourceHealthView {
            source_id: self.source_id.clone(),
            source_epoch: self.source_epoch.clone(),
            display_name: self.source_metadata.display_name.clone(),
            source_kind: self.source_metadata.source_kind.clone(),
            provisioner_kind: self.source_metadata.provisioner_kind.clone(),
            state: self.source_health.state,
            last_source_seq: self.source_health.last_source_seq,
            last_error: self.source_health.last_error.clone(),
            observed_at_unix_ms: self.source_health.updated_at,
            active_session_count: self
                .sessions
                .values()
                .filter(|session| !matches!(session.status.as_str(), "closed" | "failed"))
                .count(),
            active_process_count: self
                .processes
                .values()
                .filter(|process| matches!(process.status.as_str(), "queued" | "running"))
                .count(),
            provider_kinds: self.source_metadata.provider_kinds.clone(),
            models: self.source_metadata.models.clone(),
            model_capabilities: self.source_metadata.model_capabilities.clone(),
            process_capacity: self.source_metadata.process_capacity,
            supports_worktrees: self.source_metadata.supports_worktrees,
            supports_teams: self.source_metadata.supports_teams,
            replay_window_events: self.source_metadata.replay_window_events,
            replay_window_ms: self.source_metadata.replay_window_ms,
            region: self.source_metadata.region.clone(),
            cost_hint: self.source_metadata.cost_hint.clone(),
            provider_status: self.provider_status.clone(),
            diagnostics_summary: self.diagnostics_summary.clone(),
            version: self.version("source", &self.source_id),
        }
    }

    pub fn worktree_view(&self, worktree_id: &str) -> Option<WorktreeView> {
        let worktree = self.worktrees.get(worktree_id)?;
        Some(WorktreeView {
            source_id: self.source_id.clone(),
            worktree_id: worktree.id.clone(),
            repo_root: worktree.repo_root.clone(),
            worktree_root: worktree.worktree_root.clone(),
            worktree_cwd: worktree.worktree_cwd.clone(),
            branch_name: worktree.branch_name.clone(),
            worktree_name: worktree.worktree_name.clone(),
            status: if self
                .active_worktree_claims
                .get(worktree_id)
                .is_some_and(|claims| !claims.is_empty())
            {
                "claimed".to_string()
            } else {
                "available".to_string()
            },
            active_claim_session_ids: self
                .active_worktree_claims
                .get(worktree_id)
                .map(|claims| claims.iter().cloned().collect())
                .unwrap_or_default(),
            version: self.version("worktree", worktree_id),
        })
    }

    pub fn agent_row(&self, session_id: &str) -> Option<AgentRowView> {
        let session = self.sessions.get(session_id)?;
        let team_member = self.team_member_for_session(session_id);
        let team_id = team_member.as_ref().map(|(team_id, _)| team_id.clone());
        let title = team_member.and_then(|(_, member)| member.title.clone());
        let worktree_path = session
            .worktree_id
            .as_deref()
            .and_then(|id| self.worktree_view(id))
            .map(|view| view.worktree_cwd);
        Some(AgentRowView {
            source_id: self.source_id.clone(),
            row_id: format!("{}:{session_id}", self.source_id),
            session_id: session.id.clone(),
            team_id,
            title,
            provider: session.provider.clone(),
            model: session.model.clone(),
            status: session.status.clone(),
            cwd: session.cwd.clone(),
            worktree_id: session.worktree_id.clone(),
            worktree_path,
            active_turn_id: session.active_turn_id.clone(),
            pending_approval_count: self
                .approvals
                .values()
                .filter(|approval| {
                    approval.session_id == session_id && approval.status == "pending"
                })
                .count(),
            active_process_count: self
                .processes
                .values()
                .filter(|process| {
                    process.session_id.as_deref() == Some(session_id)
                        && matches!(process.status.as_str(), "queued" | "running")
                })
                .count(),
            delivery_status_counts: self.delivery_status_counts_for_session(session_id),
            latest_activity_unix_ms: latest_session_activity(self, session),
            source_health: self.source_health.state,
            version: self.version("session", session_id),
        })
    }

    fn team_member_for_session(&self, session_id: &str) -> Option<(String, TeamMemberRecord)> {
        self.members_by_team.iter().find_map(|(team_id, members)| {
            members
                .get(session_id)
                .cloned()
                .map(|member| (team_id.clone(), member))
        })
    }

    fn delivery_status_counts_for_session(&self, session_id: &str) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for delivery in self
            .deliveries_by_team
            .values()
            .flatten()
            .filter(|delivery| delivery.recipient_agent_id == session_id)
        {
            *counts.entry(delivery.status.clone()).or_insert(0) += 1;
        }
        counts
    }

    fn bump_source_health_version(&mut self) -> EntityVersion {
        self.bump_version("source", self.source_id.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionContextUsageView {
    pub remaining_percent: u8,
    pub window_tokens: u64,
    pub used_tokens: u64,
}

impl SessionContextUsageView {
    pub fn from_session(session: &SessionRecord) -> Option<Self> {
        serde_json::from_value(
            session
                .metadata
                .get(SESSION_CONTEXT_METADATA_KEY)?
                .clone(),
        )
        .ok()
    }

    pub fn from_usage(usage: &Value) -> Option<Self> {
        let window_tokens = context_window_tokens_from_usage(usage)?;
        if window_tokens == 0 {
            return None;
        }
        let used_tokens = total_tokens_from_usage(usage)?;
        let remaining_tokens = window_tokens.saturating_sub(used_tokens);
        let remaining_percent =
            ((remaining_tokens as f64 / window_tokens as f64) * 100.0).round() as i64;
        Some(Self {
            remaining_percent: remaining_percent.clamp(0, 100) as u8,
            window_tokens,
            used_tokens,
        })
    }
}

fn context_window_tokens_from_usage(usage: &Value) -> Option<u64> {
    find_numeric_field(
        usage,
        &[
            "contextWindowSize",
            "context_window_size",
            "contextWindow",
            "context_window",
            "modelContextWindow",
            "model_context_window",
        ],
    )
    .or_else(|| {
        usage
            .get("raw_usage")
            .and_then(context_window_tokens_from_usage)
    })
}

fn total_tokens_from_usage(usage: &Value) -> Option<u64> {
    find_numeric_field(usage, &["last_total_tokens", "lastTotalTokens"])
        .or_else(|| {
            usage
                .get("last")
                .or_else(|| usage.get("last_token_usage"))
                .or_else(|| usage.get("lastTokenUsage"))
                .and_then(total_tokens_from_usage)
        })
        .or_else(|| extract_total_tokens_from_usage(usage))
        .or_else(|| usage.get("raw_usage").and_then(total_tokens_from_usage))
}

fn extract_total_tokens_from_usage(value: &Value) -> Option<u64> {
    find_numeric_field(value, &["total_tokens", "totalTokens", "total"]).or_else(|| {
        let input = find_numeric_field(value, &["input_tokens", "inputTokens"]).unwrap_or(0);
        let output = find_numeric_field(value, &["output_tokens", "outputTokens"]).unwrap_or(0);
        let cache_creation = find_numeric_field(
            value,
            &["cache_creation_input_tokens", "cacheCreationInputTokens"],
        )
        .unwrap_or(0);
        let cache_read =
            find_numeric_field(value, &["cache_read_input_tokens", "cacheReadInputTokens"])
                .unwrap_or(0);
        let total = input
            .saturating_add(output)
            .saturating_add(cache_creation)
            .saturating_add(cache_read);
        (total > 0).then_some(total)
    })
}

fn find_numeric_field(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(json_number_as_u64))
}

fn json_number_as_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        value
            .as_i64()
            .and_then(|value| u64::try_from(value).ok())
            .or_else(|| value.as_f64().filter(|value| *value >= 0.0).map(|value| value as u64))
            .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
    })
}

fn latest_session_activity(state: &MaterializedState, session: &SessionRecord) -> i64 {
    let approval_latest = state
        .approvals
        .values()
        .filter(|approval| approval.session_id == session.id)
        .map(|approval| approval.resolved_at.unwrap_or(approval.created_at))
        .max()
        .unwrap_or_default();
    let process_latest = state
        .processes
        .values()
        .filter(|process| process.session_id.as_deref() == Some(session.id.as_str()))
        .map(|process| process.ended_at.unwrap_or(process.started_at))
        .max()
        .unwrap_or_default();
    session.updated_at.max(approval_latest).max(process_latest)
}

pub fn bounded_push<T>(deque: &mut VecDeque<T>, value: T, limit: usize) {
    deque.push_back(value);
    while deque.len() > limit {
        deque.pop_front();
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
