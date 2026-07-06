use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde_json::Value;

use crate::auth::{now_ms, AuthContext};
use crate::config::{GoosetowerConfig, LaneQueueConfig, RuntimeSourceConfig};
use crate::materializer::{
    ApprovalInboxSubscription, BoardSubscription, LedgerSubscription, MaterializedPatch,
    MaterializedState, ProcessTailSubscription, SelectedSessionSubscription,
    SelectedTeamSubscription,
};
use crate::protocol::generated::goosetower::v1::realtime_envelope::Payload;
use crate::protocol::generated::goosetower::v1::{
    CommandDuplicate, CommandRejected, CursorVector, ErrorDetail, Lane, MessageKind, Pong,
    RealtimeEnvelope, SourceCursor, Subscribe, Unsubscribe,
};
use crate::protocol::PROTOCOL_VERSION;
use crate::runtime::client::{
    GooselakeRuntimeClient, GooselakeRuntimeClientConfig, RuntimeClientError,
};

pub(super) const REASON_UNAUTHORIZED: &str = "unauthorized";
pub(super) const REASON_INVALID_SCOPE: &str = "invalid_scope";
pub(super) const REASON_INVALID_TARGET: &str = "invalid_target";
pub(super) const REASON_STALE_ENTITY_VERSION: &str = "stale_entity_version";
pub(super) const REASON_SOURCE_UNAVAILABLE: &str = "source_unavailable";
pub(super) const REASON_SOURCE_STALE: &str = "source_stale";
pub(super) const REASON_SOURCE_GAP: &str = "source_gap";
pub(super) const REASON_UPSTREAM_REJECTED: &str = "upstream_rejected";
pub(super) const REASON_UPSTREAM_TIMEOUT: &str = "upstream_timeout";
pub(super) const REASON_DUPLICATE: &str = "duplicate";

#[derive(Debug)]
pub(super) struct ConnectionState {
    pub(super) connection_id: String,
    pub(super) auth: AuthContext,
    pub(super) subscriptions: BTreeMap<String, Subscription>,
    pub(super) cursor: Option<CursorVector>,
    pub(super) last_acked_gateway_seq: u64,
    pub(super) status: ConnectionStatus,
    lanes: OutboundLanes,
    pub(super) max_message_bytes: usize,
    backpressure_drops: u64,
}

impl ConnectionState {
    pub(super) fn new(
        connection_id: String,
        auth: AuthContext,
        lane_config: LaneQueueConfig,
        max_message_bytes: usize,
    ) -> Self {
        Self {
            connection_id,
            auth,
            subscriptions: BTreeMap::new(),
            cursor: None,
            last_acked_gateway_seq: 0,
            status: ConnectionStatus::Connected,
            lanes: OutboundLanes::new(lane_config),
            max_message_bytes,
            backpressure_drops: 0,
        }
    }

    pub(super) fn enqueue(&mut self, envelope: RealtimeEnvelope, coalesce_key: Option<String>) {
        self.backpressure_drops += self.lanes.enqueue(envelope, coalesce_key) as u64;
    }

    pub(super) fn next_outbound(&mut self) -> Option<RealtimeEnvelope> {
        self.lanes.next()
    }

    pub(super) fn unsubscribe(&mut self, unsubscribe: Unsubscribe) {
        self.subscriptions.remove(&unsubscribe.subscription_id);
    }

    pub(super) fn patch_matches(&self, patch: &MaterializedPatch) -> bool {
        self.subscriptions
            .values()
            .any(|subscription| subscription.matches_patch(patch))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConnectionStatus {
    Connected,
    Degraded,
    Reconnecting,
    Replaying,
    Stale,
    Offline,
}

impl ConnectionStatus {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Degraded => "degraded",
            Self::Reconnecting => "reconnecting",
            Self::Replaying => "replaying",
            Self::Stale => "stale",
            Self::Offline => "offline",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ReplayEntry {
    pub(super) gateway_seq: u64,
    pub(super) source_cursor: Option<SourceCursor>,
    pub(super) envelope: RealtimeEnvelope,
    pub(super) encoded_len: usize,
}

#[derive(Debug)]
pub(super) struct GatewayReplayBuffer {
    entries: VecDeque<ReplayEntry>,
    capacity: usize,
}

impl GatewayReplayBuffer {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    pub(super) fn push(&mut self, entry: ReplayEntry) {
        self.entries.push_back(entry);
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    pub(super) fn replay_after(&self, gateway_seq: u64) -> ReplayWindow {
        let entries = self
            .entries
            .iter()
            .filter(|entry| entry.gateway_seq > gateway_seq)
            .cloned()
            .collect::<Vec<_>>();
        let earliest = self.entries.front().map(|entry| entry.gateway_seq);
        let complete = entries
            .first()
            .map(|entry| entry.gateway_seq == gateway_seq.saturating_add(1))
            .unwrap_or_else(|| {
                earliest
                    .map(|earliest| gateway_seq >= earliest)
                    .unwrap_or(gateway_seq == 0)
            });
        ReplayWindow { entries, complete }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ReplayWindow {
    pub(super) entries: Vec<ReplayEntry>,
    pub(super) complete: bool,
}

#[derive(Debug, Default)]
pub(super) struct GatewayMetrics {
    pub(super) resume_success: AtomicU64,
    pub(super) resume_partial: AtomicU64,
    pub(super) resume_rejected: AtomicU64,
    pub(super) replay_events: AtomicU64,
    pub(super) replay_bytes: AtomicU64,
    pub(super) replay_catch_up_ms: AtomicU64,
    pub(super) source_stale_age_ms: AtomicU64,
    pub(super) gap_count: AtomicU64,
    pub(super) snapshot_resync_count: AtomicU64,
}

impl GatewayMetrics {
    pub(super) fn record_replay(&self, events: usize, bytes: usize, elapsed: Duration) {
        self.replay_events
            .fetch_add(events as u64, Ordering::Relaxed);
        self.replay_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.replay_catch_up_ms
            .store(elapsed.as_millis() as u64, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct OutboundLanes {
    critical: VecDeque<RealtimeEnvelope>,
    state: VecDeque<RealtimeEnvelope>,
    tokens: VecDeque<RealtimeEnvelope>,
    bulk: VecDeque<RealtimeEnvelope>,
    capacities: LaneQueueConfig,
    state_coalesce: HashMap<String, usize>,
}

impl OutboundLanes {
    fn new(capacities: LaneQueueConfig) -> Self {
        Self {
            critical: VecDeque::new(),
            state: VecDeque::new(),
            tokens: VecDeque::new(),
            bulk: VecDeque::new(),
            capacities,
            state_coalesce: HashMap::new(),
        }
    }

    fn enqueue(&mut self, envelope: RealtimeEnvelope, coalesce_key: Option<String>) -> bool {
        match Lane::try_from(envelope.lane).unwrap_or(Lane::State) {
            Lane::Critical => {
                self.critical.push_back(envelope);
                self.critical.len() > self.capacities.critical_capacity
            }
            Lane::State => {
                if let Some(key) = coalesce_key {
                    if let Some(index) = self.state_coalesce.get(&key).copied() {
                        if let Some(slot) = self.state.get_mut(index) {
                            *slot = envelope;
                            return false;
                        }
                    }
                    self.state_coalesce.insert(key, self.state.len());
                }
                self.state.push_back(envelope);
                while self.state.len() > self.capacities.state_capacity {
                    self.state.pop_front();
                    self.rebuild_state_coalesce();
                    return true;
                }
                false
            }
            Lane::Tokens => {
                self.tokens.push_back(envelope);
                if self.tokens.len() > self.capacities.tokens_capacity {
                    self.tokens.pop_front();
                    return true;
                }
                false
            }
            Lane::Bulk | Lane::Unspecified => {
                self.bulk.push_back(envelope);
                if self.bulk.len() > self.capacities.bulk_capacity {
                    self.bulk.pop_front();
                    return true;
                }
                false
            }
        }
    }

    fn next(&mut self) -> Option<RealtimeEnvelope> {
        if let Some(envelope) = self.critical.pop_front() {
            return Some(envelope);
        }
        if let Some(envelope) = self.state.pop_front() {
            self.rebuild_state_coalesce();
            return Some(envelope);
        }
        if let Some(envelope) = self.tokens.pop_front() {
            return Some(envelope);
        }
        self.bulk.pop_front()
    }

    fn rebuild_state_coalesce(&mut self) {
        self.state_coalesce.clear();
        for (index, envelope) in self.state.iter().enumerate() {
            if let Some(Payload::Patch(patch)) = envelope.payload.as_ref() {
                if let Some(entity) = patch.entity.as_ref() {
                    self.state_coalesce.insert(
                        format!(
                            "{}:{}:{}",
                            patch.view_kind, entity.scope_id, entity.entity_id
                        ),
                        index,
                    );
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Subscription {
    Board(BoardSubscription),
    ApprovalInbox(ApprovalInboxSubscription),
    Session(SelectedSessionSubscription),
    Team(SelectedTeamSubscription),
    ProcessTail(ProcessTailSubscription),
    Ledger(LedgerSubscription),
    Fleet,
    Worktrees,
}

impl Subscription {
    pub(super) fn from_proto(subscribe: &Subscribe) -> Result<Self> {
        let filters = &subscribe.filters;
        Ok(match subscribe.view_kind.as_str() {
            "board" => Self::Board(BoardSubscription {
                offset: parse_usize(filters.get("offset"), 0),
                limit: parse_usize(filters.get("limit"), 100),
                status_filter: filters.get("status").cloned(),
                team_id: filters.get("team_id").cloned(),
                source_id: filters.get("source_id").cloned(),
                query: filters.get("query").cloned(),
            }),
            "approval_inbox" => Self::ApprovalInbox(ApprovalInboxSubscription {
                include_resolved: filters
                    .get("include_resolved")
                    .is_some_and(|value| value == "true"),
                session_id: filters.get("session_id").cloned(),
            }),
            "session" => Self::Session(SelectedSessionSubscription {
                session_id: required_filter(filters, "session_id")?,
                include_text: filters
                    .get("include_text")
                    .is_none_or(|value| value == "true"),
            }),
            "team" => Self::Team(SelectedTeamSubscription {
                team_id: required_filter(filters, "team_id")?,
                message_limit: parse_usize(filters.get("message_limit"), 100),
            }),
            "process_tail" => Self::ProcessTail(ProcessTailSubscription {
                process_id: required_filter(filters, "process_id")?,
                tail_lines: parse_usize(filters.get("tail_lines"), 200),
            }),
            "ledger" => Self::Ledger(LedgerSubscription {
                offset: parse_usize(filters.get("offset"), 0),
                limit: parse_usize(filters.get("limit"), 200),
                scope: filters.get("scope").cloned(),
                session_id: filters.get("session_id").cloned(),
                team_id: filters.get("team_id").cloned(),
                process_id: filters.get("process_id").cloned(),
                kind: filters.get("kind").cloned(),
                criticality: filters.get("criticality").cloned(),
            }),
            "fleet" => Self::Fleet,
            "worktrees" => Self::Worktrees,
            other => return Err(anyhow!("unknown subscription view_kind {other}")),
        })
    }

    fn matches_patch(&self, patch: &MaterializedPatch) -> bool {
        match self {
            Self::Board(_) => patch.view_kind == "board" || patch.view_kind == "session",
            Self::ApprovalInbox(_) => {
                patch.view_kind == "approval_inbox" || patch.view_kind == "approval"
            }
            Self::Session(subscription) => {
                patch.view_kind == "session"
                    && patch
                        .entity
                        .as_ref()
                        .is_some_and(|entity| entity.entity_id == subscription.session_id)
            }
            Self::Team(subscription) => {
                patch.view_kind == "team"
                    && patch
                        .entity
                        .as_ref()
                        .is_some_and(|entity| entity.entity_id == subscription.team_id)
            }
            Self::ProcessTail(subscription) => {
                patch.view_kind == "process_tail"
                    && patch
                        .entity
                        .as_ref()
                        .is_some_and(|entity| entity.entity_id == subscription.process_id)
            }
            Self::Ledger(_) => patch.view_kind == "ledger",
            Self::Fleet => patch.view_kind == "fleet" || patch.view_kind == "source_health",
            Self::Worktrees => patch.view_kind == "worktrees" || patch.view_kind == "worktree",
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct CommandIdStore {
    entries: BTreeMap<String, CommandEntry>,
}

impl CommandIdStore {
    pub(super) fn prune(&mut self, now: i64) {
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    pub(super) fn get(&self, command_id: &str) -> Option<&CommandEntry> {
        self.entries.get(command_id)
    }

    pub(super) fn insert_pending(&mut self, command_id: &str) {
        self.entries.insert(
            command_id.to_string(),
            CommandEntry {
                original_command_id: command_id.to_string(),
                disposition: CommandDisposition::Pending,
                expires_at: now_ms() + 10 * 60 * 1000,
            },
        );
    }

    pub(super) fn complete(&mut self, command_id: &str, disposition: CommandDisposition) {
        if let Some(entry) = self.entries.get_mut(command_id) {
            entry.disposition = disposition;
        }
    }
}

#[derive(Debug)]
pub(super) struct CommandEntry {
    pub(super) original_command_id: String,
    pub(super) disposition: CommandDisposition,
    expires_at: i64,
}

#[derive(Debug)]
pub(super) enum CommandDisposition {
    Pending,
    Accepted {
        gateway_seq: u64,
    },
    Rejected {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Debug)]
pub(super) struct CommandRouteError {
    pub(super) code: String,
    pub(super) message: String,
    pub(super) retryable: bool,
}

impl CommandRouteError {
    pub(super) fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            code: REASON_UPSTREAM_REJECTED.to_string(),
            message: message.into(),
            retryable: false,
        }
    }

    pub(super) fn with_code(
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }
}

impl From<RuntimeClientError> for CommandRouteError {
    fn from(value: RuntimeClientError) -> Self {
        let retryable = matches!(value, RuntimeClientError::Transport(_));
        let code = match &value {
            RuntimeClientError::Transport(error) if error.is_timeout() => REASON_UPSTREAM_TIMEOUT,
            RuntimeClientError::Transport(_) => REASON_SOURCE_UNAVAILABLE,
            RuntimeClientError::Http { status, .. }
                if *status == reqwest::StatusCode::GATEWAY_TIMEOUT
                    || *status == reqwest::StatusCode::REQUEST_TIMEOUT =>
            {
                REASON_UPSTREAM_TIMEOUT
            }
            RuntimeClientError::Http { .. }
            | RuntimeClientError::Decode(_)
            | RuntimeClientError::Json(_) => REASON_UPSTREAM_REJECTED,
        };
        Self {
            code: code.to_string(),
            message: value.to_string(),
            retryable,
        }
    }
}

pub(super) fn runtime_client_from_source(
    config: &GoosetowerConfig,
    source: &RuntimeSourceConfig,
) -> Result<GooselakeRuntimeClient> {
    let token = config.resolve_runtime_auth(source)?;
    GooselakeRuntimeClient::new(GooselakeRuntimeClientConfig::new(
        source.source_id.clone(),
        source.source_epoch.clone(),
        source.base_url.clone(),
        token,
    ))
    .map_err(Into::into)
}

pub(super) fn snapshot_body(
    state: &MaterializedState,
    view_kind: &str,
    filters: &HashMap<String, String>,
) -> Result<Value> {
    Ok(match view_kind {
        "board" => serde_json::to_value(state.snapshot_board(&BoardSubscription {
            offset: parse_usize(filters.get("offset"), 0),
            limit: parse_usize(filters.get("limit"), 100),
            status_filter: filters.get("status").cloned(),
            team_id: filters.get("team_id").cloned(),
            source_id: filters.get("source_id").cloned(),
            query: filters.get("query").cloned(),
        }))?,
        "approval_inbox" => serde_json::to_value(
            state.snapshot_approval_inbox(&ApprovalInboxSubscription {
                include_resolved: filters
                    .get("include_resolved")
                    .is_some_and(|value| value == "true"),
                session_id: filters.get("session_id").cloned(),
            }),
        )?,
        "session" => serde_json::to_value(
            state.snapshot_session(&SelectedSessionSubscription {
                session_id: required_filter(filters, "session_id")?,
                include_text: filters
                    .get("include_text")
                    .is_none_or(|value| value == "true"),
            }),
        )?,
        "team" => serde_json::to_value(state.snapshot_team(&SelectedTeamSubscription {
            team_id: required_filter(filters, "team_id")?,
            message_limit: parse_usize(filters.get("message_limit"), 100),
        }))?,
        "process_tail" => {
            serde_json::to_value(state.snapshot_process_tail(&ProcessTailSubscription {
                process_id: required_filter(filters, "process_id")?,
                tail_lines: parse_usize(filters.get("tail_lines"), 200),
            }))?
        }
        "ledger" => serde_json::to_value(state.snapshot_ledger(&LedgerSubscription {
            offset: parse_usize(filters.get("offset"), 0),
            limit: parse_usize(filters.get("limit"), 200),
            scope: filters.get("scope").cloned(),
            session_id: filters.get("session_id").cloned(),
            team_id: filters.get("team_id").cloned(),
            process_id: filters.get("process_id").cloned(),
            kind: filters.get("kind").cloned(),
            criticality: filters.get("criticality").cloned(),
        }))?,
        "fleet" | "source_health" => serde_json::to_value(state.snapshot_source_health())?,
        "worktrees" => serde_json::to_value(state.snapshot_worktrees())?,
        other => return Err(anyhow!("unknown view_kind {other}")),
    })
}

pub(super) fn envelope_with_payload(
    kind: MessageKind,
    lane: Lane,
    payload: Payload,
) -> RealtimeEnvelope {
    RealtimeEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: format!("msg_{}", now_ms()),
        message_kind: kind as i32,
        lane: lane as i32,
        observed_at_unix_ms: now_ms(),
        payload: Some(payload),
        ..RealtimeEnvelope::default()
    }
}

pub(super) fn raw_pong_envelope(bytes: Bytes) -> RealtimeEnvelope {
    let client_time_unix_ms = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_default();
    envelope_with_payload(
        MessageKind::Pong,
        Lane::Critical,
        Payload::Pong(Pong {
            client_time_unix_ms,
            server_time_unix_ms: now_ms(),
        }),
    )
}

pub(super) fn error_envelope(code: &str, message: String, retryable: bool) -> RealtimeEnvelope {
    envelope_with_payload(
        MessageKind::Error,
        Lane::Critical,
        Payload::Error(ErrorDetail {
            code: code.to_string(),
            message,
            retryable,
        }),
    )
}

pub(super) fn command_rejected(
    command_id: &str,
    code: &str,
    message: &str,
    retryable: bool,
) -> RealtimeEnvelope {
    envelope_with_payload(
        MessageKind::CommandRejected,
        Lane::Critical,
        Payload::CommandRejected(CommandRejected {
            command_id: command_id.to_string(),
            error: Some(ErrorDetail {
                code: code.to_string(),
                message: message.to_string(),
                retryable,
            }),
        }),
    )
}

pub(super) fn command_duplicate(command_id: &str, original_command_id: &str) -> RealtimeEnvelope {
    envelope_with_payload(
        MessageKind::CommandDuplicate,
        Lane::Critical,
        Payload::CommandDuplicate(CommandDuplicate {
            command_id: command_id.to_string(),
            original_command_id: original_command_id.to_string(),
        }),
    )
}

pub(super) fn non_empty<'a>(value: &'a str, field: &str) -> Result<&'a str, CommandRouteError> {
    if value.trim().is_empty() {
        Err(CommandRouteError::with_code(
            REASON_INVALID_TARGET,
            format!("{field} is required"),
            false,
        ))
    } else {
        Ok(value)
    }
}

pub(super) fn optional_string(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_string())
}

fn parse_usize(value: Option<&String>, default: usize) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn required_filter(filters: &HashMap<String, String>, key: &str) -> Result<String> {
    filters
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| anyhow!("{key} filter is required"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::materializer::{EntityKey, EntityVersion, MaterializedPatchKind};
    use crate::protocol::generated::goosetower::v1::{CommandAccepted, Patch};
    use serde_json::json;

    #[test]
    fn lane_scheduler_prioritizes_critical_and_coalesces_state() {
        let mut lanes = OutboundLanes::new(LaneQueueConfig {
            critical_capacity: 4,
            state_capacity: 4,
            tokens_capacity: 4,
            bulk_capacity: 4,
        });
        lanes.enqueue(
            envelope_with_payload(
                MessageKind::Patch,
                Lane::Bulk,
                Payload::Patch(Patch::default()),
            ),
            None,
        );
        lanes.enqueue(
            envelope_with_payload(
                MessageKind::Patch,
                Lane::State,
                Payload::Patch(Patch::default()),
            ),
            Some("row:1".to_string()),
        );
        lanes.enqueue(
            envelope_with_payload(
                MessageKind::Patch,
                Lane::State,
                Payload::Patch(Patch::default()),
            ),
            Some("row:1".to_string()),
        );
        lanes.enqueue(
            envelope_with_payload(
                MessageKind::CommandAccepted,
                Lane::Critical,
                Payload::CommandAccepted(CommandAccepted {
                    command_id: "cmd_1".to_string(),
                    gateway_seq: 1,
                }),
            ),
            None,
        );

        assert_eq!(
            Lane::try_from(lanes.next().expect("critical").lane).unwrap(),
            Lane::Critical
        );
        assert_eq!(lanes.state.len(), 1);
        assert_eq!(
            Lane::try_from(lanes.next().expect("state").lane).unwrap(),
            Lane::State
        );
        assert_eq!(
            Lane::try_from(lanes.next().expect("bulk").lane).unwrap(),
            Lane::Bulk
        );
    }

    #[test]
    fn subscription_interest_filters_matching_patches() {
        let subscription = Subscription::Session(SelectedSessionSubscription {
            session_id: "session_1".to_string(),
            include_text: true,
        });
        let matching = MaterializedPatch {
            kind: MaterializedPatchKind::EntityUpsert,
            view_kind: "session".to_string(),
            entity: Some(EntityKey::new("local", "session", "session_1")),
            version: Some(EntityVersion(1)),
            source_cursor: None,
            body: json!({}),
        };
        let other = MaterializedPatch {
            entity: Some(EntityKey::new("local", "session", "session_2")),
            ..matching.clone()
        };

        assert!(subscription.matches_patch(&matching));
        assert!(!subscription.matches_patch(&other));
    }
}
