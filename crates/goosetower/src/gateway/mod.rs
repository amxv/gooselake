use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::http::StatusCode;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use runtime_core::{ApprovalResponseInput, ProviderKind, SendTurnInput};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::auth::{now_ms, AuthContext, TicketValidator};
use crate::config::{GoosetowerConfig, RuntimeSourceConfig};
use crate::materializer::{
    snapshot_cross_source_approval_inbox, snapshot_cross_source_board,
    snapshot_cross_source_health, snapshot_cross_source_ledger, snapshot_cross_source_worktrees,
    ApprovalInboxSubscription, BoardSubscription, BootstrapOptions, LedgerSubscription,
    SelectedSessionSubscription, SelectedTeamSubscription, SourceBootstrap,
};
use crate::materializer::{MaterializedPatch, MaterializedPatchKind, MaterializedState};
use crate::protocol::generated::goosetower::v1::realtime_envelope::Payload;
use crate::protocol::generated::goosetower::v1::{
    AuthExpiring, AuthRefresh, AuthRefreshed, Command, CommandAccepted, ConnectionDegraded,
    CursorVector, EntityRef, GatewayEvent, Hello, Lane, MessageKind, Patch, Ping, Pong,
    RealtimeEnvelope, Resume, Scope, Snapshot, SourceCursor, SourceGapDetected, SourceGapFilled,
    SourceSnapshotResync, Subscribe,
};
use crate::protocol::PROTOCOL_VERSION;
use crate::runtime::client::{ProcessKillInput, ProcessStartInput};
use crate::runtime::events::{SourceEvent, SourceHealth, SourceHealthState};
use crate::runtime::{
    GooselakeRuntimeClient, RuntimeSseFanIn, RuntimeSseFanInConfig, TeamBroadcastInput,
    TeamCreateInput, TeamDirectInput, TeamJoinInput, TeamMemberSpawnInput,
};
mod commands;
mod envelopes;
mod observers;
mod socket;
mod support;
#[cfg(test)]
mod tests;
pub use self::observers::ServedFrameDebug;
pub use self::support::GatewayMetricsSnapshot;

fn materialized_state_from_source(source: &RuntimeSourceConfig) -> MaterializedState {
    let mut state = MaterializedState::new(source.source_id.clone(), source.source_epoch.clone());
    state.apply_source_config(source);
    state
}

use self::support::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayStatus {
    Starting,
    AcceptingConnections,
}

#[derive(Debug)]
pub struct GatewayState {
    config: Arc<GoosetowerConfig>,
    ticket_validator: TicketValidator,
    materialized: RwLock<BTreeMap<String, MaterializedState>>,
    command_store: Mutex<CommandIdStore>,
    replay_buffer: Mutex<GatewayReplayBuffer>,
    metrics: GatewayMetrics,
    active_connections: Mutex<BTreeMap<String, ActiveConnectionDebug>>,
    audit: Mutex<VecDeque<GatewayAuditRecord>>,
    served_frames: Mutex<VecDeque<ServedFrameDebug>>,
    next_frame_capture: AtomicU64,
    next_connection_id: AtomicU64,
    next_gateway_seq: AtomicU64,
    patches: broadcast::Sender<MaterializedPatch>,
}

impl GatewayState {
    pub fn new(config: Arc<GoosetowerConfig>) -> Result<Self> {
        let (patches, _) = broadcast::channel(1024);
        let mut materialized = BTreeMap::new();
        for source in config
            .runtimes
            .sources
            .iter()
            .filter(|source| source.enabled)
        {
            materialized.insert(
                source.source_id.clone(),
                materialized_state_from_source(source),
            );
        }
        let replay_buffer_capacity = config.materializer.event_buffer_size;
        Ok(Self {
            ticket_validator: TicketValidator::from_config(&config)?,
            config,
            materialized: RwLock::new(materialized),
            command_store: Mutex::new(CommandIdStore::default()),
            replay_buffer: Mutex::new(GatewayReplayBuffer::new(replay_buffer_capacity)),
            metrics: GatewayMetrics::default(),
            active_connections: Mutex::new(BTreeMap::new()),
            audit: Mutex::new(VecDeque::new()),
            served_frames: Mutex::new(VecDeque::new()),
            next_frame_capture: AtomicU64::new(1),
            next_connection_id: AtomicU64::new(1),
            next_gateway_seq: AtomicU64::new(1),
            patches,
        })
    }

    #[cfg(test)]
    pub(crate) fn verification_patch_receiver(&self) -> broadcast::Receiver<MaterializedPatch> {
        self.patches.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn verification_frame_for_patch(
        &self,
        patch: MaterializedPatch,
    ) -> RealtimeEnvelope {
        self.patch_envelope(patch)
    }

    #[cfg(test)]
    pub(crate) async fn verification_serve_frame(
        &self,
        connection_id: &str,
        frame: RealtimeEnvelope,
    ) -> Vec<u8> {
        let encoded = frame.encode_to_vec();
        self.record_served_envelope(connection_id, &frame).await;
        encoded
    }

    pub async fn bootstrap_enabled_sources(&self) {
        for source in self
            .config
            .runtimes
            .sources
            .iter()
            .filter(|source| source.enabled)
        {
            let result = async {
                let client = runtime_client_from_source(&self.config, source)?;
                let bootstrap =
                    SourceBootstrap::from_runtime_client(&client, BootstrapOptions::default())
                        .await
                        .map_err(|error| anyhow!(error.to_string()))?;
                let mut state = bootstrap.state;
                state.apply_source_config(source);
                self.materialized
                    .write()
                    .await
                    .insert(source.source_id.clone(), state);
                Result::<()>::Ok(())
            }
            .await;

            if let Err(error) = result {
                self.record_audit(
                    "source.bootstrap_failed",
                    Some(source.source_id.clone()),
                    json!({ "error": error.to_string() }),
                )
                .await;
                tracing::warn!(
                    source_id = %source.source_id,
                    error = %error,
                    "failed to bootstrap runtime source"
                );
            }
        }
    }

    pub async fn spawn_runtime_source_tasks(self: &Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<SourceEvent>(1024);
        let mut handles = Vec::new();
        for source in self
            .config
            .runtimes
            .sources
            .iter()
            .filter(|source| source.enabled)
        {
            let client = match runtime_client_from_source(&self.config, source) {
                Ok(client) => client,
                Err(error) => {
                    self.record_audit(
                        "source.connect_failed",
                        Some(source.source_id.clone()),
                        json!({ "error": error.to_string() }),
                    )
                    .await;
                    continue;
                }
            };
            let fan_in = RuntimeSseFanIn::new(
                client,
                RuntimeSseFanInConfig {
                    replay_page_limit: self.config.replay.max_events_per_request,
                    reconnect_delay: Duration::from_millis(250),
                    stale_after: Duration::from_millis(self.config.replay.source_stale_after_ms),
                },
            );
            let initial_cursor = self
                .materialized
                .read()
                .await
                .get(&source.source_id)
                .and_then(|state| state.source_health.last_source_seq);
            handles.push(fan_in.clone().spawn(initial_cursor, tx.clone()));

            let gateway = Arc::clone(self);
            let mut health_rx = fan_in.subscribe_health();
            handles.push(tokio::spawn(async move {
                while health_rx.changed().await.is_ok() {
                    let health = health_rx.borrow().clone();
                    gateway.update_source_health(health).await;
                }
            }));
        }
        drop(tx);

        let gateway = Arc::clone(self);
        handles.push(tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                gateway.ingest_source_event(event).await;
            }
        }));
        handles
    }

    pub async fn ingest_source_event(&self, event: SourceEvent) {
        let patches = {
            let mut materialized = self.materialized.write().await;
            let state = materialized
                .entry(event.source_id.clone())
                .or_insert_with(|| MaterializedState::new(&event.source_id, &event.source_epoch));
            if let Some(source) = self.config.runtimes.sources.iter().find(|source| {
                source.source_id == event.source_id && source.source_epoch == event.source_epoch
            }) {
                state.apply_source_config(source);
            }
            if state.source_epoch != event.source_epoch {
                state.mark_discontinuity("source epoch changed");
                vec![state.transition_source_health(
                    SourceHealthState::GapDetected,
                    Some("source epoch changed".to_string()),
                )]
            } else if state
                .source_health
                .last_source_seq
                .is_some_and(|last| event.source_seq > last + 1)
            {
                state.mark_discontinuity("source sequence gap detected");
                vec![state.transition_source_health(
                    SourceHealthState::GapDetected,
                    Some(format!(
                        "expected source seq {}, received {}",
                        state.source_health.last_source_seq.unwrap_or_default() + 1,
                        event.source_seq
                    )),
                )]
            } else {
                let ingest_lag = now_ms().saturating_sub(event.created_at);
                self.metrics
                    .event_ingest_lag_ms
                    .store(ingest_lag as u64, Ordering::Relaxed);
                let reduce_started = Instant::now();
                let effect = state.reduce_source_event(event);
                self.metrics.materializer_reduce_time_ms.store(
                    reduce_started.elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
                if effect.duplicate {
                    Vec::new()
                } else {
                    effect.patches
                }
            }
        };

        for patch in patches {
            self.publish_materialized_patch(patch).await;
        }
    }

    async fn update_source_health(&self, health: SourceHealth) {
        let patch = {
            let mut materialized = self.materialized.write().await;
            let state = materialized
                .entry(health.source_id.clone())
                .or_insert_with(|| MaterializedState::new(&health.source_id, &health.source_epoch));
            if let Some(source) = self.config.runtimes.sources.iter().find(|source| {
                source.source_id == health.source_id && source.source_epoch == health.source_epoch
            }) {
                state.apply_source_config(source);
            }
            // RuntimeSseFanIn health is observational and is delivered on a
            // separate task from source events. It must never advance the
            // materializer's authoritative reduced-event cursor, otherwise a
            // health update can race ahead of the matching event and make the
            // reducer discard that retained event as a duplicate.
            let materialized_cursor = state.source_health.last_source_seq;
            state.source_health = SourceHealth {
                last_source_seq: materialized_cursor,
                ..health.clone()
            };
            state.transition_source_health(health.state, health.last_error.clone())
        };
        if matches!(health.state, SourceHealthState::GapDetected) {
            self.metrics.gap_count.fetch_add(1, Ordering::Relaxed);
            self.record_audit(
                "source.gap",
                Some(health.source_id.clone()),
                json!({ "last_source_seq": health.last_source_seq, "error": health.last_error }),
            )
            .await;
        }
        self.publish_materialized_patch(patch).await;
    }

    #[cfg(test)]
    pub(crate) async fn verification_update_source_health(&self, health: SourceHealth) {
        self.update_source_health(health).await;
    }

    pub fn allowed_origins(&self) -> Result<Vec<String>> {
        self.config.allowed_gooseweb_origins()
    }

    pub async fn validate_ticket(
        &self,
        ticket: &str,
        origin: &str,
    ) -> Result<AuthContext, GatewayReject> {
        self.ticket_validator
            .validate_and_consume(ticket, origin)
            .await
            .map_err(|error| GatewayReject {
                status: StatusCode::UNAUTHORIZED,
                code: error.to_string(),
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolDebugSnapshot {
    pub protocol_version: u32,
    pub crate_version: String,
    pub max_message_bytes: usize,
    pub heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDebugSnapshot {
    pub source_id: String,
    pub source_epoch: String,
    pub source_kind: String,
    pub enabled: bool,
    pub display_name: String,
    pub workspace_id: String,
    pub base_url: String,
    pub health: Option<crate::runtime::events::SourceHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveConnectionDebug {
    pub connection_id: String,
    pub subject: String,
    pub workspace_id: String,
    pub status: String,
    pub connected_at_unix_ms: i64,
    pub subscriptions: Vec<String>,
    pub last_acked_gateway_seq: u64,
    pub buffered_messages: usize,
    pub backpressure_drops: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializerDebugSummary {
    pub source_id: String,
    pub source_epoch: String,
    pub status: String,
    pub source_health: crate::runtime::events::SourceHealth,
    pub sessions: usize,
    pub approvals: usize,
    pub teams: usize,
    pub processes: usize,
    pub worktrees: usize,
    pub ledger_events: usize,
    pub discontinuities: usize,
    pub recent_ledger: Vec<Value>,
    pub session_details: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayAuditRecord {
    pub observed_at_unix_ms: i64,
    pub kind: String,
    pub subject: Option<String>,
    pub details: Value,
}

fn parse_filter_usize(filters: &HashMap<String, String>, key: &str, default: usize) -> usize {
    filters
        .get(key)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn optional_subscribe_filter(filters: &HashMap<String, String>, key: &str) -> Option<String> {
    filters
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty() && *value != "all")
        .map(str::to_string)
}

fn cursor_vector_from_states(
    states: &BTreeMap<String, MaterializedState>,
    source_id: Option<&str>,
    gateway_seq: u64,
) -> Option<CursorVector> {
    let sources = states
        .values()
        .filter(|state| source_id.is_none_or(|source_id| state.source_id == source_id))
        .filter_map(|state| {
            state.cursor().map(|cursor| SourceCursor {
                source_id: cursor.source_id,
                source_epoch: cursor.source_epoch,
                source_seq: cursor.source_seq.max(0) as u64,
            })
        })
        .collect::<Vec<_>>();
    (!sources.is_empty()).then_some(CursorVector {
        gateway_seq,
        sources,
    })
}

pub struct GatewayReject {
    pub status: StatusCode,
    pub code: String,
}
