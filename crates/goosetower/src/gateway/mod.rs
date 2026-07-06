use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::http::StatusCode;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use runtime_core::{ApprovalResponseInput, SendTurnInput};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::auth::{now_ms, AuthContext, TicketValidator};
use crate::config::GoosetowerConfig;
use crate::materializer::{BootstrapOptions, SourceBootstrap};
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
use crate::runtime::events::{SourceEvent, SourceHealthState};
use crate::runtime::{
    GooselakeRuntimeClient, TeamBroadcastInput, TeamDirectInput, TeamMemberSpawnInput,
};
mod support;
pub use self::support::GatewayMetricsSnapshot;

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
                MaterializedState::new(source.source_id.clone(), source.source_epoch.clone()),
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
            next_connection_id: AtomicU64::new(1),
            next_gateway_seq: AtomicU64::new(1),
            patches,
        })
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
                self.materialized
                    .write()
                    .await
                    .insert(source.source_id.clone(), bootstrap.state);
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

    pub async fn handle_socket(self: Arc<Self>, socket: WebSocket, auth: AuthContext) {
        let connection_id = format!(
            "conn_{}",
            self.next_connection_id.fetch_add(1, Ordering::Relaxed)
        );
        self.metrics
            .connection_open_count
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        self.active_connections.lock().await.insert(
            connection_id.clone(),
            ActiveConnectionDebug {
                connection_id: connection_id.clone(),
                subject: auth.subject.clone(),
                workspace_id: auth.workspace_id.clone(),
                status: "connected".to_string(),
                connected_at_unix_ms: now_ms(),
                subscriptions: Vec::new(),
                last_acked_gateway_seq: 0,
                buffered_messages: 0,
                backpressure_drops: 0,
            },
        );
        self.record_audit(
            "connection.open",
            Some(connection_id.clone()),
            json!({ "user": auth.subject.clone(), "workspace_id": auth.workspace_id.clone() }),
        )
        .await;
        tracing::info!(
            connection_id,
            user = %auth.subject,
            workspace = %auth.workspace_id,
            "gateway audit connection.open"
        );

        let (mut sender, mut receiver) = socket.split();
        let mut conn = ConnectionState::new(
            connection_id.clone(),
            auth,
            self.config.lanes.clone(),
            self.config.websocket.max_message_bytes,
        );
        self.enqueue_connection(
            &mut conn,
            self.hello(&connection_id),
            Some("hello".to_string()),
        );
        self.enqueue_connection(
            &mut conn,
            self.audit_event("connection.open", Scope::System, ""),
            None,
        );
        if sender
            .send(Message::Binary(
                self.encode_next(&mut conn).unwrap_or_default().into(),
            ))
            .await
            .is_err()
        {
            return;
        }
        while let Some(envelope) = conn.next_outbound() {
            if sender
                .send(Message::Binary(envelope.encode_to_vec().into()))
                .await
                .is_err()
            {
                return;
            }
        }

        let mut patch_rx = self.patches.subscribe();
        let heartbeat_timeout = Duration::from_millis(
            self.config
                .websocket
                .heartbeat_interval_ms
                .saturating_mul(2),
        );

        loop {
            tokio::select! {
                biased;
                patch = patch_rx.recv() => {
                    match patch {
                        Ok(patch) if conn.patch_matches(&patch) => {
                            self.enqueue_patch(&mut conn, patch).await;
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            conn.status = ConnectionStatus::Degraded;
                            self.enqueue_connection(
                                &mut conn,
                                self.connection_degraded("gateway replay buffer lagged"),
                                Some("connection_status".to_string()),
                            );
                            self.enqueue_connection(&mut conn, self.audit_event("source.gap", Scope::Source, ""), None);
                            self.record_audit(
                                "source.gap",
                                Some(conn.connection_id.clone()),
                                json!({ "reason": "gateway replay buffer lagged" }),
                            ).await;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                next = tokio::time::timeout(heartbeat_timeout, receiver.next()) => {
                    let message = match next {
                        Ok(Some(Ok(message))) => message,
                        Ok(Some(Err(_))) | Ok(None) => break,
                        Err(_) => {
                            let _ = sender.send(Message::Close(Some(CloseFrame {
                                code: 4000,
                                reason: "heartbeat timeout".into(),
                            }))).await;
                            break;
                        }
                    };
                    match self.handle_inbound_message(message, &mut conn).await {
                        Ok(Continue::Yes) => {}
                        Ok(Continue::No) => break,
                        Err(error) => {
                            self.enqueue_connection(
                                &mut conn,
                                error_envelope("protocol_error", error.to_string(), false),
                                None,
                            );
                        }
                    }
                }
            }

            while let Some(envelope) = conn.next_outbound() {
                if sender
                    .send(Message::Binary(envelope.encode_to_vec().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            self.refresh_connection_debug(&conn).await;
        }

        tracing::info!(
            connection_id,
            user = %conn.auth.subject,
            workspace = %conn.auth.workspace_id,
            "gateway audit connection.close"
        );
        self.record_audit(
            "connection.close",
            Some(conn.connection_id.clone()),
            json!({ "user": conn.auth.subject, "workspace_id": conn.auth.workspace_id }),
        )
        .await;
        self.metrics
            .connection_close_count
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .active_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
        self.active_connections
            .lock()
            .await
            .remove(&conn.connection_id);
        conn.status = ConnectionStatus::Offline;
    }

    async fn handle_inbound_message(
        &self,
        message: Message,
        conn: &mut ConnectionState,
    ) -> Result<Continue> {
        let bytes = match message {
            Message::Binary(bytes) => bytes,
            Message::Ping(bytes) => {
                self.enqueue_connection(conn, raw_pong_envelope(bytes), None);
                return Ok(Continue::Yes);
            }
            Message::Pong(_) => return Ok(Continue::Yes),
            Message::Close(_) => return Ok(Continue::No),
            Message::Text(_) => return Err(anyhow!("text frames are not accepted")),
        };
        if bytes.len() > conn.max_message_bytes {
            return Err(anyhow!("message exceeds max size"));
        }
        let envelope = RealtimeEnvelope::decode(bytes.as_ref())?;
        if envelope.protocol_version != PROTOCOL_VERSION {
            return Err(anyhow!("unsupported protocol version"));
        }
        match envelope.payload {
            Some(Payload::Ping(ping)) => {
                let pong = self.pong(ping);
                if let Some(Payload::Pong(pong_payload)) = pong.payload.as_ref() {
                    let rtt = now_ms().saturating_sub(pong_payload.client_time_unix_ms);
                    self.metrics
                        .browser_rtt_ms
                        .store(rtt as u64, Ordering::Relaxed);
                }
                self.enqueue_connection(conn, pong, Some("pong".to_string()));
            }
            Some(Payload::Ack(ack)) => {
                conn.last_acked_gateway_seq = conn.last_acked_gateway_seq.max(ack.gateway_seq);
            }
            Some(Payload::Resume(resume)) => {
                self.handle_resume(conn, resume).await?;
            }
            Some(Payload::Subscribe(subscribe)) => {
                let snapshot = self.subscribe(conn, subscribe).await?;
                self.enqueue_connection(conn, snapshot, None);
            }
            Some(Payload::Unsubscribe(unsubscribe)) => {
                let subscription_id = unsubscribe.subscription_id.clone();
                conn.unsubscribe(unsubscribe);
                self.enqueue_connection(
                    conn,
                    self.audit_event("subscribe.removed", Scope::System, ""),
                    None,
                );
                self.refresh_connection_debug(conn).await;
                self.record_audit(
                    "subscribe.removed",
                    Some(conn.connection_id.clone()),
                    json!({ "subscription_id": subscription_id }),
                )
                .await;
            }
            Some(Payload::AuthRefresh(AuthRefresh { ticket })) => {
                let origin = conn
                    .auth
                    .allowed_origins
                    .first()
                    .cloned()
                    .ok_or_else(|| anyhow!("missing origin for auth refresh"))?;
                conn.auth = self
                    .ticket_validator
                    .validate_and_consume(ticket.as_str(), origin.as_str())
                    .await
                    .map_err(|error| anyhow!(error.to_string()))?;
                self.enqueue_connection(
                    conn,
                    envelope_with_payload(
                        MessageKind::AuthRefreshed,
                        Lane::Critical,
                        Payload::AuthRefreshed(AuthRefreshed {
                            expires_at_unix_ms: conn.auth.expires_at_unix_ms,
                        }),
                    ),
                    None,
                );
                self.enqueue_connection(
                    conn,
                    self.audit_event("auth.refresh", Scope::System, ""),
                    None,
                );
                self.record_audit(
                    "auth.refresh",
                    Some(conn.connection_id.clone()),
                    json!({ "subject": conn.auth.subject }),
                )
                .await;
            }
            Some(Payload::Command(command)) => {
                let response = self.admit_and_route_command(conn, command).await;
                self.enqueue_connection(conn, response, None);
            }
            _ => {
                return Err(anyhow!("unsupported client message kind"));
            }
        }

        if conn.auth.expires_at_unix_ms - now_ms() < 15_000 {
            self.enqueue_connection(
                conn,
                envelope_with_payload(
                    MessageKind::AuthExpiring,
                    Lane::Critical,
                    Payload::AuthExpiring(AuthExpiring {
                        expires_at_unix_ms: conn.auth.expires_at_unix_ms,
                    }),
                ),
                Some("auth_expiring".to_string()),
            );
        }

        Ok(Continue::Yes)
    }

    async fn handle_resume(&self, conn: &mut ConnectionState, resume: Resume) -> Result<()> {
        let started = Instant::now();
        let Some(cursor) = resume.cursor else {
            self.metrics.resume_rejected.fetch_add(1, Ordering::Relaxed);
            self.enqueue_connection(
                conn,
                error_envelope(
                    "resume_rejected",
                    "resume cursor is required".to_string(),
                    false,
                ),
                None,
            );
            return Ok(());
        };

        conn.status = ConnectionStatus::Reconnecting;
        conn.cursor = Some(cursor.clone());
        conn.last_acked_gateway_seq = conn.last_acked_gateway_seq.max(cursor.gateway_seq);
        for subscribe in resume.active_subscriptions {
            let subscription = Subscription::from_proto(&subscribe)?;
            conn.subscriptions
                .insert(subscribe.subscription_id.clone(), subscription);
        }

        conn.status = ConnectionStatus::Replaying;
        self.enqueue_connection(
            conn,
            self.connection_degraded(format!(
                "{}: resume from {}",
                conn.status.as_str(),
                resume.previous_connection_id
            )),
            Some("connection_status".to_string()),
        );

        let window = self
            .replay_buffer
            .lock()
            .await
            .replay_after(cursor.gateway_seq);
        let mut replayed_events = 0usize;
        let mut replayed_bytes = 0usize;
        for entry in &window.entries {
            if replay_entry_matches(conn, entry) {
                self.enqueue_connection(conn, entry.envelope.clone(), None);
                replayed_events += 1;
                replayed_bytes += entry.encoded_len;
            }
        }

        if window.complete {
            self.metrics.resume_success.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .record_replay(replayed_events, replayed_bytes, started.elapsed());
            conn.status = ConnectionStatus::Connected;
            for source in &cursor.sources {
                self.enqueue_connection(conn, self.source_gap_filled(source.clone()), None);
            }
            return Ok(());
        }

        self.metrics.resume_partial.fetch_add(1, Ordering::Relaxed);
        match self.replay_sources(conn, &cursor).await {
            Ok(source_replay) => {
                replayed_events += source_replay.events;
                replayed_bytes += source_replay.bytes;
                self.metrics
                    .record_replay(replayed_events, replayed_bytes, started.elapsed());
                conn.status = ConnectionStatus::Connected;
                for source in source_replay.filled {
                    self.enqueue_connection(conn, self.source_gap_filled(source), None);
                }
            }
            Err(gap) => {
                self.metrics.gap_count.fetch_add(1, Ordering::Relaxed);
                conn.status = ConnectionStatus::Stale;
                self.enqueue_connection(
                    conn,
                    self.source_gap_detected(gap.last_seen.clone(), gap.next_available.clone()),
                    None,
                );
                self.snapshot_resync(conn, &gap.source_id, gap.reason)
                    .await?;
            }
        }
        Ok(())
    }

    async fn replay_sources(
        &self,
        conn: &mut ConnectionState,
        cursor: &CursorVector,
    ) -> Result<SourceReplayOutcome, ResumeGap> {
        let mut outcome = SourceReplayOutcome::default();
        for source_cursor in &cursor.sources {
            let source = self
                .config
                .runtimes
                .sources
                .iter()
                .find(|candidate| {
                    candidate.enabled
                        && candidate.source_id == source_cursor.source_id
                        && candidate.workspace_id == conn.auth.workspace_id
                })
                .ok_or_else(|| ResumeGap::new(source_cursor, None, "source unavailable"))?;

            if source.source_epoch != source_cursor.source_epoch {
                return Err(ResumeGap::new(source_cursor, None, "source epoch changed"));
            }

            let client = runtime_client_from_source(&self.config, source)
                .map_err(|error| ResumeGap::new(source_cursor, None, error.to_string()))?;
            let after_seq = source_cursor.source_seq as i64;
            let mut cursor_seq = after_seq;
            let mut source_events = Vec::new();
            loop {
                let page = client
                    .replay_global_events(
                        Some(cursor_seq),
                        Some(self.config.replay.max_events_per_request),
                    )
                    .await
                    .map_err(|error| ResumeGap::new(source_cursor, None, error.to_string()))?;
                if page.is_empty() {
                    break;
                }
                let first = page.first().map(|event| event.row_id).unwrap_or(cursor_seq);
                if first > cursor_seq + 1 {
                    return Err(ResumeGap::new(
                        source_cursor,
                        Some(SourceCursor {
                            source_id: source_cursor.source_id.clone(),
                            source_epoch: source_cursor.source_epoch.clone(),
                            source_seq: first as u64,
                        }),
                        "source replay cannot fill requested range",
                    ));
                }
                let page_len = page.len();
                for runtime_event in page {
                    cursor_seq = cursor_seq.max(runtime_event.row_id);
                    source_events.push(SourceEvent::from_runtime_event(
                        source.source_id.clone(),
                        source.source_epoch.clone(),
                        runtime_event,
                    ));
                }
                if page_len < self.config.replay.max_events_per_request {
                    break;
                }
            }

            let patches = {
                let mut materialized = self.materialized.write().await;
                let state = materialized
                    .entry(source.source_id.clone())
                    .or_insert_with(|| {
                        MaterializedState::new(&source.source_id, &source.source_epoch)
                    });
                let mut patches = Vec::new();
                for event in source_events {
                    let ingest_lag = now_ms().saturating_sub(event.created_at);
                    self.metrics
                        .event_ingest_lag_ms
                        .store(ingest_lag as u64, Ordering::Relaxed);
                    if state
                        .source_health
                        .last_source_seq
                        .is_some_and(|last| event.source_seq > last + 1)
                    {
                        return Err(ResumeGap::new(
                            source_cursor,
                            Some(SourceCursor {
                                source_id: event.source_id.clone(),
                                source_epoch: event.source_epoch.clone(),
                                source_seq: event.source_seq as u64,
                            }),
                            "source sequence jumped during replay",
                        ));
                    }
                    let reduce_started = Instant::now();
                    let effect = state.reduce_source_event(event);
                    self.metrics.materializer_reduce_time_ms.store(
                        reduce_started.elapsed().as_millis() as u64,
                        Ordering::Relaxed,
                    );
                    if !effect.duplicate {
                        patches.extend(effect.patches);
                    }
                }
                patches
            };
            for patch in patches {
                if conn.patch_matches(&patch) {
                    let envelope = self.patch_envelope(patch.clone());
                    let replay_entry = self.record_replayable(envelope).await;
                    outcome.events += 1;
                    outcome.bytes += replay_entry.encoded_len;
                    self.enqueue_connection(conn, replay_entry.envelope, None);
                }
                let _ = self.patches.send(patch);
            }
            outcome.filled.push(SourceCursor {
                source_id: source.source_id.clone(),
                source_epoch: source.source_epoch.clone(),
                source_seq: cursor_seq.max(0) as u64,
            });
        }
        Ok(outcome)
    }

    async fn snapshot_resync(
        &self,
        conn: &mut ConnectionState,
        source_id: &str,
        reason: String,
    ) -> Result<()> {
        let source = self
            .config
            .runtimes
            .sources
            .iter()
            .find(|candidate| candidate.enabled && candidate.source_id == source_id)
            .ok_or_else(|| anyhow!("source unavailable for snapshot resync"))?;
        let client = runtime_client_from_source(&self.config, source)?;
        let bootstrap = SourceBootstrap::from_runtime_client(&client, BootstrapOptions::default())
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        let mut state = bootstrap.state;
        state.mark_discontinuity(reason.clone());
        let patch = state.transition_source_health(SourceHealthState::Live, None);
        self.materialized
            .write()
            .await
            .insert(source_id.to_string(), state);
        self.metrics
            .snapshot_resync_count
            .fetch_add(1, Ordering::Relaxed);
        let event = self.source_snapshot_resync(source_id, &reason);
        self.enqueue_connection(conn, event, None);
        self.enqueue_patch(conn, patch.clone()).await;
        let _ = self.patches.send(patch);
        Ok(())
    }

    async fn subscribe(
        &self,
        conn: &mut ConnectionState,
        subscribe: Subscribe,
    ) -> Result<RealtimeEnvelope> {
        let subscription = Subscription::from_proto(&subscribe)?;
        conn.subscriptions
            .insert(subscribe.subscription_id.clone(), subscription);
        tracing::info!(
            connection_id = %conn.connection_id,
            subscription_id = %subscribe.subscription_id,
            view_kind = %subscribe.view_kind,
            "gateway audit subscribe.changed"
        );
        self.refresh_connection_debug(conn).await;
        self.record_audit(
            "subscribe.changed",
            Some(conn.connection_id.clone()),
            json!({
                "subscription_id": subscribe.subscription_id.clone(),
                "view_kind": subscribe.view_kind.clone(),
            }),
        )
        .await;
        Ok(self.snapshot_for_subscription(subscribe).await)
    }

    async fn snapshot_for_subscription(&self, subscribe: Subscribe) -> RealtimeEnvelope {
        let materialized = self.materialized.read().await;
        let source_id = subscribe
            .filters
            .get("source_id")
            .cloned()
            .or_else(|| materialized.keys().next().cloned());
        let body = match source_id
            .as_deref()
            .and_then(|source_id| materialized.get(source_id))
        {
            Some(state) => snapshot_body(state, &subscribe.view_kind, &subscribe.filters)
                .unwrap_or_else(|error| json!({ "error": error.to_string() })),
            None => Value::Null,
        };
        let cursor = materialized
            .values()
            .next()
            .and_then(|state| state.cursor())
            .map(|cursor| CursorVector {
                gateway_seq: self.next_gateway_seq.load(Ordering::Relaxed),
                sources: vec![SourceCursor {
                    source_id: cursor.source_id,
                    source_epoch: cursor.source_epoch,
                    source_seq: cursor.source_seq.max(0) as u64,
                }],
            });
        envelope_with_payload(
            MessageKind::Snapshot,
            Lane::State,
            Payload::Snapshot(Snapshot {
                view_kind: subscribe.view_kind,
                cursor,
                body: serde_json::to_vec(&body).unwrap_or_default().into(),
            }),
        )
    }

    async fn admit_and_route_command(
        &self,
        conn: &mut ConnectionState,
        command: Command,
    ) -> RealtimeEnvelope {
        let admission_started = Instant::now();
        if command.command_id.trim().is_empty() {
            self.metrics
                .command_rejected_count
                .fetch_add(1, Ordering::Relaxed);
            return command_rejected("", "missing_command_id", "command_id is required", false);
        }
        if command.created_at_client_unix_ms <= 0 {
            self.metrics
                .command_rejected_count
                .fetch_add(1, Ordering::Relaxed);
            return command_rejected(
                &command.command_id,
                REASON_INVALID_TARGET,
                "created_at_client_unix_ms is required",
                false,
            );
        }
        if !conn.auth.has_scope("gateway:command") {
            self.metrics
                .command_rejected_count
                .fetch_add(1, Ordering::Relaxed);
            return command_rejected(
                &command.command_id,
                REASON_UNAUTHORIZED,
                "ticket does not include gateway:command scope",
                false,
            );
        }

        let payload_kind = command_payload_kind(&command);
        let (target_scope, target_scope_id, target_entity_id) = command_target_labels(&command);
        {
            let mut store = self.command_store.lock().await;
            store.prune(now_ms());
            if let Some(existing) = store.get(&command.command_id) {
                match &existing.disposition {
                    CommandDisposition::Pending => {
                        tracing::debug!(command_id = %command.command_id, "duplicate command is still pending");
                    }
                    CommandDisposition::Accepted { gateway_seq } => {
                        tracing::debug!(command_id = %command.command_id, gateway_seq, "duplicate command was accepted");
                    }
                    CommandDisposition::Rejected {
                        code,
                        message,
                        retryable,
                    } => {
                        tracing::debug!(
                            command_id = %command.command_id,
                            code,
                            message,
                            retryable,
                            "duplicate command was rejected"
                        );
                    }
                }
                tracing::info!(
                    command_id = %command.command_id,
                    reason = REASON_DUPLICATE,
                    payload_kind = payload_kind,
                    target_scope = target_scope,
                    target_scope_id = target_scope_id,
                    target_entity_id = target_entity_id,
                    "gateway audit command.duplicate"
                );
                return command_duplicate(&command.command_id, &existing.original_command_id);
            }
            store.insert_pending(&command.command_id);
        }

        let upstream_started = Instant::now();
        let result = self.route_command(conn, &command).await;
        self.metrics.upstream_command_latency_ms.store(
            upstream_started.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        let mut store = self.command_store.lock().await;
        match result {
            Ok(()) => {
                let gateway_seq = self.next_gateway_seq.fetch_add(1, Ordering::Relaxed);
                store.complete(
                    &command.command_id,
                    CommandDisposition::Accepted { gateway_seq },
                );
                self.metrics
                    .command_accepted_count
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.command_admission_latency_ms.store(
                    admission_started.elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
                tracing::info!(
                    command_id = %command.command_id,
                    payload_kind = payload_kind,
                    target_scope = target_scope,
                    target_scope_id = target_scope_id,
                    target_entity_id = target_entity_id,
                    gateway_seq,
                    "gateway audit command.accepted"
                );
                envelope_with_payload(
                    MessageKind::CommandAccepted,
                    Lane::Critical,
                    Payload::CommandAccepted(CommandAccepted {
                        command_id: command.command_id,
                        gateway_seq,
                    }),
                )
            }
            Err(error) => {
                store.complete(
                    &command.command_id,
                    CommandDisposition::Rejected {
                        code: error.code.clone(),
                        message: error.message.clone(),
                        retryable: error.retryable,
                    },
                );
                self.metrics
                    .command_rejected_count
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.command_admission_latency_ms.store(
                    admission_started.elapsed().as_millis() as u64,
                    Ordering::Relaxed,
                );
                tracing::info!(
                    command_id = %command.command_id,
                    reason = %error.code,
                    retryable = error.retryable,
                    payload_kind = payload_kind,
                    target_scope = target_scope,
                    target_scope_id = target_scope_id,
                    target_entity_id = target_entity_id,
                    "gateway audit command.rejected"
                );
                command_rejected(
                    &command.command_id,
                    &error.code,
                    &error.message,
                    error.retryable,
                )
            }
        }
    }

    async fn route_command(
        &self,
        conn: &ConnectionState,
        command: &Command,
    ) -> Result<(), CommandRouteError> {
        let client = self
            .runtime_client_for_command(conn, command)
            .await
            .map_err(|error| {
                let message = error.to_string();
                match message.as_str() {
                    REASON_SOURCE_UNAVAILABLE | REASON_SOURCE_STALE | REASON_SOURCE_GAP => {
                        CommandRouteError::with_code(message.clone(), message, true)
                    }
                    "ownership_unknown" => CommandRouteError::with_code(
                        REASON_INVALID_TARGET,
                        "target source is not materialized",
                        true,
                    ),
                    _ => CommandRouteError::non_retryable(message),
                }
            })?;
        let Some(payload) = command.payload.as_ref() else {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_SCOPE,
                "missing command payload",
                false,
            ));
        };
        self.validate_command_target(command, payload)?;
        match payload {
            crate::protocol::generated::goosetower::v1::command::Payload::SendTurn(input) => {
                let session_id = non_empty(&input.session_id, "session_id")?;
                client
                    .send_turn(
                        session_id,
                        &SendTurnInput {
                            input: vec![json!({ "type": "text", "text": input.text })],
                            expected_turn_id: None,
                            permission_mode: None,
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::ResolveApproval(
                input,
            ) => {
                let approval_id = non_empty(&input.approval_id, "approval_id")?;
                let session_id = command
                    .target
                    .as_ref()
                    .map(|target| target.scope_id.as_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CommandRouteError::with_code(
                            REASON_INVALID_TARGET,
                            "target.scope_id session_id is required",
                            false,
                        )
                    })?;
                client
                    .respond_approval(
                        session_id,
                        approval_id,
                        &ApprovalResponseInput {
                            decision: if input.approved { "accept" } else { "reject" }.to_string(),
                            payload: Some(json!({ "reason": input.reason })),
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::InterruptTurn(input) => {
                client
                    .interrupt_turn(
                        non_empty(&input.session_id, "session_id")?,
                        non_empty(&input.turn_id, "turn_id")?,
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::SendTeamMessage(
                input,
            ) => {
                client
                    .send_team_direct(
                        non_empty(&input.team_id, "team_id")?,
                        &TeamDirectInput {
                            sender_agent_id: conn.auth.subject.clone(),
                            recipient_agent_id: non_empty(
                                &input.recipient_member_id,
                                "recipient_member_id",
                            )?
                            .to_string(),
                            input: json!({ "text": input.text }),
                            image_paths: None,
                            priority: Some("normal".to_string()),
                            policy: Some("non_interrupting".to_string()),
                            correlation_id: Some(command.command_id.clone()),
                            reply_to_message_id: None,
                            idempotency_key: Some(command.command_id.clone()),
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::BroadcastTeamMessage(
                input,
            ) => {
                client
                    .send_team_broadcast(
                        non_empty(&input.team_id, "team_id")?,
                        &TeamBroadcastInput {
                            sender_agent_id: conn.auth.subject.clone(),
                            input: json!({ "text": input.text }),
                            image_paths: None,
                            priority: Some("normal".to_string()),
                            policy: Some("non_interrupting".to_string()),
                            include_sender: Some(false),
                            correlation_id: Some(command.command_id.clone()),
                            idempotency_key: Some(command.command_id.clone()),
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::SpawnTeamMember(
                input,
            ) => {
                let source_session_id = command
                    .target
                    .as_ref()
                    .map(|target| target.entity_id.as_str())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(conn.auth.subject.as_str());
                client
                    .spawn_team_member(
                        non_empty(&input.team_id, "team_id")?,
                        &TeamMemberSpawnInput {
                            source_session_id: source_session_id.to_string(),
                            provider: None,
                            model: if input.model_preset.is_empty() {
                                None
                            } else {
                                Some(input.model_preset.clone())
                            },
                            title: optional_string(&input.title),
                            prompt: optional_string(&input.prompt),
                            permission_mode: None,
                            metadata: None,
                            worktree: None,
                            creator_agent_id: Some(conn.auth.subject.clone()),
                            creator_compaction_subscription: None,
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::RetryDelivery(input) => {
                let team_id = command
                    .target
                    .as_ref()
                    .map(|target| target.scope_id.as_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CommandRouteError::with_code(
                            REASON_INVALID_TARGET,
                            "target.scope_id team_id is required",
                            false,
                        )
                    })?;
                client
                    .retry_team_delivery(team_id, non_empty(&input.delivery_id, "delivery_id")?)
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::CancelDelivery(input) => {
                let team_id = command
                    .target
                    .as_ref()
                    .map(|target| target.scope_id.as_str())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CommandRouteError::with_code(
                            REASON_INVALID_TARGET,
                            "target.scope_id team_id is required",
                            false,
                        )
                    })?;
                client
                    .cancel_team_message(team_id, non_empty(&input.message_id, "message_id")?)
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::KillProcess(input) => {
                client
                    .kill_process(
                        non_empty(&input.process_id, "process_id")?,
                        &ProcessKillInput {
                            session_id: command
                                .target
                                .as_ref()
                                .map(|target| target.scope_id.clone())
                                .filter(|value| !value.is_empty()),
                            reason: Some(format!("goosetower command {}", command.command_id)),
                        },
                    )
                    .await?;
            }
            crate::protocol::generated::goosetower::v1::command::Payload::StartProcess(input) => {
                client
                    .start_process(&ProcessStartInput {
                        command: non_empty(&input.command, "command")?.to_string(),
                        cwd: optional_string(&input.cwd),
                        timeout_ms: (input.timeout_ms > 0).then_some(input.timeout_ms),
                        session_id: command
                            .target
                            .as_ref()
                            .map(|target| target.scope_id.clone())
                            .filter(|value| !value.is_empty()),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    fn validate_command_target(
        &self,
        command: &Command,
        payload: &crate::protocol::generated::goosetower::v1::command::Payload,
    ) -> Result<(), CommandRouteError> {
        let Some(target) = command.target.as_ref() else {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                "command target is required",
                false,
            ));
        };
        let target_scope = Scope::try_from(target.scope).unwrap_or(Scope::Unspecified);
        let expected_scope = expected_scope_for_payload(payload);
        if target_scope != expected_scope {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_SCOPE,
                format!(
                    "command target scope must be {:?} for {}",
                    expected_scope,
                    command_payload_kind(command)
                ),
                false,
            ));
        }
        self.validate_entity_version(command, target, expected_scope)
    }

    fn validate_entity_version(
        &self,
        command: &Command,
        target: &EntityRef,
        target_scope: Scope,
    ) -> Result<(), CommandRouteError> {
        let expected = command.base_entity_version.max(target.entity_version);
        if expected == 0 {
            return Ok(());
        }
        let Some(entity_kind) = materialized_entity_kind_for_scope(target_scope) else {
            return Ok(());
        };
        let entity_id = if target.entity_id.starts_with("source:") || target.entity_id.is_empty() {
            target.scope_id.as_str()
        } else {
            target.entity_id.as_str()
        };
        if entity_id.is_empty() {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                "target entity id is required",
                false,
            ));
        }
        let source_id = target.entity_id.strip_prefix("source:").or_else(|| {
            command
                .target
                .as_ref()
                .and_then(|target| target.entity_id.strip_prefix("source:"))
        });
        let materialized = self.materialized.try_read().map_err(|_| {
            CommandRouteError::with_code(REASON_SOURCE_STALE, "source state is busy", true)
        })?;
        let Some((_, state)) = materialized.iter().find(|(candidate_source_id, _)| {
            source_id.is_none_or(|source_id| candidate_source_id.as_str() == source_id)
        }) else {
            return Err(CommandRouteError::with_code(
                REASON_INVALID_TARGET,
                "target source is not materialized",
                true,
            ));
        };
        let actual = state.version(entity_kind, entity_id).0;
        if actual > 0 && actual != expected {
            return Err(CommandRouteError::with_code(
                REASON_STALE_ENTITY_VERSION,
                format!(
                    "stale {entity_kind} version for {entity_id}: expected {expected}, current {actual}"
                ),
                true,
            ));
        }
        Ok(())
    }

    async fn runtime_client_for_command(
        &self,
        conn: &ConnectionState,
        command: &Command,
    ) -> Result<GooselakeRuntimeClient> {
        let source_id = command
            .target
            .as_ref()
            .map(|target| target.entity_id.as_str())
            .filter(|value| value.starts_with("source:"))
            .map(|value| value.trim_start_matches("source:"));
        let source = self
            .config
            .runtimes
            .sources
            .iter()
            .find(|candidate| {
                candidate.enabled
                    && candidate.workspace_id == conn.auth.workspace_id
                    && source_id.is_none_or(|source_id| candidate.source_id == source_id)
            })
            .or_else(|| {
                self.config
                    .runtimes
                    .sources
                    .iter()
                    .find(|candidate| candidate.enabled)
            })
            .ok_or_else(|| anyhow!("source_unavailable"))?;
        let materialized = self.materialized.read().await;
        let state = materialized
            .get(source.source_id.as_str())
            .ok_or_else(|| anyhow!("ownership_unknown"))?;
        let stale_age = now_ms().saturating_sub(state.source_health.updated_at) as u64;
        self.metrics
            .source_stale_age_ms
            .store(stale_age, Ordering::Relaxed);
        let stale_after = self.config.replay.source_stale_after_ms;
        match state.source_health.state {
            SourceHealthState::Live if stale_age <= stale_after => {}
            SourceHealthState::GapDetected => {
                return Err(anyhow!("source_gap"));
            }
            SourceHealthState::Offline => {
                return Err(anyhow!("source_unavailable"));
            }
            _ => {
                return Err(anyhow!("source_stale"));
            }
        }
        drop(materialized);
        runtime_client_from_source(&self.config, source)
    }

    fn hello(&self, connection_id: &str) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::Hello,
            Lane::Critical,
            Payload::Hello(Hello {
                connection_id: connection_id.to_string(),
                server_time_unix_ms: now_ms(),
                heartbeat_interval_ms: self.config.websocket.heartbeat_interval_ms as u32,
                max_message_bytes: self.config.websocket.max_message_bytes as u32,
                protocol_version: PROTOCOL_VERSION,
                resume_supported: true,
            }),
        )
    }

    fn pong(&self, ping: Ping) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::Pong,
            Lane::Critical,
            Payload::Pong(Pong {
                client_time_unix_ms: ping.client_time_unix_ms,
                server_time_unix_ms: now_ms(),
            }),
        )
    }

    fn patch_envelope(&self, patch: MaterializedPatch) -> RealtimeEnvelope {
        let lane = match patch.kind {
            MaterializedPatchKind::TextAppend => Lane::Tokens,
            MaterializedPatchKind::LogAppend | MaterializedPatchKind::LogSample => Lane::Bulk,
            MaterializedPatchKind::SourceHealthTransition => Lane::Critical,
            _ => Lane::State,
        };
        envelope_with_payload(
            MessageKind::Patch,
            lane,
            Payload::Patch(Patch {
                view_kind: patch.view_kind,
                entity: patch.entity.map(|entity| {
                    crate::protocol::generated::goosetower::v1::EntityRef {
                        scope: Scope::Unspecified as i32,
                        scope_id: entity.entity_kind,
                        entity_id: entity.entity_id,
                        entity_version: patch.version.map(|version| version.0).unwrap_or_default(),
                    }
                }),
                cursor: patch.source_cursor.map(|cursor| CursorVector {
                    gateway_seq: self.next_gateway_seq.load(Ordering::Relaxed),
                    sources: vec![SourceCursor {
                        source_id: cursor.source_id,
                        source_epoch: cursor.source_epoch,
                        source_seq: cursor.source_seq.max(0) as u64,
                    }],
                }),
                body: serde_json::to_vec(&patch.body).unwrap_or_default().into(),
            }),
        )
    }

    async fn enqueue_patch(&self, conn: &mut ConnectionState, patch: MaterializedPatch) {
        let envelope = self.patch_envelope(patch);
        let entry = self.record_replayable(envelope).await;
        self.enqueue_connection(conn, entry.envelope, None);
    }

    async fn record_replayable(&self, mut envelope: RealtimeEnvelope) -> ReplayEntry {
        let gateway_seq = self.next_gateway_seq.fetch_add(1, Ordering::Relaxed);
        envelope.gateway_seq = gateway_seq;
        let source_cursor = envelope.payload.as_ref().and_then(|payload| match payload {
            Payload::Patch(patch) => patch
                .cursor
                .as_ref()
                .and_then(|cursor| cursor.sources.first().cloned()),
            Payload::SourceGapFilled(filled) => filled.cursor.clone(),
            Payload::SourceGapDetected(detected) => detected.last_seen.clone(),
            _ => None,
        });
        let encoded_len = envelope.encode_to_vec().len();
        let entry = ReplayEntry {
            gateway_seq,
            source_cursor,
            envelope,
            encoded_len,
        };
        self.replay_buffer.lock().await.push(entry.clone());
        entry
    }

    fn connection_degraded(&self, reason: impl Into<String>) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::ConnectionDegraded,
            Lane::Critical,
            Payload::ConnectionDegraded(ConnectionDegraded {
                reason: reason.into(),
            }),
        )
    }

    fn source_gap_detected(
        &self,
        last_seen: SourceCursor,
        next_available: Option<SourceCursor>,
    ) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::SourceGapDetected,
            Lane::Critical,
            Payload::SourceGapDetected(SourceGapDetected {
                last_seen: Some(last_seen),
                next_available,
            }),
        )
    }

    fn source_gap_filled(&self, cursor: SourceCursor) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::SourceGapFilled,
            Lane::Critical,
            Payload::SourceGapFilled(SourceGapFilled {
                cursor: Some(cursor),
            }),
        )
    }

    fn source_snapshot_resync(&self, source_id: &str, reason: &str) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::SourceSnapshotResync,
            Lane::Critical,
            Payload::SourceSnapshotResync(SourceSnapshotResync {
                source_id: source_id.to_string(),
                reason: reason.to_string(),
            }),
        )
    }

    fn audit_event(&self, kind: &str, scope: Scope, scope_id: &str) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::Event,
            Lane::Critical,
            Payload::Event(GatewayEvent {
                event_id: format!("gateway_{}", now_ms()),
                kind: kind.to_string(),
                scope: scope as i32,
                scope_id: scope_id.to_string(),
                source_cursor: None,
                criticality: "critical".to_string(),
                payload_json: serde_json::to_vec(&json!({ "kind": kind })).unwrap_or_default(),
            }),
        )
    }

    fn enqueue_connection(
        &self,
        conn: &mut ConnectionState,
        envelope: RealtimeEnvelope,
        coalesce_key: Option<String>,
    ) {
        let outcome = conn.enqueue(envelope, coalesce_key);
        self.metrics.record_outbound(outcome);
    }

    async fn refresh_connection_debug(&self, conn: &ConnectionState) {
        if let Some(active) = self
            .active_connections
            .lock()
            .await
            .get_mut(&conn.connection_id)
        {
            active.status = conn.status.as_str().to_string();
            active.subscriptions = conn.subscriptions.keys().cloned().collect();
            active.last_acked_gateway_seq = conn.last_acked_gateway_seq;
            active.buffered_messages = conn.buffered_messages();
            active.backpressure_drops = conn.backpressure_drops();
        }
    }

    async fn record_audit(&self, kind: &str, subject: Option<String>, details: Value) {
        let mut audit = self.audit.lock().await;
        audit.push_back(GatewayAuditRecord {
            observed_at_unix_ms: now_ms(),
            kind: kind.to_string(),
            subject,
            details,
        });
        while audit.len() > 200 {
            audit.pop_front();
        }
    }

    pub async fn debug_protocol_version(&self) -> ProtocolDebugSnapshot {
        ProtocolDebugSnapshot {
            protocol_version: PROTOCOL_VERSION,
            crate_version: env!("CARGO_PKG_VERSION").to_string(),
            max_message_bytes: self.config.websocket.max_message_bytes,
            heartbeat_interval_ms: self.config.websocket.heartbeat_interval_ms,
        }
    }

    pub async fn debug_active_sources(&self) -> Vec<SourceDebugSnapshot> {
        let materialized = self.materialized.read().await;
        self.config
            .runtimes
            .sources
            .iter()
            .map(|source| {
                let health = materialized
                    .get(&source.source_id)
                    .map(|state| state.source_health.clone());
                SourceDebugSnapshot {
                    source_id: source.source_id.clone(),
                    source_epoch: source.source_epoch.clone(),
                    source_kind: source.source_kind.clone(),
                    enabled: source.enabled,
                    display_name: source.display_name.clone(),
                    workspace_id: source.workspace_id.clone(),
                    base_url: source.base_url.clone(),
                    health,
                }
            })
            .collect()
    }

    pub async fn debug_active_subscriptions(&self) -> Vec<ActiveConnectionDebug> {
        self.active_connections
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn debug_materializer_summary(&self) -> Vec<MaterializerDebugSummary> {
        self.materialized
            .read()
            .await
            .values()
            .map(|state| MaterializerDebugSummary {
                source_id: state.source_id.clone(),
                source_epoch: state.source_epoch.clone(),
                status: format!("{:?}", state.status).to_ascii_lowercase(),
                source_health: state.source_health.clone(),
                sessions: state.sessions.len(),
                approvals: state.approvals.len(),
                teams: state.teams.len(),
                processes: state.processes.len(),
                worktrees: state.worktrees.len(),
                ledger_events: state.ledger.len(),
                discontinuities: state.discontinuities.len(),
            })
            .collect()
    }

    pub async fn recent_gateway_audit(&self) -> Vec<GatewayAuditRecord> {
        self.audit.lock().await.iter().cloned().collect()
    }

    pub fn metrics_snapshot(&self) -> GatewayMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn encode_next(&self, conn: &mut ConnectionState) -> Option<Vec<u8>> {
        conn.next_outbound()
            .map(|envelope| envelope.encode_to_vec())
    }

    #[cfg(test)]
    pub async fn replace_materialized_state(&self, source_id: String, state: MaterializedState) {
        self.materialized.write().await.insert(source_id, state);
    }

    #[cfg(test)]
    pub fn publish_patch(&self, patch: MaterializedPatch) {
        let _ = self.patches.send(patch);
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayAuditRecord {
    pub observed_at_unix_ms: i64,
    pub kind: String,
    pub subject: Option<String>,
    pub details: Value,
}

fn command_payload_kind(command: &Command) -> &'static str {
    match command.payload.as_ref() {
        Some(crate::protocol::generated::goosetower::v1::command::Payload::SendTurn(_)) => {
            "send_turn"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::ResolveApproval(_)) => {
            "resolve_approval"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::InterruptTurn(_)) => {
            "interrupt_turn"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::SendTeamMessage(_)) => {
            "send_team_message"
        }
        Some(
            crate::protocol::generated::goosetower::v1::command::Payload::BroadcastTeamMessage(_),
        ) => "broadcast_team_message",
        Some(crate::protocol::generated::goosetower::v1::command::Payload::SpawnTeamMember(_)) => {
            "spawn_team_member"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::RetryDelivery(_)) => {
            "retry_delivery"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::CancelDelivery(_)) => {
            "cancel_delivery"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::KillProcess(_)) => {
            "kill_process"
        }
        Some(crate::protocol::generated::goosetower::v1::command::Payload::StartProcess(_)) => {
            "start_process"
        }
        None => "missing",
    }
}

fn command_target_labels(command: &Command) -> (String, String, String) {
    command
        .target
        .as_ref()
        .map(|target| {
            let scope = Scope::try_from(target.scope).unwrap_or(Scope::Unspecified);
            (
                format!("{scope:?}"),
                target.scope_id.clone(),
                target.entity_id.clone(),
            )
        })
        .unwrap_or_else(|| ("missing".to_string(), String::new(), String::new()))
}

fn expected_scope_for_payload(
    payload: &crate::protocol::generated::goosetower::v1::command::Payload,
) -> Scope {
    match payload {
        crate::protocol::generated::goosetower::v1::command::Payload::SendTurn(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::ResolveApproval(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::InterruptTurn(_) => {
            Scope::Session
        }
        crate::protocol::generated::goosetower::v1::command::Payload::SendTeamMessage(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::BroadcastTeamMessage(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::SpawnTeamMember(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::RetryDelivery(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::CancelDelivery(_) => {
            Scope::Team
        }
        crate::protocol::generated::goosetower::v1::command::Payload::KillProcess(_)
        | crate::protocol::generated::goosetower::v1::command::Payload::StartProcess(_) => {
            Scope::Process
        }
    }
}

fn materialized_entity_kind_for_scope(scope: Scope) -> Option<&'static str> {
    match scope {
        Scope::Session => Some("session"),
        Scope::Team => Some("team"),
        Scope::Process => Some("process"),
        Scope::Worktree => Some("worktree"),
        Scope::Source => Some("source"),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct SourceReplayOutcome {
    events: usize,
    bytes: usize,
    filled: Vec<SourceCursor>,
}

#[derive(Debug)]
struct ResumeGap {
    source_id: String,
    last_seen: SourceCursor,
    next_available: Option<SourceCursor>,
    reason: String,
}

impl ResumeGap {
    fn new(
        last_seen: &SourceCursor,
        next_available: Option<SourceCursor>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            source_id: last_seen.source_id.clone(),
            last_seen: last_seen.clone(),
            next_available,
            reason: reason.into(),
        }
    }
}

fn replay_entry_matches(conn: &ConnectionState, entry: &ReplayEntry) -> bool {
    match entry.envelope.payload.as_ref() {
        Some(Payload::Patch(patch)) => {
            let materialized_patch = MaterializedPatch {
                kind: MaterializedPatchKind::EntityUpsert,
                view_kind: patch.view_kind.clone(),
                entity: patch.entity.as_ref().map(|entity| {
                    crate::materializer::EntityKey::new(
                        entry
                            .source_cursor
                            .as_ref()
                            .map(|cursor| cursor.source_id.clone())
                            .unwrap_or_default(),
                        entity.scope_id.clone(),
                        entity.entity_id.clone(),
                    )
                }),
                version: None,
                source_cursor: None,
                body: Value::Null,
            };
            conn.patch_matches(&materialized_patch)
        }
        Some(Payload::SourceGapDetected(_))
        | Some(Payload::SourceGapFilled(_))
        | Some(Payload::SourceSnapshotResync(_))
        | Some(Payload::ConnectionDegraded(_)) => true,
        _ => true,
    }
}

#[derive(Debug, Clone)]
pub struct GatewayReject {
    pub status: StatusCode,
    pub code: String,
}

#[derive(Debug)]
enum Continue {
    Yes,
    No,
}

#[cfg(test)]
mod resume_tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::extract::Query;
    use axum::routing::get;
    use axum::{Json, Router};
    use runtime_core::{
        RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, SessionRecord,
    };
    use serde::Deserialize;
    use serde_json::{json, Value};
    use tokio::net::TcpListener;

    use super::*;
    use crate::materializer::state::SourceCursorView;
    use crate::protocol::generated::goosetower::v1::command::Payload as CommandPayload;
    use crate::protocol::generated::goosetower::v1::{Command, CommandSendTurn, EntityRef};

    #[tokio::test]
    async fn resume_clean_reconnect_uses_gateway_replay_without_duplicates() {
        let gateway = test_gateway(GoosetowerConfig::default());
        let mut conn = test_connection(&gateway);
        let patch = ledger_patch(1);
        let envelope = gateway.patch_envelope(patch);
        gateway.record_replayable(envelope).await;

        gateway
            .handle_resume(
                &mut conn,
                resume_request(0, 1, "static-0", vec![ledger_sub()]),
            )
            .await
            .expect("resume");

        let replayed = drain_payloads(&mut conn);
        assert_eq!(payload_count(&replayed, MessageKind::Patch), 1);
        assert_eq!(payload_count(&replayed, MessageKind::SourceGapFilled), 1);
        assert_eq!(gateway.metrics.resume_success.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn resume_source_replay_fills_missing_events_and_dedupes_overlap() {
        let runtime_addr = spawn_resume_runtime(ResumeRuntimeMode::ReplayOverlap).await;
        let mut config = GoosetowerConfig::default();
        config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
        config.replay.max_events_per_request = 10;
        let gateway = test_gateway(config);
        let mut conn = test_connection(&gateway);

        gateway
            .handle_resume(
                &mut conn,
                resume_request(10, 1, "static-0", vec![ledger_sub()]),
            )
            .await
            .expect("resume fallback");

        let replayed = drain_payloads(&mut conn);
        assert_eq!(payload_count(&replayed, MessageKind::Patch), 2);
        assert_eq!(payload_count(&replayed, MessageKind::SourceGapFilled), 1);
        assert_eq!(gateway.metrics.resume_partial.load(Ordering::Relaxed), 1);
        assert_eq!(gateway.metrics.replay_events.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn resume_gap_detection_triggers_snapshot_resync() {
        let runtime_addr = spawn_resume_runtime(ResumeRuntimeMode::ReplayGap).await;
        let mut config = GoosetowerConfig::default();
        config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
        config.replay.max_events_per_request = 10;
        let gateway = test_gateway(config);
        let mut conn = test_connection(&gateway);

        gateway
            .handle_resume(
                &mut conn,
                resume_request(10, 1, "static-0", vec![ledger_sub()]),
            )
            .await
            .expect("snapshot resync");

        let replayed = drain_payloads(&mut conn);
        assert_eq!(payload_count(&replayed, MessageKind::SourceGapDetected), 1);
        assert_eq!(
            payload_count(&replayed, MessageKind::SourceSnapshotResync),
            1
        );
        assert_eq!(gateway.metrics.gap_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            gateway
                .metrics
                .snapshot_resync_count
                .load(Ordering::Relaxed),
            1
        );
        let materialized = gateway.materialized.read().await;
        let state = materialized.get("local").expect("local state");
        assert_eq!(state.discontinuities.len(), 1);
        assert!(
            state
                .snapshot_ledger(&Default::default())
                .discontinuities
                .len()
                == 1
        );
    }

    #[tokio::test]
    async fn resume_epoch_change_is_gap_detected_and_resynced() {
        let runtime_addr = spawn_resume_runtime(ResumeRuntimeMode::ReplayOverlap).await;
        let mut config = GoosetowerConfig::default();
        config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
        let gateway = test_gateway(config);
        let mut conn = test_connection(&gateway);

        gateway
            .handle_resume(
                &mut conn,
                resume_request(10, 1, "old-epoch", vec![ledger_sub()]),
            )
            .await
            .expect("epoch gap resync");

        let replayed = drain_payloads(&mut conn);
        assert_eq!(payload_count(&replayed, MessageKind::SourceGapDetected), 1);
        assert_eq!(
            payload_count(&replayed, MessageKind::SourceSnapshotResync),
            1
        );
    }

    #[tokio::test]
    async fn resume_stale_source_disables_destructive_commands() {
        let gateway = test_gateway(GoosetowerConfig::default());
        let mut state = MaterializedState::new("local", "static-0");
        state.mark_live();
        state.transition_source_health(SourceHealthState::Stale, Some("test stale".to_string()));
        gateway
            .replace_materialized_state("local".to_string(), state)
            .await;
        let mut conn = test_connection(&gateway);

        let response = gateway
            .admit_and_route_command(&mut conn, send_turn_command("cmd_stale"))
            .await;

        let Some(Payload::CommandRejected(rejected)) = response.payload else {
            panic!("expected command rejection");
        };
        assert_eq!(rejected.error.expect("error").code, "source_stale");
    }

    #[tokio::test]
    async fn command_without_scope_is_rejected_as_unauthorized_without_leaving_pending() {
        let gateway = test_gateway(GoosetowerConfig::default());
        let mut conn = test_connection(&gateway);
        conn.auth.scopes = vec!["gateway:connect".to_string()];

        let response = gateway
            .admit_and_route_command(&mut conn, send_turn_command("cmd_unauthorized"))
            .await;

        let Some(Payload::CommandRejected(rejected)) = response.payload else {
            panic!("expected command rejection");
        };
        assert_eq!(rejected.error.expect("error").code, REASON_UNAUTHORIZED);
        assert!(gateway
            .command_store
            .lock()
            .await
            .get("cmd_unauthorized")
            .is_none());
    }

    #[tokio::test]
    async fn command_scope_mismatch_is_rejected_before_runtime_route() {
        let gateway = live_gateway_with_session_version(GoosetowerConfig::default(), 1).await;
        let mut conn = test_connection(&gateway);
        let mut command = send_turn_command("cmd_invalid_scope");
        command.target.as_mut().expect("target").scope = Scope::Team as i32;

        let response = gateway.admit_and_route_command(&mut conn, command).await;

        let Some(Payload::CommandRejected(rejected)) = response.payload else {
            panic!("expected command rejection");
        };
        assert_eq!(rejected.error.expect("error").code, REASON_INVALID_SCOPE);
    }

    #[tokio::test]
    async fn stale_entity_version_is_rejected_with_refreshable_reason() {
        let gateway = live_gateway_with_session_version(GoosetowerConfig::default(), 2).await;
        let mut conn = test_connection(&gateway);
        let response = gateway
            .admit_and_route_command(&mut conn, send_turn_command("cmd_stale_version"))
            .await;

        let Some(Payload::CommandRejected(rejected)) = response.payload else {
            panic!("expected command rejection");
        };
        let error = rejected.error.expect("error");
        assert_eq!(error.code, REASON_STALE_ENTITY_VERSION);
        assert!(error.retryable);
    }

    #[tokio::test]
    async fn upstream_runtime_http_error_is_rejected_as_upstream_rejected() {
        let runtime_addr = spawn_rejecting_command_runtime().await;
        let mut config = GoosetowerConfig::default();
        config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
        let gateway = live_gateway_with_session_version(config, 1).await;
        let mut conn = test_connection(&gateway);

        let response = gateway
            .admit_and_route_command(&mut conn, send_turn_command("cmd_upstream_reject"))
            .await;

        let Some(Payload::CommandRejected(rejected)) = response.payload else {
            panic!("expected command rejection");
        };
        assert_eq!(
            rejected.error.expect("error").code,
            REASON_UPSTREAM_REJECTED
        );
    }

    #[tokio::test]
    async fn duplicate_command_returns_duplicate_disposition_reason() {
        let runtime_addr = spawn_rejecting_command_runtime().await;
        let mut config = GoosetowerConfig::default();
        config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
        let gateway = live_gateway_with_session_version(config, 1).await;
        let mut conn = test_connection(&gateway);

        let _ = gateway
            .admit_and_route_command(&mut conn, send_turn_command("cmd_duplicate"))
            .await;
        let response = gateway
            .admit_and_route_command(&mut conn, send_turn_command("cmd_duplicate"))
            .await;

        let Some(Payload::CommandDuplicate(duplicate)) = response.payload else {
            panic!("expected duplicate command response");
        };
        assert_eq!(duplicate.command_id, "cmd_duplicate");
        assert_eq!(duplicate.original_command_id, "cmd_duplicate");
    }

    fn test_gateway(config: GoosetowerConfig) -> GatewayState {
        GatewayState::new(Arc::new(config)).expect("gateway")
    }

    async fn live_gateway_with_session_version(
        config: GoosetowerConfig,
        session_version: usize,
    ) -> GatewayState {
        let gateway = test_gateway(config);
        let mut state = MaterializedState::new("local", "static-0");
        state.mark_live();
        for _ in 0..session_version {
            state.upsert_session(session_record());
        }
        gateway
            .replace_materialized_state("local".to_string(), state)
            .await;
        gateway
    }

    fn test_connection(gateway: &GatewayState) -> ConnectionState {
        ConnectionState::new(
            "conn_test".to_string(),
            AuthContext {
                subject: "session_1".to_string(),
                workspace_id: "default".to_string(),
                scopes: vec!["gateway:connect".to_string(), "gateway:command".to_string()],
                allowed_origins: vec!["http://localhost:3000".to_string()],
                expires_at_unix_ms: now_ms() + 60_000,
                jti: "jti_test".to_string(),
            },
            gateway.config.lanes.clone(),
            gateway.config.websocket.max_message_bytes,
        )
    }

    fn resume_request(
        gateway_seq: u64,
        source_seq: u64,
        source_epoch: &str,
        active_subscriptions: Vec<Subscribe>,
    ) -> Resume {
        Resume {
            previous_connection_id: "conn_previous".to_string(),
            cursor: Some(CursorVector {
                gateway_seq,
                sources: vec![SourceCursor {
                    source_id: "local".to_string(),
                    source_epoch: source_epoch.to_string(),
                    source_seq,
                }],
            }),
            active_subscriptions,
        }
    }

    fn ledger_sub() -> Subscribe {
        Subscribe {
            subscription_id: "sub_ledger".to_string(),
            view_kind: "ledger".to_string(),
            filters: Default::default(),
        }
    }

    fn ledger_patch(source_seq: i64) -> MaterializedPatch {
        MaterializedPatch {
            kind: MaterializedPatchKind::ListInsert,
            view_kind: "ledger".to_string(),
            entity: Some(crate::materializer::EntityKey::new(
                "local",
                "ledger_event",
                source_seq.to_string(),
            )),
            version: None,
            source_cursor: Some(SourceCursorView {
                source_id: "local".to_string(),
                source_epoch: "static-0".to_string(),
                source_seq,
            }),
            body: json!({ "source_seq": source_seq }),
        }
    }

    fn drain_payloads(conn: &mut ConnectionState) -> Vec<Payload> {
        let mut payloads = Vec::new();
        while let Some(envelope) = conn.next_outbound() {
            if let Some(payload) = envelope.payload {
                payloads.push(payload);
            }
        }
        payloads
    }

    fn payload_count(payloads: &[Payload], kind: MessageKind) -> usize {
        payloads
            .iter()
            .filter(|payload| payload_kind(payload) == kind)
            .count()
    }

    fn payload_kind(payload: &Payload) -> MessageKind {
        match payload {
            Payload::Patch(_) => MessageKind::Patch,
            Payload::SourceGapDetected(_) => MessageKind::SourceGapDetected,
            Payload::SourceGapFilled(_) => MessageKind::SourceGapFilled,
            Payload::SourceSnapshotResync(_) => MessageKind::SourceSnapshotResync,
            Payload::CommandRejected(_) => MessageKind::CommandRejected,
            Payload::ConnectionDegraded(_) => MessageKind::ConnectionDegraded,
            _ => MessageKind::Unspecified,
        }
    }

    fn send_turn_command(command_id: &str) -> Command {
        Command {
            command_id: command_id.to_string(),
            target: Some(EntityRef {
                scope: Scope::Session as i32,
                scope_id: "session_1".to_string(),
                entity_id: "source:local".to_string(),
                entity_version: 1,
            }),
            base_entity_version: 1,
            created_at_client_unix_ms: 1,
            payload: Some(CommandPayload::SendTurn(CommandSendTurn {
                session_id: "session_1".to_string(),
                text: "hello".to_string(),
            })),
            ..Command::default()
        }
    }

    async fn spawn_rejecting_command_runtime() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind rejecting runtime");
        let addr = listener.local_addr().expect("runtime addr");
        tokio::spawn(async move {
            let app = axum::Router::new().route(
                "/v1/sessions/{session_id}/turns",
                axum::routing::post(|| async {
                    (
                        axum::http::StatusCode::CONFLICT,
                        axum::Json(json!({
                            "error": "session already has an active turn"
                        })),
                    )
                }),
            );
            axum::serve(listener, app)
                .await
                .expect("serve rejecting runtime");
        });
        addr
    }

    #[derive(Debug, Clone, Copy)]
    enum ResumeRuntimeMode {
        ReplayOverlap,
        ReplayGap,
    }

    async fn spawn_resume_runtime(mode: ResumeRuntimeMode) -> SocketAddr {
        #[derive(Debug, Deserialize)]
        struct ReplayQuery {
            after_seq: Option<i64>,
        }

        let replay = move |Query(query): Query<ReplayQuery>| async move {
            let events = match (mode, query.after_seq) {
                (ResumeRuntimeMode::ReplayOverlap, Some(1)) => vec![
                    runtime_event(2, "turn.completed"),
                    runtime_event(2, "turn.completed"),
                    runtime_event(3, "turn.completed"),
                ],
                (ResumeRuntimeMode::ReplayGap, Some(1)) => vec![runtime_event(3, "turn.completed")],
                _ => vec![runtime_event(3, "session.created")],
            };
            Json(events)
        };

        let app = Router::new()
            .route("/v1/events", get(replay))
            .route(
                "/v1/sessions",
                get(|| async { Json(vec![session_record()]) }),
            )
            .route("/v1/teams", get(|| async { Json(Vec::<Value>::new()) }))
            .route("/v1/processes", get(|| async { Json(Vec::<Value>::new()) }))
            .route("/v1/worktrees", get(|| async { Json(Vec::<Value>::new()) }))
            .route(
                "/v1/providers",
                get(|| async { Json(json!({ "providers": [] })) }),
            )
            .route(
                "/v1/diagnostics",
                get(|| async {
                    Json(json!({
                        "providers": {},
                        "comms": {},
                        "processes": {},
                        "worktrees": {},
                        "recovery": {},
                    }))
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind runtime");
        let addr = listener.local_addr().expect("runtime addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("runtime server");
        });
        addr
    }

    fn runtime_event(row_id: i64, kind: &str) -> RuntimeEventRecord {
        RuntimeEventRecord {
            row_id,
            event_id: format!("evt_{row_id}"),
            scope: RuntimeEventScope::Session,
            scope_id: "session_1".to_string(),
            session_id: Some("session_1".to_string()),
            team_id: None,
            turn_id: Some("turn_1".to_string()),
            seq: row_id,
            kind: kind.to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({ "assistant_text": format!("event {row_id}") }),
            provider: Some("codex".to_string()),
            provider_seq: Some(row_id),
            created_at: row_id,
        }
    }

    fn session_record() -> SessionRecord {
        SessionRecord {
            id: "session_1".to_string(),
            provider: "codex".to_string(),
            status: "ready".to_string(),
            cwd: Some("/repo".to_string()),
            model: Some("gpt-5".to_string()),
            permission_mode: None,
            system_prompt: None,
            metadata: json!({}),
            provider_session_ref: None,
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: 1,
            updated_at: 1,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        }
    }
}
