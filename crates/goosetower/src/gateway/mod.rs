use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::http::StatusCode;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use runtime_core::{ApprovalResponseInput, SendTurnInput};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::auth::{now_ms, AuthContext, TicketValidator};
use crate::config::GoosetowerConfig;
use crate::materializer::{MaterializedPatch, MaterializedPatchKind, MaterializedState};
use crate::protocol::generated::goosetower::v1::realtime_envelope::Payload;
use crate::protocol::generated::goosetower::v1::{
    AuthExpiring, AuthRefresh, AuthRefreshed, Command, CommandAccepted, CursorVector, GatewayEvent,
    Hello, Lane, MessageKind, Patch, Ping, Pong, RealtimeEnvelope, Scope, Snapshot, SourceCursor,
    Subscribe,
};
use crate::protocol::PROTOCOL_VERSION;
use crate::runtime::client::{ProcessKillInput, ProcessStartInput};
use crate::runtime::{
    GooselakeRuntimeClient, TeamBroadcastInput, TeamDirectInput, TeamMemberSpawnInput,
};
mod support;

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
        Ok(Self {
            ticket_validator: TicketValidator::from_config(&config)?,
            config,
            materialized: RwLock::new(materialized),
            command_store: Mutex::new(CommandIdStore::default()),
            next_connection_id: AtomicU64::new(1),
            next_gateway_seq: AtomicU64::new(1),
            patches,
        })
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
        conn.enqueue(self.hello(&connection_id), Some("hello".to_string()));
        conn.enqueue(self.audit_event("connection.open", Scope::System, ""), None);
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
                            conn.enqueue(self.patch_envelope(patch), None);
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            conn.enqueue(self.audit_event("source.gap", Scope::Source, ""), None);
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
                            conn.enqueue(error_envelope("protocol_error", error.to_string(), false), None);
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
        }

        tracing::info!(
            connection_id,
            user = %conn.auth.subject,
            workspace = %conn.auth.workspace_id,
            "gateway audit connection.close"
        );
    }

    async fn handle_inbound_message(
        &self,
        message: Message,
        conn: &mut ConnectionState,
    ) -> Result<Continue> {
        let bytes = match message {
            Message::Binary(bytes) => bytes,
            Message::Ping(bytes) => {
                conn.enqueue(raw_pong_envelope(bytes), None);
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
                conn.enqueue(self.pong(ping), Some("pong".to_string()));
            }
            Some(Payload::Ack(ack)) => {
                conn.last_acked_gateway_seq = conn.last_acked_gateway_seq.max(ack.gateway_seq);
            }
            Some(Payload::Resume(resume)) => {
                if let Some(cursor) = resume.cursor {
                    conn.cursor = Some(cursor);
                }
                conn.enqueue(self.audit_event("snapshot.resync", Scope::System, ""), None);
            }
            Some(Payload::Subscribe(subscribe)) => {
                let snapshot = self.subscribe(conn, subscribe).await?;
                conn.enqueue(snapshot, None);
            }
            Some(Payload::Unsubscribe(unsubscribe)) => {
                conn.unsubscribe(unsubscribe);
                conn.enqueue(
                    self.audit_event("subscribe.removed", Scope::System, ""),
                    None,
                );
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
                conn.enqueue(
                    envelope_with_payload(
                        MessageKind::AuthRefreshed,
                        Lane::Critical,
                        Payload::AuthRefreshed(AuthRefreshed {
                            expires_at_unix_ms: conn.auth.expires_at_unix_ms,
                        }),
                    ),
                    None,
                );
                conn.enqueue(self.audit_event("auth.refresh", Scope::System, ""), None);
            }
            Some(Payload::Command(command)) => {
                let response = self.admit_and_route_command(conn, command).await;
                conn.enqueue(response, None);
            }
            _ => {
                return Err(anyhow!("unsupported client message kind"));
            }
        }

        if conn.auth.expires_at_unix_ms - now_ms() < 15_000 {
            conn.enqueue(
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
        if command.command_id.trim().is_empty() {
            return command_rejected("", "missing_command_id", "command_id is required", false);
        }
        if command.created_at_client_unix_ms <= 0 {
            return command_rejected(
                &command.command_id,
                "missing_client_time",
                "created_at_client_unix_ms is required",
                false,
            );
        }
        if !conn.auth.has_scope("gateway:command") {
            return command_rejected(
                &command.command_id,
                "forbidden",
                "ticket does not include gateway:command scope",
                false,
            );
        }

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
                    "gateway audit command.duplicate"
                );
                return command_duplicate(&command.command_id, &existing.original_command_id);
            }
            store.insert_pending(&command.command_id);
        }

        let result = self.route_command(conn, &command).await;
        let mut store = self.command_store.lock().await;
        match result {
            Ok(()) => {
                let gateway_seq = self.next_gateway_seq.fetch_add(1, Ordering::Relaxed);
                store.complete(
                    &command.command_id,
                    CommandDisposition::Accepted { gateway_seq },
                );
                tracing::info!(
                    command_id = %command.command_id,
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
                tracing::info!(
                    command_id = %command.command_id,
                    reason = %error.code,
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
            .map_err(|error| CommandRouteError::non_retryable(error.to_string()))?;
        let Some(payload) = command.payload.as_ref() else {
            return Err(CommandRouteError::non_retryable("missing command payload"));
        };
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
                        CommandRouteError::non_retryable("target.scope_id session_id is required")
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
                        CommandRouteError::non_retryable("target.scope_id team_id is required")
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
                        CommandRouteError::non_retryable("target.scope_id team_id is required")
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
