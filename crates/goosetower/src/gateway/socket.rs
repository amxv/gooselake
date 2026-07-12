use super::*;

impl GatewayState {
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
        #[cfg(not(feature = "p02-verification"))]
        if sender
            .send(Message::Binary(
                self.encode_next(&mut conn).unwrap_or_default().into(),
            ))
            .await
            .is_err()
        {
            return;
        }
        #[cfg(feature = "p02-verification")]
        {
            let first = conn.next_outbound().unwrap_or_default();
            let encoded = first.encode_to_vec();
            if sender.send(Message::Binary(encoded.into())).await.is_err() {
                return;
            }
            self.record_served_envelope(&conn.connection_id, &first)
                .await;
        }
        #[cfg(not(feature = "p02-verification"))]
        while let Some(envelope) = conn.next_outbound() {
            if sender
                .send(Message::Binary(envelope.encode_to_vec().into()))
                .await
                .is_err()
            {
                return;
            }
        }
        #[cfg(feature = "p02-verification")]
        while let Some(envelope) = conn.next_outbound() {
            let encoded = envelope.encode_to_vec();
            if sender.send(Message::Binary(encoded.into())).await.is_err() {
                return;
            }
            self.record_served_envelope(&conn.connection_id, &envelope)
                .await;
        }

        let mut patch_rx = self.patches.subscribe();
        let mut recovery_rx = self.recoveries.subscribe();
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
                recovery = recovery_rx.recv() => {
                    match recovery {
                        Ok(SourceRecoverySignal::Filled(cursor)) => {
                            self.enqueue_connection(&mut conn, self.source_gap_filled(cursor), None);
                        }
                        Ok(SourceRecoverySignal::Resync { source_id, reason }) => {
                            self.enqueue_connection(
                                &mut conn,
                                self.source_snapshot_resync(&source_id, &reason),
                                None,
                            );
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            conn.status = ConnectionStatus::Degraded;
                            self.enqueue_connection(
                                &mut conn,
                                self.connection_degraded("source recovery signal lagged"),
                                Some("connection_status".to_string()),
                            );
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

            #[cfg(not(feature = "p02-verification"))]
            while let Some(envelope) = conn.next_outbound() {
                if sender
                    .send(Message::Binary(envelope.encode_to_vec().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            #[cfg(feature = "p02-verification")]
            while let Some(envelope) = conn.next_outbound() {
                let encoded = envelope.encode_to_vec();
                if sender.send(Message::Binary(encoded.into())).await.is_err() {
                    return;
                }
                self.record_served_envelope(&conn.connection_id, &envelope)
                    .await;
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

    pub(super) async fn handle_resume(
        &self,
        conn: &mut ConnectionState,
        resume: Resume,
    ) -> Result<()> {
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
                    .or_insert_with(|| materialized_state_from_source(source));
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
        let mut materialized = self.materialized.write().await;
        if materialized.get(source_id).is_some_and(|current| {
            current.source_epoch == state.source_epoch
                && current.source_health.last_source_seq.unwrap_or(0)
                    > state.source_health.last_source_seq.unwrap_or(0)
        }) {
            tracing::info!(source_id, "discarding stale snapshot resync bootstrap");
            return Ok(());
        }
        materialized.insert(source_id.to_string(), state);
        drop(materialized);
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

    pub(super) async fn snapshot_for_subscription(&self, subscribe: Subscribe) -> RealtimeEnvelope {
        let materialized = self.materialized.read().await;
        let source_id = optional_subscribe_filter(&subscribe.filters, "source_id");
        let canonical_view_kind = canonical_subscription_view_kind(&subscribe.view_kind);
        let body = match canonical_view_kind {
            "board" => serde_json::to_value(snapshot_cross_source_board(
                &materialized,
                &BoardSubscription {
                    offset: parse_filter_usize(&subscribe.filters, "offset", 0),
                    limit: parse_filter_usize(&subscribe.filters, "limit", 100),
                    status_filter: optional_subscribe_filter(&subscribe.filters, "status"),
                    team_id: optional_subscribe_filter(&subscribe.filters, "team_id"),
                    source_id: source_id.clone(),
                    query: optional_subscribe_filter(&subscribe.filters, "query"),
                },
            ))
            .unwrap_or_else(|error| json!({ "error": error.to_string() })),
            "approval_inbox" => serde_json::to_value(snapshot_cross_source_approval_inbox(
                &materialized,
                &ApprovalInboxSubscription {
                    include_resolved: subscribe
                        .filters
                        .get("include_resolved")
                        .is_some_and(|value| value == "true"),
                    session_id: optional_subscribe_filter(&subscribe.filters, "session_id"),
                    source_id: source_id.clone(),
                },
            ))
            .unwrap_or_else(|error| json!({ "error": error.to_string() })),
            "ledger" => serde_json::to_value(snapshot_cross_source_ledger(
                &materialized,
                &LedgerSubscription {
                    offset: parse_filter_usize(&subscribe.filters, "offset", 0),
                    limit: parse_filter_usize(&subscribe.filters, "limit", 200),
                    scope: optional_subscribe_filter(&subscribe.filters, "scope"),
                    session_id: optional_subscribe_filter(&subscribe.filters, "session_id"),
                    team_id: optional_subscribe_filter(&subscribe.filters, "team_id"),
                    process_id: optional_subscribe_filter(&subscribe.filters, "process_id"),
                    kind: optional_subscribe_filter(&subscribe.filters, "kind"),
                    criticality: optional_subscribe_filter(&subscribe.filters, "criticality"),
                    source_id: source_id.clone(),
                },
            ))
            .unwrap_or_else(|error| json!({ "error": error.to_string() })),
            "fleet" | "source_health" => serde_json::to_value(snapshot_cross_source_health(
                &materialized,
                source_id.as_deref(),
            ))
            .unwrap_or_else(|error| json!({ "error": error.to_string() })),
            "worktrees" => serde_json::to_value(snapshot_cross_source_worktrees(
                &materialized,
                source_id.as_deref(),
            ))
            .unwrap_or_else(|error| json!({ "error": error.to_string() })),
            _ => {
                let selected_source_id = source_id
                    .clone()
                    .or_else(|| materialized.keys().next().cloned());
                match selected_source_id
                    .as_deref()
                    .and_then(|source_id| materialized.get(source_id))
                {
                    Some(state) => snapshot_body(state, canonical_view_kind, &subscribe.filters)
                        .unwrap_or_else(|error| json!({ "error": error.to_string() })),
                    None => Value::Null,
                }
            }
        };
        let cursor = cursor_vector_from_states(
            &materialized,
            source_id.as_deref(),
            self.next_gateway_seq.load(Ordering::Relaxed),
        );
        let mut envelope = envelope_with_payload(
            MessageKind::Snapshot,
            Lane::State,
            Payload::Snapshot(Snapshot {
                view_kind: canonical_view_kind.to_string(),
                cursor,
                body: serde_json::to_vec(&body).unwrap_or_default().into(),
                schema_version: DETAIL_SCHEMA_VERSION,
                operation: ViewOperation::Replace as i32,
                coverage: Some(super::envelopes::view_coverage(
                    canonical_view_kind,
                    subscription_entity_id(canonical_view_kind, &subscribe.filters),
                )),
            }),
        );
        envelope.message_id = format!(
            "view_{}_{}",
            now_ms(),
            self.next_message_id.fetch_add(1, Ordering::Relaxed)
        );
        envelope
    }
}

fn canonical_subscription_view_kind(view_kind: &str) -> &str {
    match view_kind {
        "session" => "session_detail",
        "team" | "team_stream" => "team_workspace",
        other => other,
    }
}

fn subscription_entity_id(view_kind: &str, filters: &HashMap<String, String>) -> Option<String> {
    match view_kind {
        "session_detail" => optional_subscribe_filter(filters, "session_id"),
        "team_workspace" => optional_subscribe_filter(filters, "team_id"),
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

enum Continue {
    Yes,
    No,
}
