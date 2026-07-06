use super::*;

impl GatewayState {
    pub(super) fn hello(&self, connection_id: &str) -> RealtimeEnvelope {
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

    pub(super) fn pong(&self, ping: Ping) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::Pong,
            Lane::Critical,
            Payload::Pong(Pong {
                client_time_unix_ms: ping.client_time_unix_ms,
                server_time_unix_ms: now_ms(),
            }),
        )
    }

    pub(super) fn patch_envelope(&self, patch: MaterializedPatch) -> RealtimeEnvelope {
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

    pub(super) async fn enqueue_patch(&self, conn: &mut ConnectionState, patch: MaterializedPatch) {
        let envelope = self.patch_envelope(patch);
        let entry = self.record_replayable(envelope).await;
        self.enqueue_connection(conn, entry.envelope, None);
    }

    pub(super) async fn publish_materialized_patch(&self, patch: MaterializedPatch) {
        let envelope = self.patch_envelope(patch.clone());
        self.record_replayable(envelope).await;
        let _ = self.patches.send(patch);
    }

    pub(super) async fn record_replayable(&self, mut envelope: RealtimeEnvelope) -> ReplayEntry {
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

    pub(super) fn connection_degraded(&self, reason: impl Into<String>) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::ConnectionDegraded,
            Lane::Critical,
            Payload::ConnectionDegraded(ConnectionDegraded {
                reason: reason.into(),
            }),
        )
    }

    pub(super) fn source_gap_detected(
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

    pub(super) fn source_gap_filled(&self, cursor: SourceCursor) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::SourceGapFilled,
            Lane::Critical,
            Payload::SourceGapFilled(SourceGapFilled {
                cursor: Some(cursor),
            }),
        )
    }

    pub(super) fn source_snapshot_resync(&self, source_id: &str, reason: &str) -> RealtimeEnvelope {
        envelope_with_payload(
            MessageKind::SourceSnapshotResync,
            Lane::Critical,
            Payload::SourceSnapshotResync(SourceSnapshotResync {
                source_id: source_id.to_string(),
                reason: reason.to_string(),
            }),
        )
    }

    pub(super) fn audit_event(&self, kind: &str, scope: Scope, scope_id: &str) -> RealtimeEnvelope {
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

    pub(super) fn enqueue_connection(
        &self,
        conn: &mut ConnectionState,
        envelope: RealtimeEnvelope,
        coalesce_key: Option<String>,
    ) {
        let outcome = conn.enqueue(envelope, coalesce_key);
        self.metrics.record_outbound(outcome);
    }

    pub(super) async fn refresh_connection_debug(&self, conn: &ConnectionState) {
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

    pub(super) async fn record_audit(&self, kind: &str, subject: Option<String>, details: Value) {
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

    pub(super) fn encode_next(&self, conn: &mut ConnectionState) -> Option<Vec<u8>> {
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
