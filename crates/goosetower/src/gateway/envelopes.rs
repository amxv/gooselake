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
                gateway_epoch: self.gateway_epoch.clone(),
                gateway_started_at_unix_ns: self.gateway_started_at_unix_ns,
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
        let entity_id = patch.entity.as_ref().map(|entity| entity.entity_id.clone());
        let operation = match patch.kind {
            MaterializedPatchKind::EntityRemove | MaterializedPatchKind::ListRemove => {
                ViewOperation::Remove
            }
            _ if matches!(
                patch.view_kind.as_str(),
                "session_detail" | "team_workspace"
            ) =>
            {
                ViewOperation::Replace
            }
            _ => ViewOperation::Upsert,
        };
        let coverage = view_coverage(&patch.view_kind, entity_id);
        let mut envelope = envelope_with_payload(
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
                    // A verification-only direct frame still needs valid
                    // authority. Production record_replayable atomically
                    // overwrites this sample with its reserved publication.
                    gateway_seq: self.next_gateway_seq.load(Ordering::Relaxed),
                    sources: vec![SourceCursor {
                        source_id: cursor.source_id,
                        source_epoch: cursor.source_epoch,
                        source_seq: cursor.source_seq.max(0) as u64,
                    }],
                    gateway_epoch: self.gateway_epoch.clone(),
                    gateway_started_at_unix_ns: self.gateway_started_at_unix_ns,
                }),
                body: serde_json::to_vec(&patch.body).unwrap_or_default().into(),
                schema_version: DETAIL_SCHEMA_VERSION,
                operation: operation as i32,
                coverage: Some(coverage),
            }),
        );
        envelope.message_id = self.next_view_message_id();
        envelope
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
        let mut replay_buffer = self.replay_buffer.lock().await;
        let gateway_seq = self.next_gateway_seq.fetch_add(1, Ordering::Relaxed);
        envelope.gateway_seq = gateway_seq;
        match envelope.payload.as_mut() {
            Some(Payload::Patch(patch)) => {
                if let Some(cursor) = patch.cursor.as_mut() {
                    cursor.gateway_seq = gateway_seq;
                }
            }
            Some(Payload::SourceSnapshotResync(resync)) => {
                if let Some(cursor) = resync.cursor.as_mut() {
                    cursor.gateway_seq = gateway_seq;
                }
            }
            _ => {}
        }
        let source_cursor = envelope.payload.as_ref().and_then(|payload| match payload {
            Payload::Patch(patch) => patch
                .cursor
                .as_ref()
                .and_then(|cursor| cursor.sources.first().cloned()),
            Payload::SourceSnapshotResync(resync) => resync
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
        replay_buffer.push(entry.clone());
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

    pub(super) fn source_snapshot_resync(
        &self,
        state: &MaterializedState,
        reason: &str,
    ) -> Result<RealtimeEnvelope> {
        let source_cursor = state
            .cursor()
            .ok_or_else(|| anyhow!("source snapshot resync lacks a source cursor"))?;
        // This is the atomic ownership reset/commit. It is intentionally
        // independent of source cardinality; bounded active subscriptions
        // repopulate summaries and selected details after the commit.
        let body = serde_json::to_vec(&state.snapshot_source_replacement())?;
        let mut envelope = envelope_with_payload(
            MessageKind::SourceSnapshotResync,
            Lane::Critical,
            Payload::SourceSnapshotResync(SourceSnapshotResync {
                source_id: state.source_id.clone(),
                reason: reason.to_string(),
                cursor: Some(CursorVector {
                    gateway_seq: 0,
                    sources: vec![SourceCursor {
                        source_id: source_cursor.source_id,
                        source_epoch: source_cursor.source_epoch,
                        source_seq: source_cursor.source_seq.max(0) as u64,
                    }],
                    gateway_epoch: self.gateway_epoch.clone(),
                    gateway_started_at_unix_ns: self.gateway_started_at_unix_ns,
                }),
                body: body.into(),
                schema_version: DETAIL_SCHEMA_VERSION,
                coverage: Some(source_replacement_coverage()),
            }),
        );
        envelope.message_id = self.next_view_message_id();
        envelope.gateway_seq = u64::MAX;
        if let Some(Payload::SourceSnapshotResync(resync)) = envelope.payload.as_mut() {
            if let Some(cursor) = resync.cursor.as_mut() {
                cursor.gateway_seq = u64::MAX;
            }
        }
        let encoded_len = envelope.encode_to_vec().len();
        if encoded_len > self.config.websocket.max_message_bytes {
            return Err(anyhow!(
                "source snapshot resync frame exceeds websocket limit ({encoded_len} > {})",
                self.config.websocket.max_message_bytes
            ));
        }
        envelope.gateway_seq = 0;
        if let Some(Payload::SourceSnapshotResync(resync)) = envelope.payload.as_mut() {
            if let Some(cursor) = resync.cursor.as_mut() {
                cursor.gateway_seq = 0;
            }
        }
        Ok(envelope)
    }

    pub(super) fn next_view_message_id(&self) -> String {
        format!(
            "view_{}_{}",
            self.gateway_epoch,
            self.next_message_id.fetch_add(1, Ordering::Relaxed)
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

    #[cfg(any(test, feature = "p02-verification"))]
    pub async fn verification_materializer_observer(&self) -> Vec<MaterializerObserverSnapshot> {
        self.materialized
            .read()
            .await
            .values()
            .map(|state| MaterializerObserverSnapshot {
                source_id: state.source_id.clone(),
                source_epoch: state.source_epoch.clone(),
                source_health: state.source_health.clone(),
                recent_ledger: state
                    .ledger
                    .iter()
                    .rev()
                    .take(64)
                    .rev()
                    .filter_map(|event| serde_json::to_value(event).ok())
                    .map(|value| observers::redact_debug_value(&value))
                    .collect(),
                session_details: state
                    .sessions
                    .keys()
                    .take(32)
                    .filter_map(|session_id| {
                        state.snapshot_session(&SelectedSessionSubscription {
                            session_id: session_id.clone(),
                            include_text: true,
                        })
                    })
                    .filter_map(|detail| serde_json::to_value(detail).ok())
                    .map(|value| observers::redact_debug_value(&value))
                    .collect(),
            })
            .collect()
    }

    #[cfg(any(test, feature = "p02-verification"))]
    pub async fn debug_served_frames(&self) -> Vec<ServedFrameDebug> {
        self.served_frames.lock().await.iter().cloned().collect()
    }

    #[cfg(any(test, feature = "p02-verification"))]
    pub(super) async fn record_served_envelope(
        &self,
        connection_id: &str,
        envelope: &RealtimeEnvelope,
    ) {
        let capture_index = self.next_frame_capture.fetch_add(1, Ordering::Relaxed);
        let frame = ServedFrameDebug::from_envelope(capture_index, connection_id, envelope);
        let mut frames = self.served_frames.lock().await;
        if frames.len() == 128 {
            frames.pop_front();
        }
        frames.push_back(frame);
    }

    pub async fn recent_gateway_audit(&self) -> Vec<GatewayAuditRecord> {
        self.audit.lock().await.iter().cloned().collect()
    }

    pub fn metrics_snapshot(&self) -> GatewayMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[cfg(not(feature = "p02-verification"))]
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

pub(super) fn view_coverage(view_kind: &str, entity_id: Option<String>) -> ViewCoverage {
    let domains = match view_kind {
        "session_summary" | "session" => vec!["sessions".to_string()],
        "session_detail" => vec!["session_details".to_string()],
        "team_summary" | "team" | "teams" => vec!["teams".to_string()],
        "team_workspace" | "team_stream" => vec!["team_workspaces".to_string()],
        "board" | "fleet_board" => vec!["fleet_rows".to_string()],
        "approval" | "approval_inbox" => vec!["approvals".to_string()],
        "ledger" => vec!["ledger_events".to_string()],
        "process" | "process_tail" => vec!["processes".to_string()],
        "worktree" | "worktrees" => vec!["worktrees".to_string()],
        "source" | "source-health" | "source_health" | "fleet" => {
            vec!["sources".to_string()]
        }
        _ => Vec::new(),
    };
    ViewCoverage {
        domains,
        entity_ids: entity_id.into_iter().collect(),
        authoritative: true,
    }
}

fn source_replacement_coverage() -> ViewCoverage {
    ViewCoverage {
        domains: [
            "fleet_rows",
            "sessions",
            "session_details",
            "teams",
            "team_workspaces",
            "approvals",
            "processes",
            "worktrees",
            "sources",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        entity_ids: Vec::new(),
        authoritative: true,
    }
}
