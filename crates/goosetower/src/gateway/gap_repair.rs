use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use serde_json::json;

use super::*;

#[derive(Debug, Default)]
pub(super) struct GapRepairQueue {
    repairing: bool,
    pending: BTreeMap<i64, SourceEvent>,
    overflowed: bool,
    highest_observed_seq: Option<i64>,
}

impl GapRepairQueue {
    fn insert(&mut self, event: SourceEvent, limit: usize) {
        self.highest_observed_seq = Some(
            self.highest_observed_seq
                .map_or(event.source_seq, |current| current.max(event.source_seq)),
        );
        self.pending.entry(event.source_seq).or_insert(event);
        while self.pending.len() > limit.max(1) {
            self.pending.pop_last();
            self.overflowed = true;
        }
    }

    fn requires_epoch_rebase(&self, current_epoch: &str) -> bool {
        self.pending
            .values()
            .any(|event| event.source_epoch != current_epoch)
    }

    fn covered_by(&self, source_epoch: &str, high_watermark: i64, allow_overflow: bool) -> bool {
        (allow_overflow || !self.overflowed)
            && !self.requires_epoch_rebase(source_epoch)
            && self
                .highest_observed_seq
                .is_none_or(|highest| high_watermark >= highest)
    }
}

impl GatewayState {
    #[cfg(test)]
    pub(crate) async fn verification_gap_queue(&self, source_id: &str) -> (usize, bool, bool) {
        self.gap_repairs
            .lock()
            .await
            .get(source_id)
            .map(|queue| (queue.pending.len(), queue.overflowed, queue.repairing))
            .unwrap_or_default()
    }

    pub(super) async fn source_repair_active(&self, source_id: &str) -> bool {
        self.gap_repairs
            .lock()
            .await
            .get(source_id)
            .is_some_and(|queue| queue.repairing)
    }

    pub(super) async fn retry_idle_gap_after_reconnect(&self, source_id: &str) {
        if self
            .materialized
            .read()
            .await
            .get(source_id)
            .is_none_or(|state| state.source_health.state != SourceHealthState::GapDetected)
        {
            return;
        }
        let retry = {
            let mut repairs = self.gap_repairs.lock().await;
            let Some(queue) = repairs.get_mut(source_id) else {
                return;
            };
            if queue.repairing {
                false
            } else {
                queue.repairing = true;
                true
            }
        };
        if retry {
            self.repair_source_gap(source_id).await;
        }
    }

    pub async fn ingest_source_event(&self, event: SourceEvent) {
        if self.queue_if_repairing(&event).await {
            return;
        }

        let gap_patch = {
            let mut materialized = self.materialized.write().await;
            let state = materialized
                .entry(event.source_id.clone())
                .or_insert_with(|| MaterializedState::new(&event.source_id, &event.source_epoch));
            if let Some(source) = self
                .config
                .runtimes
                .sources
                .iter()
                .find(|source| source.source_id == event.source_id)
            {
                state.apply_source_config(source);
            }
            let reason = if state.source_epoch != event.source_epoch {
                Some("source epoch changed".to_string())
            } else if state
                .source_health
                .last_source_seq
                .is_some_and(|last| event.source_seq > last + 1)
            {
                Some(format!(
                    "expected source seq {}, received {}",
                    state.source_health.last_source_seq.unwrap_or_default() + 1,
                    event.source_seq
                ))
            } else {
                None
            };
            reason.map(|reason| {
                state.mark_discontinuity(&reason);
                state.transition_source_health(SourceHealthState::GapDetected, Some(reason))
            })
        };

        if let Some(patch) = gap_patch {
            self.metrics.gap_count.fetch_add(1, Ordering::Relaxed);
            self.publish_materialized_patch(patch).await;
            let source_id = event.source_id.clone();
            if self.begin_gap_repair(event).await {
                self.repair_source_gap(&source_id).await;
            }
            return;
        }

        self.reduce_contiguous_event(event).await;
    }

    async fn queue_if_repairing(&self, event: &SourceEvent) -> bool {
        let mut repairs = self.gap_repairs.lock().await;
        let Some(queue) = repairs.get_mut(&event.source_id) else {
            return false;
        };
        if !queue.repairing {
            return false;
        }
        queue.insert(event.clone(), self.pending_gap_event_limit());
        true
    }

    async fn begin_gap_repair(&self, event: SourceEvent) -> bool {
        let mut repairs = self.gap_repairs.lock().await;
        let queue = repairs.entry(event.source_id.clone()).or_default();
        queue.insert(event, self.pending_gap_event_limit());
        if queue.repairing {
            false
        } else {
            queue.repairing = true;
            true
        }
    }

    async fn repair_source_gap(&self, source_id: &str) {
        let attempt_epoch = self.source_cursor_and_epoch(source_id).await.1;
        let Some(source) = self
            .config
            .runtimes
            .sources
            .iter()
            .find(|source| source.enabled && source.source_id == source_id)
            .cloned()
        else {
            self.fail_gap_repair(source_id, &attempt_epoch, "runtime source unavailable")
                .await;
            return;
        };
        let client = match runtime_client_from_source(&self.config, &source) {
            Ok(client) => client,
            Err(error) => {
                self.fail_gap_repair(source_id, &attempt_epoch, &error.to_string())
                    .await;
                return;
            }
        };
        let page_limit = self.config.replay.max_events_per_request.max(1);
        let repair_started = Instant::now();
        let mut replayed = 0usize;
        let Some(mut candidate) = self.materialized.read().await.get(source_id).cloned() else {
            self.fail_gap_repair(source_id, &attempt_epoch, "materialized source unavailable")
                .await;
            return;
        };
        if candidate.source_epoch != attempt_epoch {
            self.fail_gap_repair(source_id, &attempt_epoch, "repair attempt superseded")
                .await;
            return;
        }
        let requires_epoch_rebase = self
            .gap_repairs
            .lock()
            .await
            .get(source_id)
            .is_some_and(|queue| queue.requires_epoch_rebase(&attempt_epoch));

        if !requires_epoch_rebase {
            for _ in 0..32 {
                if self.source_gap_overflowed(source_id).await {
                    break;
                }
                self.stage_pending_contiguous(source_id, &mut candidate)
                    .await;
                let recovery_reason =
                    "source gap filled by replay and authoritative snapshot resync";
                if self
                    .complete_gap_candidate(
                        source_id,
                        &attempt_epoch,
                        candidate.clone(),
                        recovery_reason,
                        false,
                    )
                    .await
                {
                    self.metrics
                        .record_replay(replayed, 0, repair_started.elapsed());
                    self.metrics
                        .snapshot_resync_count
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                let cursor = candidate.source_health.last_source_seq;
                let rows = match client.replay_global_events(cursor, Some(page_limit)).await {
                    Ok(rows) => rows,
                    Err(error) => {
                        tracing::warn!(source_id, error = %error, "source gap replay failed");
                        break;
                    }
                };
                if rows.is_empty() {
                    break;
                }
                let row_count = rows.len();
                let mut page_progress = false;
                for row in rows {
                    if row.row_id <= candidate.source_health.last_source_seq.unwrap_or(0) {
                        continue;
                    }
                    let event = SourceEvent::from_runtime_event(
                        source_id.to_string(),
                        candidate.source_epoch.clone(),
                        row,
                    );
                    if !Self::stage_contiguous_event(&mut candidate, event) {
                        break;
                    }
                    replayed += 1;
                    page_progress = true;
                }
                if self.source_gap_overflowed(source_id).await {
                    break;
                }
                self.stage_pending_contiguous(source_id, &mut candidate)
                    .await;
                let recovery_reason =
                    "source gap filled by replay and authoritative snapshot resync";
                if self
                    .complete_gap_candidate(
                        source_id,
                        &attempt_epoch,
                        candidate.clone(),
                        recovery_reason,
                        false,
                    )
                    .await
                {
                    self.metrics
                        .record_replay(replayed, 0, repair_started.elapsed());
                    self.metrics
                        .snapshot_resync_count
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                if !page_progress || row_count < page_limit {
                    break;
                }
            }
        }

        match SourceBootstrap::from_runtime_client(&client, BootstrapOptions::default()).await {
            Ok(mut bootstrap) => {
                bootstrap.state.apply_source_config(&source);
                let mut fallback = bootstrap.state;
                self.stage_pending_contiguous(source_id, &mut fallback)
                    .await;
                if self
                    .complete_gap_candidate(
                        source_id,
                        &attempt_epoch,
                        fallback,
                        "source gap repaired by snapshot resync",
                        true,
                    )
                    .await
                {
                    self.metrics
                        .snapshot_resync_count
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                self.fail_gap_repair(
                    source_id,
                    &attempt_epoch,
                    "snapshot did not restore continuity",
                )
                .await;
            }
            Err(error) => {
                self.fail_gap_repair(
                    source_id,
                    &attempt_epoch,
                    &format!("snapshot resync failed: {error}"),
                )
                .await;
            }
        }
    }

    fn stage_contiguous_event(candidate: &mut MaterializedState, event: SourceEvent) -> bool {
        let cursor = candidate.source_health.last_source_seq.unwrap_or(0);
        if event.source_id != candidate.source_id
            || event.source_epoch != candidate.source_epoch
            || event.source_seq != cursor + 1
        {
            return false;
        }
        !candidate.reduce_source_event(event).duplicate
    }

    async fn stage_pending_contiguous(&self, source_id: &str, candidate: &mut MaterializedState) {
        loop {
            let cursor = candidate.source_health.last_source_seq.unwrap_or(0);
            let event = self
                .gap_repairs
                .lock()
                .await
                .get(source_id)
                .and_then(|queue| queue.pending.get(&(cursor + 1)).cloned());
            let Some(event) = event else {
                return;
            };
            if !Self::stage_contiguous_event(candidate, event) {
                return;
            }
        }
    }

    async fn complete_gap_candidate(
        &self,
        source_id: &str,
        expected_base_epoch: &str,
        mut candidate: MaterializedState,
        reason: &str,
        authoritative_fallback: bool,
    ) -> bool {
        let mut repairs = self.gap_repairs.lock().await;
        if candidate.source_id != source_id
            || candidate.source_epoch.is_empty()
            || candidate.source_health.source_epoch != candidate.source_epoch
        {
            return false;
        }
        let candidate_cursor = candidate.source_health.last_source_seq.unwrap_or(0);
        let mut materialized = self.materialized.write().await;
        let Some(current) = materialized.get(source_id) else {
            return false;
        };
        if current.source_epoch != expected_base_epoch {
            return false;
        }
        let current_cursor = current.source_health.last_source_seq.unwrap_or(0);
        {
            let Some(queue) = repairs.get_mut(source_id) else {
                return false;
            };
            if !queue.repairing {
                return false;
            }
            if candidate.source_epoch == current.source_epoch {
                if candidate_cursor < current_cursor
                    || !queue.covered_by(
                        &candidate.source_epoch,
                        candidate_cursor,
                        authoritative_fallback,
                    )
                {
                    return false;
                }
            } else if authoritative_fallback {
                queue.pending.clear();
                queue.overflowed = false;
                queue.highest_observed_seq = None;
            } else {
                return false;
            }
        }
        candidate.transition_source_health(SourceHealthState::Live, None);
        let Some(cursor) = candidate.cursor().map(|cursor| SourceCursor {
            source_id: cursor.source_id,
            source_epoch: cursor.source_epoch,
            source_seq: cursor.source_seq.max(0) as u64,
        }) else {
            return false;
        };
        let recovery_state = candidate.clone();
        materialized.insert(source_id.to_string(), candidate);
        repairs.remove(source_id);
        let _ = self.recoveries.send(SourceRecoverySignal::Resync {
            cursor,
            state: recovery_state,
            reason: reason.to_string(),
        });
        drop(materialized);
        drop(repairs);
        self.record_audit(
            "source.gap_repaired",
            Some(source_id.to_string()),
            json!({ "reason": reason }),
        )
        .await;
        true
    }

    pub(super) async fn install_epoch_replacement(
        &self,
        source_id: &str,
        expected_epoch: &str,
        state: MaterializedState,
    ) -> Option<i64> {
        if state.source_id != source_id
            || state.source_epoch != expected_epoch
            || state.source_health.source_epoch != expected_epoch
            || state.source_health.state != SourceHealthState::Live
        {
            return None;
        }
        let installed_cursor = state.source_health.last_source_seq.unwrap_or(0);
        let mut repairs = self.gap_repairs.lock().await;
        let mut materialized = self.materialized.write().await;
        materialized.insert(source_id.to_string(), state);
        repairs.remove(source_id);
        Some(installed_cursor)
    }

    async fn reduce_contiguous_event(&self, event: SourceEvent) -> bool {
        let patches = {
            let mut materialized = self.materialized.write().await;
            let state = materialized
                .entry(event.source_id.clone())
                .or_insert_with(|| MaterializedState::new(&event.source_id, &event.source_epoch));
            if state.source_epoch != event.source_epoch
                || state
                    .source_health
                    .last_source_seq
                    .is_some_and(|last| event.source_seq > last + 1)
            {
                return false;
            }
            let ingest_lag = now_ms().saturating_sub(event.created_at);
            self.metrics
                .event_ingest_lag_ms
                .store(ingest_lag as u64, Ordering::Relaxed);
            let started = Instant::now();
            let effect = state.reduce_source_event(event);
            self.metrics
                .materializer_reduce_time_ms
                .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
            if effect.duplicate {
                Vec::new()
            } else {
                effect.patches
            }
        };
        for patch in patches {
            self.publish_materialized_patch(patch).await;
        }
        true
    }

    async fn source_cursor_and_epoch(&self, source_id: &str) -> (Option<i64>, String) {
        self.materialized
            .read()
            .await
            .get(source_id)
            .map(|state| {
                (
                    state.source_health.last_source_seq,
                    state.source_epoch.clone(),
                )
            })
            .unwrap_or((None, "unavailable".to_string()))
    }

    fn pending_gap_event_limit(&self) -> usize {
        self.config.materializer.event_buffer_size.max(1)
    }

    async fn source_gap_overflowed(&self, source_id: &str) -> bool {
        self.gap_repairs
            .lock()
            .await
            .get(source_id)
            .is_some_and(|queue| queue.overflowed)
    }

    async fn fail_gap_repair(&self, source_id: &str, attempt_epoch: &str, reason: &str) {
        let (patch, superseded) = {
            let mut repairs = self.gap_repairs.lock().await;
            let mut materialized = self.materialized.write().await;
            let superseded = materialized
                .get(source_id)
                .is_none_or(|state| state.source_epoch != attempt_epoch);
            if superseded {
                (None, true)
            } else {
                if let Some(queue) = repairs.get_mut(source_id) {
                    queue.repairing = false;
                }
                let patch = materialized.get_mut(source_id).map(|state| {
                    state.transition_source_health(
                        SourceHealthState::GapDetected,
                        Some(reason.to_string()),
                    )
                });
                (patch, false)
            }
        };
        if let Some(patch) = patch {
            self.publish_materialized_patch(patch).await;
        }
        self.record_audit(
            if superseded {
                "source.gap_repair_superseded"
            } else {
                "source.gap_repair_failed"
            },
            Some(source_id.to_string()),
            json!({ "reason": reason }),
        )
        .await;
    }
}
