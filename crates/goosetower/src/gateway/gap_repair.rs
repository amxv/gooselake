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

    fn cover_through(&mut self, high_watermark: i64) {
        self.pending.retain(|seq, _| *seq > high_watermark);
        if self
            .highest_observed_seq
            .is_some_and(|highest| high_watermark >= highest)
        {
            self.overflowed = false;
        }
        if self.pending.is_empty() && !self.overflowed {
            self.highest_observed_seq = None;
        }
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
        let Some(source) = self
            .config
            .runtimes
            .sources
            .iter()
            .find(|source| source.enabled && source.source_id == source_id)
            .cloned()
        else {
            self.finish_gap_repair(source_id, false, "runtime source unavailable")
                .await;
            return;
        };
        let client = match runtime_client_from_source(&self.config, &source) {
            Ok(client) => client,
            Err(error) => {
                self.finish_gap_repair(source_id, false, &error.to_string())
                    .await;
                return;
            }
        };
        let page_limit = self.config.replay.max_events_per_request.max(1);
        let repair_started = Instant::now();
        let mut replayed = 0usize;
        let current_epoch = self.source_cursor_and_epoch(source_id).await.1;
        let requires_epoch_rebase = self
            .gap_repairs
            .lock()
            .await
            .get(source_id)
            .is_some_and(|queue| queue.requires_epoch_rebase(&current_epoch));

        if !requires_epoch_rebase {
            for _ in 0..32 {
                if self.source_gap_overflowed(source_id).await {
                    break;
                }
                if self.drain_pending_contiguous(source_id).await {
                    self.metrics
                        .record_replay(replayed, 0, repair_started.elapsed());
                    self.finish_gap_repair(source_id, true, "source gap filled by replay")
                        .await;
                    return;
                }
                let (cursor, epoch) = self.source_cursor_and_epoch(source_id).await;
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
                for row in rows {
                    let event =
                        SourceEvent::from_runtime_event(source_id.to_string(), epoch.clone(), row);
                    if !self.reduce_contiguous_event(event).await {
                        break;
                    }
                    replayed += 1;
                }
                if self.source_gap_overflowed(source_id).await {
                    break;
                }
                if row_count < page_limit && !self.drain_pending_contiguous(source_id).await {
                    break;
                }
            }
        }

        match SourceBootstrap::from_runtime_client(&client, BootstrapOptions::default()).await {
            Ok(mut bootstrap) => {
                bootstrap.state.apply_source_config(&source);
                let next_epoch = bootstrap.state.source_epoch.clone();
                let next_cursor = bootstrap.state.source_health.last_source_seq.unwrap_or(0);
                let old_epoch = self.source_cursor_and_epoch(source_id).await.1;
                let installed = self.install_gap_fallback(source_id, bootstrap.state).await;
                if installed {
                    if let Some(queue) = self.gap_repairs.lock().await.get_mut(source_id) {
                        if old_epoch != next_epoch {
                            queue.pending.clear();
                            queue.overflowed = false;
                            queue.highest_observed_seq = None;
                        } else {
                            queue.cover_through(next_cursor);
                        }
                    }
                }
                if installed && self.drain_pending_contiguous(source_id).await {
                    self.metrics
                        .snapshot_resync_count
                        .fetch_add(1, Ordering::Relaxed);
                    self.finish_gap_repair(
                        source_id,
                        true,
                        "source gap repaired by snapshot resync",
                    )
                    .await;
                    return;
                }
                self.finish_gap_repair(source_id, false, "snapshot did not restore continuity")
                    .await;
            }
            Err(error) => {
                self.finish_gap_repair(
                    source_id,
                    false,
                    &format!("snapshot resync failed: {error}"),
                )
                .await;
            }
        }
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

    async fn drain_pending_contiguous(&self, source_id: &str) -> bool {
        loop {
            if self.source_gap_overflowed(source_id).await {
                return false;
            }
            let cursor = self.source_cursor_and_epoch(source_id).await.0.unwrap_or(0);
            let event = {
                let mut repairs = self.gap_repairs.lock().await;
                let Some(queue) = repairs.get_mut(source_id) else {
                    return true;
                };
                queue.pending.retain(|seq, _| *seq > cursor);
                queue.pending.remove(&(cursor + 1))
            };
            match event {
                Some(event) => {
                    if self.reduce_contiguous_event(event.clone()).await {
                        continue;
                    }
                    self.gap_repairs
                        .lock()
                        .await
                        .entry(source_id.to_string())
                        .or_default()
                        .insert(event, self.pending_gap_event_limit());
                    return false;
                }
                None => {
                    return self
                        .gap_repairs
                        .lock()
                        .await
                        .get(source_id)
                        .is_none_or(|queue| queue.pending.is_empty() && !queue.overflowed);
                }
            }
        }
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

    async fn install_gap_fallback(&self, source_id: &str, next: MaterializedState) -> bool {
        let mut materialized = self.materialized.write().await;
        let current_cursor = materialized
            .get(source_id)
            .and_then(|state| state.source_health.last_source_seq)
            .unwrap_or(0);
        let next_cursor = next.source_health.last_source_seq.unwrap_or(0);
        if next.source_epoch
            == materialized
                .get(source_id)
                .map(|state| state.source_epoch.as_str())
                .unwrap_or(next.source_epoch.as_str())
            && next_cursor < current_cursor
        {
            return false;
        }
        materialized.insert(source_id.to_string(), next);
        true
    }

    async fn finish_gap_repair(&self, source_id: &str, restored: bool, reason: &str) {
        let patch = {
            let mut repairs = self.gap_repairs.lock().await;
            if restored {
                repairs.remove(source_id);
            } else if let Some(queue) = repairs.get_mut(source_id) {
                queue.repairing = false;
            }
            let mut materialized = self.materialized.write().await;
            materialized.get_mut(source_id).map(|state| {
                state.transition_source_health(
                    if restored {
                        SourceHealthState::Live
                    } else {
                        SourceHealthState::GapDetected
                    },
                    (!restored).then(|| reason.to_string()),
                )
            })
        };
        if let Some(patch) = patch {
            self.publish_materialized_patch(patch).await;
        }
        if restored {
            let signal = if reason.contains("snapshot resync") {
                SourceRecoverySignal::Resync {
                    source_id: source_id.to_string(),
                    reason: reason.to_string(),
                }
            } else if let Some(cursor) =
                self.materialized
                    .read()
                    .await
                    .get(source_id)
                    .and_then(|state| {
                        state.cursor().map(|cursor| SourceCursor {
                            source_id: cursor.source_id,
                            source_epoch: cursor.source_epoch,
                            source_seq: cursor.source_seq.max(0) as u64,
                        })
                    })
            {
                SourceRecoverySignal::Filled(cursor)
            } else {
                return;
            };
            let _ = self.recoveries.send(signal);
        }
        self.record_audit(
            if restored {
                "source.gap_repaired"
            } else {
                "source.gap_repair_failed"
            },
            Some(source_id.to_string()),
            json!({ "reason": reason }),
        )
        .await;
    }
}
