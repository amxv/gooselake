//! Replay through the observer's retained SQLite connection and generation.
#[cfg(test)]
use super::Phase;
use super::{unavailable, verified_epoch, EventObserver, Failure};
use crate::repository::list_runtime_events_on_connection;
use crate::repository_event_cursor::validate_cursor;
use runtime_core::{
    BoundEventPage, EventCollection, EventCursor, EventEpoch, EventObservationCounters,
    EventObserverHealth, EventReadError, RuntimeError, RuntimeEventRecord,
    RuntimeEventReplayReader, RuntimeEventScope,
};
use rusqlite::{Transaction, TransactionBehavior};
use std::sync::atomic::Ordering;

impl EventObserver {
    fn replay_failure(&self, failure: Failure) -> RuntimeError {
        self.shared.fail(failure.clone());
        self.shared
            .healthy()
            .err()
            .unwrap_or(failure)
            .runtime_error()
    }

    fn read_snapshot<T, E>(
        &self,
        read: impl FnOnce(&Transaction<'_>, &EventEpoch) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<RuntimeError>,
    {
        if !matches!(*self.shared.health.borrow(), EventObserverHealth::Ready) {
            return Err(E::from(
                self.shared
                    .healthy()
                    .err()
                    .unwrap_or_else(|| Failure::unavailable("event observer is not ready"))
                    .runtime_error(),
            ));
        }
        let mut state = self.shared.state.lock().map_err(|_| {
            E::from(self.replay_failure(Failure::unavailable("event observer state poisoned")))
        })?;
        self.shared
            .scan(&mut state, true)
            .map_err(|failure| E::from(failure.runtime_error()))?;
        let captured = state.epoch.clone().ok_or_else(|| {
            E::from(self.replay_failure(Failure::unavailable(
                "ready event observer has no captured journal epoch",
            )))
        })?;
        self.shared
            .counters
            .replay_reads
            .fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        self.shared
            .control
            .checkpoint(Phase::BeforeReplay)
            .map_err(|failure| E::from(self.replay_failure(failure)))?;
        let transaction =
            Transaction::new_unchecked(&state.connection, TransactionBehavior::Deferred).map_err(
                |error| {
                    E::from(self.replay_failure(unavailable("begin event replay snapshot", error)))
                },
            )?;
        let epoch = verified_epoch(&transaction, Some(&captured))
            .map_err(|failure| E::from(self.replay_failure(failure)))?;
        #[cfg(test)]
        self.shared
            .control
            .checkpoint(Phase::AfterReplayEpoch)
            .map_err(|failure| E::from(self.replay_failure(failure)))?;
        // A request-local rejection cannot hide an actual generation change.
        let result = read(&transaction, &epoch);
        // Close the snapshot before the fresh identity read. The State lock
        // still excludes the worker and retains every consumed write hint.
        transaction.rollback().map_err(|error| {
            E::from(self.replay_failure(unavailable("release event replay snapshot", error)))
        })?;
        #[cfg(test)]
        self.shared
            .control
            .checkpoint(Phase::AfterReplay)
            .map_err(|failure| E::from(self.replay_failure(failure)))?;
        self.shared
            .scan(&mut state, true)
            .map_err(|failure| E::from(failure.runtime_error()))?;
        verified_epoch(&state.connection, Some(&captured))
            .map_err(|failure| E::from(self.replay_failure(failure)))?;
        self.shared
            .scan(&mut state, true)
            .map_err(|failure| E::from(failure.runtime_error()))?;
        result
    }
}

impl RuntimeEventReplayReader for EventObserver {
    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.read_snapshot(|transaction, _epoch| {
            list_runtime_events_on_connection(transaction, scope, after_seq, limit)
        })
    }

    fn list_runtime_events_bound(
        &self,
        collection: &EventCollection,
        after: Option<&EventCursor>,
        limit: usize,
    ) -> Result<BoundEventPage<RuntimeEventRecord>, EventReadError> {
        self.read_snapshot(|transaction, epoch| {
            collection.require_stream()?;
            validate_cursor(transaction, epoch, collection, after)?;
            let rows = list_runtime_events_on_connection(
                transaction,
                collection.scope(),
                after.map(EventCursor::position),
                limit,
            )?;
            Ok(BoundEventPage {
                epoch: epoch.clone(),
                rows,
            })
        })
    }

    fn observation_counters(&self) -> EventObservationCounters {
        self.shared.counters.snapshot()
    }
}
