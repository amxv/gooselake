//! A live event subscription retains the same journal generation for its
//! readiness, cursor validation, replay and commit hints. Hints contain no
//! event data; retained health cannot be lost through broadcast lag.
use std::sync::Arc;

use tokio::sync::{broadcast, watch};

use crate::{RuntimeError, RuntimeEventRecord, RuntimeEventScope};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventObservationFailure {
    pub code: &'static str,
    pub message: String,
}

impl EventObservationFailure {
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: "event_observer_unavailable",
            message: message.into(),
        }
    }

    pub fn generation_changed(message: impl Into<String>) -> Self {
        Self {
            code: "journal_generation_changed",
            message: message.into(),
        }
    }

    pub fn runtime_error(&self) -> RuntimeError {
        RuntimeError::Bootstrap(format!("{}: {}", self.code, self.message))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventObserverHealth {
    Starting,
    Ready,
    Failed(EventObservationFailure),
}

/// Generation-local counters increment at the actual observer/replay call.
/// Taking a snapshot never probes SQLite or changes observer readiness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventObservationCounters {
    pub probe_attempts: u64,
    pub high_water_reads: u64,
    pub replay_reads: u64,
    pub write_hint_overflows: u64,
}

/// Implementations retain their observer and database generation. Every page
/// must fail if that generation can no longer be verified; reopening a path
/// for each page does not satisfy this contract. Calls may block and should
/// run on a blocking worker when the caller is asynchronous.
pub trait RuntimeEventReplayReader: Send + Sync {
    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError>;

    fn observation_counters(&self) -> EventObservationCounters;
}

pub struct EventCommitSubscription {
    pub commits: broadcast::Receiver<()>,
    pub health: watch::Receiver<EventObserverHealth>,
    pub replay: Arc<dyn RuntimeEventReplayReader>,
}

impl EventCommitSubscription {
    pub async fn ready(&mut self) -> Result<(), RuntimeError> {
        loop {
            let state = self.health.borrow_and_update().clone();
            match state {
                EventObserverHealth::Ready if self.health.has_changed().is_ok() => return Ok(()),
                EventObserverHealth::Failed(failure) => return Err(failure.runtime_error()),
                _ => {}
            }
            self.health.changed().await.map_err(|_| {
                EventObservationFailure::unavailable("event observer health channel closed")
                    .runtime_error()
            })?;
        }
    }

    /// A hint is not a readiness or health acknowledgment. Callers must also
    /// retain/observe health and read event payloads through `replay`.
    pub async fn recv(&mut self) -> Result<(), broadcast::error::RecvError> {
        self.commits.recv().await
    }

    pub fn try_recv(&mut self) -> Result<(), broadcast::error::TryRecvError> {
        self.commits.try_recv()
    }
}
