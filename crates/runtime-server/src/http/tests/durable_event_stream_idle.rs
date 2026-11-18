use super::*;
use async_trait::async_trait;
use axum::response::IntoResponse;
use runtime_core::{
    ApprovalRecord, EventCommitSubscription, EventObservationCounters, ManagedWorktreeClaimRecord,
    ManagedWorktreeRecord, NewRuntimeEvent, ProcessRecord, RuntimeError, RuntimeEventRecord,
    RuntimeEventReplayReader, RuntimeEventScope, RuntimeHydratedState, SessionRecord,
    TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord,
    TeamOperationJournalRecord, TeamRecord, TurnRecord,
};
use runtime_store_sqlite::{SqliteRuntimeRepository, SqliteRuntimeStore, SqliteStoreConfig};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_stream::StreamExt;

mod bound;
mod cancellation;
mod control;
mod handoffs;
mod health;
use control::{ReadPhase, ReplayControl};

/// Test-only proxy over the *real* SQLite runtime store. The observed counter
/// measures every replay call requested by the actual SSE path, so a
/// regression to 250ms polling fails without fake/manual event feeds.
struct CountingEventStore {
    sqlite: Arc<SqliteRuntimeStore>,
    reads: Arc<AtomicUsize>,
    fail_next_read: Arc<AtomicBool>,
    control: Option<Arc<ReplayControl>>,
}

struct CountingReplayReader {
    inner: Arc<dyn RuntimeEventReplayReader>,
    reads: Arc<AtomicUsize>,
    fail_next_read: Arc<AtomicBool>,
    control: Option<Arc<ReplayControl>>,
    // Field order matters: acknowledge only after the actual inner replay
    // lease and every other wrapper field have been dropped.
    _lifetime: Option<control::ReaderLifetime>,
}

impl RuntimeEventReplayReader for CountingReplayReader {
    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        let call = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(control) = &self.control {
            control.visit(ReadPhase::Before(call));
        }
        let result = if self.fail_next_read.swap(false, Ordering::SeqCst) {
            Err(RuntimeError::Bootstrap(
                "test-injected durable journal read failure".into(),
            ))
        } else {
            self.inner.list_runtime_events(scope, after_seq, limit)
        };
        if let Some(control) = &self.control {
            control.visit(ReadPhase::After(call));
        }
        result
    }

    fn list_runtime_events_bound(
        &self,
        collection: &runtime_core::EventCollection,
        after: Option<&runtime_core::EventCursor>,
        limit: usize,
    ) -> Result<runtime_core::BoundEventPage<RuntimeEventRecord>, runtime_core::EventReadError>
    {
        let call = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(control) = &self.control {
            control.visit(ReadPhase::Before(call));
        }
        let result = if self.fail_next_read.swap(false, Ordering::SeqCst) {
            Err(RuntimeError::Bootstrap("test-injected durable journal read failure".into()).into())
        } else {
            self.inner
                .list_runtime_events_bound(collection, after, limit)
        };
        if let Some(control) = &self.control {
            control.visit(ReadPhase::After(call));
        }
        result
    }

    fn observation_counters(&self) -> EventObservationCounters {
        self.inner.observation_counters()
    }
}

impl runtime_core::WorkspaceWorktreeCreationStore for CountingEventStore {}
impl runtime_core::OperatorUserTurnStore for CountingEventStore {}

#[async_trait]
impl RuntimeStore for CountingEventStore {
    async fn initialize(&self) -> Result<(), RuntimeError> {
        self.sqlite.initialize().await
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        self.sqlite.healthcheck().await
    }

    fn subscribe_event_commits(&self) -> Result<EventCommitSubscription, RuntimeError> {
        if let Some(control) = &self.control {
            control.visit(ReadPhase::Subscribe);
        }
        let mut subscription = self.sqlite.subscribe_event_commits()?;
        if let Some(control) = &self.control {
            if let Some(health) = control.health() {
                subscription.health = health;
            }
            control.reader_created();
        }
        subscription.replay = Arc::new(CountingReplayReader {
            inner: subscription.replay,
            reads: self.reads.clone(),
            fail_next_read: self.fail_next_read.clone(),
            control: self.control.clone(),
            _lifetime: self
                .control
                .as_ref()
                .map(|control| control.reader_lifetime()),
        });
        Ok(subscription)
    }

    fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.sqlite.append_runtime_event(event)
    }

    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_read.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Bootstrap(
                "test-injected durable journal read failure".into(),
            ));
        }
        self.sqlite.list_runtime_events(scope, after_seq, limit)
    }

    // The SSE handler must never read or mutate unrelated runtime authority.
    fn upsert_session(&self, _: &SessionRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a session")
    }
    fn upsert_turn(&self, _: &TurnRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a turn")
    }
    fn upsert_approval(&self, _: &ApprovalRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate an approval")
    }
    fn upsert_team(&self, _: &TeamRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a team")
    }
    fn upsert_team_member(&self, _: &TeamMemberRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a member")
    }
    fn delete_team_member(&self, _: &str, _: &str) -> Result<(), RuntimeError> {
        unreachable!("event stream may not delete a member")
    }
    fn upsert_team_message(&self, _: &TeamMessageRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a message")
    }
    fn upsert_team_delivery(&self, _: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a delivery")
    }
    fn upsert_managed_worktree(&self, _: &ManagedWorktreeRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a worktree")
    }
    fn upsert_managed_worktree_claim(
        &self,
        _: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a claim")
    }
    fn upsert_process(&self, _: &ProcessRecord) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate a process")
    }
    fn upsert_team_operation_journal(
        &self,
        _: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError> {
        unreachable!("event stream may not mutate the operation journal")
    }
    fn append_team_operation_diagnostic(
        &self,
        _: Option<&str>,
        _: Option<&str>,
        _: &str,
        _: &str,
        _: &Value,
        _: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
        unreachable!("event stream may not append diagnostic state")
    }
    fn list_team_operation_journal(
        &self,
        _: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
        unreachable!("event stream may not scan legacy operations")
    }
    fn list_team_operation_diagnostics(
        &self,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
        unreachable!("event stream may not scan legacy diagnostics")
    }
    fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError> {
        unreachable!("event stream may not hydrate the whole runtime")
    }
}

#[tokio::test]
async fn idle_durable_sse_does_not_requery_sqlite_until_a_commit_wakes_replay() {
    let temp = tempfile::tempdir().expect("isolated SQLite fixture");
    let path = temp.path().join("runtime.sqlite3");
    let sqlite = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    }));
    sqlite.initialize().await.expect("initialize SQLite");
    let store = Arc::new(CountingEventStore {
        sqlite,
        reads: Arc::new(AtomicUsize::new(0)),
        fail_next_read: Arc::new(AtomicBool::new(false)),
        control: None,
    });
    let runtime_store: Arc<dyn RuntimeStore> = store.clone();
    // This extra lease is a passive counter meter, used only in the idle test.
    let mut meter = store.sqlite.subscribe_event_commits().unwrap();
    timeout(Duration::from_secs(3), meter.ready())
        .await
        .unwrap()
        .unwrap();

    // A cursor at zero on an empty journal performs exactly one initial
    // SELECT and then sleeps on the commit notifier, not a 250ms DB timer.
    let sse = durable_event_stream(runtime_store, None, Some(0), 2, &HeaderMap::new())
        .await
        .expect("start global SSE");
    let mut chunks = sse.into_response().into_body().into_data_stream();
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    let first_keepalive = timeout(Duration::from_secs(12), chunks.next())
        .await
        .expect("first real HTTP keepalive")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first_keepalive).starts_with(':'));
    let idle_before = meter.replay.observation_counters();
    let second_keepalive = timeout(Duration::from_secs(12), chunks.next())
        .await
        .expect("second real HTTP keepalive")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&second_keepalive).starts_with(':'));
    assert_eq!(meter.replay.observation_counters(), idle_before,
        "observer probes, high-water reads and replay reads stay unchanged across a full real keepalive interval");
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        1,
        "idle SSE must not periodically SELECT from SQLite, including keepalives"
    );

    // An independent SQLite repository connection commits the canonical
    // journal row. The stream wakes, re-SELECTs exactly once, and delivers
    // that durable global row ID rather than an in-memory event payload.
    let repository = SqliteRuntimeRepository::new(path);
    let first = repository
        .append_runtime_event(&super::durable_event_stream_routes::new_event(
            "idle_wakeup_first",
            RuntimeEventScope::System,
            "global",
        ))
        .expect("first independent commit");
    let first_bytes = timeout(Duration::from_secs(3), chunks.next())
        .await
        .expect("committed event should wake SSE")
        .expect("SSE should remain open")
        .expect("SSE body bytes");
    assert!(
        String::from_utf8_lossy(&first_bytes).contains(&format!("id: {}\n", first.row_id)),
        "wrong event cursor for first commit"
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 2);
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        2,
        "after delivering a committed row, SSE must again stay idle"
    );

    let second = repository
        .append_runtime_event(&super::durable_event_stream_routes::new_event(
            "idle_wakeup_second",
            RuntimeEventScope::System,
            "global",
        ))
        .expect("second independent commit");
    let next_bytes = timeout(Duration::from_secs(3), chunks.next())
        .await
        .expect("second commit should wake SSE")
        .expect("SSE should remain open")
        .expect("SSE body bytes");
    assert!(
        String::from_utf8_lossy(&next_bytes).contains(&format!("id: {}\n", second.row_id)),
        "wrong event cursor for second commit"
    );
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        3,
        "exactly one indexed catchup query per committed wakeup"
    );
    drop(chunks);
}

#[tokio::test]
async fn journal_read_failure_emits_unsequenced_retryable_error_before_durable_reconnect() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let sqlite = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    }));
    sqlite.initialize().await.unwrap();
    let repository = SqliteRuntimeRepository::new(path.clone());
    let seed = repository
        .append_runtime_event(&super::durable_event_stream_routes::new_event(
            "fault_seed",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let store = Arc::new(CountingEventStore {
        sqlite,
        reads: Arc::new(AtomicUsize::new(0)),
        fail_next_read: Arc::new(AtomicBool::new(false)),
        control: None,
    });
    let observed: Arc<dyn RuntimeStore> = store.clone();
    let sse = durable_event_stream(observed, None, Some(0), 10, &HeaderMap::new())
        .await
        .unwrap();
    let mut chunks = sse.into_response().into_body().into_data_stream();
    let first = timeout(Duration::from_secs(3), chunks.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains(&format!("id: {}\n", seed.row_id)));

    // Fail only the next real journal catch-up SELECT. The committed row
    // remains readable by a new connection after this stream terminates.
    store.fail_next_read.store(true, Ordering::SeqCst);
    let unread = repository
        .append_runtime_event(&super::durable_event_stream_routes::new_event(
            "fault_unread",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    let failure = timeout(Duration::from_secs(3), chunks.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let failure = String::from_utf8_lossy(&failure);
    assert!(failure.contains("event: stream.error\n"), "{failure}");
    assert!(failure.contains("journal_unavailable"), "{failure}");
    assert!(failure.contains(r#""retryable":true"#), "{failure}");
    assert!(
        !failure.lines().any(|line| line.starts_with("id:")),
        "a failed read must never advance Last-Event-ID: {failure}"
    );
    assert!(
        timeout(Duration::from_secs(3), chunks.next())
            .await
            .unwrap()
            .is_none(),
        "failed stream must close and force durable reconnect"
    );
    assert_eq!(store.reads.load(Ordering::SeqCst), 2);

    let reopened: Arc<dyn RuntimeStore> = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path,
    }));
    let reconnected =
        durable_event_stream(reopened, None, Some(seed.row_id), 10, &HeaderMap::new())
            .await
            .unwrap();
    super::durable_event_stream_routes::observed_sse_ids(
        reconnected.into_response(),
        &[unread.row_id],
    )
    .await;
}
