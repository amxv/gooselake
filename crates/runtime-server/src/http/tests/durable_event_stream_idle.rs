use super::*;
use async_trait::async_trait;
use axum::response::IntoResponse;
use runtime_core::{
    ApprovalRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent,
    ProcessRecord, RuntimeError, RuntimeEventRecord, RuntimeEventScope, RuntimeHydratedState,
    SessionRecord, TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord,
    TeamOperationDiagnosticRecord, TeamOperationJournalRecord, TeamRecord, TurnRecord,
};
use runtime_store_sqlite::{SqliteRuntimeRepository, SqliteRuntimeStore, SqliteStoreConfig};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Test-only proxy over the *real* SQLite runtime store. The observed counter
/// measures every journal SELECT requested by the actual SSE path, so a
/// regression to 250ms polling fails without fake/manual event feeds.
struct CountingEventStore {
    sqlite: Arc<SqliteRuntimeStore>,
    reads: AtomicUsize,
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

    fn subscribe_event_commits(&self) -> Option<tokio::sync::broadcast::Receiver<()>> {
        self.sqlite.subscribe_event_commits()
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
        reads: AtomicUsize::new(0),
    });
    let runtime_store: Arc<dyn RuntimeStore> = store.clone();

    // A cursor at zero on an empty journal performs exactly one initial
    // SELECT and then sleeps on the commit notifier, not a 250ms DB timer.
    let sse = durable_event_stream(runtime_store, None, Some(0), 2, &HeaderMap::new())
        .await
        .expect("start global SSE");
    let mut chunks = sse.into_response().into_body().into_data_stream();
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        1,
        "idle SSE must not periodically SELECT from SQLite"
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
