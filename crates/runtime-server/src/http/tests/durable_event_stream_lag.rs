use super::*;
use axum::response::IntoResponse;
use runtime_core::RuntimeStore;
use runtime_store_sqlite::{SqliteRuntimeRepository, SqliteRuntimeStore, SqliteStoreConfig};
use tokio::sync::broadcast::error::TryRecvError;

use super::durable_event_stream_routes::new_event;

#[tokio::test]
async fn lagged_notifications_and_bounded_stream_backpressure_preserve_all_committed_rows() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    }));
    store.initialize().await.unwrap();
    let mut lag_monitor = store.subscribe_event_commits().unwrap();
    let mut sentinel = store.subscribe_event_commits().unwrap();
    timeout(Duration::from_secs(3), lag_monitor.ready())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), sentinel.ready())
        .await
        .unwrap()
        .unwrap();
    let runtime: Arc<dyn RuntimeStore> = store.clone();
    let (producer, headers) = super::super::durable_event_stream_test_control::register();

    // Open before commits but deliberately do not read from the SSE client.
    // The server's bounded output queue eventually backpressures the writer
    // while the 256-entry notification ring is overrun by later commits.
    let response = durable_event_stream(runtime, None, Some(0), 7, &headers)
        .await
        .unwrap()
        .into_response();
    let repository = SqliteRuntimeRepository::new(path.clone());
    const FILL_ROWS: usize = 129;
    const LAG_ROWS: usize = 257;
    const EVENTS: usize = FILL_ROWS + LAG_ROWS;
    let mut expected = Vec::with_capacity(EVENTS);
    for i in 0..EVENTS {
        let committed = repository
            .append_runtime_event(&new_event(
                &format!("lagged_{i:04}"),
                RuntimeEventScope::System,
                "global",
            ))
            .unwrap();
        expected.push(committed.row_id);
        // Filesystem hints may coalesce. A real received batch must separate
        // every append; row count alone cannot prove notification-ring lag.
        timeout(Duration::from_secs(3), sentinel.recv())
            .await
            .expect("actual observer publication for each committed batch")
            .unwrap();
        if i + 1 == FILL_ROWS {
            // No client frame has been consumed. Actual Pending on the
            // full 128-frame queue fixes the SSE receiver's position before
            // the next 257 independently observed publication batches.
            producer.pending().await;
        }
    }
    assert_eq!(expected, (1..=EVENTS as i64).collect::<Vec<_>>());
    assert!(
        matches!(
            lag_monitor.try_recv(),
            Err(TryRecvError::Lagged(skipped)) if skipped > 0
        ),
        "a deliberately stalled notification subscriber must overrun the broadcast ring"
    );

    // Resume reading. Event payloads and cursors come from indexed SQLite
    // pages, never from discarded notifications or the bounded output queue.
    let mut chunks = response.into_body().into_data_stream();
    let mut received = Vec::new();
    let mut buffered = String::new();
    while received.len() < EVENTS {
        let bytes = timeout(Duration::from_secs(5), chunks.next())
            .await
            .expect("SSE drain stalled")
            .expect("SSE closed before replay completed")
            .expect("SSE body error");
        buffered.push_str(&String::from_utf8_lossy(&bytes));
        while let Some(end) = buffered.find("\n\n") {
            let frame = buffered[..end].to_string();
            buffered.drain(..end + 2);
            if let Some(id) = frame.lines().find_map(|line| line.strip_prefix("id: ")) {
                received.push(id.parse::<i64>().expect("numeric SSE cursor"));
            }
        }
    }
    assert_eq!(
        received, expected,
        "no gaps, duplicates, or reordering under lag/backpressure"
    );
    assert!(buffered.is_empty());
    // This passive acknowledgement belongs to the actual SSE receiver,
    // not the separate lag monitor, and cannot wake or repair the stream.
    producer.lagged().await;
    // Observe the real keepalive only after the full original burst arrived.
    // Unlike a later database write, it cannot rescue a missing live wake.
    let tail = timeout(Duration::from_secs(12), chunks.next())
        .await
        .expect("post-drain keepalive")
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&tail).starts_with(':'),
        "a trailing duplicate would appear before the keepalive: {tail:?}"
    );
    drop(chunks);
    producer.finished().await;

    // A reopened SQLite store has the same retained data and can resume from
    // the final delivered global row ID without depending on the old bus.
    let restarted = SqliteRuntimeRepository::new(path);
    assert!(restarted
        .list_runtime_events(None, expected.last().copied(), 17)
        .unwrap()
        .is_empty());
}
