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
    let runtime: Arc<dyn RuntimeStore> = store.clone();

    // Open before commits but deliberately do not read from the SSE client.
    // The server's bounded output queue eventually backpressures the writer
    // while the 256-entry notification ring is overrun by later commits.
    let response = durable_event_stream(runtime, None, Some(0), 7, &HeaderMap::new())
        .await
        .unwrap()
        .into_response();
    let repository = SqliteRuntimeRepository::new(path.clone());
    const EVENTS: usize = 320;
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

    // A reopened SQLite store has the same retained data and can resume from
    // the final delivered global row ID without depending on the old bus.
    let restarted = SqliteRuntimeRepository::new(path);
    assert!(restarted
        .list_runtime_events(None, expected.last().copied(), 17)
        .unwrap()
        .is_empty());
}
