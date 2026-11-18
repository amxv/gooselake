use super::super::operator_event_cursor_wire::{drain_bound, read_bound_record};
use super::control::*;
use super::*;
use crate::http::durable_event_stream_test_control::register;
use runtime_core::{EventCollection, EventCursor, EventObserverHealth, RuntimeStore};
use tokio::sync::broadcast::error::TryRecvError;

#[tokio::test(flavor = "current_thread")]
async fn bound_observed_raw_token_survives_a_fresh_store_instance() {
    let fixture = Fixture::new(None).await;
    let first = fixture.append("bound_restart_first");
    let second = fixture.append("bound_restart_second");
    assert_eq!(second.row_id, first.row_id + 1);
    let collection = EventCollection::stream(None).unwrap();
    let old_store = Arc::downgrade(&fixture.store.sqlite);
    let (producer, headers) = register();
    let response = fixture.open_bound(None, 1, &headers).await;
    let mut chunks = response.into_body().into_data_stream();
    let mut pending = Vec::new();
    let saved_raw =
        read_bound_record(&mut chunks, &mut pending, &collection, first.row_id, &first).await;
    drop(chunks);
    producer.finished().await;
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
    let Fixture {
        temp: _temp,
        path,
        store,
        control,
    } = fixture;
    drop(store);
    drop(control);
    assert!(
        old_store.upgrade().is_none(),
        "original SQLite store object survived"
    );
    let reopened = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    }));
    reopened.initialize().await.unwrap();
    let repository = SqliteRuntimeRepository::new(path);
    let third = repository
        .append_runtime_event(&super::super::durable_event_stream_routes::new_event(
            "bound_restart_third",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    assert_eq!(third.row_id, second.row_id + 1);
    let saved = EventCursor::decode(&saved_raw).unwrap();
    let original_epoch = saved.epoch().clone();
    let (resumed, headers) = register();
    let response =
        checked_durable_event_stream(reopened, collection.clone(), Some(saved), 2, &headers)
            .await
            .unwrap()
            .into_response();
    let mut chunks = response.into_body().into_data_stream();
    let mut pending = Vec::new();
    for record in [&second, &third] {
        let raw = read_bound_record(
            &mut chunks,
            &mut pending,
            &collection,
            record.row_id,
            record,
        )
        .await;
        assert_eq!(EventCursor::decode(&raw).unwrap().epoch(), &original_epoch);
    }
    assert!(pending.is_empty());
    drop(chunks);
    resumed.finished().await;
}

#[tokio::test]
async fn bound_stream_actual_lag_and_backpressure_preserve_committed_records_and_tokens() {
    let fixture = Fixture::new(None).await;
    let mut lag_monitor = fixture.store.sqlite.subscribe_event_commits().unwrap();
    let mut sentinel = fixture.store.sqlite.subscribe_event_commits().unwrap();
    timeout(Duration::from_secs(3), lag_monitor.ready())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), sentinel.ready())
        .await
        .unwrap()
        .unwrap();
    let collection = EventCollection::stream(None).unwrap();
    let (producer, headers) = register();
    let response = fixture.open_bound(None, 7, &headers).await;
    const FILL: usize = 129;
    const LAG: usize = 257;
    const TOTAL: usize = FILL + LAG;
    let mut committed = Vec::with_capacity(TOTAL);
    for index in 0..TOTAL {
        committed.push(fixture.append(&format!("bound_lag_{index:04}")));
        timeout(Duration::from_secs(3), sentinel.recv())
            .await
            .expect("actual observer publication for each commit")
            .unwrap();
        if index + 1 == FILL {
            // No body polling before the actual full-output Pending ACK.
            producer.pending().await;
        }
    }
    assert_eq!(
        committed
            .iter()
            .map(|record| record.row_id)
            .collect::<Vec<_>>(),
        (1..=TOTAL as i64).collect::<Vec<_>>()
    );
    assert!(matches!(lag_monitor.try_recv(), Err(TryRecvError::Lagged(skipped)) if skipped > 0));
    let mut chunks = response.into_body().into_data_stream();
    let mut pending = Vec::new();
    timeout(Duration::from_secs(20), async {
        for record in &committed {
            read_bound_record(
                &mut chunks,
                &mut pending,
                &collection,
                record.row_id,
                record,
            )
            .await;
        }
    })
    .await
    .expect("finite drain of original committed burst");
    assert!(
        pending.is_empty(),
        "extra buffered frame after the exact committed burst"
    );
    // This acknowledgement belongs to the actual SSE receiver, not the meter.
    producer.lagged().await;
    let tail = timeout(Duration::from_secs(12), chunks.next())
        .await
        .expect("post-burst keepalive")
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&tail).starts_with(':'),
        "trailing event would precede keepalive: {tail:?}"
    );
    drop(chunks);
    producer.finished().await;
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn bound_reconnect_uses_last_consumed_raw_token_after_server_queue_runs_ahead() {
    let fixture = Fixture::new(Some(EventObserverHealth::Ready)).await;
    let committed: Vec<_> = (0..130)
        .map(|index| fixture.append(&format!("bound_unread_{index:03}")))
        .collect();
    let collection = EventCollection::stream(None).unwrap();
    let (producer, headers) = register();
    let response = fixture.open_bound(None, 1, &headers).await;
    producer.pending().await;
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), 129);
    let mut chunks = response.into_body().into_data_stream();
    let mut pending = Vec::new();
    let saved_raw = read_bound_record(
        &mut chunks,
        &mut pending,
        &collection,
        committed[0].row_id,
        &committed[0],
    )
    .await;
    assert!(pending.is_empty());
    drop(chunks);
    producer.finished().await;
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
    // Decode only the bytes consumed by the client, never a new epoch/position.
    let saved = EventCursor::decode(&saved_raw).unwrap();
    assert_eq!(saved.position(), committed[0].row_id);
    let (resumed, headers) = register();
    let response = fixture.open_bound(Some(saved), 7, &headers).await;
    let mut chunks = response.into_body().into_data_stream();
    let mut pending = Vec::new();
    timeout(Duration::from_secs(10), async {
        for record in &committed[1..] {
            read_bound_record(
                &mut chunks,
                &mut pending,
                &collection,
                record.row_id,
                record,
            )
            .await;
        }
    })
    .await
    .expect("finite unread replay from the consumed raw token");
    fixture.control.fail();
    resumed.finished().await;
    fixture
        .control
        .wait_count(&fixture.control.dropped, 2)
        .await;
    let tail = drain_bound(&mut chunks, &mut pending).await;
    assert!(
        !tail.lines().any(|line| line.starts_with("id:")),
        "trailing duplicate or sequenced failure: {tail}"
    );
    assert_terminal(&tail, "event_observer_unavailable");
}
