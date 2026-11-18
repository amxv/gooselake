use super::control::{ids, next, Fixture};
use super::*;

#[tokio::test]
async fn external_commit_after_initial_sql_result_crosses_http_handoff_once() {
    let fixture = Fixture::new(None).await;
    let held_result = fixture.control.arm(ReadPhase::After(1));
    let (producer, headers) = super::super::super::durable_event_stream_test_control::register();
    let store = fixture.store.clone();
    let opening = tokio::spawn(async move {
        durable_event_stream(store, None, Some(0), 2, &headers)
            .await
            .unwrap()
            .into_response()
    });

    // The real first SELECT has finished but its empty result is held before
    // the HTTP handler can start the producer. This is a causal handoff
    // boundary, not a scheduling delay or synthetic observer notification.
    held_result.reached().await;
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), 1);
    assert!(!producer.has_started());
    let reader = SqliteRuntimeRepository::new(fixture.path.clone());
    assert!(reader
        .list_runtime_events(None, None, 2)
        .unwrap()
        .is_empty());
    let path = fixture.path.clone();
    tokio::task::spawn_blocking(move || {
        super::super::durable_event_stream_cross_process::commit_from_child(&path);
    })
    .await
    .unwrap();
    let committed = reader.list_runtime_events(None, None, 2).unwrap();
    assert_eq!(committed.len(), 1, "the independent child must execute");
    assert_eq!(committed[0].event_id, "external_sse_commit");
    assert_eq!(committed[0].scope, RuntimeEventScope::Workspace);
    assert_eq!(committed[0].row_id, 1);
    assert_eq!(committed[0].seq, 1);
    held_result.release();

    let response = timeout(Duration::from_secs(3), opening)
        .await
        .expect("HTTP handoff completes after releasing its result")
        .unwrap();
    let mut chunks = response.into_body().into_data_stream();
    assert_eq!(ids(&next(&mut chunks).await), vec![committed[0].row_id]);
    // No local rescue append is permitted. The next real frame is a
    // keepalive, so an enqueued trailing duplicate cannot remain hidden.
    let tail = timeout(Duration::from_secs(12), chunks.next())
        .await
        .expect("post-handoff real keepalive")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&tail).starts_with(':'), "{tail:?}");
    drop(chunks);
    producer.finished().await;
    fixture.control.wait_count(&fixture.control.live, 0).await;
    assert_eq!(reader.list_runtime_events(None, None, 2).unwrap().len(), 1);
}
