use super::control::*;
use super::*;
use crate::http::durable_event_stream_test_control::register;
use runtime_core::EventObserverHealth;
use std::time::Instant;

#[tokio::test(flavor = "current_thread")]
async fn blocked_replay_keeps_only_worker_lease_after_health_failure_or_body_drop() {
    for after_read in [false, true] {
        for fail_health in [false, true] {
            let fixture = Fixture::new(Some(EventObserverHealth::Ready)).await;
            let (producer, headers) = register();
            let response = fixture.open(0, 10, &headers).await;
            let mut chunks = Some(response.into_body().into_data_stream());
            let hold = fixture.control.arm(if after_read {
                ReadPhase::After(2)
            } else {
                ReadPhase::Before(2)
            });
            fixture.append(&format!("blocked_{after_read}_{fail_health}"));
            hold.reached().await;
            let started = Instant::now();
            if fail_health {
                fixture.control.fail();
            } else {
                drop(chunks.take());
            }
            // This current-thread runtime can service the timer and producer
            // while its real SQLite read is held on the blocking pool.
            tokio::time::sleep(Duration::from_millis(10)).await;
            producer.finished().await;
            assert!(started.elapsed() < Duration::from_millis(500));
            assert_eq!(fixture.control.live.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.control.dropped.load(Ordering::SeqCst), 0);
            if let Some(chunks) = chunks.as_mut() {
                let text = drain(chunks).await;
                assert!(
                    ids(&text).is_empty(),
                    "blocked rows cannot escape after failure"
                );
                assert_terminal(&text, "event_observer_unavailable");
            }
            hold.release();
            fixture
                .control
                .wait_count(&fixture.control.dropped, 1)
                .await;
            assert_eq!(fixture.control.live.load(Ordering::SeqCst), 0);
            if let Some(chunks) = chunks.as_mut() {
                assert!(
                    timeout(Duration::from_secs(3), chunks.next())
                        .await
                        .unwrap()
                        .is_none(),
                    "late blocking results cannot append after terminal EOF"
                );
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_actual_tokio_startup_job_releases_result_and_captured_store() {
    let fixture = Fixture::new(None).await;
    let Fixture {
        temp: _temp,
        path: _path,
        store,
        control,
    } = fixture;
    let weak_store = Arc::downgrade(&store);
    let hold = control.arm(ReadPhase::Subscribe);
    let (producer, headers) = register();
    let request = tokio::spawn(async move {
        durable_event_stream(store, None, Some(0), 10, &headers)
            .await
            .map(IntoResponse::into_response)
    });
    hold.reached().await;
    let started = Instant::now();
    request.abort();
    assert!(timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(
        !producer.has_started(),
        "cancelled startup never spawned an SSE producer"
    );
    assert_eq!(control.created.load(Ordering::SeqCst), 0);
    assert!(
        weak_store.upgrade().is_some(),
        "blocking closure still owns the store"
    );
    hold.release();
    control.wait_count(&control.dropped, 1).await;
    assert_eq!(control.created.load(Ordering::SeqCst), 1);
    assert_eq!(control.live.load(Ordering::SeqCst), 0);
    assert!(weak_store.upgrade().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_initial_read_returns_promptly_and_drops_lease_after_job_finishes() {
    let fixture = Fixture::new(None).await;
    let hold = fixture.control.arm(ReadPhase::After(1));
    let (producer, headers) = register();
    let store = fixture.store.clone();
    let request = tokio::spawn(async move {
        durable_event_stream(store, None, Some(0), 10, &headers)
            .await
            .map(IntoResponse::into_response)
    });
    hold.reached().await;
    request.abort();
    assert!(timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap_err()
        .is_cancelled());
    assert!(!producer.has_started());
    assert_eq!(fixture.control.live.load(Ordering::SeqCst), 1);
    hold.release();
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
}
