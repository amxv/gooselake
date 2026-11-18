use super::control::*;
use super::*;
use crate::http::durable_event_stream_test_control::register;
use runtime_core::EventObserverHealth;

#[tokio::test(flavor = "current_thread")]
async fn retained_readiness_precedes_replay_and_http_success() {
    let fixture = Fixture::new(Some(EventObserverHealth::Starting)).await;
    let (producer, headers) = register();
    let store = fixture.store.clone();
    let request = tokio::spawn(async move {
        durable_event_stream(store, None, Some(0), 10, &headers)
            .await
            .map(IntoResponse::into_response)
    });
    fixture
        .control
        .wait_count(&fixture.control.created, 1)
        .await;
    producer.waiting_for_readiness().await;
    assert!(!request.is_finished());
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), 0);
    assert!(!producer.has_started());
    fixture.control.set_health(EventObserverHealth::Ready);
    let response = timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), 1);
    drop(response);
    producer.finished().await;
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn retained_failed_and_closed_health_refuse_initial_and_late_requests() {
    let fixture = Fixture::new(Some(EventObserverHealth::Starting)).await;
    let (producer, headers) = register();
    let store = fixture.store.clone();
    let request = tokio::spawn(async move {
        durable_event_stream(store, None, Some(0), 10, &headers)
            .await
            .map(IntoResponse::into_response)
    });
    fixture
        .control
        .wait_count(&fixture.control.created, 1)
        .await;
    producer.waiting_for_readiness().await;
    fixture.control.fail();
    assert!(timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(!producer.has_started());
    assert!(
        durable_event_stream(fixture.store.clone(), None, Some(0), 10, &HeaderMap::new())
            .await
            .is_err()
    );
    fixture
        .control
        .wait_count(&fixture.control.dropped, 2)
        .await;
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), 0);

    let closed = Fixture::new(Some(EventObserverHealth::Ready)).await;
    closed.control.close_health();
    assert!(
        durable_event_stream(closed.store.clone(), None, Some(0), 10, &HeaderMap::new())
            .await
            .is_err()
    );
    closed.control.wait_count(&closed.control.dropped, 1).await;
    assert_eq!(closed.store.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn health_failure_after_initial_sql_read_prevents_http_success() {
    let fixture = Fixture::new(Some(EventObserverHealth::Ready)).await;
    fixture.append("initial_in_flight");
    let hold = fixture.control.arm(ReadPhase::After(1));
    let (producer, headers) = register();
    let store = fixture.store.clone();
    let request = tokio::spawn(async move {
        durable_event_stream(store, None, Some(0), 10, &headers)
            .await
            .map(IntoResponse::into_response)
    });
    hold.reached().await;
    fixture.control.fail();
    assert!(timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(!producer.has_started());
    assert_eq!(
        fixture.control.live.load(Ordering::SeqCst),
        1,
        "the blocked closure retains its reader after the HTTP request exits"
    );
    hold.release();
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn bound_anchor_and_first_page_use_one_pinned_replay_call() {
    for real_generation_loss in [false, true] {
        let fixture =
            Fixture::new((!real_generation_loss).then_some(EventObserverHealth::Ready)).await;
        let anchor = fixture.append("cursor_anchor");
        fixture.append("cursor_tail");
        let cursor = fixture.bound_anchor(&anchor);
        // One bound call validates the anchor and reads the first page.
        // Store fixtures independently prove their shared snapshot.
        let hold = fixture.control.arm(ReadPhase::Before(1));
        let (producer, headers) = register();
        let store = fixture.store.clone();
        let request = tokio::spawn(async move {
            checked_durable_event_stream(
                store,
                runtime_core::EventCollection::stream(None).unwrap(),
                Some(cursor),
                10,
                &headers,
            )
            .await
            .map(IntoResponse::into_response)
        });
        hold.reached().await;
        assert_eq!(fixture.store.reads.load(Ordering::SeqCst), 1);
        if real_generation_loss {
            std::fs::rename(&fixture.path, fixture.temp.path().join("retired.sqlite3")).unwrap();
        } else {
            fixture.control.fail();
        }
        hold.release();
        assert!(timeout(Duration::from_secs(3), request)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert!(!producer.has_started());
        fixture
            .control
            .wait_count(&fixture.control.dropped, 1)
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bound_health_failure_after_combined_read_prevents_http_success() {
    let fixture = Fixture::new(Some(EventObserverHealth::Ready)).await;
    fixture.append("bound_initial_in_flight");
    let hold = fixture.control.arm(ReadPhase::After(1));
    let (producer, headers) = register();
    let store = fixture.store.clone();
    let request = tokio::spawn(async move {
        checked_durable_event_stream(
            store,
            runtime_core::EventCollection::stream(None).unwrap(),
            None,
            10,
            &headers,
        )
        .await
        .map(IntoResponse::into_response)
    });
    hold.reached().await;
    fixture.control.fail();
    assert!(timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(!producer.has_started());
    assert_eq!(fixture.control.live.load(Ordering::SeqCst), 1);
    hold.release();
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_health_releases_full_output_producer_before_client_drains() {
    let fixture = Fixture::new(Some(EventObserverHealth::Ready)).await;
    let expected: Vec<_> = (0..130)
        .map(|index| fixture.append(&format!("full_{index}")).row_id)
        .collect();
    let (producer, headers) = register();
    let response = fixture.open(0, 1, &headers).await;
    producer.pending().await;
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), 129);
    fixture.control.fail();
    producer.finished().await;
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;
    assert_eq!(fixture.control.live.load(Ordering::SeqCst), 0);
    // No body polling occurred before both acknowledgements above.
    let text = drain(&mut response.into_body().into_data_stream()).await;
    assert_eq!(ids(&text), expected[..128]);
    assert_terminal(&text, "event_observer_unavailable");
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_uses_last_consumed_id_not_last_server_enqueued_id() {
    let fixture = Fixture::new(Some(EventObserverHealth::Ready)).await;
    let expected: Vec<_> = (0..130)
        .map(|index| fixture.append(&format!("unread_{index}")).row_id)
        .collect();
    let (producer, headers) = register();
    let response = fixture.open(0, 1, &headers).await;
    producer.pending().await;
    let mut chunks = response.into_body().into_data_stream();
    let consumed = ids(&next(&mut chunks).await);
    assert_eq!(consumed, expected[..1]);
    drop(chunks);
    producer.finished().await;
    fixture
        .control
        .wait_count(&fixture.control.dropped, 1)
        .await;

    let (reopened, headers) = register();
    let response = fixture.open(consumed[0], 7, &headers).await;
    let mut chunks = response.into_body().into_data_stream();
    let mut text = String::new();
    while ids(&text).len() < expected.len() - 1 {
        text.push_str(&next(&mut chunks).await);
    }
    assert_eq!(ids(&text), expected[1..]);
    fixture.control.fail();
    reopened.finished().await;
    text.push_str(&drain(&mut chunks).await);
    assert_eq!(
        ids(&text),
        expected[1..],
        "EOF catches trailing duplicate frames"
    );
    assert_terminal(&text, "event_observer_unavailable");
    fixture
        .control
        .wait_count(&fixture.control.dropped, 2)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_durable_event_name_emits_unsequenced_terminal_error_without_panicking() {
    for invalid in ["bad\nname", "bad\rname"] {
        let fixture = Fixture::new(None).await;
        let valid = fixture.append("valid_before_invalid");
        let mut bad = super::super::durable_event_stream_routes::new_event(
            "invalid_event_name",
            RuntimeEventScope::System,
            "global",
        );
        bad.kind = invalid.into();
        SqliteRuntimeRepository::new(fixture.path.clone())
            .append_runtime_event(&bad)
            .unwrap();
        fixture.append("never_delivered_after_invalid");
        let (producer, headers) = register();
        let response = fixture.open(0, 10, &headers).await;
        let text = drain(&mut response.into_body().into_data_stream()).await;
        producer.finished().await;
        fixture
            .control
            .wait_count(&fixture.control.dropped, 1)
            .await;
        assert_eq!(ids(&text), [valid.row_id]);
        assert_terminal(&text, "journal_record_invalid");
    }
}
