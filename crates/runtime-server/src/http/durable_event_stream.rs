use super::*;
use runtime_core::{
    EventCommitSubscription, EventObservationFailure, EventObserverHealth,
    RuntimeEventReplayReader, RuntimeStore,
};
use std::convert::Infallible;
use tokio::sync::watch;

type Frame = Result<Event, Infallible>;

/// Legacy cursor policy is preserved; operator streams use the checked wrapper.
pub(super) async fn durable_event_stream(
    store: Arc<dyn RuntimeStore>,
    scope: Option<(RuntimeEventScope, String)>,
    after: Option<i64>,
    page_size: usize,
    headers: &HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Frame>>, ApiError> {
    open_stream(store, scope, after, page_size, headers, false).await
}

pub(super) async fn checked_durable_event_stream(
    store: Arc<dyn RuntimeStore>,
    scope: Option<(RuntimeEventScope, String)>,
    after: Option<i64>,
    page_size: usize,
    headers: &HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Frame>>, ApiError> {
    open_stream(store, scope, after, page_size, headers, true).await
}

fn observer_failure(
    health: &watch::Receiver<EventObserverHealth>,
) -> Option<EventObservationFailure> {
    let state = health.borrow().clone();
    if let EventObserverHealth::Failed(failure) = state {
        Some(failure)
    } else if health.has_changed().is_err() {
        Some(EventObservationFailure::unavailable(
            "event observer health channel closed",
        ))
    } else {
        None
    }
}

enum ReadFailure {
    Journal(RuntimeError),
    Observer(EventObservationFailure),
    InvalidRecord,
}

impl ReadFailure {
    fn into_api(self) -> ApiError {
        match self {
            Self::Journal(error) => error.into(),
            Self::Observer(failure) => failure.runtime_error().into(),
            Self::InvalidRecord => RuntimeError::Bootstrap(
                "durable event kind contains an invalid SSE line break".into(),
            )
            .into(),
        }
    }
}

async fn read_page(
    replay: Arc<dyn RuntimeEventReplayReader>,
    scope: Option<(RuntimeEventScope, String)>,
    after: Option<i64>,
    page_size: usize,
    health: &mut watch::Receiver<EventObserverHealth>,
) -> Result<Vec<RuntimeEventRecord>, ReadFailure> {
    if let Some(failure) = observer_failure(health) {
        return Err(ReadFailure::Observer(failure));
    }
    let mut read = tokio::task::spawn_blocking(move || {
        replay.list_runtime_events(
            scope.as_ref().map(|(kind, id)| (*kind, id.as_str())),
            after,
            page_size,
        )
    });
    loop {
        if let Some(failure) = observer_failure(health) {
            return Err(ReadFailure::Observer(failure));
        }
        tokio::select! {
            biased;
            changed = health.changed() => {
                if changed.is_err() {
                    return Err(ReadFailure::Observer(EventObservationFailure::unavailable("event observer health channel closed")));
                }
            }
            result = &mut read => {
                if let Some(failure) = observer_failure(health) { return Err(ReadFailure::Observer(failure)); }
                return result.map_err(|_| ReadFailure::Journal(RuntimeError::Bootstrap("event replay worker failed".into())))?
                    .map_err(ReadFailure::Journal);
            }
        }
    }
}

async fn open_stream(
    store: Arc<dyn RuntimeStore>,
    scope: Option<(RuntimeEventScope, String)>,
    after: Option<i64>,
    page_size: usize,
    headers: &HeaderMap,
    check_cursor: bool,
) -> Result<Sse<impl tokio_stream::Stream<Item = Frame>>, ApiError> {
    #[cfg(test)]
    let stream_control = super::durable_event_stream_test_control::from_headers(headers);
    // Synchronous stores need no Tokio reactor. Blocking filesystem/SQLite
    // startup is kept off the async worker, then readiness is awaited before
    // cursor validation or a successful HTTP streaming response.
    let mut subscription = tokio::task::spawn_blocking(move || store.subscribe_event_commits())
        .await
        .map_err(|_| RuntimeError::Bootstrap("event observer startup worker failed".into()))??;
    {
        let readiness = subscription.ready();
        tokio::pin!(readiness);
        std::future::poll_fn(|cx| {
            let result = std::future::Future::poll(readiness.as_mut(), cx);
            #[cfg(test)]
            if result.is_pending() {
                if let Some(control) = &stream_control {
                    control.readiness_pending();
                }
            }
            result
        })
        .await?;
    }
    let use_row_id = scope
        .as_ref()
        .is_none_or(|(kind, _)| kind.is_subject_projection());
    if let Some(cursor) = after.filter(|cursor| check_cursor && *cursor > 0) {
        let selected = read_page(
            subscription.replay.clone(),
            scope.clone(),
            Some(cursor - 1),
            1,
            &mut subscription.health,
        )
        .await
        .map_err(ReadFailure::into_api)?;
        let valid = selected.first().is_some_and(|event| {
            if use_row_id {
                event.row_id == cursor
            } else {
                event.seq == cursor
            }
        });
        if !valid {
            return Err(ApiError::bad_request(
                "event stream cursor is not in the selected collection".into(),
            ));
        }
    }
    let first_page = read_page(
        subscription.replay.clone(),
        scope.clone(),
        after,
        page_size,
        &mut subscription.health,
    )
    .await
    .map_err(ReadFailure::into_api)?;

    // This existing fixture header has no effect in production.
    #[cfg(test)]
    if let Some(delay_ms) = headers
        .get("x-gg-test-handoff-delay-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|delay| *delay > 0)
    {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }
    #[cfg(not(test))]
    let _ = headers;
    if let Some(failure) = observer_failure(&subscription.health) {
        return Err(failure.runtime_error().into());
    }

    let EventCommitSubscription {
        mut commits,
        mut health,
        replay,
    } = subscription;
    let (tx, rx) = mpsc::channel::<Frame>(128);
    // A reserved terminal slot lets failure release the producer/reader even
    // if the consumer has filled the data queue and stopped reading. Queued
    // valid frames drain first, then one unsequenced error and EOF.
    let (terminal_tx, terminal_rx) = mpsc::channel::<Frame>(1);
    tokio::spawn(async move {
        #[cfg(test)]
        let _producer = stream_control.as_ref().map(|control| control.start());
        let mut cursor = after.unwrap_or(0);
        let mut page = first_page;
        loop {
            let count = page.len();
            for record in page {
                let next = if use_row_id {
                    record.row_id
                } else {
                    record.seq
                };
                if next <= cursor {
                    continue;
                }
                // Generic journal writers can persist arbitrary text. Axum's
                // event-name builder panics on CR/LF; never let malformed
                // durable data silently terminate the producer or get an ID.
                if record
                    .kind
                    .bytes()
                    .any(|byte| matches!(byte, b'\r' | b'\n'))
                {
                    terminal_error(&terminal_tx, ReadFailure::InvalidRecord);
                    return;
                }
                let payload = match serde_json::to_string(&record) {
                    Ok(payload) => payload,
                    Err(_) => {
                        terminal_error(
                            &terminal_tx,
                            ReadFailure::Journal(RuntimeError::Bootstrap(
                                "event serialization failed".into(),
                            )),
                        );
                        return;
                    }
                };
                let event = Event::default()
                    .id(next.to_string())
                    .event(record.kind)
                    .data(payload);
                let reservation = tx.reserve();
                tokio::pin!(reservation);
                let pending_reservation = std::future::poll_fn(|cx| {
                    let result = std::future::Future::poll(reservation.as_mut(), cx);
                    #[cfg(test)]
                    if result.is_pending() && tx.capacity() == 0 {
                        if let Some(control) = &stream_control {
                            control.output_pending();
                        }
                    }
                    result
                });
                tokio::pin!(pending_reservation);
                loop {
                    if let Some(failure) = observer_failure(&health) {
                        terminal_error(&terminal_tx, ReadFailure::Observer(failure));
                        return;
                    }
                    tokio::select! {
                        biased;
                        changed = health.changed() => {
                            if changed.is_err() {
                                terminal_error(&terminal_tx, ReadFailure::Observer(EventObservationFailure::unavailable("event observer health channel closed")));
                                return;
                            }
                        }
                        permit = &mut pending_reservation => {
                            let Ok(permit) = permit else { return; };
                            if let Some(failure) = observer_failure(&health) {
                                terminal_error(&terminal_tx, ReadFailure::Observer(failure));
                                return;
                            }
                            permit.send(Ok(event));
                            cursor = next;
                            break;
                        }
                    }
                }
            }
            if count < page_size {
                loop {
                    if let Some(failure) = observer_failure(&health) {
                        terminal_error(&terminal_tx, ReadFailure::Observer(failure));
                        return;
                    }
                    tokio::select! {
                        biased;
                        _ = tx.closed() => return,
                        changed = health.changed() => {
                            if changed.is_err() {
                                terminal_error(&terminal_tx, ReadFailure::Observer(EventObservationFailure::unavailable("event observer health channel closed")));
                                return;
                            }
                        }
                        notification = commits.recv() => {
                            match notification {
                                Ok(()) => break,
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                    #[cfg(test)]
                                    if let Some(control) = &stream_control {
                                        control.notification_lagged();
                                    }
                                    break;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    terminal_error(&terminal_tx, ReadFailure::Observer(EventObservationFailure::unavailable("event commit channel closed")));
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            let next = tokio::select! {
                _ = tx.closed() => return,
                next = read_page(replay.clone(), scope.clone(), Some(cursor), page_size, &mut health) => next,
            };
            page = match next {
                Ok(page) => page,
                Err(failure) => {
                    terminal_error(&terminal_tx, failure);
                    return;
                }
            };
        }
    });
    let frames =
        tokio_stream::StreamExt::chain(ReceiverStream::new(rx), ReceiverStream::new(terminal_rx));
    Ok(Sse::new(frames).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

fn terminal_error(tx: &mpsc::Sender<Frame>, failure: ReadFailure) {
    let payload = match failure {
        ReadFailure::Journal(_) => serde_json::json!({
            "code":"journal_unavailable", "retryable":true,
            "message":"durable event replay interrupted; reconnect with the last received event ID"
        }),
        ReadFailure::Observer(failure) => serde_json::json!({
            "code":failure.code, "retryable":false,
            "message":failure.message,
            "continuation":"inspect observer health and database generation before resuming; numeric cursors do not identify a replaced database"
        }),
        ReadFailure::InvalidRecord => serde_json::json!({
            "code":"journal_record_invalid", "retryable":false,
            "message":"durable event kind contains an invalid SSE line break; inspect the record before resuming"
        }),
    };
    let _ = tx.try_send(Ok(Event::default()
        .event("stream.error")
        .data(payload.to_string())));
}
