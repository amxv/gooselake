use super::*;
use runtime_core::RuntimeStore;
use std::convert::Infallible;

/// One journal-backed SSE path for global and scoped event history. The
/// broadcast channel is only a commit wakeup, never an event data source:
/// replay/catchup always goes through the durable, monotonically ordered
/// SQLite journal, including after a lagged notification.
pub(super) async fn durable_event_stream(
    store: Arc<dyn RuntimeStore>,
    scope: Option<(RuntimeEventScope, String)>,
    after: Option<i64>,
    page_size: usize,
    headers: &HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Subscribe before the first SELECT. Every commit racing the replay
    // contributes either to the queried rows or a later wakeup (or both).
    let mut commits = store.subscribe_event_commits().ok_or_else(|| {
        ApiError::from(RuntimeError::Unsupported(
            "durable event commit notifications are not supported by the runtime store".into(),
        ))
    })?;
    let scoped = scope.is_some();
    let first_page = store.list_runtime_events(
        scope.as_ref().map(|(kind, id)| (*kind, id.as_str())),
        after,
        page_size,
    )?;

    // Exercise the subscribe/replay/live handoff under a deterministic delay.
    // This header has no effect in production and must never be an API control.
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

    // Bounded channel + streaming pages prevent unbounded backlog materialization
    // even when a consumer is slow or has a large initial event history.
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(128);
    tokio::spawn(async move {
        let mut cursor = after.unwrap_or(0);
        let mut page = first_page;
        loop {
            let count = page.len();
            for record in page {
                let next = if scoped { record.seq } else { record.row_id };
                if next <= cursor {
                    continue;
                }
                let payload = match serde_json::to_string(&record) {
                    Ok(payload) => payload,
                    Err(_) => {
                        send_reconnect_error(&tx).await;
                        return;
                    }
                };
                let event = Event::default()
                    .id(next.to_string())
                    .event(record.kind)
                    .data(payload);
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }
                cursor = next;
            }

            // Drain all pages before subscribing to another wakeup, so a
            // producer burst cannot leave an old committed row stranded.
            if count == page_size {
                page = match store.list_runtime_events(
                    scope.as_ref().map(|(kind, id)| (*kind, id.as_str())),
                    Some(cursor),
                    page_size,
                ) {
                    Ok(next) => next,
                    Err(_) => {
                        send_reconnect_error(&tx).await;
                        return;
                    }
                };
                continue;
            }
            tokio::select! {
                _ = tx.closed() => return,
                notification = commits.recv() => {
                    match notification {
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            page = match store.list_runtime_events(
                                scope.as_ref().map(|(kind, id)| (*kind, id.as_str())),
                                Some(cursor),
                                page_size,
                            ) {
                                Ok(next) => next,
                                Err(_) => {
                                    send_reconnect_error(&tx).await;
                                    return;
                                }
                            };
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        }
    });
    Ok(Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

async fn send_reconnect_error(tx: &mpsc::Sender<Result<Event, Infallible>>) {
    // Leave the last successful event ID unchanged. A reconnect resumes
    // from that durable cursor rather than guessing that unread rows arrived.
    let _ = tx
        .send(Ok(Event::default().event("stream.error").data(
            r#"{"code":"journal_unavailable","retryable":true,"message":"durable event replay interrupted; reconnect with the last received event ID"}"#,
        )))
        .await;
}
