use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct EventReplayQuery {
    pub(super) after_seq: Option<i64>,
    pub(super) limit: Option<usize>,
}

pub(super) async fn source_bootstrap(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::RuntimeSourceBootstrap>, ApiError> {
    let store = state.app.services.store.clone();
    let bootstrap = tokio::task::spawn_blocking(move || store.source_bootstrap())
        .await
        .map_err(|error| {
            ApiError::from(runtime_core::RuntimeError::Bootstrap(format!(
                "source bootstrap worker failed: {error}"
            )))
        })?
        .map_err(ApiError::from)?;
    Ok(Json(bootstrap))
}

pub(super) async fn replay_session_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .runtime
        .replay_session_events(
            session_id.as_str(),
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

pub(super) async fn stream_session_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<EventReplayQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let _ = state
        .runtime
        .get_session(session_id.as_str())
        .await
        .map_err(ApiError::from)?;

    // Subscribe before replay to avoid missing events appended during replay/live handoff.
    let receiver = state.runtime.subscribe_events();
    let last_event_id = parse_last_event_id_header(&headers)?;
    let cursor = query.after_seq.or(last_event_id);
    let replay_page_limit = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    let mut replay_events = Vec::new();
    let mut replay_cursor = cursor;

    loop {
        let page = state
            .runtime
            .replay_session_events(session_id.as_str(), replay_cursor, replay_page_limit)
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        replay_cursor = page.last().map(|event| event.seq);
        let page_len = page.len();
        replay_events.extend(page);
        if page_len < replay_page_limit {
            break;
        }
    }

    #[cfg(test)]
    if let Some(delay_ms) = headers
        .get("x-gg-test-handoff-delay-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
    {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }

    let replay_high_watermark_seq = replay_cursor.or(cursor).unwrap_or(0);

    let replay_stream = tokio_stream::iter(replay_events.into_iter().filter_map(|event| {
        let payload = serde_json::to_string(&event).ok()?;
        Some(Ok(Event::default()
            .id(event.seq.to_string())
            .event(event.kind)
            .data(payload)))
    }));

    let live_stream = BroadcastStream::new(receiver).filter_map(move |next| match next {
        Ok(event) if event.session_id.as_deref() == Some(session_id.as_str()) => {
            if event.seq <= replay_high_watermark_seq {
                return None;
            }
            let payload = match serde_json::to_string(&event) {
                Ok(payload) => payload,
                Err(_) => return None,
            };
            Some(Ok(Event::default()
                .id(event.seq.to_string())
                .event(event.kind)
                .data(payload)))
        }
        Ok(_) => None,
        Err(_) => None,
    });
    let stream = replay_stream.chain(live_stream);

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}

pub(super) async fn replay_global_events(
    State(state): State<AppState>,
    Query(query): Query<EventReplayQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .app
        .services
        .store
        .list_runtime_events(
            None,
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

pub(super) async fn stream_global_events(
    State(state): State<AppState>,
    Query(query): Query<EventReplayQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let last_event_id = parse_last_event_id_header(&headers)?;
    let cursor = query.after_seq.or(last_event_id);
    let replay_page_limit = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    let mut replay_events = Vec::new();
    let mut replay_cursor = cursor;

    loop {
        let page = state
            .app
            .services
            .store
            .list_runtime_events(None, replay_cursor, replay_page_limit)
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        replay_cursor = page.last().map(|event| event.row_id);
        let page_len = page.len();
        replay_events.extend(page);
        if page_len < replay_page_limit {
            break;
        }
    }

    let replay_high_watermark_seq = replay_cursor.or(cursor).unwrap_or(0);
    let replay_stream = tokio_stream::iter(replay_events.into_iter().filter_map(|event| {
        let payload = serde_json::to_string(&event).ok()?;
        Some(Ok(Event::default()
            .id(event.row_id.to_string())
            .event(event.kind)
            .data(payload)))
    }));

    let (tx, rx) = mpsc::channel::<Result<Event, std::convert::Infallible>>(128);
    let store = state.app.services.store.clone();
    tokio::spawn(async move {
        let mut cursor = replay_high_watermark_seq;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let page = match store.list_runtime_events(None, Some(cursor), replay_page_limit) {
                Ok(page) => page,
                Err(_) => continue,
            };
            if page.is_empty() {
                continue;
            }
            for event in page {
                cursor = cursor.max(event.row_id);
                let payload = match serde_json::to_string(&event) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                let sse = Event::default()
                    .id(event.row_id.to_string())
                    .event(event.kind)
                    .data(payload);
                if tx.send(Ok(sse)).await.is_err() {
                    return;
                }
            }
        }
    });

    let live_stream = ReceiverStream::new(rx);
    let stream = replay_stream.chain(live_stream);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(10))))
}
