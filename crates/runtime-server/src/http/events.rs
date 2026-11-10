use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct EventReplayQuery {
    pub(super) after_seq: Option<i64>,
    pub(super) limit: Option<usize>,
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
    let last_event_id = parse_last_event_id_header(&headers)?;
    let cursor = query.after_seq.or(last_event_id);
    let page_size = query.limit.unwrap_or(2_000).clamp(1, 10_000);
    durable_event_stream(
        state.app.services.store.clone(),
        Some((RuntimeEventScope::Session, session_id)),
        cursor,
        page_size,
        &headers,
    )
    .await
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
    durable_event_stream(
        state.app.services.store.clone(),
        None,
        cursor,
        query.limit.unwrap_or(2_000).clamp(1, 10_000),
        &headers,
    )
    .await
}
