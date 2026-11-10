use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct ProcessStartInput {
    command: String,
    cwd: Option<String>,
    timeout_ms: Option<u64>,
    session_id: Option<String>,
}

pub(super) async fn start_process(
    State(state): State<AppState>,
    Json(input): Json<ProcessStartInput>,
) -> Result<Json<runtime_core::ProcessDetails>, ApiError> {
    let details = state
        .app
        .services
        .process_manager
        .run_process(ProcessRunRequest {
            caller_session_id: input.session_id,
            tool_call_id: None,
            command: input.command,
            cwd: input.cwd,
            timeout_ms: input.timeout_ms,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(details))
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessListQuery {
    session_id: Option<String>,
    include_completed: Option<bool>,
}

pub(super) async fn list_processes(
    State(state): State<AppState>,
    Query(query): Query<ProcessListQuery>,
) -> Result<Json<Vec<runtime_core::ProcessSummary>>, ApiError> {
    let rows = state
        .app
        .services
        .process_manager
        .list_processes(ProcessListRequest {
            caller_session_id: query.session_id,
            include_completed: query.include_completed.unwrap_or(true),
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessSchedulerUpdateInput {
    max_concurrent: Option<usize>,
    workspace_max_concurrent: Option<BTreeMap<String, usize>>,
    capture_limit_bytes: Option<usize>,
    paused: Option<bool>,
    pause_reason: Option<String>,
}

pub(super) async fn get_process_scheduler(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProcessSchedulerSnapshot>, ApiError> {
    let snapshot = state
        .app
        .services
        .process_manager
        .scheduler_snapshot()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

pub(super) async fn update_process_scheduler(
    State(state): State<AppState>,
    Json(input): Json<ProcessSchedulerUpdateInput>,
) -> Result<Json<runtime_core::ProcessSchedulerSnapshot>, ApiError> {
    let current = state
        .app
        .services
        .process_manager
        .scheduler_snapshot()
        .await
        .map_err(ApiError::from)?;
    let paused = input.paused.unwrap_or(current.settings.paused);
    let pause_reason = if paused {
        input
            .pause_reason
            .or(current.settings.pause_reason)
            .or_else(|| Some("operator_http".to_string()))
    } else {
        None
    };
    let settings = ProcessSchedulerSettings {
        max_concurrent: input
            .max_concurrent
            .unwrap_or(current.settings.max_concurrent),
        workspace_max_concurrent: input
            .workspace_max_concurrent
            .unwrap_or(current.settings.workspace_max_concurrent),
        capture_limit_bytes: input
            .capture_limit_bytes
            .unwrap_or(current.settings.capture_limit_bytes),
        paused,
        pause_reason,
        updated_at: current.settings.updated_at,
    };
    let snapshot = state
        .app
        .services
        .process_manager
        .update_scheduler_settings(settings)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessQueueReorderInput {
    process_id: String,
    before_process_id: Option<String>,
    after_process_id: Option<String>,
}

pub(super) async fn reorder_process_queue(
    State(state): State<AppState>,
    Json(input): Json<ProcessQueueReorderInput>,
) -> Result<Json<runtime_core::ProcessSchedulerSnapshot>, ApiError> {
    let snapshot = state
        .app
        .services
        .process_manager
        .reorder_process_queue(
            input.process_id,
            input.before_process_id,
            input.after_process_id,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessSessionQuery {
    session_id: Option<String>,
}

pub(super) async fn get_process(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessSessionQuery>,
) -> Result<Json<runtime_core::ProcessDetails>, ApiError> {
    let details = state
        .app
        .services
        .process_manager
        .get_process(ProcessGetRequest {
            process_id,
            caller_session_id: query.session_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(details))
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessLogsQuery {
    session_id: Option<String>,
    stream: Option<String>,
    head_lines: Option<usize>,
    tail_lines: Option<usize>,
    max_bytes: Option<usize>,
}

pub(super) async fn get_process_logs(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessLogsQuery>,
) -> Result<Json<Vec<runtime_core::ProcessLogsChunk>>, ApiError> {
    let logs = state
        .app
        .services
        .process_manager
        .read_process_logs(ProcessLogReadRequest {
            process_id,
            caller_session_id: query.session_id,
            stream: query.stream,
            head_lines: query.head_lines,
            tail_lines: query.tail_lines,
            max_bytes: query.max_bytes,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(logs))
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessEventsQuery {
    session_id: Option<String>,
    after_seq: Option<i64>,
    limit: Option<usize>,
}

pub(super) async fn replay_process_events(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessEventsQuery>,
) -> Result<Json<Vec<RuntimeEventRecord>>, ApiError> {
    let events = state
        .app
        .services
        .process_manager
        .replay_events(
            process_id,
            query.session_id,
            query.after_seq,
            query.limit.unwrap_or(500).min(10_000),
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(events))
}

pub(super) async fn stream_process_events(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<ProcessEventsQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let last_event_id = parse_last_event_id_header(&headers)?;
    let cursor = query.after_seq.or(last_event_id);
    let page_size = query.limit.unwrap_or(2_000).clamp(1, 10_000);

    // A caller-visible process is the only permissible scoped journal stream.
    // Keep the process service's owner/workspace authorization before opening
    // the stream; querying a raw event scope directly would bypass it.
    state
        .app
        .services
        .process_manager
        .replay_events(process_id.clone(), query.session_id, cursor, 1)
        .await
        .map_err(ApiError::from)?;
    durable_event_stream(
        state.app.services.store.clone(),
        Some((RuntimeEventScope::Process, process_id)),
        cursor,
        page_size,
        &headers,
    )
    .await
}

#[derive(Debug, Deserialize)]
pub(super) struct ProcessKillInput {
    session_id: Option<String>,
    reason: Option<String>,
}

pub(super) async fn kill_process(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    input: Option<Json<ProcessKillInput>>,
) -> Result<Json<runtime_core::ProcessDetails>, ApiError> {
    let input = input.map(|Json(value)| value).unwrap_or(ProcessKillInput {
        session_id: None,
        reason: None,
    });
    let details = state
        .app
        .services
        .process_manager
        .kill_process(ProcessKillRequest {
            process_id,
            caller_session_id: input.session_id,
            reason: input.reason,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(details))
}
