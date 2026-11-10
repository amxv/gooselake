use super::*;

/// Operator-only live projections use the same committed journal as `/v1`,
/// without exposing a separate per-workspace or per-agent event bus. An agent
/// ID is the canonical runtime session ID for its event scope.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorEventStreamQuery {
    #[schemars(range(min = 0))]
    after_seq: Option<i64>,
    #[schemars(range(min = 1, max = 10000))]
    limit: Option<usize>,
}

impl OperatorEventStreamQuery {
    fn parse(&self, headers: &HeaderMap) -> Result<(Option<i64>, usize), ApiError> {
        let last_event_id = parse_last_event_id_header(headers)?;
        let cursor = self.after_seq.or(last_event_id);
        if cursor.is_some_and(|value| value < 0) {
            return Err(ApiError::bad_request(
                "after_seq must be a non-negative event cursor".into(),
            ));
        }
        let page_size = self.limit.unwrap_or(2_000);
        if !(1..=10_000).contains(&page_size) {
            return Err(ApiError::bad_request(
                "stream limit must be between 1 and 10000".into(),
            ));
        }
        Ok((cursor, page_size))
    }
}

async fn operator_event_stream(
    state: AppState,
    scope: Option<(RuntimeEventScope, String)>,
    query: OperatorEventStreamQuery,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    let (cursor, page_size) = query.parse(&headers)?;
    durable_event_stream(
        state.app.services.store.clone(),
        scope,
        cursor,
        page_size,
        &headers,
    )
    .await
}

pub(super) async fn stream_operator_global_events(
    State(state): State<AppState>,
    Query(query): Query<OperatorEventStreamQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    operator_event_stream(state, None, query, headers).await
}

pub(super) async fn stream_operator_workspace_events(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<OperatorEventStreamQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    state
        .app
        .services
        .store
        .get_workspace(&workspace_id)?
        .ok_or_else(|| ApiError::not_found(format!("workspace {workspace_id}")))?;
    operator_event_stream(
        state,
        Some((RuntimeEventScope::Workspace, workspace_id)),
        query,
        headers,
    )
    .await
}

pub(super) async fn stream_operator_agent_events(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
    Query(query): Query<OperatorEventStreamQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    state
        .app
        .services
        .store
        .get_workspace_agent_by_id(&agent_id)?
        .ok_or_else(|| ApiError::not_found(format!("workspace agent {agent_id}")))?;
    operator_event_stream(
        state,
        Some((RuntimeEventScope::Session, agent_id)),
        query,
        headers,
    )
    .await
}

pub(super) async fn stream_operator_process_events(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
    Query(query): Query<OperatorEventStreamQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
{
    state
        .app
        .services
        .store
        .get_managed_process(&process_id)?
        .ok_or_else(|| ApiError::not_found(format!("process {process_id}")))?;
    operator_event_stream(
        state,
        Some((RuntimeEventScope::Process, process_id)),
        query,
        headers,
    )
    .await
}
