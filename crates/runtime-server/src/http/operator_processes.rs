use super::*;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OperatorProcessListQuery {
    workspace_id: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

/// This operator-only list reads eight metadata columns using indexed keyset
/// pagination. Full process commands, native PIDs, logs and tool identities
/// are confined to the explicit single-process diagnostic route.
pub(super) async fn list_operator_processes(
    State(state): State<AppState>,
    Query(query): Query<OperatorProcessListQuery>,
) -> Result<Response, ApiError> {
    let (limit, cursor) = ResourcePageQuery {
        cursor: query.cursor,
        limit: query.limit,
    }
    .parse()?;
    if query
        .workspace_id
        .as_deref()
        .is_some_and(|id| id.is_empty() || id.trim() != id || id.len() > 128)
    {
        return Err(ApiError::bad_request(
            "workspace_id must be a nonempty canonical identifier".into(),
        ));
    }
    let rows = state.app.services.store.list_managed_processes_page(
        query.workspace_id.as_deref(),
        cursor,
        limit + 1,
    )?;
    Ok(paged_response(rows, limit, |row| {
        (row.admitted_at, &row.process_id)
    }))
}

pub(super) async fn get_operator_process(
    State(state): State<AppState>,
    Path(process_id): Path<String>,
) -> Result<Json<runtime_core::ProcessDetails>, ApiError> {
    Ok(Json(
        state
            .app
            .services
            .process_manager
            .get_process(ProcessGetRequest {
                process_id,
                caller_session_id: None,
            })
            .await?,
    ))
}
