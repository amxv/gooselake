use super::*;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OperatorEventHistoryQuery {
    scope: Option<RuntimeEventScope>,
    scope_id: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
}

/// The cursor is the global durable row ID in both global and scoped views.
/// Per-scope `seq` is retained for historical/native event stream clients,
/// but is never confused with this operator collection's pagination cursor.
pub(super) async fn list_operator_events(
    State(state): State<AppState>,
    Query(query): Query<OperatorEventHistoryQuery>,
) -> Result<Response, ApiError> {
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request(
            "limit must be between 1 and 100".into(),
        ));
    }
    if query.scope.is_some() != query.scope_id.is_some() {
        return Err(ApiError::bad_request(
            "scope and scope_id must be supplied together".into(),
        ));
    }
    if query
        .scope_id
        .as_deref()
        .is_some_and(|id| id.is_empty() || id.trim() != id || id.len() > 256)
    {
        return Err(ApiError::bad_request(
            "scope_id must be a nonempty exact scope identifier".into(),
        ));
    }
    let after_id = query
        .cursor
        .as_deref()
        .map(|cursor| {
            cursor
                .parse::<i64>()
                .ok()
                .filter(|value| *value >= 0)
                .ok_or_else(|| ApiError::bad_request("invalid event history cursor".into()))
        })
        .transpose()?;

    let mut rows = state.app.services.store.list_operator_events_page(
        query.scope.zip(query.scope_id.as_deref()),
        after_id,
        limit + 1,
    )?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let mut response = Json(&rows).into_response();
    if has_more {
        if let Some(row) = rows.last() {
            response.headers_mut().insert(
                "x-next-cursor",
                axum::http::HeaderValue::from_str(&row.row_id.to_string())
                    .expect("positive SQLite row ID is valid header text"),
            );
        }
    }
    Ok(response)
}

/// The large payload/native diagnostics are available only when an operator
/// explicitly inspects one event ID rather than through list materialization.
pub(super) async fn get_operator_event(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<Json<RuntimeEventRecord>, ApiError> {
    Ok(Json(
        state
            .app
            .services
            .store
            .get_operator_event(&event_id)?
            .ok_or_else(|| ApiError::not_found(format!("event {event_id}")))?,
    ))
}
