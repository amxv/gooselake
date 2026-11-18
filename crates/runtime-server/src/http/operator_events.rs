use super::*;
use runtime_core::{EventCollection, EventCursor};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorEventHistoryQuery {
    scope: Option<RuntimeEventScope>,
    scope_id: Option<String>,
    /// Opaque ev1 continuation for this exact collection and journal incarnation.
    #[schemars(length(min = 1, max = 1024))]
    cursor: Option<String>,
    #[schemars(range(min = 1, max = 100))]
    limit: Option<usize>,
}

/// History uses a global row coordinate inside an opaque journal-bound cursor.
/// Derive the requested collection independently of the supplied cursor.
pub(super) async fn list_operator_events(
    State(state): State<AppState>,
    BoundedEventQuery(query): BoundedEventQuery<OperatorEventHistoryQuery>,
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
    let after = parse_operator_cursor(query.cursor.as_deref())?;
    let collection = EventCollection::history(query.scope.zip(query.scope_id))?;
    let mut page = state.app.services.store.list_operator_events_page_bound(
        &collection,
        after.as_ref(),
        limit + 1,
    )?;
    let has_more = page.rows.len() > limit;
    page.rows.truncate(limit);
    let mut response = Json(&page.rows).into_response();
    if has_more {
        if let Some(row) = page.rows.last() {
            // The lookahead row must never become the continuation.
            let cursor = EventCursor::for_event(
                &page.epoch,
                &collection,
                row.row_id,
                row.seq,
                &row.event_id,
            )?;
            response.headers_mut().insert(
                "x-next-cursor",
                axum::http::HeaderValue::from_str(&cursor.encode())
                    .expect("canonical event cursor is ASCII header text"),
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
