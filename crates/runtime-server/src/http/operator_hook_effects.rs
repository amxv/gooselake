use super::*;
use runtime_core::{TeamHookEffectPageCursor, TeamHookEffectSummary, TeamHookEventName};
use std::fmt::Write;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkspaceHookEffectsQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

fn encode_hex(text: &str) -> String {
    let mut encoded = String::with_capacity(text.len() * 2);
    for byte in text.as_bytes() {
        write!(encoded, "{byte:02x}").expect("infallible String writer");
    }
    encoded
}

fn decode_hex(text: &str, max_bytes: usize) -> Result<String, ApiError> {
    let invalid = || ApiError::bad_request("invalid hook effect cursor".into());
    if text.is_empty() || text.len() > max_bytes * 2 || !text.len().is_multiple_of(2) {
        return Err(invalid());
    }
    let bytes = text
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| invalid())?;
            u8::from_str_radix(text, 16).map_err(|_| invalid())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let value = String::from_utf8(bytes).map_err(|_| invalid())?;
    if value.trim().is_empty() {
        return Err(invalid());
    }
    Ok(value)
}

fn parse_cursor(raw: &str) -> Result<TeamHookEffectPageCursor, ApiError> {
    let invalid = || ApiError::bad_request("invalid hook effect cursor".into());
    if raw.len() > 1024 {
        return Err(invalid());
    }
    let parts = raw.split(':').collect::<Vec<_>>();
    let [created, parent, event, name] = parts.as_slice() else {
        return Err(invalid());
    };
    let created_at_ms = created.parse::<i64>().map_err(|_| invalid())?;
    if created_at_ms < 0 {
        return Err(invalid());
    }
    let event_name = TeamHookEventName::ALL
        .into_iter()
        .find(|kind| kind.as_str() == *event)
        .ok_or_else(invalid)?;
    Ok(TeamHookEffectPageCursor {
        created_at_ms,
        parent_event_id: decode_hex(parent, 256)?,
        event_name,
        hook_name: decode_hex(name, 128)?,
    })
}

fn next_cursor(row: &TeamHookEffectSummary) -> String {
    format!(
        "{}:{}:{}:{}",
        row.created_at_ms,
        encode_hex(&row.parent_event_id),
        row.event_name.as_str(),
        encode_hex(&row.hook_name)
    )
}

/// Operator list is a redacted indexed projection; detailed hook command,
/// input and receipt are available only by explicit exact effect address.
pub(super) async fn list_workspace_hook_effects(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<WorkspaceHookEffectsQuery>,
) -> Result<Response, ApiError> {
    let workspace_id = workspace_id.trim();
    state
        .app
        .services
        .store
        .get_workspace(workspace_id)?
        .ok_or_else(|| ApiError::not_found(format!("workspace {workspace_id}")))?;
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request(
            "limit must be between 1 and 100".into(),
        ));
    }
    let cursor = query.cursor.as_deref().map(parse_cursor).transpose()?;
    let mut rows = state
        .app
        .services
        .store
        .list_team_hook_effect_summaries_page(workspace_id, cursor.as_ref(), limit + 1)?;
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let mut response = Json(&rows).into_response();
    if has_more {
        if let Some(row) = rows.last() {
            response.headers_mut().insert(
                "x-next-cursor",
                axum::http::HeaderValue::from_str(&next_cursor(row))
                    .expect("encoded bounded hook history key is a safe header value"),
            );
        }
    }
    Ok(response)
}

pub(super) async fn get_workspace_hook_effect(
    State(state): State<AppState>,
    Path((workspace_id, parent_event_id, event_name, hook_name)): Path<(
        String,
        String,
        String,
        String,
    )>,
) -> Result<Json<runtime_core::TeamHookEffectRecord>, ApiError> {
    state
        .app
        .services
        .store
        .get_workspace(&workspace_id)?
        .ok_or_else(|| ApiError::not_found(format!("workspace {workspace_id}")))?;
    let event = TeamHookEventName::ALL
        .into_iter()
        .find(|event| event.as_str() == event_name)
        .ok_or_else(|| ApiError::bad_request("invalid hook effect event name".into()))?;
    Ok(Json(
        state
            .app
            .services
            .store
            .get_team_hook_effect(&workspace_id, &parent_event_id, event, &hook_name)?
            .ok_or_else(|| {
                ApiError::not_found(format!("hook effect {parent_event_id}/{hook_name}"))
            })?,
    ))
}
