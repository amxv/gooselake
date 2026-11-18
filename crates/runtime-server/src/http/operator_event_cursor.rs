use super::{ApiError, HeaderMap, Query};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use runtime_core::{EventCursor, EventCursorRejection, MAX_EVENT_CURSOR_BYTES};
use serde::de::DeserializeOwned;

const MAX_EVENT_QUERY_BYTES: usize = 4096;

/// Bound the raw URL before serde allocates decoded query field values.
pub(super) struct BoundedEventQuery<T>(pub T);

impl<S, T> FromRequestParts<S> for BoundedEventQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        if parts
            .uri
            .query()
            .is_some_and(|query| query.len() > MAX_EVENT_QUERY_BYTES)
        {
            return Err(ApiError::bad_request(
                "event query exceeds 4096 bytes".into(),
            ));
        }
        let Query(query) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(|_| ApiError::bad_request("invalid event query parameters".into()))?;
        Ok(Self(query))
    }
}

pub(super) fn parse_operator_cursor(raw: Option<&str>) -> Result<Option<EventCursor>, ApiError> {
    // Present empty query values deliberately reach decode and reject.
    raw.map(EventCursor::decode)
        .transpose()
        .map_err(ApiError::from)
}

pub(super) fn resolve_operator_stream_cursor(
    query: Option<&str>,
    headers: &HeaderMap,
) -> Result<Option<EventCursor>, ApiError> {
    let query = parse_operator_cursor(query)?;
    let mut values = headers.get_all("last-event-id").iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(ApiError::bad_request(
            "exactly one Last-Event-ID header is allowed".into(),
        ));
    }
    let header = match first {
        None => None,
        Some(value) => {
            if value.as_bytes().len() > MAX_EVENT_CURSOR_BYTES {
                return Err(EventCursorRejection::Malformed.into());
            }
            let raw = value
                .to_str()
                .map_err(|_| ApiError::from(EventCursorRejection::Malformed))?;
            // Exactly empty is absent; nonempty values are never trimmed.
            if raw.is_empty() {
                None
            } else {
                Some(EventCursor::decode(raw)?)
            }
        }
    };
    match (query, header) {
        (Some(query), Some(header)) if query != header => {
            Err(ApiError::conflicting_event_cursors())
        }
        (Some(cursor), _) | (_, Some(cursor)) => Ok(Some(cursor)),
        (None, None) => Ok(None),
    }
}
