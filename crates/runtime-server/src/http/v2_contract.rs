use super::*;
use axum::http::HeaderValue;

/// Errors from application handlers carry typed facts through response
/// extensions; framework rejections (JSON/query/method/path) are normalized
/// here as well. /v1 and the independently authenticated /v2/mcp gateway are
/// intentionally not wrapped by this operator API middleware.
pub(super) async fn v2_operator_response_contract(request: Request<Body>, next: Next) -> Response {
    let request_id = format!("req_{:032x}", rand::random::<u128>());
    let response = next.run(request).await;
    let status = response.status();
    let mut unresolved_operation_id = None;
    let mut response = if status.is_client_error() || status.is_server_error() {
        let error = response.extensions().get::<ApiError>().cloned();
        let (code, message, retryable, details) = if let Some(error) = error {
            let message = if status.is_server_error() {
                "internal server error".to_string()
            } else {
                error.message
            };
            (error.code, message, error.retryable, error.details)
        } else {
            let (code, message) = match status {
                StatusCode::NOT_FOUND => ("not_found", "operator API route not found"),
                StatusCode::METHOD_NOT_ALLOWED => ("method_not_allowed", "method not allowed"),
                StatusCode::PAYLOAD_TOO_LARGE => ("payload_too_large", "request body too large"),
                StatusCode::UNSUPPORTED_MEDIA_TYPE => {
                    ("unsupported_media_type", "unsupported request content type")
                }
                StatusCode::UNPROCESSABLE_ENTITY | StatusCode::BAD_REQUEST => (
                    "invalid_request",
                    "invalid request body or query parameters",
                ),
                StatusCode::UNAUTHORIZED => {
                    ("unauthorized", "missing or invalid operator bearer token")
                }
                StatusCode::FORBIDDEN => ("forbidden", "operator access forbidden"),
                StatusCode::CONFLICT => ("conflict", "request conflicts with current state"),
                _ => ("request_failed", "request failed"),
            };
            (code.to_string(), message.to_string(), false, None)
        };
        if code == "outcome_unresolved" {
            unresolved_operation_id = details
                .as_ref()
                .and_then(|value: &serde_json::Value| value.get("operation_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        }
        (
            status,
            Json(serde_json::json!({
                "error": {
                    "code":code,
                    "message":message,
                    "retryable":retryable,
                    "details":details,
                },
                "request_id": request_id,
            })),
        )
            .into_response()
    } else {
        response
    };
    if let Some(operation_id) = unresolved_operation_id {
        if let Ok(value) = HeaderValue::from_str(&operation_id) {
            response.headers_mut().insert("x-operation-id", value);
        }
    }
    // A server-minted ID, not an untrusted echoed client header. The same ID
    // is in the error body and response header for cross-layer diagnostics.
    response.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id).expect("generated request id is valid header text"),
    );
    response
}

/// Name/ID and kind/ID collection cursors use hex-encoded JSON pairs rather
/// than timestamps. This is URL-safe even for legacy names with punctuation or
/// Unicode, and the codec rejects corrupt or oversized cursors.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorktreeInventoryPageQuery {
    pub(super) cursor: Option<String>,
    pub(super) limit: Option<usize>,
}

impl WorktreeInventoryPageQuery {
    pub(super) fn parse(&self) -> Result<(usize, Option<(String, String)>), ApiError> {
        let limit = validate_opaque_page_limit(self.limit)?;
        let cursor = self
            .cursor
            .as_deref()
            .map(decode_opaque_pair_cursor)
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|(name, id)| name.len() > 128 || id.len() > 512)
        {
            return Err(ApiError::bad_request(
                "invalid worktree pagination cursor".into(),
            ));
        }
        Ok((limit, cursor))
    }
}

pub(super) fn validate_opaque_page_limit(limit: Option<usize>) -> Result<usize, ApiError> {
    let limit = limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::bad_request(
            "limit must be between 1 and 100".into(),
        ));
    }
    Ok(limit)
}

pub(super) fn encode_opaque_pair_cursor(name: &str, id: &str) -> String {
    use std::fmt::Write;
    let serialized = serde_json::to_vec(&(name, id)).expect("cursor identifiers must serialize");
    let mut cursor = String::with_capacity(serialized.len() * 2);
    for byte in serialized {
        write!(&mut cursor, "{byte:02x}").expect("writing cursor cannot fail");
    }
    cursor
}

pub(super) fn decode_opaque_pair_cursor(cursor: &str) -> Result<(String, String), ApiError> {
    let invalid = || ApiError::bad_request("invalid pagination cursor".into());
    if cursor.is_empty() || cursor.len() > 4096 || cursor.len() % 2 != 0 {
        return Err(invalid());
    }
    let mut decoded = Vec::with_capacity(cursor.len() / 2);
    for pair in cursor.as_bytes().chunks_exact(2) {
        let hex = std::str::from_utf8(pair).map_err(|_| invalid())?;
        decoded.push(u8::from_str_radix(hex, 16).map_err(|_| invalid())?);
    }
    let (name, id): (String, String) = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
    if name.is_empty() || name.len() > 128 || id.is_empty() || id.len() > 512 {
        return Err(invalid());
    }
    Ok((name, id))
}

pub(super) fn paged_worktree_inventory(
    mut inventory: runtime_core::WorkspaceWorktreeInventory,
    limit: usize,
    cursor: Option<(String, String)>,
) -> Response {
    if let Some((name, id)) = cursor {
        inventory.worktrees.retain(|row| {
            (row.normalized_name.as_str(), row.worktree_id.as_str()) > (name.as_str(), id.as_str())
        });
    }
    let has_more = inventory.worktrees.len() > limit;
    inventory.worktrees.truncate(limit);
    let next_cursor = if has_more {
        inventory
            .worktrees
            .last()
            .map(|row| encode_opaque_pair_cursor(&row.normalized_name, &row.worktree_id))
    } else {
        None
    };
    let mut response = Json(inventory).into_response();
    if let Some(cursor) = next_cursor {
        response.headers_mut().insert(
            "x-next-cursor",
            HeaderValue::from_str(&cursor).expect("hex cursor produces valid header text"),
        );
    }
    response
}
