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
    // A server-minted ID, not an untrusted echoed client header. The same ID
    // is in the error body and response header for cross-layer diagnostics.
    response.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id).expect("generated request id is valid header text"),
    );
    response
}
