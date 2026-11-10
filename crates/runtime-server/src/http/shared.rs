use super::*;

/// Mutations without a JSON input contract must not silently execute while
/// discarding options such as `dry_run` or `force`. Old generic API clients
/// may send `{}` for every POST; preserve that no-options representation.
/// The extractor runs before any operation admission or native effect.
pub(super) fn require_empty_request_body(body: &[u8]) -> Result<(), ApiError> {
    if body.is_empty()
        || serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(body)
            .is_ok_and(|value| value.is_empty())
    {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "this operation does not accept a request body".into(),
        ))
    }
}

pub(super) fn parse_provider_kind(value: &str) -> Result<ProviderKind, ApiError> {
    ProviderKind::from_str(value)
        .ok_or_else(|| ApiError::bad_request(format!("unknown provider {}", value)))
}

pub(super) fn parse_last_event_id_header(headers: &HeaderMap) -> Result<Option<i64>, ApiError> {
    let mut values = headers.get_all("last-event-id").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        // The cursor is a continuation of already observed history. Choosing
        // the first of multiple values may silently skip events when a proxy
        // and client disagree about the selected resume point.
        return Err(ApiError::bad_request(
            "exactly one Last-Event-ID header is allowed".into(),
        ));
    }
    let raw = value
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid last-event-id header encoding".to_string()))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = trimmed.parse::<i64>().map_err(|_| {
        ApiError::bad_request("invalid last-event-id header; expected integer".to_string())
    })?;
    if parsed < 0 {
        return Err(ApiError::bad_request(
            "invalid last-event-id header; expected non-negative integer".to_string(),
        ));
    }
    Ok(Some(parsed))
}

/// Resource lists use a stable `(created_at, id)` keyset rather than offset
/// pages, which can skip rows when new records arrive between requests.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct ResourcePageQuery {
    pub(super) cursor: Option<String>,
    pub(super) limit: Option<usize>,
}

impl ResourcePageQuery {
    pub(super) fn parse(&self) -> Result<(usize, Option<(i64, String)>), ApiError> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) {
            return Err(ApiError::bad_request(
                "limit must be between 1 and 100".into(),
            ));
        }
        let cursor = self
            .cursor
            .as_ref()
            .map(|cursor| {
                let (time, id) = cursor
                    .split_once(':')
                    .ok_or_else(|| ApiError::bad_request("invalid pagination cursor".into()))?;
                let timestamp = time
                    .parse::<i64>()
                    .map_err(|_| ApiError::bad_request("invalid pagination cursor".into()))?;
                if timestamp < 0
                    || id.is_empty()
                    || id.len() > 128
                    || !id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
                {
                    return Err(ApiError::bad_request("invalid pagination cursor".into()));
                }
                Ok((timestamp, id.to_string()))
            })
            .transpose()?;
        Ok((limit, cursor))
    }
}

pub(super) fn paged_response<T: Serialize>(
    mut items: Vec<T>,
    limit: usize,
    row_key: impl Fn(&T) -> (i64, &str),
) -> Response {
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_cursor = if has_more {
        items.last().map(|item| {
            let (timestamp, id) = row_key(item);
            format!("{timestamp}:{id}")
        })
    } else {
        None
    };
    let mut response = Json(items).into_response();
    if let Some(cursor) = next_cursor {
        response.headers_mut().insert(
            "x-next-cursor",
            axum::http::HeaderValue::from_str(&cursor)
                .expect("bounded database ids produce valid cursor header"),
        );
    }
    response
}

pub(super) fn revisioned_response<T: Serialize>(revision: u64, record: T) -> Response {
    let mut response = Json(record).into_response();
    response.headers_mut().insert(
        header::ETAG,
        axum::http::HeaderValue::from_str(&format!("\"{revision}\""))
            .expect("numeric revision produces a valid ETag"),
    );
    response
}

/// Include the exact durable operation coordinate in the header without
/// changing the existing JSON response body, status or ETag. This is only
/// used for actions whose result names a durable operation; an ordinary
/// session run or ephemeral refresh must not invent an operation ID.
pub(super) fn with_operation_id<R: IntoResponse>(body: R, operation_id: &str) -> Response {
    let mut response = body.into_response();
    response.headers_mut().insert(
        "x-operation-id",
        axum::http::HeaderValue::from_str(operation_id)
            .expect("durable operation IDs are valid HTTP header text"),
    );
    response
}

/// A caller-supplied replay key must never be silently ignored on a native
/// provider mutation. Until that specific operation has a durable intent and
/// exact-replay journal, fail before provider or lifecycle effects rather
/// than falsely advertising at-least-once retries as idempotent.
pub(super) fn reject_unjournaled_idempotency_key(
    headers: &HeaderMap,
    action: &str,
) -> Result<(), ApiError> {
    if headers.contains_key("idempotency-key") {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "idempotency_not_supported",
            format!(
                "Idempotency-Key is not supported for {action}; no durable provider mutation was admitted"
            ),
        ));
    }
    Ok(())
}

/// A nonrevisioned operator action may still have durable native-effect
/// fences, but it has no `If-Match` compare-and-set contract. Never silently
/// ignore a supplied strong ETag while performing that action.
pub(super) fn reject_unsupported_if_match(
    headers: &HeaderMap,
    action: &str,
) -> Result<(), ApiError> {
    if headers.contains_key(header::IF_MATCH) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "revision_precondition_not_supported",
            format!("If-Match is not supported for {action}; no revision precondition was checked"),
        ));
    }
    Ok(())
}

pub(super) fn check_if_match_revision(
    headers: &HeaderMap,
    expected_revision: u64,
) -> Result<(), ApiError> {
    let mut values = headers.get_all(header::IF_MATCH).iter();
    let Some(value) = values.next() else {
        return Ok(());
    };
    if values.next().is_some() || value.as_bytes() != format!("\"{expected_revision}\"").as_bytes()
    {
        return Err(ApiError::new(
            StatusCode::PRECONDITION_FAILED,
            "revision_precondition_failed",
            "If-Match must be the strong ETag of the expected revision".into(),
        ));
    }
    Ok(())
}

pub(super) async fn bearer_auth(
    State(expected_token): State<String>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // A proxy and this runtime must not disagree about which duplicate
    // Authorization value has authority. Require exactly one header even
    // when a later (or earlier) duplicate contains the correct token.
    let mut auth_values = request.headers().get_all(header::AUTHORIZATION).iter();
    let auth_header = match (auth_values.next(), auth_values.next()) {
        (Some(value), None) => value.to_str().ok(),
        _ => None,
    };

    let expected = format!("Bearer {expected_token}");
    if auth_header == Some(expected.as_str()) {
        return next.run(request).await;
    }

    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "error": "missing or invalid bearer token",
        })),
    )
        .into_response()
}

#[derive(Debug, Clone)]
pub(super) struct ApiError {
    status: StatusCode,
    pub(super) message: String,
    pub(super) code: String,
    pub(super) retryable: bool,
    pub(super) details: Option<Value>,
}

impl ApiError {
    fn new(status: StatusCode, code: impl Into<String>, message: String) -> Self {
        Self {
            status,
            code: code.into(),
            message,
            retryable: false,
            details: None,
        }
    }

    pub(super) fn unauthorized(message: String) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }
    pub(super) fn bad_request(message: String) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    pub(super) fn not_found(message: String) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }
}

impl From<RuntimeError> for ApiError {
    fn from(value: RuntimeError) -> Self {
        match value {
            RuntimeError::NotFound(message) | RuntimeError::ProviderNotRegistered(message) => {
                Self::not_found(message)
            }
            RuntimeError::Configuration(message) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_configuration", message)
            }
            RuntimeError::InvalidState(message) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_state", message)
            }
            RuntimeError::ProtocolViolation(message) => {
                Self::new(StatusCode::BAD_REQUEST, "protocol_violation", message)
            }
            RuntimeError::Unsupported(message) => {
                Self::new(StatusCode::BAD_REQUEST, "unsupported_capability", message)
            }
            RuntimeError::Conflict(message) => Self::new(StatusCode::CONFLICT, "conflict", message),
            RuntimeError::OperationUnresolved {
                operation_id,
                message,
            } => {
                let mut error = Self::new(
                    StatusCode::CONFLICT,
                    "outcome_unresolved",
                    format!("{message}; inspect /v2/operations/{operation_id}"),
                );
                error.details = Some(serde_json::json!({"operation_id": operation_id}));
                error
            }
            RuntimeError::OperatorPolicyNotApplied { operation_id } => {
                let mut error = Self::new(
                    StatusCode::CONFLICT,
                    "operator_verified_not_applied",
                    format!("native policy was verified not applied; original key is consumed; inspect /v2/operations/{operation_id}"),
                );
                error.details = Some(serde_json::json!({"operation_id": operation_id}));
                error
            }
            RuntimeError::OperatorRestoreNotResumed { operation_id } => {
                let mut error = Self::new(
                    StatusCode::CONFLICT,
                    "operator_verified_not_resumed",
                    format!("native agent restoration was verified not applied; original key is consumed; inspect /v2/operations/{operation_id}"),
                );
                error.details = Some(serde_json::json!({"operation_id": operation_id}));
                error
            }
            RuntimeError::OperatorArchiveNotClosed { operation_id } => {
                let mut error = Self::new(
                    StatusCode::CONFLICT,
                    "operator_verified_not_closed",
                    format!("native agent archive was verified not closed; original key is consumed; inspect /v2/operations/{operation_id}"),
                );
                error.details = Some(serde_json::json!({"operation_id": operation_id}));
                error
            }
            RuntimeError::OperatorAgentNotCreated { operation_id } => {
                let mut error = Self::new(
                    StatusCode::CONFLICT,
                    "operator_verified_not_created",
                    format!("native agent creation was independently verified absent; original key is consumed; inspect /v2/operations/{operation_id}"),
                );
                error.details = Some(serde_json::json!({"operation_id": operation_id}));
                error
            }
            RuntimeError::OperatorTurnNotDispatched {
                operation_id,
                code,
                message,
            } => {
                let mut error = Self::new(StatusCode::CONFLICT, code, message);
                error.details = Some(serde_json::json!({
                    "dispatch_outcome":"not_dispatched",
                    "operation_id": operation_id,
                }));
                error
            }
            RuntimeError::OnboardingPending(message) => {
                let mut error = Self::new(StatusCode::CONFLICT, "onboarding_pending", message);
                error.retryable = true;
                error
            }
            RuntimeError::ProviderDispatch {
                code,
                outcome,
                operation_id,
                message,
            } => {
                let mut error = Self::new(StatusCode::CONFLICT, code, message);
                let mut details = serde_json::json!({"dispatch_outcome":outcome});
                if let Some(operation_id) = operation_id {
                    details["operation_id"] = serde_json::Value::String(operation_id);
                }
                error.details = Some(details);
                error
            }
            RuntimeError::ProviderAlreadyRegistered(message) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "provider_already_registered",
                message,
            ),
            RuntimeError::Bootstrap(message) | RuntimeError::Io(message) => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // The legacy /v1 body remains stable. /v2 wraps this internal error
        // evidence in its versioned machine-readable response contract.
        let mut response = (
            self.status,
            Json(serde_json::json!({
                "error": self.message.clone(),
            })),
        )
            .into_response();
        response.extensions_mut().insert(self);
        response
    }
}
