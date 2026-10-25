use super::*;

pub(super) fn parse_provider_kind(value: &str) -> Result<ProviderKind, ApiError> {
    ProviderKind::from_str(value)
        .ok_or_else(|| ApiError::bad_request(format!("unknown provider {}", value)))
}

pub(super) fn parse_last_event_id_header(headers: &HeaderMap) -> Result<Option<i64>, ApiError> {
    let Some(value) = headers.get("last-event-id") else {
        return Ok(None);
    };
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
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

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
            RuntimeError::OnboardingPending(message) => {
                let mut error = Self::new(StatusCode::CONFLICT, "onboarding_pending", message);
                error.retryable = true;
                error
            }
            RuntimeError::ProviderDispatch {
                code,
                outcome,
                message,
            } => {
                let mut error = Self::new(StatusCode::CONFLICT, code, message);
                error.details = Some(serde_json::json!({"dispatch_outcome":outcome}));
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
