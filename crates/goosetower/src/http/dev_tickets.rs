use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::{origin_is_allowed, TicketIssuer};

use super::AppState;

#[derive(Debug, Deserialize)]
pub(super) struct DevTicketRequest {
    subject: Option<String>,
    workspace_id: Option<String>,
    scopes: Option<Vec<String>>,
    allowed_origins: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub(super) struct DevTicketResponse {
    ticket: String,
    expires_in_secs: u64,
    issuer: String,
    audience: String,
}

pub(super) async fn dev_ticket_preflight(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::FORBIDDEN)?;
    let allowed_origins = state
        .config
        .allowed_gooseweb_origins()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !origin_is_allowed(origin, &allowed_origins) {
        return Err(StatusCode::FORBIDDEN);
    }

    let origin_value = HeaderValue::from_str(origin).map_err(|_| StatusCode::FORBIDDEN)?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin_value);
    response_headers.insert(header::VARY, HeaderValue::from_static("origin"));
    response_headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("POST, OPTIONS"),
    );
    response_headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type"),
    );
    response_headers.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
    Ok((StatusCode::NO_CONTENT, response_headers))
}

pub(super) async fn mint_dev_ticket(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<DevTicketRequest>,
) -> Result<Response, StatusCode> {
    if !state.config.debug.endpoints_enabled {
        return Err(StatusCode::FORBIDDEN);
    }
    let cors_origin = dev_ticket_cors_origin(&state, &headers)?;
    let issuer =
        TicketIssuer::from_config(&state.config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let allowed_origins = input
        .allowed_origins
        .unwrap_or_else(|| state.config.allowed_gooseweb_origins().unwrap_or_default());
    let ticket = issuer
        .mint_dev_ticket(
            input.subject.unwrap_or_else(|| "dev-user".to_string()),
            input.workspace_id.unwrap_or_else(|| {
                state
                    .config
                    .runtimes
                    .sources
                    .first()
                    .map(|source| source.workspace_id.clone())
                    .unwrap_or_else(|| "default".to_string())
            }),
            input.scopes.unwrap_or_else(|| {
                vec!["gateway:connect".to_string(), "gateway:command".to_string()]
            }),
            allowed_origins,
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut response = Json(DevTicketResponse {
        ticket,
        expires_in_secs: state.config.tickets.ttl_secs,
        issuer: state.config.tickets.issuer.clone(),
        audience: state.config.tickets.audience.clone(),
    })
    .into_response();
    if let Some(origin) = cors_origin {
        let headers = response.headers_mut();
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(header::VARY, HeaderValue::from_static("origin"));
    }
    Ok(response)
}

fn dev_ticket_cors_origin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<HeaderValue>, StatusCode> {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(None);
    };
    let allowed_origins = state
        .config
        .allowed_gooseweb_origins()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !origin_is_allowed(origin, &allowed_origins) {
        return Err(StatusCode::FORBIDDEN);
    }
    HeaderValue::from_str(origin)
        .map(Some)
        .map_err(|_| StatusCode::FORBIDDEN)
}
