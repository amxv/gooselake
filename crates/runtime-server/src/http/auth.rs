use super::*;

pub(super) async fn openapi_yaml() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/yaml; charset=utf-8")],
        generated_openapi_yaml(),
    )
}

#[derive(Debug, Serialize)]
pub(super) struct HealthResponse {
    status: &'static str,
    providers: usize,
    public_base_url: String,
}

#[derive(Debug, Serialize)]
pub(super) struct VersionResponse {
    version: &'static str,
}

pub(super) async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        providers: state.app.provider_registry.len(),
        public_base_url: state.public_base_url,
    })
}

pub(super) async fn protected_health(State(state): State<AppState>) -> Json<HealthResponse> {
    health(State(state)).await
}

pub(super) async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[derive(Debug, Serialize)]
pub(super) struct ProviderListResponse {
    providers: Vec<runtime_core::ProviderMetadata>,
}

pub(super) async fn list_providers(State(state): State<AppState>) -> Json<ProviderListResponse> {
    Json(ProviderListResponse {
        providers: state.app.provider_registry.metadata(),
    })
}

#[derive(Debug, Serialize)]
pub(super) struct ProviderModelsResponse {
    provider: String,
    models: Vec<runtime_core::ProviderModel>,
}

pub(super) async fn list_provider_models(
    State(state): State<AppState>,
    Path(provider): Path<String>,
) -> Result<Json<ProviderModelsResponse>, ApiError> {
    let provider = parse_provider_kind(provider.as_str())?;
    let adapter = state
        .app
        .provider_registry
        .get(provider)
        .ok_or_else(|| ApiError::not_found(format!("provider {}", provider.as_str())))?;
    let models = adapter.list_models().await.map_err(ApiError::from)?;
    Ok(Json(ProviderModelsResponse {
        provider: provider.as_str().to_string(),
        models,
    }))
}

pub(super) async fn codex_auth_status(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    provider_auth_status_response(&state, ProviderKind::Codex).await
}

pub(super) async fn acp_auth_status(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    provider_auth_status_response(&state, ProviderKind::Acp).await
}

pub(super) async fn claude_auth_status(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    provider_auth_status_response(&state, ProviderKind::Claude).await
}

pub(super) async fn provider_auth_status_response(
    state: &AppState,
    provider: ProviderKind,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    let status = state
        .runtime
        .provider_auth_status(provider)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(status))
}

#[derive(Debug, Deserialize)]
pub(super) struct ClaudeApiKeyRequest {
    api_key: String,
}

pub(super) async fn claude_auth_api_key(
    State(state): State<AppState>,
    Json(input): Json<ClaudeApiKeyRequest>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    let status = state
        .runtime
        .provider_auth_set_api_key(ProviderKind::Claude, input.api_key)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(status))
}

#[derive(Debug, Deserialize)]
pub(super) struct ClaudeAuthImportJsonRequest {
    auth_json: Option<Value>,
    auth_json_text: Option<String>,
}

pub(super) async fn claude_auth_import_json(
    State(state): State<AppState>,
    Json(input): Json<ClaudeAuthImportJsonRequest>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    if let Some(auth_json) = input.auth_json {
        let status = state
            .runtime
            .provider_auth_import_json(ProviderKind::Claude, auth_json)
            .await
            .map_err(ApiError::from)?;
        return Ok(Json(status));
    }
    if let Some(auth_json_text) = input.auth_json_text {
        let status = state
            .runtime
            .provider_auth_import_json_text(ProviderKind::Claude, auth_json_text)
            .await
            .map_err(ApiError::from)?;
        return Ok(Json(status));
    }
    Err(ApiError::bad_request(
        "expected auth_json or auth_json_text".to_string(),
    ))
}

pub(super) async fn claude_auth_import_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid multipart payload: {error}")))?
    {
        let name = field.name().unwrap_or_default().to_string();
        if name != "file" {
            continue;
        }
        let bytes = field.bytes().await.map_err(|error| {
            ApiError::bad_request(format!("failed reading upload field: {error}"))
        })?;
        let auth_json_text = String::from_utf8(bytes.to_vec()).map_err(|error| {
            ApiError::bad_request(format!("uploaded file is not utf-8: {error}"))
        })?;
        let status = state
            .runtime
            .provider_auth_import_json_text(ProviderKind::Claude, auth_json_text)
            .await
            .map_err(ApiError::from)?;
        return Ok(Json(status));
    }
    Err(ApiError::bad_request(
        "multipart field 'file' is required".to_string(),
    ))
}

pub(super) async fn claude_auth_logout(
    State(state): State<AppState>,
) -> Result<Json<runtime_core::ProviderAuthStatus>, ApiError> {
    let status = state
        .runtime
        .provider_auth_logout(ProviderKind::Claude)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(status))
}
