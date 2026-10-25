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

#[derive(Debug, Serialize)]
pub(super) struct ProviderCapabilitiesResponse {
    provider: runtime_core::ProviderKind,
    capabilities: runtime_core::ProviderCapabilities,
    harness: runtime_core::HarnessContractMetadata,
}

#[derive(Debug, Serialize)]
pub(super) struct RuntimeCapabilitiesResponse {
    runtime_version: &'static str,
    api_version: &'static str,
    harness_version: &'static str,
    tool_contract_version: String,
    tool_manifest_hash: String,
    /// Exactly the model-visible tools enabled by current runtime policy.
    tools: Vec<&'static str>,
    managed_processes: bool,
    managed_worktrees: bool,
    providers: Vec<RuntimeProviderCapabilities>,
}

#[derive(Debug, Serialize)]
pub(super) struct RuntimeProviderCapabilities {
    metadata: runtime_core::ProviderMetadata,
    capabilities: runtime_core::ProviderCapabilities,
    harness: runtime_core::HarnessContractMetadata,
}

pub(super) async fn runtime_capabilities_v2(
    State(state): State<AppState>,
) -> Result<Json<RuntimeCapabilitiesResponse>, ApiError> {
    let envelope = state.app.services.tool_gateway.capabilities().await?;
    let result = envelope
        .get("result")
        .filter(|_| envelope["ok"] == true)
        .ok_or_else(|| {
            RuntimeError::Bootstrap("tool gateway capability snapshot is unavailable".into())
        })?;
    let team_enabled = result["ggTeamEnabled"]
        .as_bool()
        .ok_or_else(|| RuntimeError::Bootstrap("tool gateway omitted GG team enablement".into()))?;
    let process_enabled = result["ggProcessEnabled"].as_bool().ok_or_else(|| {
        RuntimeError::Bootstrap("tool gateway omitted GG process enablement".into())
    })?;
    let mut tools = Vec::new();
    if team_enabled {
        tools.extend(["gg_team", "gg_message"]);
    }
    if process_enabled {
        tools.push("gg_process");
    }
    let actual = result["tools"]
        .as_array()
        .ok_or_else(|| RuntimeError::Bootstrap("tool gateway omitted tool discovery".into()))?;
    if actual.len() != tools.len()
        || !tools
            .iter()
            .all(|tool| actual.iter().any(|observed| observed == tool))
    {
        return Err(RuntimeError::Bootstrap(
            "tool gateway discovery and enabled capability flags disagree".into(),
        )
        .into());
    }

    let mut providers = Vec::new();
    let mut manifest = None;
    for metadata in state.app.provider_registry.metadata() {
        if !metadata.enabled {
            continue;
        }
        let adapter = state
            .app
            .provider_registry
            .get(metadata.kind)
            .ok_or_else(|| {
                RuntimeError::Bootstrap("enabled provider missing from registry".into())
            })?;
        let harness = runtime_core::harness_contract_metadata(metadata.kind)?;
        if let Some((version, hash)) = &manifest {
            if version != &harness.tool_manifest_version || hash != &harness.tool_manifest_hash {
                return Err(RuntimeError::Bootstrap(
                    "provider tool-contract manifests disagree".into(),
                )
                .into());
            }
        }
        manifest = Some((
            harness.tool_manifest_version.clone(),
            harness.tool_manifest_hash.clone(),
        ));
        providers.push(RuntimeProviderCapabilities {
            metadata,
            capabilities: adapter.capabilities(),
            harness,
        });
    }
    let (tool_contract_version, tool_manifest_hash) = manifest
        .ok_or_else(|| RuntimeError::Bootstrap("runtime has no enabled providers".into()))?;
    Ok(Json(RuntimeCapabilitiesResponse {
        runtime_version: env!("CARGO_PKG_VERSION"),
        api_version: "v2",
        harness_version: runtime_core::HARNESS_VERSION,
        tool_contract_version,
        tool_manifest_hash,
        tools,
        managed_processes: process_enabled,
        managed_worktrees: state.app.worktree_settings.enabled,
        providers,
    }))
}

pub(super) async fn provider_capabilities_v2(
    State(state): State<AppState>,
    Path(provider): Path<String>,
) -> Result<Json<ProviderCapabilitiesResponse>, ApiError> {
    let provider = parse_provider_kind(provider.as_str())?;
    let adapter = state
        .app
        .provider_registry
        .get(provider)
        .ok_or_else(|| ApiError::not_found(format!("provider {}", provider.as_str())))?;
    let harness = runtime_core::harness_contract_metadata(provider).map_err(ApiError::from)?;
    Ok(Json(ProviderCapabilitiesResponse {
        provider,
        capabilities: adapter.capabilities(),
        harness,
    }))
}

pub(super) async fn discover_provider_models_v2(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Json(input): Json<runtime_core::ProviderModelDiscoveryRequest>,
) -> Result<Json<runtime_core::ProviderModelDiscoveryResponse>, ApiError> {
    let provider = parse_provider_kind(provider.as_str())?;
    let adapter = state
        .app
        .provider_registry
        .get(provider)
        .ok_or_else(|| ApiError::not_found(format!("provider {}", provider.as_str())))?;
    input
        .setting_sources_intent
        .resolved_sources(input.cwd.as_deref())
        .map_err(ApiError::from)?;
    Ok(Json(
        adapter
            .discover_models(input)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(super) async fn discover_provider_skills_v2(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Json(input): Json<runtime_core::ProviderSkillDiscoveryRequest>,
) -> Result<Json<runtime_core::ProviderSkillDiscoveryResponse>, ApiError> {
    let provider = parse_provider_kind(provider.as_str())?;
    let adapter = state
        .app
        .provider_registry
        .get(provider)
        .ok_or_else(|| ApiError::not_found(format!("provider {}", provider.as_str())))?;
    input
        .setting_sources_intent
        .resolved_sources(input.cwd.as_deref())
        .map_err(ApiError::from)?;
    Ok(Json(
        adapter
            .discover_skills(input)
            .await
            .map_err(ApiError::from)?,
    ))
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
