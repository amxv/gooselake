use super::*;

pub(super) async fn mcp_capabilities(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let mut result = state
        .app
        .services
        .tool_gateway
        .capabilities()
        .await
        .map_err(ApiError::from)?;
    // Keep the operator-only v1 discovery compatible with existing split-tool
    // clients. Provider sessions never use v1 or its operator credential.
    if let Some(capabilities) = result.get_mut("result") {
        let process = capabilities["ggProcessEnabled"].as_bool().unwrap_or(false);
        let team = capabilities["ggTeamEnabled"].as_bool().unwrap_or(false);
        let mut tools = Vec::new();
        let mut namespaces = Vec::new();
        if process {
            namespaces.push("gg_process");
            tools.extend([
                "gg_process_run",
                "gg_process_status",
                "gg_process_logs",
                "gg_process_kill",
            ]);
        }
        if team {
            namespaces.push("gg_team");
            tools.extend(["gg_team_status", "gg_team_message", "gg_team_manage"]);
        }
        capabilities["tools"] = serde_json::json!(tools);
        capabilities["supportedNamespaces"] = serde_json::json!(namespaces);
    }
    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
pub(super) struct McpInvokeRequest {
    namespace: Option<String>,
    #[serde(alias = "toolName")]
    tool_name: String,
    #[serde(alias = "callerAgentId")]
    caller_agent_id: String,
    #[serde(default, alias = "invocationId")]
    invocation_id: Option<String>,
    #[serde(default)]
    args: serde_json::Value,
}

pub(super) async fn mcp_invoke(
    State(state): State<AppState>,
    Json(request): Json<McpInvokeRequest>,
) -> Result<Json<Value>, ApiError> {
    let caller_session_id = request.caller_agent_id.trim();
    if caller_session_id.is_empty() {
        return Err(ApiError::bad_request(
            "caller_agent_id is required".to_string(),
        ));
    }
    if request.tool_name.trim().is_empty() {
        return Err(ApiError::bad_request("tool_name is required".to_string()));
    }
    let caller_session = state
        .runtime
        .get_session(caller_session_id)
        .await
        .map_err(ApiError::from)?;
    if matches!(caller_session.status.as_str(), "closed" | "failed") {
        return Err(ApiError::bad_request(format!(
            "caller session {} is not active (status={})",
            caller_session_id, caller_session.status
        )));
    }
    let result = state
        .app
        .services
        .tool_gateway
        .invoke_tool(ToolInvokeRequest {
            namespace: request.namespace,
            tool_name: request.tool_name,
            caller_session_id: caller_session_id.to_string(),
            invocation_id: request.invocation_id,
            args: request.args,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopedMcpInvokeRequest {
    namespace: Option<String>,
    tool_name: String,
    #[serde(default)]
    args: Value,
}

async fn scoped_mcp_caller(state: &AppState, headers: &HeaderMap) -> Result<String, ApiError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|authorization| authorization.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::unauthorized("scoped MCP credential required".to_string()))?;
    let caller = runtime_core::agent_from_scoped_mcp_token(&state.bearer_token, token)
        .ok_or_else(|| ApiError::unauthorized("invalid scoped MCP credential".to_string()))?;
    let agent = state
        .runtime
        .get_active_workspace_agent_by_id(&caller)
        .map_err(|_| ApiError::unauthorized("scoped MCP agent not active".to_string()))?;
    let workspace = state
        .app
        .services
        .store
        .get_workspace(&agent.workspace_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::unauthorized("scoped MCP workspace is unavailable".to_string()))?;
    if workspace.lifecycle_state != runtime_core::WorkspaceLifecycleState::Active {
        return Err(ApiError::unauthorized(
            "scoped MCP workspace not active".to_string(),
        ));
    }
    let session = state
        .runtime
        .get_session(&caller)
        .await
        .map_err(|_| ApiError::unauthorized("scoped MCP session not active".to_string()))?;
    if matches!(session.status.as_str(), "closed" | "failed") {
        return Err(ApiError::unauthorized(
            "scoped MCP session closed".to_string(),
        ));
    }
    Ok(caller)
}

pub(super) async fn scoped_mcp_capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    scoped_mcp_caller(&state, &headers).await?;
    let result = state
        .app
        .services
        .tool_gateway
        .capabilities()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(result))
}

pub(super) async fn scoped_mcp_invoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ScopedMcpInvokeRequest>,
) -> Result<Json<Value>, ApiError> {
    let caller_session_id = scoped_mcp_caller(&state, &headers).await?;
    if request.tool_name.trim().is_empty() {
        return Err(ApiError::bad_request("tool_name is required".into()));
    }
    if !matches!(
        request.tool_name.as_str(),
        "gg_team" | "gg_message" | "gg_process"
    ) {
        return Ok(Json(serde_json::json!({"ok":false,"error":{
            "code":"bad_request", "message":"unknown model-facing GG tool", "retryable":false
        }})));
    }
    // Invocation correlation belongs to the trusted MCP host transport, not
    // the model-authored JSON body. The sidecar reuses this value on retries.
    let invocation_id = headers
        .get("x-gg-tool-invocation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'_' | b'-' | b'.' | b':'))
        })
        .map(str::to_owned);
    let mode = request.args.get("mode").and_then(Value::as_str);
    let mutating = match request.tool_name.as_str() {
        "gg_team" => mode != Some("status"),
        "gg_message" => true,
        "gg_process" => matches!(mode, Some("run" | "cancel")),
        _ => false,
    };
    if mutating && invocation_id.is_none() {
        // A model must not be able to create a non-idempotent mutation by
        // omitting host correlation. The bundled sidecar supplies one stable
        // request identity across transport retries.
        return Ok(Json(serde_json::json!({"ok":false,"error":{
            "code":"missing_invocation_id",
            "message":"mutating GG tools require a trusted tool invocation ID; no mutation was admitted",
            "retryable":false
        }})));
    }
    let result = state
        .app
        .services
        .tool_gateway
        .invoke_tool(ToolInvokeRequest {
            namespace: request.namespace,
            tool_name: request.tool_name,
            caller_session_id,
            invocation_id,
            args: request.args,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(result))
}
