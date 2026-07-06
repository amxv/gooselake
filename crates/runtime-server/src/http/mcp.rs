use super::*;

pub(super) async fn mcp_capabilities(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let result = state
        .app
        .services
        .tool_gateway
        .capabilities()
        .await
        .map_err(ApiError::from)?;
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
