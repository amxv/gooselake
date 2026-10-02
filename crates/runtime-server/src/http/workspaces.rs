use super::*;
use runtime_core::{
    prepare_workspace_registration, OperationActor, WorkspaceAgentArchiveRequest,
    WorkspaceAgentCreateRequest, WorkspaceAgentLifecycleState, WorkspaceRegisterRequest,
};

pub(super) const OPERATOR_PRINCIPAL: &str = "runtime_operator";

pub(super) async fn register_workspace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<WorkspaceRegisterRequest>,
) -> Result<Json<runtime_core::WorkspaceRegisterResponse>, ApiError> {
    let idempotency_key = parse_idempotency_key(&headers)?;
    let command = prepare_workspace_registration(
        request,
        OperationActor::operator(OPERATOR_PRINCIPAL),
        idempotency_key,
    )?;
    let response = state.app.services.store.register_workspace(&command)?;
    Ok(Json(response))
}

pub(super) async fn list_workspaces(
    State(state): State<AppState>,
) -> Result<Json<Vec<runtime_core::WorkspaceRecord>>, ApiError> {
    Ok(Json(state.app.services.store.list_workspaces()?))
}

pub(super) async fn get_workspace(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
) -> Result<Json<runtime_core::WorkspaceRecord>, ApiError> {
    let workspace = state
        .app
        .services
        .store
        .get_workspace(workspace_id.trim())?
        .ok_or_else(|| ApiError::not_found(format!("workspace {workspace_id}")))?;
    Ok(Json(workspace))
}

#[derive(Debug, Deserialize)]
pub(super) struct WorkspaceAgentListQuery {
    lifecycle: Option<String>,
}

pub(super) async fn create_workspace_agent(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Json(request): Json<WorkspaceAgentCreateRequest>,
) -> Result<Json<runtime_core::WorkspaceAgentRecord>, ApiError> {
    let agent = state
        .runtime
        .create_workspace_agent(workspace_id.trim(), request, OPERATOR_PRINCIPAL)
        .await?;
    Ok(Json(agent))
}

pub(super) async fn list_workspace_agents(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<WorkspaceAgentListQuery>,
) -> Result<Json<Vec<runtime_core::WorkspaceAgentRecord>>, ApiError> {
    ensure_workspace_exists(&state, workspace_id.trim())?;
    let lifecycle = match query.lifecycle.as_deref().map(str::trim) {
        None | Some("") | Some("active") => Some(WorkspaceAgentLifecycleState::Active),
        Some("archived") => Some(WorkspaceAgentLifecycleState::Archived),
        Some("all") => None,
        Some(value) => {
            return Err(ApiError::bad_request(format!(
                "unknown workspace agent lifecycle {value:?}; expected active, archived, or all"
            )))
        }
    };
    Ok(Json(
        state
            .runtime
            .list_workspace_agents(workspace_id.trim(), lifecycle)?,
    ))
}

pub(super) async fn get_workspace_agent(
    State(state): State<AppState>,
    Path((workspace_id, agent_id)): Path<(String, String)>,
) -> Result<Json<runtime_core::WorkspaceAgentRecord>, ApiError> {
    ensure_workspace_exists(&state, workspace_id.trim())?;
    Ok(Json(state.runtime.get_workspace_agent(
        workspace_id.trim(),
        agent_id.trim(),
    )?))
}

pub(super) async fn archive_workspace_agent(
    State(state): State<AppState>,
    Path((workspace_id, agent_id)): Path<(String, String)>,
    Json(request): Json<WorkspaceAgentArchiveRequest>,
) -> Result<Json<runtime_core::WorkspaceAgentRecord>, ApiError> {
    let archived = state
        .runtime
        .archive_workspace_agent(
            workspace_id.trim(),
            agent_id.trim(),
            request.reason.as_deref(),
        )
        .await?;
    Ok(Json(archived))
}

pub(super) async fn restore_workspace_agent(
    State(state): State<AppState>,
    Path((workspace_id, agent_id)): Path<(String, String)>,
) -> Result<Json<runtime_core::WorkspaceAgentRecord>, ApiError> {
    let restored = state
        .runtime
        .restore_workspace_agent(workspace_id.trim(), agent_id.trim())
        .await?;
    Ok(Json(restored))
}

pub(super) async fn get_operation(
    State(state): State<AppState>,
    Path(operation_id): Path<String>,
) -> Result<Json<runtime_core::OperationDetails>, ApiError> {
    let operation = state
        .app
        .services
        .store
        .get_operation(operation_id.trim())?
        .ok_or_else(|| ApiError::not_found(format!("operation {operation_id}")))?;
    Ok(Json(operation))
}

pub(super) fn parse_idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        ApiError::bad_request("invalid Idempotency-Key header encoding".to_string())
    })?;
    Ok(Some(value.to_string()))
}

fn ensure_workspace_exists(state: &AppState, workspace_id: &str) -> Result<(), ApiError> {
    state
        .app
        .services
        .store
        .get_workspace(workspace_id)?
        .ok_or_else(|| ApiError::not_found(format!("workspace {workspace_id}")))?;
    Ok(())
}
