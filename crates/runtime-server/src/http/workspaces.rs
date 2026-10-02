use super::*;
use runtime_core::{prepare_workspace_registration, OperationActor, WorkspaceRegisterRequest};

const OPERATOR_PRINCIPAL: &str = "runtime_operator";

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

fn parse_idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        ApiError::bad_request("invalid Idempotency-Key header encoding".to_string())
    })?;
    Ok(Some(value.to_string()))
}
