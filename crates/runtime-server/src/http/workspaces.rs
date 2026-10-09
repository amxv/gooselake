use super::*;
use runtime_core::{
    prepare_workspace_interrupt, prepare_workspace_lead_transition, prepare_workspace_registration,
    OperationActor, WorkspaceAgentArchiveRequest, WorkspaceAgentCreateRequest,
    WorkspaceAgentInitialRoute, WorkspaceAgentLifecycleState, WorkspaceLeadTransitionRequest,
    WorkspaceRegisterRequest, WorkspaceWorktreeCreateRequest,
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

pub(super) async fn list_workspace_worktree_inventory(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
) -> Result<Json<runtime_core::WorkspaceWorktreeInventory>, ApiError> {
    if !state.app.worktree_settings.enabled {
        return Err(RuntimeError::Unsupported("managed worktrees are disabled".into()).into());
    }
    Ok(Json(
        state
            .runtime
            .list_workspace_worktree_inventory(&workspace_id)?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkspaceWorktreeCreateInput {
    worktree_name: String,
    branch_prefix: Option<String>,
    base_ref: Option<String>,
    deletion_policy: Option<String>,
    run_init_script: Option<bool>,
}

pub(super) async fn create_workspace_worktree(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Json(input): Json<WorkspaceWorktreeCreateInput>,
) -> Result<Json<runtime_core::WorktreeCreateResponse>, ApiError> {
    if !state.app.worktree_settings.enabled {
        return Err(RuntimeError::Unsupported("managed worktrees are disabled".into()).into());
    }
    Ok(Json(
        state
            .app
            .services
            .worktrees
            .create_workspace_worktree(WorkspaceWorktreeCreateRequest {
                workspace_id,
                worktree_name: input.worktree_name,
                branch_prefix: input.branch_prefix,
                base_ref: input.base_ref,
                deletion_policy: input.deletion_policy,
                run_init_script: input.run_init_script,
            })
            .await?,
    ))
}

pub(super) async fn rebind_workspace_agent(
    State(state): State<AppState>,
    Path((workspace_id, agent_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<runtime_core::WorkspaceAgentRebindRequest>,
) -> Result<Json<runtime_core::WorkspaceAgentRebindResponse>, ApiError> {
    if !state.app.worktree_settings.enabled {
        return Err(RuntimeError::Unsupported("managed worktrees are disabled".into()).into());
    }
    let idempotency_key = parse_idempotency_key(&headers)?;
    let mut response = state
        .runtime
        .rebind_workspace_agent(&workspace_id, &agent_id, request, idempotency_key)
        .await?;
    if response.newly_completed {
        response.operation =
            attempt_previous_worktree_cleanup(&state, &workspace_id, &agent_id, response.operation)
                .await?;
    }
    Ok(Json(response))
}

async fn attempt_previous_worktree_cleanup(
    state: &AppState,
    workspace_id: &str,
    agent_id: &str,
    operation: runtime_core::WorkspaceAgentRebindOperation,
) -> Result<runtime_core::WorkspaceAgentRebindOperation, ApiError> {
    if operation.phase != "completed"
        || operation.previous_cleanup_status.as_deref() != Some("pending")
    {
        return Ok(operation);
    }
    let previous_id = operation.previous_worktree_id.as_deref().ok_or_else(|| {
        RuntimeError::ProtocolViolation("pending worktree cleanup has no prior route".into())
    })?;
    let observed = match state
        .app
        .services
        .worktrees
        .cleanup_worktree(runtime_core::WorktreeCleanupRequest {
            worktree_id: previous_id.to_string(),
            reason: Some(format!(
                "verified_workspace_rebind:{}",
                operation.operation_id
            )),
        })
        .await
    {
        Ok(result) => result.status,
        Err(_) => "cleanup_error".into(),
    };
    Ok(state.runtime.record_workspace_agent_rebind_cleanup(
        workspace_id,
        agent_id,
        &operation.operation_id,
        &observed,
    )?)
}

pub(super) async fn retry_workspace_agent_rebind_cleanup(
    State(state): State<AppState>,
    Path((workspace_id, agent_id, operation_id)): Path<(String, String, String)>,
) -> Result<Json<runtime_core::WorkspaceAgentRebindOperation>, ApiError> {
    if !state.app.worktree_settings.enabled {
        return Err(RuntimeError::Unsupported("managed worktrees are disabled".into()).into());
    }
    let operation =
        state
            .runtime
            .get_workspace_agent_rebind(&workspace_id, &agent_id, &operation_id)?;
    if operation.phase != "completed" || !operation.cleanup_previous_worktree {
        return Err(RuntimeError::Conflict(
            "no completed provider rebind with pending previous-worktree cleanup".into(),
        )
        .into());
    }
    Ok(Json(
        attempt_previous_worktree_cleanup(&state, &workspace_id, &agent_id, operation).await?,
    ))
}

pub(super) async fn get_workspace_agent_rebind(
    State(state): State<AppState>,
    Path((workspace_id, agent_id, operation_id)): Path<(String, String, String)>,
) -> Result<Json<runtime_core::WorkspaceAgentRebindOperation>, ApiError> {
    Ok(Json(state.runtime.get_workspace_agent_rebind(
        &workspace_id,
        &agent_id,
        &operation_id,
    )?))
}

pub(super) async fn set_workspace_lead(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<WorkspaceLeadTransitionRequest>,
) -> Result<Json<runtime_core::WorkspaceLeadTransitionResponse>, ApiError> {
    let idempotency_key = parse_idempotency_key(&headers)?;
    let command = prepare_workspace_lead_transition(
        workspace_id.trim(),
        request,
        OperationActor::operator(OPERATOR_PRINCIPAL),
        idempotency_key,
    )?;
    Ok(Json(
        state
            .app
            .services
            .store
            .transition_workspace_lead(&command)?,
    ))
}

pub(super) async fn interrupt_workspace_turns(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<runtime_core::WorkspaceInterruptResponse>, ApiError> {
    let idempotency_key = parse_idempotency_key(&headers)?;
    let command = prepare_workspace_interrupt(
        workspace_id.trim(),
        OperationActor::operator(OPERATOR_PRINCIPAL),
        idempotency_key,
    )?;
    Ok(Json(state.runtime.interrupt_workspace(command).await?))
}

#[derive(Debug, Deserialize)]
pub(super) struct WorkspaceAgentListQuery {
    lifecycle: Option<String>,
}

pub(super) async fn create_workspace_agent(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Json(mut request): Json<WorkspaceAgentCreateRequest>,
) -> Result<Json<runtime_core::WorkspaceAgentRecord>, ApiError> {
    if let Some(WorkspaceAgentInitialRoute::New {
        worktree_name,
        branch_prefix,
        base_ref,
        deletion_policy,
        run_init_script,
    }) = request.worktree.clone()
    {
        if !state.app.worktree_settings.enabled {
            return Err(RuntimeError::Unsupported("managed worktrees are disabled".into()).into());
        }
        if request.cwd.is_some() {
            return Err(RuntimeError::Conflict(
                "new managed worktree selection cannot also specify cwd".into(),
            )
            .into());
        }
        let result = state
            .app
            .services
            .worktrees
            .create_workspace_worktree(WorkspaceWorktreeCreateRequest {
                workspace_id: workspace_id.clone(),
                worktree_name,
                branch_prefix,
                base_ref,
                deletion_policy,
                run_init_script,
            })
            .await?;
        request.worktree = Some(WorkspaceAgentInitialRoute::Existing {
            worktree_id: result.worktree.id,
        });
    }
    if !state.app.worktree_settings.enabled
        && matches!(
            request.worktree,
            Some(WorkspaceAgentInitialRoute::Existing { .. })
        )
    {
        return Err(RuntimeError::Unsupported("managed worktrees are disabled".into()).into());
    }
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
