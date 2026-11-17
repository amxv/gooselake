use super::*;

pub(super) async fn create_worktree() -> Result<StatusCode, ApiError> {
    Err(legacy_native_refusal(
        "create a managed worktree through /v2/workspaces/{workspace_id}/worktrees",
    ))
}

pub(super) async fn list_worktrees(
    State(state): State<AppState>,
) -> Result<Json<Vec<runtime_core::ManagedWorktreeRecord>>, ApiError> {
    let rows = state
        .app
        .services
        .worktrees
        .list_worktrees()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(rows))
}

pub(super) async fn get_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
) -> Result<Json<runtime_core::ManagedWorktreeRecord>, ApiError> {
    let row = state
        .app
        .services
        .worktrees
        .get_worktree(worktree_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(row))
}

#[derive(Debug, Deserialize)]
pub(super) struct WorktreeClaimInput {
    session_id: String,
    claim_role: String,
}

pub(super) async fn claim_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
    Json(input): Json<WorktreeClaimInput>,
) -> Result<Json<runtime_core::WorktreeClaimResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .claim_worktree(WorktreeClaimRequest {
            worktree_id,
            session_id: input.session_id,
            claim_role: input.claim_role,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
pub(super) struct WorktreeReleaseInput {
    session_id: String,
    cleanup_if_last_claim: Option<bool>,
}

pub(super) async fn release_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
    Json(input): Json<WorktreeReleaseInput>,
) -> Result<Json<runtime_core::WorktreeReleaseResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .release_worktree(WorktreeReleaseRequest {
            worktree_id,
            session_id: input.session_id,
            cleanup_if_last_claim: input.cleanup_if_last_claim,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

#[derive(Debug, Deserialize)]
pub(super) struct WorktreeCleanupInput {
    reason: Option<String>,
}

pub(super) async fn cleanup_worktree(
    State(state): State<AppState>,
    Path(worktree_id): Path<String>,
    input: Option<Json<WorktreeCleanupInput>>,
) -> Result<Json<runtime_core::WorktreeCleanupResponse>, ApiError> {
    let input = input
        .map(|Json(value)| value)
        .unwrap_or(WorktreeCleanupInput { reason: None });
    let response = state
        .app
        .services
        .worktrees
        .cleanup_worktree(WorktreeCleanupRequest {
            worktree_id,
            reason: input.reason,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}
