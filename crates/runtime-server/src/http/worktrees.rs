use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct WorktreeCreateInput {
    source_session_id: String,
    repo_root: Option<String>,
    worktree_name: String,
    branch_prefix: Option<String>,
    base_ref: Option<String>,
    deletion_policy: Option<String>,
    run_init_script: Option<bool>,
    created_by_session_id: Option<String>,
    operation_id: Option<String>,
    team_id: Option<String>,
}

pub(super) async fn create_worktree(
    State(state): State<AppState>,
    Json(input): Json<WorktreeCreateInput>,
) -> Result<Json<runtime_core::WorktreeCreateResponse>, ApiError> {
    let response = state
        .app
        .services
        .worktrees
        .create_worktree(WorktreeCreateRequest {
            team_id: input.team_id,
            source_session_id: input.source_session_id,
            repo_root: input.repo_root,
            worktree_name: input.worktree_name,
            branch_prefix: input.branch_prefix,
            base_ref: input.base_ref,
            deletion_policy: input.deletion_policy,
            run_init_script: input.run_init_script,
            created_by_session_id: input.created_by_session_id,
            operation_id: input.operation_id,
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
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
