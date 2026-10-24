use super::*;
use runtime_core::{
    prepare_workspace_retirement_review, OperationActor, WorkspaceRetirementReviewReceipt,
    WorkspaceRetirementReviewRequest,
};

/// This route belongs exclusively to the operator bearer-authenticated router;
/// it is never exposed as a model-facing tool or an agent-scoped MCP action.
pub(super) async fn reconcile_workspace_retirement(
    State(state): State<AppState>,
    Path((workspace_id, operation_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<WorkspaceRetirementReviewRequest>,
) -> Result<Json<WorkspaceRetirementReviewReceipt>, ApiError> {
    let command = prepare_workspace_retirement_review(
        &workspace_id,
        &operation_id,
        request,
        OperationActor::operator(OPERATOR_PRINCIPAL),
        parse_idempotency_key(&headers)?,
    )?;
    let outcome = state
        .runtime
        .reconcile_workspace_retirement(&command)
        .await?;
    if let Some(original) = outcome.resume_command {
        crate::workspace_retirement_worker::spawn_retirement_worker(
            state.runtime.clone(),
            state.app.services.process_manager.clone(),
            original,
        );
    }
    Ok(Json(outcome.receipt))
}
