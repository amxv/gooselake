use super::*;

pub(super) async fn create_session(
    State(state): State<AppState>,
    Json(input): Json<CreateSessionInput>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let session = state
        .runtime
        .create_session(input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

pub(super) async fn list_sessions(
    State(state): State<AppState>,
) -> Json<Vec<runtime_core::SessionRecord>> {
    Json(state.runtime.list_sessions().await)
}

pub(super) async fn get_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let session = state
        .runtime
        .get_session(session_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

pub(super) async fn resume_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    input: Option<Json<ResumeSessionInput>>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let input = input
        .map(|Json(value)| value)
        .unwrap_or(ResumeSessionInput {
            provider_session_ref: None,
            canonical_provider_session_ref: None,
        });
    let session = state
        .runtime
        .resume_session(session_id.as_str(), input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

#[derive(Debug, Deserialize)]
pub(super) struct CloseSessionInput {
    reason: Option<String>,
}

pub(super) async fn close_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    input: Option<Json<CloseSessionInput>>,
) -> Result<Json<runtime_core::SessionRecord>, ApiError> {
    let reason = input.and_then(|Json(value)| value.reason);
    let session = state
        .runtime
        .close_session(session_id.as_str(), reason)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(session))
}

pub(super) async fn send_turn(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(input): Json<SendTurnInput>,
) -> Result<Json<SendTurnAccepted>, ApiError> {
    let accepted = state
        .runtime
        .send_turn(session_id.as_str(), input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(accepted))
}

pub(super) async fn interrupt_turn(
    State(state): State<AppState>,
    Path((session_id, turn_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    state
        .runtime
        .interrupt_turn(session_id.as_str(), turn_id.as_str())
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::ACCEPTED)
}

pub(super) async fn respond_approval(
    State(state): State<AppState>,
    Path((session_id, approval_id)): Path<(String, String)>,
    Json(input): Json<ApprovalResponseInput>,
) -> Result<Json<runtime_core::ApprovalRecord>, ApiError> {
    let approval = state
        .runtime
        .respond_approval(session_id.as_str(), approval_id.as_str(), input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(approval))
}
