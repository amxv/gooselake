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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicSendTurnInput {
    input: Vec<Value>,
    expected_turn_id: Option<String>,
    permission_mode: Option<String>,
}

impl From<PublicSendTurnInput> for SendTurnInput {
    fn from(input: PublicSendTurnInput) -> Self {
        Self {
            input: input.input,
            expected_turn_id: input.expected_turn_id,
            permission_mode: input.permission_mode,
            projection_source: Some(runtime_core::TurnInputProjectionSource::UserVisible),
            user_input_snapshot: None,
            correlation_id: None,
        }
    }
}

pub(super) async fn send_turn(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(input): Json<PublicSendTurnInput>,
) -> Result<Json<SendTurnAccepted>, ApiError> {
    let input = SendTurnInput::from(input);
    let accepted = state
        .runtime
        .send_turn(session_id.as_str(), input)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(accepted))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_send_turn_rejects_internal_provenance_fields() {
        let result = serde_json::from_value::<PublicSendTurnInput>(serde_json::json!({
            "input": [{"type":"text","text":"hello"}],
            "expected_turn_id": null,
            "permission_mode": null,
            "projection_source": "automation_context"
        }));
        assert!(result.is_err());
    }

    #[test]
    fn public_send_turn_is_forced_to_user_visible_provenance() {
        let public = serde_json::from_value::<PublicSendTurnInput>(serde_json::json!({
            "input": [{"type":"text","text":"hello"}],
            "expected_turn_id": "prior",
            "permission_mode": "default"
        }))
        .expect("public send-turn body");
        let internal = SendTurnInput::from(public);
        assert_eq!(
            internal.projection_source,
            Some(runtime_core::TurnInputProjectionSource::UserVisible)
        );
        assert!(internal.user_input_snapshot.is_none());
        assert!(internal.correlation_id.is_none());
    }
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
