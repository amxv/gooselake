use super::*;
use runtime_core::{
    EditAndRerunRequest, ProviderPermissionIntent, ProviderPermissionMutationRequest,
    ProviderSessionPreferences, ProviderSessionPreferencesMutationRequest, SessionCompactionRun,
    SessionContextLimitSnapshot, SessionEditRerunOperation,
};

pub(super) async fn edit_agent_turn_and_rerun(
    State(state): State<AppState>,
    Path((agent_id, turn_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<EditAndRerunRequest>,
) -> Result<Json<SessionEditRerunOperation>, ApiError> {
    let key = parse_idempotency_key(&headers)?;
    Ok(Json(
        state
            .runtime
            .edit_workspace_agent_turn(&agent_id, &turn_id, input.edited_snapshot, key)
            .await?,
    ))
}

pub(super) async fn get_agent_edit_operation(
    State(state): State<AppState>,
    Path((agent_id, operation_id)): Path<(String, String)>,
) -> Result<Json<SessionEditRerunOperation>, ApiError> {
    Ok(Json(
        state
            .runtime
            .session_edit_rerun_operation(&agent_id, &operation_id)?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EditReviewInput {
    evidence: String,
}

pub(super) async fn resolve_agent_edit_operation(
    State(state): State<AppState>,
    Path((agent_id, operation_id)): Path<(String, String)>,
    Json(input): Json<EditReviewInput>,
) -> Result<Json<SessionEditRerunOperation>, ApiError> {
    Ok(Json(state.runtime.resolve_edit_rerun_review(
        &agent_id,
        &operation_id,
        &input.evidence,
    )?))
}

pub(super) async fn compact_agent_session(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<SessionCompactionRun>, ApiError> {
    let key = parse_idempotency_key(&headers)?;
    Ok(Json(
        state
            .runtime
            .compact_workspace_agent(&agent_id, key)
            .await?,
    ))
}

pub(super) async fn get_agent_compaction_run(
    State(state): State<AppState>,
    Path((agent_id, run_id)): Path<(String, String)>,
) -> Result<Json<SessionCompactionRun>, ApiError> {
    Ok(Json(
        state.runtime.session_compaction_run(&agent_id, &run_id)?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManualCompactionReviewInput {
    phase: String,
    evidence: String,
}

pub(super) async fn resolve_agent_compaction_review(
    State(state): State<AppState>,
    Path((agent_id, run_id)): Path<(String, String)>,
    Json(input): Json<ManualCompactionReviewInput>,
) -> Result<Json<SessionCompactionRun>, ApiError> {
    Ok(Json(state.runtime.resolve_compaction_review(
        &agent_id,
        &run_id,
        &input.phase,
        &input.evidence,
    )?))
}

pub(super) async fn send_agent_turn_v2(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<PublicSendTurnInput>,
) -> Result<Response, ApiError> {
    let idempotency_key = parse_idempotency_key(&headers)?;
    let input = SendTurnInput::from(input);
    if let Some(key) = idempotency_key {
        let (accepted, operation_id) = state
            .runtime
            .send_operator_user_turn(
                agent_id.as_str(),
                input,
                runtime_core::OperationActor::operator(OPERATOR_PRINCIPAL),
                key,
            )
            .await?;
        let mut response = Json(accepted).into_response();
        response.headers_mut().insert(
            "x-operation-id",
            axum::http::HeaderValue::from_str(&operation_id)
                .expect("durable turn operation id is valid HTTP header text"),
        );
        return Ok(response);
    }
    state
        .runtime
        .get_active_workspace_agent_by_id(agent_id.as_str())?;
    Ok(Json(state.runtime.send_turn(agent_id.as_str(), input).await?).into_response())
}

/// A missing snapshot is represented as null, not a guessed percentage.
pub(super) async fn get_agent_context_limit(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
) -> Result<Json<Option<SessionContextLimitSnapshot>>, ApiError> {
    Ok(Json(
        state.runtime.session_context_limit(agent_id.as_str())?,
    ))
}

pub(super) async fn refresh_agent_context_limit(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
) -> Result<Json<SessionContextLimitSnapshot>, ApiError> {
    Ok(Json(
        state
            .runtime
            .refresh_session_context_limit(agent_id.as_str(), None)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateAgentPreferencesInput {
    expected_revision: u64,
    current_preferences: ProviderSessionPreferences,
}

pub(super) async fn update_agent_preferences(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<UpdateAgentPreferencesInput>,
) -> Result<Response, ApiError> {
    check_if_match_revision(&headers, request.expected_revision)?;
    let result = state
        .runtime
        .mutate_workspace_agent_preferences(ProviderSessionPreferencesMutationRequest {
            runtime_session_id: agent_id,
            expected_revision: Some(request.expected_revision),
            current_preferences: request.current_preferences,
        })
        .await?;
    Ok(revisioned_response(result.revision, result))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateAgentPermissionInput {
    expected_revision: u64,
    permission_intent: ProviderPermissionIntent,
}

pub(super) async fn update_agent_permission(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<UpdateAgentPermissionInput>,
) -> Result<Response, ApiError> {
    check_if_match_revision(&headers, request.expected_revision)?;
    let result = state
        .runtime
        .mutate_workspace_agent_permission(ProviderPermissionMutationRequest {
            runtime_session_id: agent_id,
            expected_revision: Some(request.expected_revision),
            permission_intent: request.permission_intent,
        })
        .await?;
    Ok(revisioned_response(result.revision, result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_mutation_requires_expected_revision_and_rejects_extra_fields() {
        assert!(
            serde_json::from_value::<UpdateAgentPreferencesInput>(serde_json::json!({
                "current_preferences": {"thinking_effort":"high"}
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<UpdateAgentPreferencesInput>(serde_json::json!({
                "expected_revision": 1,
                "current_preferences": {"thinking_effort":"high"},
                "runtime_session_id": "spoofed"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<UpdateAgentPermissionInput>(serde_json::json!({
                "permission_intent":{"kind":"provider_default"}
            }))
            .is_err()
        );
    }
}
