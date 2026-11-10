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
) -> Result<Response, ApiError> {
    reject_unsupported_if_match(&headers, "historical user turn edit-and-rerun")?;
    let key = parse_idempotency_key(&headers)?;
    let result = state
        .runtime
        .edit_workspace_agent_turn(&agent_id, &turn_id, input.edited_snapshot, key)
        .await?;
    let id = result.operation_id.clone();
    Ok(with_operation_id(Json(result), &id))
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct EditReviewInput {
    evidence: String,
}

pub(super) async fn resolve_agent_edit_operation(
    State(state): State<AppState>,
    Path((agent_id, operation_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<EditReviewInput>,
) -> Result<Response, ApiError> {
    reject_unsupported_if_match(&headers, "agent edit review")?;
    reject_unjournaled_idempotency_key(&headers, "agent edit review")?;
    let result =
        state
            .runtime
            .resolve_edit_rerun_review(&agent_id, &operation_id, &input.evidence)?;
    let id = result.operation_id.clone();
    Ok(with_operation_id(Json(result), &id))
}

pub(super) async fn compact_agent_session(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<SessionCompactionRun>, ApiError> {
    require_empty_request_body(&body)?;
    reject_unsupported_if_match(&headers, "native session compaction")?;
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManualCompactionReviewInput {
    phase: String,
    evidence: String,
}

pub(super) async fn resolve_agent_compaction_review(
    State(state): State<AppState>,
    Path((agent_id, run_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<ManualCompactionReviewInput>,
) -> Result<Json<SessionCompactionRun>, ApiError> {
    reject_unsupported_if_match(&headers, "agent compaction review")?;
    reject_unjournaled_idempotency_key(&headers, "agent compaction review")?;
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
    reject_unsupported_if_match(&headers, "user turn submission")?;
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
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<SessionContextLimitSnapshot>, ApiError> {
    require_empty_request_body(&body)?;
    reject_unsupported_if_match(&headers, "context-limit refresh")?;
    reject_unjournaled_idempotency_key(&headers, "context-limit refresh")?;
    Ok(Json(
        state
            .runtime
            .refresh_session_context_limit(agent_id.as_str(), None)
            .await?,
    ))
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateAgentPreferencesInput {
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
    let idempotency_key = parse_idempotency_key(&headers)?
        .unwrap_or_else(|| format!("implicit-policy-{:032x}", rand::random::<u128>()));
    let request = ProviderSessionPreferencesMutationRequest {
        runtime_session_id: agent_id,
        expected_revision: Some(request.expected_revision),
        current_preferences: request.current_preferences,
    };
    let (result, operation_id) = state
        .runtime
        .mutate_workspace_agent_preferences_operator(
            request,
            runtime_core::OperationActor::operator(OPERATOR_PRINCIPAL),
            idempotency_key,
        )
        .await?;
    Ok(with_operation_id(
        revisioned_response(result.revision, result),
        &operation_id,
    ))
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateAgentPermissionInput {
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
    let idempotency_key = parse_idempotency_key(&headers)?
        .unwrap_or_else(|| format!("implicit-policy-{:032x}", rand::random::<u128>()));
    let request = ProviderPermissionMutationRequest {
        runtime_session_id: agent_id,
        expected_revision: Some(request.expected_revision),
        permission_intent: request.permission_intent,
    };
    let (result, operation_id) = state
        .runtime
        .mutate_workspace_agent_permission_operator(
            request,
            runtime_core::OperationActor::operator(OPERATOR_PRINCIPAL),
            idempotency_key,
        )
        .await?;
    Ok(with_operation_id(
        revisioned_response(result.revision, result),
        &operation_id,
    ))
}

/// Explicit resolution of one quarantined native provider policy effect.
/// The caller supplies independent evidence and exact original native
/// attachment identity; the runtime never contacts the provider here.
pub(super) async fn resolve_agent_native_policy_review(
    State(state): State<AppState>,
    Path((agent_id, operation_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<runtime_core::OperatorNativePolicyReviewInput>,
) -> Result<Response, ApiError> {
    check_if_match_revision(&headers, input.expected_revision)?;
    reject_unjournaled_idempotency_key(&headers, "native policy evidence review")?;
    let result = state
        .runtime
        .resolve_operator_native_policy_review(
            agent_id.trim(),
            operation_id.trim(),
            input,
            runtime_core::OperationActor::operator(OPERATOR_PRINCIPAL),
        )
        .await?;
    Ok(with_operation_id(Json(result), operation_id.trim()))
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
