use super::*;
use runtime_core::{
    ProviderPermissionIntent, ProviderPermissionMutationRequest, ProviderPermissionMutationResult,
    ProviderSessionPreferences, ProviderSessionPreferencesMutationRequest,
    ProviderSessionPreferencesMutationResult, SessionContextLimitSnapshot,
};

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
    Json(request): Json<UpdateAgentPreferencesInput>,
) -> Result<Json<ProviderSessionPreferencesMutationResult>, ApiError> {
    Ok(Json(
        state
            .runtime
            .mutate_workspace_agent_preferences(ProviderSessionPreferencesMutationRequest {
                runtime_session_id: agent_id,
                expected_revision: Some(request.expected_revision),
                current_preferences: request.current_preferences,
            })
            .await?,
    ))
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
    Json(request): Json<UpdateAgentPermissionInput>,
) -> Result<Json<ProviderPermissionMutationResult>, ApiError> {
    Ok(Json(
        state
            .runtime
            .mutate_workspace_agent_permission(ProviderPermissionMutationRequest {
                runtime_session_id: agent_id,
                expected_revision: Some(request.expected_revision),
                permission_intent: request.permission_intent,
            })
            .await?,
    ))
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
