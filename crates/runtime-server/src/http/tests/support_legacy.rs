use super::*;

/// Explicit synthetic history setup through the internal manager. This never
/// sends a request to a disabled legacy HTTP mutation. Response wrapping lets
/// existing event/process fixtures keep their JSON decoding assertions.
pub(super) async fn legacy_session_fixture_response(
    runtime: &Arc<RuntimeSessionManager>,
    input: Value,
) -> Response {
    let input: CreateSessionInput = serde_json::from_value(input).expect("legacy fixture input");
    let session = runtime
        .create_session(input)
        .await
        .expect("seed synthetic legacy session");
    Json(session).into_response()
}

/// Internal legacy fixture/domain probe; no HTTP route is bypassed or enabled.
pub(super) async fn legacy_session_fixture_json(
    runtime: &Arc<RuntimeSessionManager>,
    input: Value,
) -> (StatusCode, Value) {
    let result = runtime
        .create_session(serde_json::from_value(input).unwrap())
        .await;
    legacy_fixture_result(result).await
}

pub(super) async fn legacy_worktree_fixture_json(
    app: &Arc<RuntimeApp>,
    input: Value,
) -> (StatusCode, Value) {
    let result = app
        .services
        .worktrees
        .create_worktree(serde_json::from_value(input).unwrap())
        .await;
    legacy_fixture_result(result).await
}

/// Explicit inert-provider historical setup; public legacy spawn stays inspection-only.
pub(super) async fn legacy_spawn_fixture_json(
    app: &Arc<RuntimeApp>,
    team_id: &str,
    key: Option<&str>,
    mut input: Value,
) -> (StatusCode, Value) {
    input["team_id"] = serde_json::json!(team_id);
    input["idempotency_key"] = serde_json::json!(key);
    let result = app
        .services
        .worktrees
        .spawn_team_member(serde_json::from_value(input).expect("historical spawn fixture"))
        .await;
    legacy_fixture_result(result).await
}

async fn legacy_fixture_result<T: serde::Serialize>(
    result: Result<T, RuntimeError>,
) -> (StatusCode, Value) {
    let response = match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => ApiError::from(error).into_response(),
    };
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
