use super::*;

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

/// Database-only historical setup. It never invokes the retired shared
/// spawn executor or a provider, Git command, hook, delivery or runtime write.
/// A supplied original key is required. Any real checkout must be prepared
/// explicitly by the caller and passed to the assignment overload below.
pub(super) async fn legacy_spawn_fixture_json(
    app: &Arc<RuntimeApp>,
    team_id: &str,
    key: Option<&str>,
    input: Value,
) -> (StatusCode, Value) {
    legacy_spawn_fixture_with_assignment_json(app, team_id, key, input, None).await
}

pub(super) async fn legacy_spawn_fixture_with_assignment_json(
    app: &Arc<RuntimeApp>,
    team_id: &str,
    key: Option<&str>,
    mut input: Value,
    assignment: Option<super::legacy_spawn_history::HistoricalWorktreeAssignment>,
) -> (StatusCode, Value) {
    use super::legacy_spawn_history::HistoricalSpawnSeed;
    let result = (|| -> Result<runtime_core::TeamMemberSpawnResponse, RuntimeError> {
        input["team_id"] = serde_json::json!(team_id);
        input["idempotency_key"] = serde_json::json!(key);
        let request: runtime_core::TeamMemberSpawnRequest =
            serde_json::from_value(input).expect("historical spawn fixture");
        let state = app.services.store.hydrate_runtime_state()?;
        let source = state
            .sessions
            .iter()
            .find(|row| row.id == request.source_session_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound("historical source fixture".into()))?;
        let team = runtime_core::TeamWithMembers {
            team: state
                .teams
                .iter()
                .find(|row| row.id == request.team_id)
                .cloned()
                .ok_or_else(|| RuntimeError::NotFound("historical team fixture".into()))?,
            members: state
                .team_members
                .into_iter()
                .filter(|row| row.team_id == request.team_id)
                .collect(),
        };
        let digest = runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap())?;
        let at = source
            .updated_at
            .max(team.team.updated_at)
            .checked_add(1)
            .expect("disposable fixture timestamp headroom");
        let completed_at = at
            .checked_add(2)
            .expect("disposable fixture timestamp headroom");
        let seed = HistoricalSpawnSeed::for_request(
            request,
            source,
            team,
            &format!("op_spawn_http_history_{digest}"),
            &format!("session_http_history_{digest}"),
            assignment,
            at,
        )?;
        seed.seed_to_onboarding_sent(app.services.store.as_ref(), at, at + 1)?;
        seed.complete_seed(app.services.store.as_ref(), completed_at)?;
        Ok(seed.response)
    })();
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
