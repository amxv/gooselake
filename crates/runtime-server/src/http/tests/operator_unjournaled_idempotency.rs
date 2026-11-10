use super::workspace_worktree_routes::{call, fixture};
use super::*;

#[tokio::test]
async fn native_agent_lifecycle_replay_is_durable_and_unsupported_policies_admit_nothing() {
    let (router, token, temp, _, workspace, _, _) = fixture().await;
    let base = format!("/v2/workspaces/{}/agents", workspace.workspace_id);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let count = |table: &str| -> i64 {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    let agent_count = count("workspace_agents");
    let worktree_count = count("managed_worktrees");
    let input = serde_json::json!({
        "provider":"codex", "model":"test-model",
        "worktree":{"mode":"new","worktree_name":"no-false-replay"}
    });

    let (status, body) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(input.clone()),
        Some("caller-believes-agent-create-is-idempotent"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["lifecycle_state"], "active");
    assert_eq!(count("workspace_agents"), agent_count + 1);
    assert_eq!(count("managed_worktrees"), worktree_count + 1);
    let after_first = count("runtime_operations");
    let (status, exact) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(input),
        Some("caller-believes-agent-create-is-idempotent"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{exact:?}");
    assert_eq!(exact, body);
    assert_eq!(count("workspace_agents"), agent_count + 1);
    assert_eq!(count("managed_worktrees"), worktree_count + 1);
    assert_eq!(count("runtime_operations"), after_first);

    let (status, agent) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{agent:?}");
    let agent_id = agent["agent_id"].as_str().unwrap();
    let revision = agent["revision"].as_u64().unwrap();
    let operation_count = count("runtime_operations");
    // Native agent creation/archive/restore now accept exact durable keys.
    // Provider policy edits do as well. Here the fake provider has no capability,
    // so neither operation admission nor provider mutation is permitted.
    for (path, input) in [
        (
            format!("/v2/agents/{agent_id}/preferences"),
            serde_json::json!({
                "expected_revision":revision,
                "current_preferences":{"thinking_effort":"high"}
            }),
        ),
        (
            format!("/v2/agents/{agent_id}/permission"),
            serde_json::json!({
                "expected_revision":revision,
                "permission_intent":{"kind":"provider_default"}
            }),
        ),
    ] {
        let (status, body) = call(
            &router,
            &token,
            "POST",
            &path,
            Some(input),
            Some("should-not-be-accepted"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
        assert_eq!(body["error"]["code"], "unsupported_capability");
    }
    assert_eq!(
        count("runtime_operations"),
        operation_count,
        "unsupported provider admission must not create an operation"
    );
    let agent_url = format!("{base}/{agent_id}");
    let (status, still_active) = call(&router, &token, "GET", &agent_url, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(still_active["lifecycle_state"], "active");
    assert_eq!(still_active["revision"], revision);

    // Unkeyed archive is now a one-shot durable native operation, not a
    // best-effort provider close that silently claims completion.
    let (status, archived) = call(
        &router,
        &token,
        "POST",
        &format!("{agent_url}/archive"),
        Some(serde_json::json!({"reason":"normal operator archive"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived:?}");
    assert_eq!(archived["lifecycle_state"], "archived");

    let (status, restored) = call(
        &router,
        &token,
        "POST",
        &format!("{agent_url}/restore"),
        None,
        Some("replay-native-restore-safely"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{restored:?}");
    assert_eq!(restored["lifecycle_state"], "active");
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &format!("{agent_url}/restore"),
        None,
        Some("replay-native-restore-safely"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(
        replay, restored,
        "keyed replay must return the exact original record"
    );
    let (status, still_active) = call(&router, &token, "GET", &agent_url, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(still_active["lifecycle_state"], "active");
}
