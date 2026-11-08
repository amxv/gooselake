use super::workspace_worktree_routes::{call, fixture};
use super::*;

fn assert_unjournaled_key_rejected(status: StatusCode, body: &serde_json::Value) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(body["error"]["code"], "idempotency_not_supported");
    assert_eq!(body["error"]["retryable"], false);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("no durable provider mutation was admitted"));
    assert!(body["request_id"]
        .as_str()
        .is_some_and(|id| id.starts_with("req_")));
}

#[tokio::test]
async fn unjournaled_provider_mutations_cannot_silently_accept_idempotency_keys() {
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
    let operation_count = count("runtime_operations");

    let (status, body) = call(
        &router,
        &token,
        "POST",
        &base,
        Some(serde_json::json!({
            "provider":"codex", "model":"test-model",
            "worktree":{"mode":"new","worktree_name":"no-false-replay"}
        })),
        Some("caller-believes-agent-create-is-idempotent"),
    )
    .await;
    assert_unjournaled_key_rejected(status, &body);
    assert_eq!(count("workspace_agents"), agent_count);
    assert_eq!(count("managed_worktrees"), worktree_count);
    assert_eq!(count("runtime_operations"), operation_count);

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
        (
            format!("{base}/{agent_id}/archive"),
            serde_json::json!({"reason":"would close native session"}),
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
        assert_unjournaled_key_rejected(status, &body);
    }
    let agent_url = format!("{base}/{agent_id}");
    let (status, still_active) = call(&router, &token, "GET", &agent_url, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(still_active["lifecycle_state"], "active");
    assert_eq!(still_active["revision"], revision);

    // The old unkeyed native lifecycle paths are intentionally unchanged.
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

    let (status, body) = call(
        &router,
        &token,
        "POST",
        &format!("{agent_url}/restore"),
        None,
        Some("do-not-replay-native-restore"),
    )
    .await;
    assert_unjournaled_key_rejected(status, &body);
    let (status, still_archived) = call(&router, &token, "GET", &agent_url, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(still_archived["lifecycle_state"], "archived");
}
