use super::*;

#[tokio::test]
async fn lead_transitions_reject_unknown_fields_before_changing_authority_or_revision() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("strict-lead-inputs");
    fs::create_dir_all(&root).unwrap();
    let (status, workspace) = super::workspace_worktree_routes::call(
        &router,
        &token,
        "POST",
        "/v2/workspaces",
        Some(serde_json::json!({"canonical_root":root,"display_name":"Strict Lead"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let workspace_url = format!("/v2/workspaces/{workspace_id}");
    let (status, agent) = super::workspace_worktree_routes::call(
        &router,
        &token,
        "POST",
        &format!("{workspace_url}/agents"),
        Some(serde_json::json!({"provider":"codex","model":"test-model"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let agent_id = agent["agent_id"].as_str().unwrap();
    for malformed in [
        serde_json::json!({
            "lead_agent_id": agent_id,
            "expected_revision": 0,
            "elect_if_missing": true,
        }),
        serde_json::json!({
            "lead_agent_id": null,
            "expected_revision": 0,
            "force": true,
        }),
    ] {
        let (status, body) = super::workspace_worktree_routes::call(
            &router,
            &token,
            "POST",
            &format!("{workspace_url}/lead"),
            Some(malformed),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "invalid_request");
    }
    let (status, workspace) =
        super::workspace_worktree_routes::call(&router, &token, "GET", &workspace_url, None, None)
            .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(workspace["lead_agent_id"], serde_json::Value::Null);
    assert_eq!(workspace["revision"], 0);
}
