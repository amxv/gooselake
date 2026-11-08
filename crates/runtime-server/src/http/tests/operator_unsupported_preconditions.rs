use super::workspace_worktree_routes::{call, fixture};
use super::*;

async fn post_with_unsupported_if_match(
    router: &Router,
    token: &str,
    uri: &str,
    input: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::IF_MATCH, "\"0\"")
                .header(header::CONTENT_TYPE, "application/json")
                .body(input.map_or_else(Body::empty, |body| Body::from(body.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, body)
}

#[tokio::test]
async fn nonrevisioned_native_mutations_refuse_false_if_match_guarantees() {
    let (router, token, temp, provider, workspace, agent, _) = fixture().await;
    let agent_base = format!(
        "/v2/workspaces/{}/agents/{}",
        workspace.workspace_id, agent.agent_id
    );
    let create_base = format!("/v2/workspaces/{}/agents", workspace.workspace_id);
    let store = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let count = |table: &str| -> i64 {
        store
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    };
    let (operations, agents, worktrees, turns) = (
        count("runtime_operations"),
        count("workspace_agents"),
        count("managed_worktrees"),
        count("turns"),
    );
    let provider_sends = provider
        .send_invocations
        .load(std::sync::atomic::Ordering::SeqCst);

    for (uri, input) in [
        (
            format!("/v2/workspaces/{}/worktrees", workspace.workspace_id),
            Some(serde_json::json!({"worktree_name":"ignored-ifmatch-worktree"})),
        ),
        (
            create_base.clone(),
            Some(serde_json::json!({
                "provider":"codex", "model":"test-model",
                "worktree":{"mode":"new","worktree_name":"ignored-ifmatch-agent"}
            })),
        ),
        (
            format!("{agent_base}/archive"),
            Some(serde_json::json!({"reason":"would archive native provider"})),
        ),
        (
            format!("/v2/agents/{}/turns", agent.agent_id),
            Some(serde_json::json!({"input":[{"type":"text","text":"do not send"}]})),
        ),
        (format!("/v2/agents/{}/compact", agent.agent_id), None),
        (
            format!("/v2/agents/{}/context-limit/refresh", agent.agent_id),
            None,
        ),
        (
            format!("/v2/workspaces/{}/interrupt", workspace.workspace_id),
            None,
        ),
    ] {
        let (status, error) = post_with_unsupported_if_match(&router, &token, &uri, input).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {error}");
        assert_eq!(
            error["error"]["code"], "revision_precondition_not_supported",
            "{uri}"
        );
        assert_eq!(error["error"]["retryable"], false);
        assert!(error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no revision precondition was checked"));
        assert_eq!(count("runtime_operations"), operations, "{uri}");
        assert_eq!(count("workspace_agents"), agents, "{uri}");
        assert_eq!(count("managed_worktrees"), worktrees, "{uri}");
        assert_eq!(count("turns"), turns, "{uri}");
    }
    assert_eq!(provider.compaction_calls(), 0);
    assert_eq!(
        provider
            .send_invocations
            .load(std::sync::atomic::Ordering::SeqCst),
        provider_sends
    );
    let worktree_list = std::process::Command::new("git")
        .arg("-C")
        .arg(&workspace.canonical_root)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .unwrap();
    assert!(worktree_list.status.success());
    let worktree_list = String::from_utf8_lossy(&worktree_list.stdout);
    assert!(!worktree_list.contains("ignored-ifmatch-worktree"));
    assert!(!worktree_list.contains("ignored-ifmatch-agent"));

    let (status, archived) = call(
        &router,
        &token,
        "POST",
        &format!("{agent_base}/archive"),
        Some(serde_json::json!({"reason":"positive archive"})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived}");
    assert_eq!(archived["lifecycle_state"], "archived");
    let (status, invalid_restore) =
        post_with_unsupported_if_match(&router, &token, &format!("{agent_base}/restore"), None)
            .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid_restore["error"]["code"],
        "revision_precondition_not_supported"
    );
    let (status, still_archived) = call(&router, &token, "GET", &agent_base, None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(still_archived["lifecycle_state"], "archived");
    let (status, restored) = call(
        &router,
        &token,
        "POST",
        &format!("{agent_base}/restore"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    assert_eq!(restored["lifecycle_state"], "active");
}
