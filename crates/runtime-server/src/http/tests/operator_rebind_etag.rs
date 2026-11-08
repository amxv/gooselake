use super::workspace_worktree_routes::fixture;
use super::*;

#[tokio::test]
async fn workspace_rebind_honors_strong_revision_precondition_before_native_effect() {
    let (router, token, temp, provider, workspace, agent, _) = fixture().await;
    let uri = format!(
        "/v2/workspaces/{}/agents/{}/worktree",
        workspace.workspace_id, agent.agent_id
    );
    let body = serde_json::json!({
        "worktree_id":"wt_linked", "expected_revision":agent.revision
    });
    let store = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let rebind_count = || -> i64 {
        store
            .query_row("SELECT COUNT(*) FROM workspace_agent_rebinds", [], |row| {
                row.get(0)
            })
            .unwrap()
    };
    let request = |etag_values: &[String], key: &str, input: &serde_json::Value| {
        let mut builder = Request::builder()
            .method("POST")
            .uri(&uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", key);
        for etag in etag_values {
            builder = builder.header(header::IF_MATCH, etag.as_str());
        }
        builder.body(Body::from(input.to_string())).unwrap()
    };

    let initial = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/workspaces/{}/agents/{}",
                    workspace.workspace_id, agent.agent_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(initial.status(), StatusCode::OK);
    let original_tag = format!("\"{}\"", agent.revision);
    assert_eq!(initial.headers()[header::ETAG], original_tag);

    for (index, etags) in [
        vec![format!("\"{}\"", agent.revision + 1)],
        vec![format!("W/\"{}\"", agent.revision)],
        vec![original_tag.clone(), original_tag.clone()],
    ]
    .into_iter()
    .enumerate()
    {
        let bad = router
            .clone()
            .oneshot(request(&etags, &format!("bad-etag-{index}"), &body))
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::PRECONDITION_FAILED);
        let error: serde_json::Value =
            serde_json::from_slice(&to_bytes(bad.into_body(), usize::MAX).await.unwrap()).unwrap();
        assert_eq!(error["error"]["code"], "revision_precondition_failed");
        assert_eq!(error["error"]["retryable"], false);
        assert_eq!(rebind_count(), 0, "bad If-Match must not admit a claim");
        assert_eq!(provider.rebind_calls().await, 0, "must not rebind provider");
    }

    let accepted = router
        .clone()
        .oneshot(request(&[original_tag.clone()], "matched-rebind", &body))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    let success_tag = format!("\"{}\"", agent.revision + 1);
    assert_eq!(accepted.headers()[header::ETAG], success_tag);
    let operation_id = accepted.headers()["x-operation-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let success: serde_json::Value =
        serde_json::from_slice(&to_bytes(accepted.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(success["operation"]["operation_id"], operation_id);
    assert_eq!(success["agent"]["revision"], agent.revision + 1);
    assert_eq!(rebind_count(), 1);
    assert_eq!(provider.rebind_calls().await, 1);

    // The old request's If-Match is compared with its original JSON revision,
    // not against the newer agent state: a lost response is still replayable.
    let replay = router
        .clone()
        .oneshot(request(&[original_tag], "matched-rebind", &body))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()[header::ETAG], success_tag);
    assert_eq!(replay.headers()["x-operation-id"], operation_id);
    let replay_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(replay_body["operation"], success["operation"]);
    assert_eq!(rebind_count(), 1);
    assert_eq!(provider.rebind_calls().await, 1);

    let stale_header = router
        .clone()
        .oneshot(request(
            &[format!("\"{}\"", agent.revision)],
            "changed-precondition",
            &serde_json::json!({
                "worktree_id":null, "expected_revision":agent.revision + 1
            }),
        ))
        .await
        .unwrap();
    assert_eq!(stale_header.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(rebind_count(), 1);
    assert_eq!(provider.rebind_calls().await, 1);
}
