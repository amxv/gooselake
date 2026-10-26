use super::*;
use std::collections::HashSet;

async fn hook_get(
    router: &Router,
    token: Option<&str>,
    uri: &str,
) -> (StatusCode, Option<String>, serde_json::Value) {
    let mut request = Request::builder().uri(uri);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let next = response
        .headers()
        .get("x-next-cursor")
        .map(|value| value.to_str().unwrap().to_owned());
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, next, body)
}

#[tokio::test]
async fn operator_hook_history_is_redacted_paginated_and_exactly_inspectable() {
    let (router, token, temp) = build_test_router().await;
    let mut connection = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let transaction = connection.transaction().unwrap();
    for workspace in ["ws_hook_alpha", "ws_hook_beta"] {
        transaction.execute(
            "INSERT INTO workspaces
             (workspace_id,canonical_root,display_name,lifecycle_state,revision,created_at,updated_at)
             VALUES(?1,?2,?1,'active',0,1,1)",
            rusqlite::params![workspace, temp.path().join(workspace).to_string_lossy()],
        ).unwrap();
    }
    for index in 0..75i64 {
        let workspace = if index % 3 == 0 {
            "ws_hook_beta"
        } else {
            "ws_hook_alpha"
        };
        let event = format!("evt_hook_{index:03}");
        let name = format!("private:hook-{index:03}");
        let hook = serde_json::json!({
            "event_name":"post_teammate_add", "name":name,
            "command":"echo private shell command", "timeout_ms":500, "scope":"project"
        });
        transaction
            .execute(
                "INSERT INTO runtime_events
             (event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
             VALUES(?1,'workspace',?2,?3,'workspace.hook_parent',0,'{}',100)",
                rusqlite::params![event, workspace, index + 1],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO workspace_hook_effects
             (workspace_id,parent_event_id,event_name,hook_name,hook_json,input_json,
              phase,attempt_count,receipt_json,created_at_ms,updated_at_ms,application_state)
             VALUES(?1,?2,'post_teammate_add',?3,?4,?5,
                    'succeeded',1,?6,100,120,'applied')",
                rusqlite::params![
                    workspace,
                    event,
                    name,
                    hook.to_string(),
                    r#"{"secret":"private input"}"#,
                    r#"{"validated_output":{"secret":"private output"},"error_code":null}"#,
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);

    let base = "/v2/workspaces/ws_hook_alpha/hooks/effects";
    let (status, _, body) = hook_get(&router, None, base).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], "unauthorized");

    let mut cursor = None;
    let mut seen = HashSet::new();
    for _ in 0..100 {
        let uri = match &cursor {
            Some(cursor) => format!("{base}?limit=7&cursor={cursor}"),
            None => format!("{base}?limit=7"),
        };
        let (status, next, body) = hook_get(&router, Some(&token), &uri).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let page = body.as_array().unwrap();
        assert!(!page.is_empty() && page.len() <= 7);
        for entry in page {
            assert_eq!(entry["workspace_id"], "ws_hook_alpha");
            assert_eq!(entry["phase"], "succeeded");
            assert_eq!(entry.as_object().unwrap().len(), 11);
            for secret in ["hook", "command", "input", "receipt", "secret"] {
                assert!(entry.get(secret).is_none(), "leaked {secret}");
            }
            assert!(seen.insert(entry["parent_event_id"].as_str().unwrap().to_owned()));
        }
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 50);
    let (status, _, beta) = hook_get(
        &router,
        Some(&token),
        "/v2/workspaces/ws_hook_beta/hooks/effects?limit=100",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(beta.as_array().unwrap().len(), 25);
    let detail = format!("{base}/evt_hook_001/post_teammate_add/private:hook-001");
    let (status, _, body) = hook_get(&router, Some(&token), &detail).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hook"]["command"], "echo private shell command");
    assert_eq!(body["input"]["secret"], "private input");
    assert_eq!(
        body["receipt"]["validated_output"]["secret"],
        "private output"
    );
    for invalid in [
        format!("{base}?limit=0"),
        format!("{base}?limit=101"),
        format!("{base}?limit=garbage"),
        format!("{base}?cursor=not-a-cursor"),
        format!("{base}?cursor=100:xyz:post_teammate_add:12"),
        format!("{base}?cursor=-1:ab:post_teammate_add:ab"),
        format!("{base}?cursor=100:6162:unknown:6162"),
        format!("{base}?unknown=1"),
    ] {
        let (status, _, body) = hook_get(&router, Some(&token), &invalid).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}: {body}");
        assert_eq!(body["error"]["code"], "invalid_request");
    }
    for nonexistent in [
        format!("{base}/evt_hook_000/post_teammate_add/private:hook-000"),
        format!("{base}/evt_missing/post_teammate_add/private:hook-001"),
        "/v2/workspaces/ws_missing/hooks/effects".to_string(),
    ] {
        let (status, _, body) = hook_get(&router, Some(&token), &nonexistent).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }
    let (status, _, body) = hook_get(
        &router,
        Some(&token),
        &format!("{base}/evt_hook_001/unknown/private:hook-001"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}
