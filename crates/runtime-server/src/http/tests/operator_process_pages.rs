use super::*;
use std::collections::HashSet;

async fn operator_get(
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
    let cursor = response
        .headers()
        .get("x-next-cursor")
        .map(|value| value.to_str().unwrap().to_string());
    let value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, cursor, value)
}

async fn new_workspace(router: &Router, token: &str, root: &Path) -> String {
    fs::create_dir(root).unwrap();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/workspaces")
                .method("POST")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"canonical_root":root}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    value["workspace"]["workspace_id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn v2_operator_process_history_is_bounded_scoped_and_redacted() {
    let (router, token, temp) = build_test_router().await;
    let first_workspace = new_workspace(&router, &token, &temp.path().join("p1")).await;
    let second_workspace = new_workspace(&router, &token, &temp.path().join("p2")).await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    for index in 0..6i64 {
        db.execute(
            "INSERT INTO managed_processes (
                process_id, workspace_id, owner_session_id, tool_call_id,
                command_json, cwd, status, admission_order, queue_order,
                capture_limit_bytes, pid, stdout_path, stderr_path, admitted_at, updated_at
             ) VALUES (?1, ?2, 'private-agent', 'private-invocation', ?3,
                       '/private/cwd', 'completed', ?4, ?4, 1024, 4242,
                       '/private/stdout', '/private/stderr', ?5, ?5)",
            rusqlite::params![
                format!("proc_httptest_{index}"),
                if index < 4 {
                    &first_workspace
                } else {
                    &second_workspace
                },
                serde_json::json!({"command":"very-sensitive-command"}).to_string(),
                index + 1,
                10_000 + index / 2,
            ],
        )
        .unwrap();
    }

    let (status, _, error) = operator_get(&router, None, "/v2/processes").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(error["error"]["code"], "unauthorized");

    let mut cursor = None;
    let mut seen = HashSet::new();
    let mut last = (i64::MAX, String::from("zzz"));
    loop {
        let uri = cursor.as_ref().map_or_else(
            || "/v2/processes?limit=2".to_string(),
            |cursor| format!("/v2/processes?limit=2&cursor={cursor}"),
        );
        let (status, next, values) = operator_get(&router, Some(&token), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let page = values.as_array().unwrap();
        assert!(!page.is_empty() && page.len() <= 2);
        for row in page {
            let id = row["process_id"].as_str().unwrap();
            assert!(seen.insert(id.to_string()));
            let key = (row["admitted_at"].as_i64().unwrap(), id.to_string());
            assert!(key < last);
            last = key;
            assert_eq!(row.as_object().unwrap().len(), 8);
            for private in [
                "command",
                "cwd",
                "pid",
                "tool_call_id",
                "stdout_path",
                "stderr_path",
                "completion_last_error",
                "os_start_identity",
            ] {
                assert!(row.get(private).is_none(), "leaked {private}");
            }
        }
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 6);

    let (status, _, scoped) = operator_get(
        &router,
        Some(&token),
        &format!("/v2/processes?workspace_id={first_workspace}&limit=10"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(scoped.as_array().unwrap().len(), 4);
    assert!(scoped
        .as_array()
        .unwrap()
        .iter()
        .all(|row| { row["workspace_id"].as_str() == Some(first_workspace.as_str()) }));

    let (status, _, detail) =
        operator_get(&router, Some(&token), "/v2/processes/proc_httptest_0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["process"]["pid"], 4242);
    assert_eq!(
        detail["process"]["command"]["command"],
        "very-sensitive-command"
    );
    assert_eq!(detail["stdout_path"], "/private/stdout");

    for invalid in [
        "/v2/processes?limit=0",
        "/v2/processes?limit=101",
        "/v2/processes?cursor=garbage",
        "/v2/processes?workspace_id=",
        "/v2/processes?workspace_id=%20",
        "/v2/processes?unexpected=x",
    ] {
        let (status, _, body) = operator_get(&router, Some(&token), invalid).await;
        assert!(status.is_client_error(), "{invalid}: {status}");
        assert!(body["error"]["code"].is_string(), "{invalid}");
    }
    let (status, _, missing) =
        operator_get(&router, Some(&token), "/v2/processes/proc_does_not_exist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["error"]["code"], "not_found");

    // No unsafe implicit process launch alias: only indexed reads enter /v2
    // until exact-idempotency native process mutations are implemented.
    let denied = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/processes")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::METHOD_NOT_ALLOWED);
}
