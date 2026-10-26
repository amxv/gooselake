use super::*;
use std::collections::HashSet;

#[tokio::test]
async fn operator_operation_history_pages_are_bounded_scoped_and_stable() {
    let (router, token, temp) = build_test_router().await;
    let auth = format!("Bearer {token}");
    let mut workspace_ids = Vec::new();
    for index in 0..5 {
        let root = temp.path().join(format!("operation-pages-{index}"));
        fs::create_dir(&root).unwrap();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/workspaces")
                    .header(header::AUTHORIZATION, &auth)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("idempotency-key", format!("operation-pages-{index}"))
                    .body(Body::from(
                        serde_json::json!({"canonical_root":root}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        workspace_ids.push(
            value["workspace"]["workspace_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }

    let lead = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{}/lead", workspace_ids[0]))
                .header(header::AUTHORIZATION, &auth)
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", "operation-pages-lead")
                .body(Body::from(
                    serde_json::json!({"lead_agent_id":null,"expected_revision":0}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lead.status(), StatusCode::OK);

    let mut cursor = None;
    let mut seen = HashSet::new();
    for _ in 0..6 {
        let uri = cursor.as_ref().map_or_else(
            || "/v2/operations?limit=2".to_owned(),
            |cursor| format!("/v2/operations?limit=2&cursor={cursor}"),
        );
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        cursor = response
            .headers()
            .get("x-next-cursor")
            .map(|value| value.to_str().unwrap().to_string());
        let values: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let page = values.as_array().unwrap();
        assert!(!page.is_empty() && page.len() <= 2);
        for value in page {
            // A list item must never materialize or disclose the full
            // admission payload, idempotency key, or terminal result.
            let fields = value.as_object().unwrap();
            assert_eq!(fields.len(), 8, "operation summary fields: {fields:?}");
            for private_field in [
                "normalized_request",
                "normalized_request_hash",
                "idempotency_key",
                "exact_terminal_result",
                "error_message",
                "actor",
            ] {
                assert!(!fields.contains_key(private_field));
            }
            assert!(seen.insert(value["operation_id"].as_str().unwrap().to_string()));
        }
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 6);

    let unauthorized = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/operations")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    // The exact single-operation inspection route keeps its diagnostic data.
    let operation_id = seen.iter().next().unwrap();
    let detail = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/operations/{operation_id}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail.status(), StatusCode::OK);
    let detail: serde_json::Value =
        serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(detail["operation"]["normalized_request"].is_object());

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/operations?workspace_id={}", workspace_ids[0]))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let rows: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let rows = rows.as_array().unwrap();
    assert!(!rows.is_empty(), "lead operation must be workspace-scoped");
    assert!(rows
        .iter()
        .all(|row| row["workspace_id"] == workspace_ids[0]));

    for uri in [
        "/v2/operations?limit=0",
        "/v2/operations?limit=101",
        "/v2/operations?cursor=bogus",
        "/v2/operations?unexpected=true",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
}
