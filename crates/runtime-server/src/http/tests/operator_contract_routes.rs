use super::*;

async fn response_json(response: Response) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn v2_operator_errors_have_stable_codes_server_request_ids_and_no_v1_auth_drift() {
    let (router, token, temp) = build_test_router().await;
    let unauthenticated = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/workspaces")
                .header("x-request-id", "spoofed")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let request_id = unauthenticated
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(request_id.starts_with("req_") && request_id != "spoofed");
    let error = response_json(unauthenticated).await;
    assert_eq!(error["error"]["code"], "unauthorized");
    assert_eq!(error["error"]["retryable"], false);
    assert_eq!(error["request_id"], request_id);

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/workspaces/ws_missing")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    let missing_id = missing.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_string();
    let missing = response_json(missing).await;
    assert_eq!(missing["error"]["code"], "not_found");
    assert_eq!(missing["request_id"], missing_id);

    let invalid_body = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"canonical_root":42}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(invalid_body.status().is_client_error());
    let invalid_body = response_json(invalid_body).await;
    assert_eq!(invalid_body["error"]["code"], "invalid_request");
    assert!(invalid_body["error"]["message"].is_string());

    let legacy = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/sessions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(legacy.status(), StatusCode::UNAUTHORIZED);
    assert!(response_json(legacy).await["error"].is_string());

    let first_root = temp.path().join("key-one");
    let second_root = temp.path().join("key-two");
    fs::create_dir_all(&first_root).unwrap();
    fs::create_dir_all(&second_root).unwrap();
    for (root, status) in [
        (&first_root, StatusCode::OK),
        (&second_root, StatusCode::CONFLICT),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/workspaces")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("idempotency-key", "one-operation-two-inputs")
                    .body(Body::from(
                        serde_json::json!({"canonical_root":root}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        if status == StatusCode::CONFLICT {
            let error = response_json(response).await;
            assert_eq!(error["error"]["code"], "conflict");
            assert_eq!(error["error"]["retryable"], false);
        }
    }
}

#[tokio::test]
async fn v2_capabilities_truthfully_reflect_provider_and_model_tool_enablement() {
    let (router, token, _temp, _acp) = build_mixed_provider_test_router().await;
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v2/capabilities")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let capabilities = response_json(response).await;
    assert_eq!(capabilities["api_version"], "v2");
    assert_eq!(capabilities["harness_version"], "gooselake-harness-v1");
    assert_eq!(capabilities["tool_contract_version"], "gooselake-tools-v1");
    assert_eq!(
        capabilities["tools"],
        serde_json::json!(["gg_team", "gg_message", "gg_process"])
    );
    assert_eq!(capabilities["managed_processes"], true);
    assert_eq!(capabilities["managed_worktrees"], true);
    assert_eq!(capabilities["providers"].as_array().unwrap().len(), 3);
    let acp = capabilities["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["metadata"]["kind"] == "acp")
        .unwrap();
    assert_eq!(acp["capabilities"]["model_discovery"], "agent_managed");
    assert_eq!(acp["capabilities"]["skill_discovery"], "unsupported");
    assert_eq!(acp["harness"]["injection_mode"], "scoped_mcp_only");
    assert_eq!(
        acp["harness"]["tool_manifest_hash"],
        capabilities["tool_manifest_hash"]
    );

    let policy = TeamMcpPolicy {
        enabled: false,
        ..TeamMcpPolicy::default()
    };
    let (router, token, _temp) = build_test_router_with_team_policy(policy).await;
    let disabled = router
        .oneshot(
            Request::builder()
                .uri("/v2/capabilities")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    assert_eq!(
        response_json(disabled).await["tools"],
        serde_json::json!(["gg_process"])
    );
}

#[tokio::test]
async fn workspace_keyset_pages_remain_bounded_with_stable_cursors() {
    let (router, token, temp) = build_test_router().await;
    let auth = format!("Bearer {token}");
    let mut registered_ids = Vec::new();
    for index in 0..5 {
        let root = temp.path().join(format!("pagination-{index}"));
        fs::create_dir_all(&root).unwrap();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/workspaces")
                    .header(header::AUTHORIZATION, auth.as_str())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({"canonical_root":root}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        registered_ids.push(
            response_json(response).await["workspace"]["workspace_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    let mut found = Vec::new();
    let mut cursor = None;
    for _ in 0..5 {
        let uri = match &cursor {
            None => "/v2/workspaces?limit=2".to_string(),
            Some(cursor) => format!("/v2/workspaces?limit=2&cursor={cursor}"),
        };
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        cursor = response
            .headers()
            .get("x-next-cursor")
            .map(|header| header.to_str().unwrap().to_owned());
        let body = response_json(response).await;
        let page = body.as_array().expect("compatible JSON array");
        assert!(!page.is_empty() && page.len() <= 2);
        for row in page {
            found.push(row["workspace_id"].as_str().unwrap().to_string());
        }
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(found.len(), 5);
    found.sort();
    registered_ids.sort();
    assert_eq!(found, registered_ids, "no duplicate/omitted workspace IDs");

    for uri in [
        "/v2/workspaces?limit=101",
        "/v2/workspaces?limit=0",
        "/v2/workspaces?limit=3&cursor=malformed",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "invalid_request"
        );
    }
}

#[tokio::test]
async fn workspace_revision_etag_and_idempotency_headers_are_guarded() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("etag-workspace");
    fs::create_dir_all(&root).unwrap();
    let auth = format!("Bearer {token}");
    let first = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"canonical_root":root}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let workspace = response_json(first).await["workspace"].clone();
    let id = workspace["workspace_id"].as_str().unwrap();
    let revision = workspace["revision"].as_u64().unwrap();
    let etag = format!("\"{revision}\"");
    let get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{id}"))
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.headers()[header::ETAG].to_str().unwrap(), etag.as_str());
    assert_eq!(response_json(get).await["revision"], revision);

    let lead_uri = format!("/v2/workspaces/{id}/lead");
    let payload = serde_json::json!({"lead_agent_id":null,"expected_revision":revision});
    let stale = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(lead_uri.as_str())
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, "\"999\"")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        response_json(stale).await["error"]["code"],
        "revision_precondition_failed"
    );

    let guarded = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(lead_uri.as_str())
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, etag.as_str())
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(guarded.status(), StatusCode::OK);

    let policy_uri = format!("/v2/workspaces/{id}/notice-policy");
    let policy_get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(policy_uri.as_str())
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(policy_get.status(), StatusCode::OK);
    let policy_etag = policy_get.headers()[header::ETAG]
        .to_str()
        .unwrap()
        .to_owned();
    let policy = response_json(policy_get).await;
    let policy_input = serde_json::json!({
        "expected_revision":policy["revision"],"policy":policy["policy"]
    });
    let policy_stale = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(policy_uri.as_str())
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, "\"404\"")
                .body(Body::from(policy_input.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(policy_stale.status(), StatusCode::PRECONDITION_FAILED);
    let policy_good = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(policy_uri.as_str())
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::IF_MATCH, policy_etag)
                .body(Body::from(policy_input.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(policy_good.status(), StatusCode::OK);
    assert!(policy_good.headers().contains_key(header::ETAG));

    for key in ["", " has-whitespace", "bad,key"] {
        let attempt = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/workspaces")
                    .header(header::AUTHORIZATION, auth.as_str())
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("idempotency-key", key)
                    .body(Body::from(
                        serde_json::json!({"canonical_root":root}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(attempt.status(), StatusCode::BAD_REQUEST, "key={key:?}");
        assert_eq!(
            response_json(attempt).await["error"]["code"],
            "invalid_request"
        );
    }
}

#[tokio::test]
async fn workspace_agent_pages_keep_filter_and_do_not_duplicate_members() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("paged-agents");
    fs::create_dir_all(&root).unwrap();
    let auth = format!("Bearer {token}");
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"canonical_root":root}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let workspace = response_json(response).await["workspace"]["workspace_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let mut agents = Vec::new();
    for index in 0..3 {
        let response = router.clone().oneshot(Request::builder().method("POST")
            .uri(format!("/v2/workspaces/{workspace}/agents"))
            .header(header::AUTHORIZATION, auth.as_str())
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::json!({
                "provider":"codex","model":"test-model","title":format!("Worker {index}")
            }).to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        agents.push(
            response_json(response).await["agent_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }

    let mut found = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..3 {
        let uri = format!(
            "/v2/workspaces/{workspace}/agents?limit=1{}",
            cursor
                .as_ref()
                .map_or(String::new(), |cursor| format!("&cursor={cursor}"))
        );
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, auth.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        cursor = response
            .headers()
            .get("x-next-cursor")
            .map(|value| value.to_str().unwrap().to_owned());
        let rows = response_json(response).await;
        let rows = rows.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        found.push(rows[0]["agent_id"].as_str().unwrap().to_owned());
    }
    assert!(cursor.is_none(), "last page has no cursor");
    found.sort();
    agents.sort();
    assert_eq!(found, agents);

    let no_archived = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/workspaces/{workspace}/agents?lifecycle=archived&limit=10"
                ))
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(no_archived.status(), StatusCode::OK);
    assert_eq!(response_json(no_archived).await, serde_json::json!([]));
}

#[tokio::test]
async fn agent_revisions_guard_policy_mutation_before_provider_work() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("agent-policy-etag");
    fs::create_dir_all(&root).unwrap();
    let auth = format!("Bearer {token}");
    let created_workspace = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"canonical_root":root}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created_workspace.status(), StatusCode::OK);
    let workspace = response_json(created_workspace).await;
    let workspace = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let created_agent = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{workspace}/agents"))
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"provider":"codex","model":"test-model"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created_agent.status(), StatusCode::OK);
    let agent = response_json(created_agent).await;
    let agent_id = agent["agent_id"].as_str().unwrap();
    let revision = agent["revision"].as_u64().unwrap();

    let get_uri = format!("/v2/workspaces/{workspace}/agents/{agent_id}");
    let get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(get_uri.as_str())
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()[header::ETAG].to_str().unwrap(),
        format!("\"{revision}\"")
    );

    let prefix = format!("/v2/agents/{agent_id}");
    for (route, payload) in [
        (
            "preferences",
            serde_json::json!({"expected_revision":revision,
                "current_preferences":{"thinking_effort":"high"}}),
        ),
        (
            "permission",
            serde_json::json!({"expected_revision":revision,
                "permission_intent":{"kind":"provider_default"}}),
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{prefix}/{route}"))
                    .header(header::AUTHORIZATION, auth.as_str())
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::IF_MATCH, "\"9999\"")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        let error = response_json(response).await;
        assert_eq!(error["error"]["code"], "revision_precondition_failed");
    }
    let after = router
        .oneshot(
            Request::builder()
                .uri(get_uri)
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_json(after).await["revision"], revision);
}

#[tokio::test]
async fn message_and_delivery_collection_queries_are_bounded_and_stably_paged() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("paged-message-workspace");
    fs::create_dir_all(&root).unwrap();
    let auth = format!("Bearer {token}");
    let create = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"canonical_root":root}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let workspace = response_json(create).await;
    let workspace_id = workspace["workspace"]["workspace_id"].as_str().unwrap();
    let mut agents = Vec::new();
    for _ in 0..3 {
        let create = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v2/workspaces/{workspace_id}/agents"))
                    .header(header::AUTHORIZATION, auth.as_str())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({"provider":"codex","model":"test-model"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::OK);
        agents.push(
            response_json(create).await["agent_id"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    let message = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/messages")
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"mode":"broadcast","sender_agent_id":agents[0],
                        "input":[{"type":"text","text":"page test"}]})
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(message.status(), StatusCode::OK);
    let message = response_json(message).await;
    let message_id = message["message"]["id"].as_str().unwrap();
    assert_eq!(message["deliveries"].as_array().unwrap().len(), 2);

    let second_message = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/messages")
                .header(header::AUTHORIZATION, auth.as_str())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"mode":"direct","sender_agent_id":agents[0],
                        "recipient_agent_id":agents[1],
                        "input":[{"type":"text","text":"second page"}]})
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_message.status(), StatusCode::OK);
    let second_message_id = response_json(second_message).await["message"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let first_messages = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/messages?limit=1")
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_messages.status(), StatusCode::OK);
    let first_messages = response_json(first_messages).await;
    assert_eq!(first_messages["messages"].as_array().unwrap().len(), 1);
    let message_cursor = first_messages["next_cursor"].as_str().unwrap();
    let next_messages = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/messages?limit=1&cursor={message_cursor}"))
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(next_messages.status(), StatusCode::OK);
    let next_messages = response_json(next_messages).await;
    assert_eq!(next_messages["messages"].as_array().unwrap().len(), 1);
    assert!(next_messages["next_cursor"].is_null());
    let mut all_ids = vec![
        first_messages["messages"][0]["id"].as_str().unwrap(),
        next_messages["messages"][0]["id"].as_str().unwrap(),
    ];
    all_ids.sort_unstable();
    let mut expected_ids = vec![message_id, second_message_id.as_str()];
    expected_ids.sort_unstable();
    assert_eq!(all_ids, expected_ids);

    let first_uri = format!("/v2/messages/{message_id}/deliveries?limit=1");
    let first = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(first_uri)
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let cursor = first.headers()["x-next-cursor"]
        .to_str()
        .unwrap()
        .to_owned();
    let first_rows = response_json(first).await;
    assert_eq!(first_rows.as_array().unwrap().len(), 1);
    let second = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/messages/{message_id}/deliveries?limit=1&cursor={cursor}"
                ))
                .header(header::AUTHORIZATION, auth.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert!(second.headers().get("x-next-cursor").is_none());
    let second_rows = response_json(second).await;
    assert_eq!(second_rows.as_array().unwrap().len(), 1);
    assert_ne!(first_rows[0]["id"], second_rows[0]["id"]);

    for uri in [
        "/v2/messages?limit=0".to_string(),
        "/v2/messages?limit=101".to_string(),
        "/v2/messages?cursor=%20".to_string(),
        "/v2/messages?unexpected=true".to_string(),
        format!("/v2/messages/{message_id}/deliveries?limit=0"),
        format!("/v2/messages/{message_id}/deliveries?cursor=broken"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri.as_str())
                    .header(header::AUTHORIZATION, auth.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            response_json(response).await["error"]["code"],
            "invalid_request"
        );
    }
}
