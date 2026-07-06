use super::*;

#[tokio::test]
async fn mcp_invoke_team_manage_policy_enabled_and_add_idempotency_are_http_visible() {
    let (router, token, temp_dir) = build_test_router_with_team_policy(TeamMcpPolicy {
        enabled: true,
        non_lead_can_add_members: true,
        non_lead_can_remove_members: true,
    })
    .await;
    let session_cwd = temp_dir.path().join("mcp-team-policy-cwd");
    std::fs::create_dir_all(&session_cwd).expect("create mcp team cwd");

    let create_session = |label: &'static str| {
        let router = router.clone();
        let token = token.clone();
        let cwd = session_cwd.display().to_string();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/sessions")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(
                            serde_json::json!({
                                "provider": "codex",
                                "model": "test-model",
                                "cwd": cwd,
                                "metadata": {"suite": "mcp_team_policy", "label": label}
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .expect("create session");
            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value = serde_json::from_slice(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("session body"),
            )
            .expect("session json");
            body["id"].as_str().expect("session id").to_string()
        }
    };

    let lead_session_id = create_session("lead").await;
    let non_lead_session_id = create_session("non-lead").await;
    let removable_session_id = create_session("removable").await;

    let create_team_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/teams")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "name": "MCP Team Policy",
                            "lead_agent_id": lead_session_id.clone(),
                            "member_agent_ids": [non_lead_session_id.clone(), removable_session_id.clone()],
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create team");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("team body"),
    )
    .expect("team json");
    let team_id = create_team_json["team"]["id"]
        .as_str()
        .expect("team id")
        .to_string();

    let invoke = |caller_agent_id: String, invocation_id: &'static str, args: serde_json::Value| {
        let router = router.clone();
        let token = token.clone();
        async move {
            let response = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/mcp/invoke")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::from(
                            serde_json::json!({
                                "namespace": "gg_team",
                                "tool_name": "gg_team_manage",
                                "caller_agent_id": caller_agent_id,
                                "invocation_id": invocation_id,
                                "args": args,
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .expect("mcp invoke");
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("mcp body"),
            )
            .expect("mcp json")
        }
    };

    let add_args = serde_json::json!({
        "team_id": team_id.clone(),
        "title": "Non-lead Spawn",
    });
    let first_add = invoke(
        non_lead_session_id.clone(),
        "mcp_policy_add_once",
        add_args.clone(),
    )
    .await;
    let replayed_add = invoke(non_lead_session_id.clone(), "mcp_policy_add_once", add_args).await;
    assert_eq!(first_add["ok"].as_bool(), Some(true));
    assert_eq!(replayed_add["ok"].as_bool(), Some(true));
    assert_eq!(
        first_add["result"]["spawned_agent_id"].as_str(),
        replayed_add["result"]["spawned_agent_id"].as_str(),
        "same caller + invocation id should return cached add result"
    );

    let list_after_add_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get team after add");
    assert_eq!(list_after_add_response.status(), StatusCode::OK);
    let list_after_add_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(list_after_add_response.into_body(), usize::MAX)
            .await
            .expect("team after add body"),
    )
    .expect("team after add json");
    assert_eq!(
        list_after_add_json["members"].as_array().map(Vec::len),
        Some(4),
        "duplicate add invocation must not create a second member"
    );

    let remove_json = invoke(
        non_lead_session_id,
        "mcp_policy_remove_once",
        serde_json::json!({
            "team_id": team_id.clone(),
            "remove_agent_ids": [removable_session_id],
        }),
    )
    .await;
    assert_eq!(remove_json["ok"].as_bool(), Some(true));
    assert_eq!(remove_json["result"]["operation"].as_str(), Some("remove"));
    assert_eq!(remove_json["result"]["removed_count"].as_u64(), Some(1));
}

#[tokio::test]
async fn process_output_events_are_sampled_while_logs_remain_authoritative() {
    let (router, token, _temp_dir) = build_test_router().await;

    let create_body = serde_json::json!({
        "provider": "codex",
        "model": "test-model",
        "cwd": null,
        "permission_mode": null,
        "metadata": {}
    });
    let create_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("create session");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_response.into_body(), usize::MAX)
            .await
            .expect("create body"),
    )
    .expect("create json");
    let session_id = create_json["id"].as_str().expect("session id").to_string();

    let run_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/processes")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "command": "seq 1 2500",
                        "session_id": session_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("run process");
    assert_eq!(run_response.status(), StatusCode::OK);
    let run_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(run_response.into_body(), usize::MAX)
            .await
            .expect("run body"),
    )
    .expect("run json");
    let process_id = run_json
        .pointer("/process/process_id")
        .and_then(serde_json::Value::as_str)
        .expect("process id")
        .to_string();

    let mut completed = false;
    for _ in 0..100 {
        let get_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}?session_id={session_id}"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("get process");
        assert_eq!(get_response.status(), StatusCode::OK);
        let process_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(get_response.into_body(), usize::MAX)
                .await
                .expect("get body"),
        )
        .expect("process json");
        let status = process_json
            .pointer("/process/status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if matches!(status, "completed" | "failed" | "timed_out" | "killed") {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(completed, "process did not reach terminal state");

    let events_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/processes/{process_id}/events?limit=10000"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("events");
    assert_eq!(events_response.status(), StatusCode::OK);
    let events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
        &to_bytes(events_response.into_body(), usize::MAX)
            .await
            .expect("events body"),
    )
    .expect("events json");
    let sampled_output_events = events
        .iter()
        .filter(|event| event.kind == "process.output")
        .count();
    assert!(
        sampled_output_events > 0,
        "expected sampled process.output events"
    );
    assert!(
        sampled_output_events < 2500,
        "expected output events to be sampled/coalesced"
    );

    let logs_response = router
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/processes/{process_id}/logs?session_id={session_id}&stream=stdout&tail_lines=3000"
                    ))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("logs");
    assert_eq!(logs_response.status(), StatusCode::OK);
    let logs_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(logs_response.into_body(), usize::MAX)
            .await
            .expect("logs body"),
    )
    .expect("logs json");
    let output = logs_json
        .as_array()
        .into_iter()
        .flat_map(|rows| rows.iter())
        .filter_map(|row| row.get("content"))
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        output.contains("2500"),
        "expected full stdout log content to remain retrievable"
    );
}

#[tokio::test]
async fn process_http_ownership_enforced_by_session_identity() {
    let (router, token, _temp_dir) = build_test_router().await;

    let create_body = serde_json::json!({
        "provider": "codex",
        "model": "test-model",
        "cwd": null,
        "permission_mode": null,
        "metadata": {}
    });
    let create_one = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("create one");
    let create_one_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_one.into_body(), usize::MAX)
            .await
            .expect("create one body"),
    )
    .expect("create one json");
    let owner_session_id = create_one_json["id"]
        .as_str()
        .expect("owner session id")
        .to_string();

    let create_two = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .expect("create two");
    let create_two_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_two.into_body(), usize::MAX)
            .await
            .expect("create two body"),
    )
    .expect("create two json");
    let other_session_id = create_two_json["id"]
        .as_str()
        .expect("other session id")
        .to_string();

    let run_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/processes")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "command": "echo owned",
                        "session_id": owner_session_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("run process");
    assert_eq!(run_response.status(), StatusCode::OK);
    let run_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(run_response.into_body(), usize::MAX)
            .await
            .expect("run body"),
    )
    .expect("run json");
    let process_id = run_json
        .pointer("/process/process_id")
        .and_then(serde_json::Value::as_str)
        .expect("process id")
        .to_string();

    let unauthorized_get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/processes/{process_id}?session_id={other_session_id}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("unauthorized get");
    assert_eq!(unauthorized_get.status(), StatusCode::BAD_REQUEST);

    let unauthorized_events = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/processes/{process_id}/events?session_id={other_session_id}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("unauthorized events");
    assert_eq!(unauthorized_events.status(), StatusCode::BAD_REQUEST);
}
