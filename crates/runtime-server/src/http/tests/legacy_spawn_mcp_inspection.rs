use super::*;
use serde_json::json;

async fn invoke(router: &Router, token: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/v1/mcp/invoke")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn legacy_spawn_mcp_inspection_survives_closed_caller_and_disabled_current_policy() {
    let (_initial_router, token, temp, provider, _runtime, app, history) =
        build_test_router_with_history(
            TeamMcpPolicy {
                enabled: false,
                non_lead_can_add_members: false,
                non_lead_can_remove_members: false,
            },
            |root| vec![json!({"provider":"codex","model":"test-model","cwd":root})],
        )
        .await;
    let source = historical_session_json(&history[0]);
    let source = source["id"].as_str().unwrap();
    let team = app
        .services
        .team_comms
        .create_team(runtime_core::TeamCreateRequest {
            name: "historical split inspection".into(),
            lead_agent_id: source.into(),
            member_agent_ids: vec![],
            created_by: None,
        })
        .await
        .unwrap();
    let team_id = &team.team.id;
    let (status, saved) = legacy_spawn_fixture_json(
        &app,
        team_id,
        Some("gg_team_manage:original-plain"),
        json!({
            "source_session_id":source, "creator_agent_id":source, "title":"original member",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    // This test owns a closed historical caller; no current native close is
    // claimed. Reopen every reader below after persisting that history.
    let mut closed_source = history[0].clone();
    closed_source.status = "closed".into();
    closed_source.closed_at = Some(200);
    closed_source.updated_at = 200;
    app.services.store.upsert_session(&closed_source).unwrap();
    let (router, reopened_runtime, _reopened_app) = reopen_test_router_with_services(
        &app,
        temp.path(),
        &token,
        TeamMcpPolicy {
            enabled: false,
            non_lead_can_add_members: false,
            non_lead_can_remove_members: false,
        },
    )
    .await;
    assert_eq!(
        reopened_runtime.get_session(source).await.unwrap().status,
        "closed"
    );
    let creates = provider.create_call_count();
    let call = |args: Value| {
        json!({"namespace":"gg_team","tool_name":"gg_team_manage",
        "caller_agent_id":source,"invocation_id":"original-plain","args":args})
    };
    let args = json!({"team_id":team_id,"title":"original member"});
    let (status, replay) = invoke(&router, Some(&token), call(args.clone())).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"]["operation_id"], saved["operation_id"]);
    assert_eq!(
        replay["result"]["spawned_session"],
        saved["spawned_session"]
    );
    assert_eq!(replay["result"]["team"], saved["team"]);
    let (status, conflict) = invoke(
        &router,
        Some(&token),
        call(json!({"team_id":team_id,"title":"changed"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(conflict["error"]["code"], "conflict");
    // Add inspection is the only exception: presence of Remove, including
    // null or empty shapes, retains the active-caller effectful boundary.
    for remove in [
        Value::Null,
        json!([]),
        json!([saved["spawned_session"]["id"]]),
    ] {
        let (status, _) = invoke(
            &router,
            Some(&token),
            call(json!({"team_id":team_id,"remove_agent_ids":remove})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let (status, _) = invoke(&router, None, call(args.clone())).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let mut wrong_namespace = call(args.clone());
    wrong_namespace["namespace"] = json!("gg_process");
    let (status, _) = invoke(&router, Some(&token), wrong_namespace).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let mut foreign = call(args);
    foreign["caller_agent_id"] = json!("missing-original-caller");
    let (status, absent) = invoke(&router, Some(&token), foreign).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(absent["error"]["code"], "unsupported_capability");
    assert!(absent["error"]["details"]["operation_id"].is_null());
    assert_eq!(provider.create_call_count(), creates);
}

#[tokio::test]
async fn legacy_spawn_mcp_inspection_preserves_original_coordinate_after_preset_drift_or_removal() {
    let (_initial_router, token, temp, provider, _runtime, app, history) =
        build_test_router_with_history(
            TeamMcpPolicy {
                enabled: false,
                non_lead_can_add_members: false,
                non_lead_can_remove_members: false,
            },
            |root| vec![json!({"provider":"codex","model":"test-model","cwd":root})],
        )
        .await;
    let source = historical_session_json(&history[0]);
    let source = source["id"].as_str().unwrap();
    let team = app
        .services
        .team_comms
        .create_team(runtime_core::TeamCreateRequest {
            name: "historical preset inspection".into(),
            lead_agent_id: source.into(),
            member_agent_ids: vec![],
            created_by: None,
        })
        .await
        .unwrap();
    let mut originals = Vec::new();
    for (id, name, model) in [
        ("unchanged", "fast", "gpt-6-luna"),
        ("drifted", "fast", "test-model"),
        ("removed", "removed-original-preset", "test-model"),
    ] {
        let key = format!("gg_team_manage:{id}");
        let (status, saved) = legacy_spawn_fixture_json(
            &app,
            &team.team.id,
            Some(&key),
            json!({
                "source_session_id":source,"creator_agent_id":source,"title":"original member",
                "provider":"codex","model":model,
                "metadata":{"model_preset":name,"thinking_effort":"low"},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{saved}");
        originals.push((id, name, saved));
    }
    // This test owns a closed historical caller; no current native close is
    // claimed. Reopen every reader below after persisting that history.
    let mut closed_source = history[0].clone();
    closed_source.status = "closed".into();
    closed_source.closed_at = Some(200);
    closed_source.updated_at = 200;
    app.services.store.upsert_session(&closed_source).unwrap();
    let (router, reopened_runtime, _reopened_app) = reopen_test_router_with_services(
        &app,
        temp.path(),
        &token,
        TeamMcpPolicy {
            enabled: false,
            non_lead_can_add_members: false,
            non_lead_can_remove_members: false,
        },
    )
    .await;
    assert_eq!(
        reopened_runtime.get_session(source).await.unwrap().status,
        "closed"
    );
    let creates = provider.create_call_count();
    for (id, name, saved) in originals {
        let (status, result) = invoke(&router, Some(&token), json!({
            "namespace":"gg_team","tool_name":"gg_team_manage","caller_agent_id":source,
            "invocation_id":id,"args":{"team_id":team.team.id,"title":"original member","model_preset":name},
        })).await;
        assert_eq!(status, StatusCode::OK, "{result}");
        if id == "unchanged" {
            assert_eq!(result["ok"], true, "{result}");
            assert_eq!(result["result"]["operation_id"], saved["operation_id"]);
            assert_eq!(
                result["result"]["spawned_session"],
                saved["spawned_session"]
            );
        } else {
            assert_eq!(result["ok"], false);
            assert_eq!(result["error"]["code"], "outcome_unresolved");
            assert_eq!(
                result["error"]["details"]["operation_id"],
                saved["operation_id"]
            );
            assert_eq!(
                result["error"]["details"]["inspection_path"],
                format!(
                    "/v1/diagnostics/team-operations/{}",
                    saved["operation_id"].as_str().unwrap()
                )
            );
            assert_eq!(result["error"]["details"]["native_retry_authorized"], false);
            assert_eq!(result["error"]["retryable"], false);
        }
    }
    assert_eq!(provider.create_call_count(), creates);
}
