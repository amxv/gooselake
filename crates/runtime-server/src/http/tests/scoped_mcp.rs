use super::*;

async fn scoped_setup() -> (Router, String, tempfile::TempDir, String, String) {
    let (router, operator_token, temp) = build_test_router().await;
    let root = temp.path().join("scoped-mcp-workspace");
    fs::create_dir_all(&root).expect("workspace dir");
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {operator_token}"))
                .body(Body::from(
                    serde_json::json!({"canonical_root":root,"display_name":"Scoped MCP"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let registration: runtime_core::WorkspaceRegisterResponse =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let workspace = registration.workspace.workspace_id;
    let mut agents = Vec::new();
    for title in ["Caller", "Peer"] {
        let response=router.clone().oneshot(Request::builder().method("POST")
            .uri(format!("/v2/workspaces/{workspace}/agents"))
            .header(header::CONTENT_TYPE,"application/json")
            .header(header::AUTHORIZATION,format!("Bearer {operator_token}"))
            .body(Body::from(serde_json::json!({"provider":"codex","model":"test-model","cwd":root,"title":title}).to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let agent: runtime_core::WorkspaceAgentRecord =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        agents.push(agent.agent_id);
    }
    (router, operator_token, temp, workspace, agents[0].clone())
}

async fn scoped_call(
    router: &Router,
    token: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let invocation_id = if body
        .pointer("/args/mode")
        .and_then(serde_json::Value::as_str)
        == Some("broadcast")
    {
        "trusted-broadcast-1"
    } else {
        "trusted-call-1"
    };
    scoped_call_with_id(router, token, body, invocation_id).await
}

async fn scoped_call_with_id(
    router: &Router,
    token: &str,
    body: serde_json::Value,
    invocation_id: &str,
) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/mcp/invoke")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("x-gg-tool-invocation-id", invocation_id)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    // Axum's JSON extractor deliberately rejects unknown request fields
    // before the handler runs, and those framework-generated 4xx bodies can
    // be plain text rather than the GG tool envelope.
    let value = serde_json::from_slice(&body).unwrap_or_else(
        |_| serde_json::json!({"non_json_response": String::from_utf8_lossy(&body)}),
    );
    (status, value)
}

#[tokio::test]
async fn scoped_team_mutations_replay_exact_receipts_without_duplicating_agents() {
    let (router, operator, _temp, workspace, caller) = scoped_setup().await;
    let scoped = runtime_core::mint_scoped_mcp_token(&operator, &caller);
    let add = serde_json::json!({"tool_name":"gg_team","args":{"mode":"add","title":"Worker"}});
    let (status, first) = scoped_call_with_id(&router, &scoped, add.clone(), "trusted-add-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["ok"], true, "{first}");
    let (_, replay) = scoped_call_with_id(&router, &scoped, add, "trusted-add-1").await;
    assert_eq!(
        first, replay,
        "the original response must replay byte-for-byte as JSON"
    );
    let (_, conflict) = scoped_call_with_id(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_team","args":{"mode":"add","title":"Other"}}),
        "trusted-add-1",
    )
    .await;
    assert_eq!(conflict["ok"], false);
    assert_eq!(conflict["error"]["code"], "conflict");

    let (_, status) = scoped_call(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_team","args":{"mode":"status"}}),
    )
    .await;
    assert_eq!(status["result"]["members_total"], 3);
    assert_eq!(status["result"]["lead_alias"], serde_json::Value::Null);
    assert_eq!(status["result"]["leadless"], true);
    let active_members = status["result"]["members"].as_array().expect("roster");
    assert!(active_members.iter().all(|row| row["role"] == "member"));
    assert!(active_members.iter().all(|row| row["alias"].is_string()));
    assert!(active_members
        .iter()
        .all(|row| row["context_remaining_pct"].is_null()));
    let new_agent = first["result"]["agent_id"].as_str().unwrap();
    let remove = serde_json::json!({"tool_name":"gg_team","args":{
        "mode":"remove","remove_agent_ids":[new_agent]
    }});
    let (_, removed) =
        scoped_call_with_id(&router, &scoped, remove.clone(), "trusted-remove-1").await;
    assert_eq!(removed["ok"], true, "{removed}");
    let (_, replay) = scoped_call_with_id(&router, &scoped, remove, "trusted-remove-1").await;
    assert_eq!(removed, replay);
    let (_, final_status) = scoped_call(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_team","args":{"mode":"status"}}),
    )
    .await;
    assert_eq!(final_status["result"]["members_total"], 2);
    let operations = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace}/agents"))
                .header(header::AUTHORIZATION, format!("Bearer {operator}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(operations.status(), StatusCode::OK);
}

#[tokio::test]
async fn scoped_process_modes_preserve_durable_handle_and_owner_only_cancel() {
    let (router, operator, _temp, workspace, caller) = scoped_setup().await;
    let scoped = runtime_core::mint_scoped_mcp_token(&operator, &caller);
    let agents_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace}/agents"))
                .header(header::AUTHORIZATION, format!("Bearer {operator}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(agents_response.status(), StatusCode::OK);
    let agents: Vec<runtime_core::WorkspaceAgentRecord> = serde_json::from_slice(
        &to_bytes(agents_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let peer = agents
        .iter()
        .find(|agent| agent.agent_id != caller)
        .unwrap();
    let peer_token = runtime_core::mint_scoped_mcp_token(&operator, &peer.agent_id);

    let run = serde_json::json!({"tool_name":"gg_process","args":{
        "mode":"run","command":"sleep 20"
    }});
    let (status, first) =
        scoped_call_with_id(&router, &scoped, run.clone(), "trusted-process-run-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["ok"], true, "{first}");
    let process_id = first["result"]["process"]["process_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_, replay) = scoped_call_with_id(&router, &scoped, run, "trusted-process-run-1").await;
    assert_eq!(replay["result"]["process"]["process_id"], process_id);
    let (_, conflict) = scoped_call_with_id(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_process","args":{
            "mode":"run","command":"sleep 21"
        }}),
        "trusted-process-run-1",
    )
    .await;
    assert_eq!(conflict["error"]["code"], "conflict");

    let (_, listed) = scoped_call(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_process","args":{"mode":"list"}}),
    )
    .await;
    assert_eq!(listed["ok"], true, "{listed}");
    assert!(listed["result"]["running"]
        .as_array()
        .unwrap()
        .iter()
        .any(|process| { process["process_id"] == process_id }));
    let (_, visible) = scoped_call(
        &router,
        &peer_token,
        serde_json::json!({"tool_name":"gg_process","args":{
            "mode":"status","process_id":process_id
        }}),
    )
    .await;
    assert_eq!(visible["ok"], true, "{visible}");

    let cancel = serde_json::json!({"tool_name":"gg_process","args":{
        "mode":"cancel","process_id":process_id
    }});
    let (_, denied) =
        scoped_call_with_id(&router, &peer_token, cancel.clone(), "peer-cancel-1").await;
    assert_eq!(denied["ok"], false, "{denied}");
    let (_, canceled) = scoped_call_with_id(&router, &scoped, cancel, "owner-cancel-1").await;
    assert_eq!(canceled["ok"], true, "{canceled}");
}

#[tokio::test]
async fn scoped_mcp_denies_operator_spoofing_and_archived_agents() {
    let (router, operator, temp, workspace, caller) = scoped_setup().await;
    let scoped = runtime_core::mint_scoped_mcp_token(&operator, &caller);
    let caps = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/mcp/capabilities")
                .header(header::AUTHORIZATION, format!("Bearer {scoped}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(caps.status(), StatusCode::OK);
    let capabilities: serde_json::Value =
        serde_json::from_slice(&to_bytes(caps.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        capabilities["result"]["tools"],
        serde_json::json!(["gg_process", "gg_team", "gg_message"])
    );

    for refused in [
        operator.clone(),
        "wrong-token".to_owned(),
        runtime_core::mint_scoped_mcp_token(&operator, "sess_absent"),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v2/mcp/capabilities")
                    .header(header::AUTHORIZATION, format!("Bearer {refused}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    let (status, valid) = scoped_call(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_team","args":{"mode":"status"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(valid["ok"], true);
    assert_eq!(valid["result"]["workspace_id"], workspace);
    assert_eq!(valid["result"]["members"].as_array().unwrap().len(), 2);

    let (status,_)=scoped_call(&router,&scoped,serde_json::json!({"tool_name":"gg_team","caller_agent_id":"sess_other","args":{"mode":"status"}})).await;
    assert!(status.is_client_error());
    let (status, legacy) = scoped_call(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_team_status","args":{"team_id":"forged"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(legacy["error"]["code"], "bad_request");
    let (status, bad) = scoped_call(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_team","args":{"mode":"status","team_id":"forged"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bad["error"]["code"], "bad_request");

    let archived = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/workspaces/{workspace}/agents/{caller}/archive"
                ))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {operator}"))
                .body(Body::from(
                    serde_json::json!({"reason":"revoked"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(archived.status(), StatusCode::OK);
    let (status, _) = scoped_call(
        &router,
        &scoped,
        serde_json::json!({"tool_name":"gg_team","args":{"mode":"status"}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    drop(temp);
}

#[tokio::test]
async fn scoped_mcp_uses_authenticated_identity_for_messages() {
    let (router, operator, temp, workspace, caller) = scoped_setup().await;
    let scoped = runtime_core::mint_scoped_mcp_token(&operator, &caller);
    let agent_list = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace}/agents"))
                .header(header::AUTHORIZATION, format!("Bearer {operator}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let agents: Vec<runtime_core::WorkspaceAgentRecord> =
        serde_json::from_slice(&to_bytes(agent_list.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let peer = agents
        .iter()
        .find(|agent| agent.agent_id != caller)
        .expect("peer");
    let direct = serde_json::json!({"tool_name":"gg_message","args":{"mode":"direct","agent_id":peer.alias,"message":"Hello from caller"}});
    let (status, first) = scoped_call(&router, &scoped, direct.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["ok"], true, "{first}");
    assert_eq!(first["result"]["recipient_count"], 1);
    let (_, replay) = scoped_call(&router, &scoped, direct).await;
    assert_eq!(
        first["result"]["message_id"],
        replay["result"]["message_id"]
    );
    assert_eq!(replay["result"]["disposition"], "existing");

    let (status,broadcast)=scoped_call(&router,&scoped,serde_json::json!({"tool_name":"gg_message","args":{"mode":"broadcast","message":"All agents"}})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(broadcast["ok"], true, "{broadcast}");
    assert_eq!(broadcast["result"]["recipient_count"], 1);
    drop(temp);
}

#[tokio::test]
async fn scoped_mcp_rejects_mutations_without_trusted_invocation_identity() {
    let (router, operator, _temp, _workspace, caller) = scoped_setup().await;
    let scoped = runtime_core::mint_scoped_mcp_token(&operator, &caller);
    let body = serde_json::json!({
        "tool_name":"gg_message",
        "args":{"mode":"broadcast","message":"must not send"}
    });
    for invocation_id in [None, Some("invalid invocation id"), Some("../invalid")] {
        let mut request = Request::builder()
            .method("POST")
            .uri("/v2/mcp/invoke")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {scoped}"));
        if let Some(value) = invocation_id {
            request = request.header("x-gg-tool-invocation-id", value);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["error"]["code"], "missing_invocation_id");
        assert_eq!(payload["error"]["retryable"], false);
    }

    // A read-only status call has no effectful retry identity requirement.
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/mcp/invoke")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {scoped}"))
                .body(Body::from(
                    serde_json::json!({"tool_name":"gg_team","args":{"mode":"status"}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["ok"], true, "{payload}");
}
