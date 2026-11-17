use axum::Router;
use axum::routing::{get, post};
use rmcp::{
    ServiceExt,
    model::CallToolRequestParams,
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde_json::{Value, json};
use std::net::Ipv4Addr;
use std::sync::{Arc, atomic::Ordering};
mod support;
use support::{capabilities_stub, extract_json_payload, mcp_server_command, stub_gateway_state};

#[tokio::test]
async fn stdio_legacy_spawn_inspection_accepts_removed_preset_and_preserves_original_invocation()
-> Result<(), Box<dyn std::error::Error>> {
    let auth_token = "integration_token";
    let gateway_state = stub_gateway_state(
        auth_token,
        vec!["claude-sonnet-5".to_string(), "gpt-5".to_string()],
    );
    let capabilities_calls = Arc::clone(&gateway_state.capabilities_calls);
    let invoke_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_invokes = Arc::clone(&invoke_calls);
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
            let calls = Arc::clone(&invoke_calls);
            async move {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(headers.get("authorization").unwrap(), "Bearer integration_token");
            axum::Json(json!({"ok":false,"error":{
                "code":"outcome_unresolved","message":"historical fixture",
                "details":{"operation_id":"op_spawn_original","inspection_path":"/v1/diagnostics/team-operations/op_spawn_original","native_retry_authorized":false,"observed_request":body},
            }}))
            }
        }))
        .with_state(gateway_state);
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let gateway_addr = listener.local_addr()?;
    let gateway_handle = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            panic!("Stub gateway exited unexpectedly: {error}");
        }
    });

    let gateway_url = format!("http://{}:{}", gateway_addr.ip(), gateway_addr.port());
    let service = ()
        .serve(TokioChildProcess::new(mcp_server_command().configure(
            |command| {
                command.env("GG_MCP_GATEWAY_URL", gateway_url);
                command.env("GG_MCP_GATEWAY_TOKEN", auth_token);
                command.env("GG_MCP_CALLER_AGENT_ID", "sess_mcp_integration");
            },
        ))?)
        .await?;

    let initial_tools = service.peer().list_tools(None).await?;
    let initial_manage_tool = initial_tools
        .tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_team_manage")
        .ok_or("missing gg_team_manage from tools/list")?;
    let initial_description = initial_manage_tool
        .description
        .as_deref()
        .ok_or("gg_team_manage description missing")?;
    assert!(
        !initial_description.contains("unsubscribe_from_compaction_notifications"),
        "initial gg_team_manage description should not reference removed unsubscribe flag"
    );
    assert!(initial_description.contains("original keyed Add request"));
    assert!(initial_description.contains("Fresh Add requests are unavailable"));
    assert!(initial_description.contains("Current configured presets (advisory; historical names are also accepted): claude-sonnet-5, gpt-5."));
    assert!(
        capabilities_calls.load(Ordering::SeqCst) >= 1,
        "capabilities endpoint should be called during initial tools/list"
    );
    let initial_model_preset_schema = initial_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("model_preset"))
        .and_then(Value::as_object)
        .ok_or("gg_team_manage.model_preset schema should be present")?;
    let alternatives = initial_model_preset_schema["anyOf"].as_array().unwrap();
    assert!(
        alternatives
            .iter()
            .all(|variant| variant.get("enum").is_none())
    );
    assert!(
        alternatives
            .iter()
            .any(|variant| variant["type"] == "string")
    );
    for _ in 0..2 {
        let result = service
            .peer()
            .call_tool(CallToolRequestParams {
                meta: None,
                name: "gg_team_manage".into(),
                task: None,
                arguments: json!({
                    "team_id":"team_integration", "model_preset":"removed-original-preset",
                    "__gg_tool_invocation_id":"historical-original-id",
                })
                .as_object()
                .cloned(),
            })
            .await?;
        assert_eq!(result.is_error, Some(true));
        let payload = extract_json_payload(&result.content)?;
        assert_eq!(payload["error"]["code"], "outcome_unresolved");
        assert_eq!(payload["error"]["retryable"], false);
        assert_eq!(
            payload["error"]["details"]["operation_id"],
            "op_spawn_original"
        );
        assert_eq!(
            payload["error"]["details"]["inspection_path"],
            "/v1/diagnostics/team-operations/op_spawn_original"
        );
        assert_eq!(
            payload["error"]["details"]["native_retry_authorized"],
            false
        );
        let request = &payload["error"]["details"]["observed_request"];
        assert_eq!(request["caller_agent_id"], "sess_mcp_integration");
        assert_eq!(request["invocation_id"], "historical-original-id");
        assert_eq!(request["args"]["model_preset"], "removed-original-preset");
        assert!(request["args"].get("__gg_tool_invocation_id").is_none());
    }

    assert_eq!(observed_invokes.load(Ordering::SeqCst), 2);

    let null_remove = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_manage".into(),
            task: None,
            arguments: json!({
                "team_id":"team_integration", "remove_agent_ids":null,
                "__gg_tool_invocation_id":"historical-original-id",
            })
            .as_object()
            .cloned(),
        })
        .await;
    let error = null_remove.expect_err("explicit null Remove must not become Add inspection");
    assert!(
        format!("{error:?}").contains("-32602"),
        "expected invalid parameters: {error:?}"
    );
    assert_eq!(
        observed_invokes.load(Ordering::SeqCst),
        2,
        "invalid Remove must be rejected before any HTTP gateway invocation"
    );

    let tools = service.peer().list_tools(None).await?;
    let manage_tool = tools
        .tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_team_manage")
        .ok_or("missing gg_team_manage from tools/list after first tool call")?;
    let description = manage_tool
        .description
        .as_deref()
        .ok_or("gg_team_manage description missing after first tool call")?;
    assert!(
        description.contains("Current configured presets (advisory; historical names are also accepted): claude-sonnet-5, gpt-5."),
        "expected dynamic model_preset values after tools/call; got {description}"
    );

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}
