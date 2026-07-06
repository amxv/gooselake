use std::net::Ipv4Addr;
use std::sync::{Arc, atomic::Ordering};

use axum::Router;
use axum::routing::{get, post};
use rmcp::{
    ServiceExt,
    model::CallToolRequestParams,
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde_json::{Value, json};

mod support;

use support::{
    capabilities_stub, extract_json_payload, invoke_non_json_stub, invoke_stub, mcp_server_command,
    stub_gateway_state,
};

#[tokio::test]
async fn stdio_server_lists_tools_and_calls_gg_ping() -> Result<(), Box<dyn std::error::Error>> {
    let service = ().serve(TokioChildProcess::new(mcp_server_command())?).await?;

    let tools = service.peer().list_tools(None).await?;
    let tool_names = tools
        .tools
        .iter()
        .map(|tool| tool.name.as_ref().to_string())
        .collect::<Vec<_>>();
    assert!(
        tool_names.contains(&"gg_ping".to_string()),
        "gg_ping must be present in tools/list, got {:?}",
        tool_names
    );

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_ping".into(),
            arguments: None,
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(false));

    let payload = extract_json_payload(&result.content)?;
    assert_eq!(payload["ok"], json!(true));
    assert_eq!(payload["result"]["name"], json!("gg-mcp-server"));

    let _ = service.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stdio_server_lists_pruned_gg_team_surface() -> Result<(), Box<dyn std::error::Error>> {
    let service = ().serve(TokioChildProcess::new(mcp_server_command())?).await?;

    let tools = service.peer().list_tools(None).await?;
    let tool_names = tools
        .tools
        .iter()
        .map(|tool| tool.name.as_ref().to_string())
        .collect::<Vec<_>>();
    let expected_team_tools = ["gg_team_status", "gg_team_message", "gg_team_manage"];
    for tool_name in expected_team_tools {
        assert!(
            tool_names.contains(&tool_name.to_string()),
            "Expected {tool_name} in tools/list, got {:?}",
            tool_names
        );
    }
    for removed_tool_name in [
        "gg_team_create",
        "gg_team_join",
        "gg_team_leave",
        "gg_team_members",
        "gg_team_add_member",
        "gg_team_change_name",
        "gg_team_remove_members",
        "gg_team_send",
        "gg_team_broadcast",
    ] {
        assert!(
            !tool_names.contains(&removed_tool_name.to_string()),
            "Did not expect removed tool {removed_tool_name} in tools/list, got {:?}",
            tool_names
        );
    }

    let status_tool = tools
        .tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_team_status")
        .ok_or("missing gg_team_status from tools/list")?;
    let status_description = status_tool
        .description
        .as_deref()
        .ok_or("gg_team_status description should be present")?;
    assert!(
        status_description.contains("context window remaining percentage"),
        "expected context metadata guidance in gg_team_status description"
    );
    assert!(
        status_description.contains("managed-worktree metadata"),
        "expected managed-worktree guidance in gg_team_status description"
    );
    assert!(
        status_description.contains("`added_by` provenance"),
        "expected added_by guidance in gg_team_status description"
    );

    let _ = service.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stdio_server_exposes_team_message_schema_with_optional_image_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let service = ().serve(TokioChildProcess::new(mcp_server_command())?).await?;

    let tools = service.peer().list_tools(None).await?;
    let team_message_tool = tools
        .tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_team_message")
        .ok_or("missing gg_team_message from tools/list")?;
    let required_fields = team_message_tool
        .input_schema
        .get("required")
        .and_then(Value::as_array)
        .ok_or("gg_team_message required fields should be present")?;
    assert!(
        required_fields
            .iter()
            .any(|entry| entry.as_str() == Some("message"))
    );
    assert!(
        !required_fields
            .iter()
            .any(|entry| entry.as_str() == Some("image_paths"))
    );

    let description = team_message_tool
        .description
        .as_deref()
        .ok_or("gg_team_message description should be present")?;
    assert!(
        description.contains("image_paths"),
        "expected image_paths guidance in team message description"
    );
    assert!(
        description.to_ascii_lowercase().contains("provided order"),
        "expected ordered image-path guidance in team message description"
    );

    let image_paths_schema = team_message_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get("image_paths"))
        .and_then(Value::as_object)
        .ok_or("gg_team_message.image_paths schema should be present")?;
    let image_paths_items = image_paths_schema
        .get("items")
        .and_then(Value::as_object)
        .ok_or("gg_team_message.image_paths.items schema should be present")?;
    assert_eq!(
        image_paths_items.get("type").and_then(Value::as_str),
        Some("string")
    );
    assert_eq!(
        image_paths_items.get("minLength").and_then(Value::as_u64),
        Some(1)
    );

    let _ = service.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stdio_server_exposes_team_manage_schema_without_legacy_unsubscribe_flag()
-> Result<(), Box<dyn std::error::Error>> {
    let service = ().serve(TokioChildProcess::new(mcp_server_command())?).await?;

    let tools = service.peer().list_tools(None).await?;
    let team_manage_tool = tools
        .tools
        .iter()
        .find(|tool| tool.name.as_ref() == "gg_team_manage")
        .ok_or("missing gg_team_manage from tools/list")?;
    let required_fields = team_manage_tool
        .input_schema
        .get("required")
        .and_then(Value::as_array)
        .ok_or("gg_team_manage required fields should be present")?;
    assert!(
        !required_fields
            .iter()
            .any(|entry| { entry.as_str() == Some("unsubscribe_from_compaction_notifications") })
    );
    assert!(
        !required_fields
            .iter()
            .any(|entry| entry.as_str() == Some("creator_compaction_subscription"))
    );

    let properties = team_manage_tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or("gg_team_manage properties schema should be present")?;
    assert!(
        !properties.contains_key("unsubscribe_from_compaction_notifications"),
        "gg_team_manage schema should not expose removed unsubscribe flag"
    );
    let worktree_name_schema = properties
        .get("worktree_name")
        .ok_or("gg_team_manage.worktree_name schema should be present")?;
    assert!(!worktree_name_schema.is_null());
    let image_paths_schema = properties
        .get("image_paths")
        .ok_or("gg_team_manage.image_paths schema should be present")?;
    assert!(!image_paths_schema.is_null());
    let use_existing_worktree_schema = properties
        .get("use_existing_worktree")
        .ok_or("gg_team_manage.use_existing_worktree schema should be present")?;
    assert!(!use_existing_worktree_schema.is_null());
    let creator_compaction_subscription_schema = properties
        .get("creator_compaction_subscription")
        .ok_or("gg_team_manage.creator_compaction_subscription schema should be present")?;
    assert!(!creator_compaction_subscription_schema.is_null());
    let creator_compaction_subscription_schema_json =
        serde_json::to_string(creator_compaction_subscription_schema)?;
    assert!(
        creator_compaction_subscription_schema_json.contains("auto"),
        "gg_team_manage.creator_compaction_subscription schema should include `auto`"
    );
    assert!(
        creator_compaction_subscription_schema_json.contains("unsubscribed"),
        "gg_team_manage.creator_compaction_subscription schema should include `unsubscribed`"
    );

    let _ = service.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stdio_server_hides_process_tools_when_disabled() -> Result<(), Box<dyn std::error::Error>>
{
    let service = ()
        .serve(TokioChildProcess::new(mcp_server_command().configure(
            |command| {
                command.env("GG_MCP_ENABLE_PROCESS_TOOLS", "0");
            },
        ))?)
        .await?;

    let tools = service.peer().list_tools(None).await?;
    let tool_names = tools
        .tools
        .iter()
        .map(|tool| tool.name.as_ref().to_string())
        .collect::<Vec<_>>();
    assert!(
        !tool_names
            .iter()
            .any(|name| name.starts_with("gg_process_")),
        "gg_process_* tools should be omitted when GG_MCP_ENABLE_PROCESS_TOOLS=0, got {:?}",
        tool_names
    );

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_process_status".into(),
            arguments: None,
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(true));
    let payload = extract_json_payload(&result.content)?;
    assert_eq!(payload["ok"], json!(false));
    assert_eq!(payload["error"]["code"], json!("feature_disabled"));

    let _ = service.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stdio_server_hides_team_tools_when_gateway_capabilities_omit_team_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    let auth_token = "integration_token";
    let mut gateway_state = stub_gateway_state(auth_token, Vec::new());
    gateway_state.team_tools_enabled = false;
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_stub))
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
                command.env("GG_MCP_CALLER_AGENT_ID", "sess_mcp_disabled_team");
            },
        ))?)
        .await?;

    let tools = service.peer().list_tools(None).await?;
    let tool_names = tools
        .tools
        .iter()
        .map(|tool| tool.name.as_ref().to_string())
        .collect::<Vec<_>>();
    assert!(
        !tool_names.iter().any(|name| name.starts_with("gg_team_")),
        "gg_team_* tools should be omitted when runtime capabilities omit gg_team, got {:?}",
        tool_names
    );
    assert!(
        tool_names.contains(&"gg_process_status".to_string()),
        "non-team tools should remain listed"
    );

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_server_rejects_unknown_fields_for_gg_team_status()
-> Result<(), Box<dyn std::error::Error>> {
    let service = ().serve(TokioChildProcess::new(mcp_server_command())?).await?;

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            arguments: json!({
                "team_id": "team_integration",
                "unexpected_field": "should_be_rejected",
            })
            .as_object()
            .cloned(),
            task: None,
        })
        .await;
    assert!(
        result.is_err(),
        "gg_team_status should reject unknown fields before gateway invocation"
    );

    let _ = service.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stdio_server_proxies_gg_team_status_to_gateway() -> Result<(), Box<dyn std::error::Error>>
{
    let auth_token = "integration_token";
    let gateway_state = stub_gateway_state(
        auth_token,
        vec!["claude-sonnet-5".to_string(), "gpt-5".to_string()],
    );
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_stub))
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

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            arguments: json!({
                "team_id": "team_integration",
            })
            .as_object()
            .cloned(),
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(false));

    let payload = extract_json_payload(&result.content)?;
    assert_eq!(payload["ok"], json!(true));
    assert_eq!(payload["result"]["team_id"], json!("team_integration"));
    assert_eq!(
        payload["result"]["members"][0]["agent_id"],
        json!("sess_mcp_integration")
    );
    assert_eq!(
        payload["result"]["members"].as_array().map(Vec::len),
        Some(1)
    );

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_server_accepts_per_call_caller_metadata_when_required()
-> Result<(), Box<dyn std::error::Error>> {
    let auth_token = "integration_token";
    let gateway_state = stub_gateway_state(
        auth_token,
        vec!["claude-sonnet-5".to_string(), "gpt-5".to_string()],
    );
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_stub))
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
                command.env("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID", "1");
            },
        ))?)
        .await?;

    for expected_caller in ["sess_dynamic_a", "sess_dynamic_b"] {
        let result = service
            .peer()
            .call_tool(CallToolRequestParams {
                meta: None,
                name: "gg_team_status".into(),
                arguments: json!({
                    "team_id": "team_dynamic_caller",
                    "__gg_caller_agent_id": expected_caller,
                })
                .as_object()
                .cloned(),
                task: None,
            })
            .await?;
        assert_eq!(result.is_error, Some(false));
        let payload = extract_json_payload(&result.content)?;
        assert_eq!(payload["ok"], json!(true));
        assert_eq!(
            payload["result"]["members"][0]["agent_id"],
            json!(expected_caller)
        );
    }

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_server_rejects_missing_per_call_caller_metadata_when_required()
-> Result<(), Box<dyn std::error::Error>> {
    let auth_token = "integration_token";
    let gateway_state = stub_gateway_state(
        auth_token,
        vec!["claude-sonnet-5".to_string(), "gpt-5".to_string()],
    );
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_stub))
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
                command.env("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID", "1");
            },
        ))?)
        .await?;

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            arguments: json!({
                "team_id": "team_missing_dynamic_caller",
            })
            .as_object()
            .cloned(),
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(true));
    let payload = extract_json_payload(&result.content)?;
    assert_eq!(payload["ok"], json!(false));
    assert_eq!(payload["error"]["code"], json!("unauthorized"));

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_server_returns_backend_unavailable_when_gateway_missing()
-> Result<(), Box<dyn std::error::Error>> {
    let service = ().serve(TokioChildProcess::new(mcp_server_command())?).await?;

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            arguments: json!({
                "team_id": "team_no_gateway",
            })
            .as_object()
            .cloned(),
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(true));

    let payload = extract_json_payload(&result.content)?;
    assert_eq!(payload["ok"], json!(false));
    assert_eq!(payload["error"]["code"], json!("backend_unavailable"));

    let _ = service.cancel().await?;
    Ok(())
}

#[tokio::test]
async fn stdio_server_handles_high_volume_gateway_invocations()
-> Result<(), Box<dyn std::error::Error>> {
    let auth_token = "integration_token";
    let gateway_state = stub_gateway_state(
        auth_token,
        vec!["claude-sonnet-5".to_string(), "gpt-5".to_string()],
    );
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_stub))
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
                command.env("GG_MCP_CALLER_AGENT_ID", "sess_mcp_stress");
            },
        ))?)
        .await?;

    for index in 0..100 {
        let team_id = format!("team_stress_{index}");
        let result = service
            .peer()
            .call_tool(CallToolRequestParams {
                meta: None,
                name: "gg_team_status".into(),
                arguments: json!({
                    "team_id": team_id,
                })
                .as_object()
                .cloned(),
                task: None,
            })
            .await?;
        assert_eq!(result.is_error, Some(false));
        let payload = extract_json_payload(&result.content)?;
        assert_eq!(payload["ok"], json!(true));
        assert!(payload["result"]["team_id"].as_str().is_some());
    }

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_server_surfaces_gateway_unauthorized_envelope()
-> Result<(), Box<dyn std::error::Error>> {
    let auth_token = "integration_token";
    let gateway_state = stub_gateway_state(
        auth_token,
        vec!["claude-sonnet-5".to_string(), "gpt-5".to_string()],
    );
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_stub))
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
                command.env("GG_MCP_GATEWAY_TOKEN", "invalid_token");
                command.env("GG_MCP_CALLER_AGENT_ID", "sess_mcp_auth_failure");
            },
        ))?)
        .await?;

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            arguments: json!({
                "team_id": "team_auth_failure",
            })
            .as_object()
            .cloned(),
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(true));

    let payload = extract_json_payload(&result.content)?;
    assert_eq!(payload["ok"], json!(false));
    assert_eq!(payload["error"]["code"], json!("unauthorized"));

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_server_returns_backend_unavailable_for_non_json_gateway_response()
-> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_non_json_stub))
        .with_state(stub_gateway_state("integration_token", Vec::new()));
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
                command.env("GG_MCP_GATEWAY_TOKEN", "integration_token");
                command.env("GG_MCP_CALLER_AGENT_ID", "sess_mcp_non_json_gateway");
            },
        ))?)
        .await?;

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            arguments: json!({
                "team_id": "team_non_json_gateway",
            })
            .as_object()
            .cloned(),
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(true));

    let payload = extract_json_payload(&result.content)?;
    assert_eq!(payload["ok"], json!(false));
    assert_eq!(payload["error"]["code"], json!("backend_unavailable"));

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_server_injects_model_presets_into_team_manage_metadata_on_initial_tools_list()
-> Result<(), Box<dyn std::error::Error>> {
    let auth_token = "integration_token";
    let gateway_state = stub_gateway_state(
        auth_token,
        vec!["claude-sonnet-5".to_string(), "gpt-5".to_string()],
    );
    let capabilities_calls = Arc::clone(&gateway_state.capabilities_calls);
    let app = Router::new()
        .route("/capabilities", get(capabilities_stub))
        .route("/invoke", post(invoke_stub))
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
    assert!(
        initial_description.contains("optional `worktree_name`"),
        "expected worktree_name guidance in initial gg_team_manage description"
    );
    assert!(
        initial_description.contains("optional `image_paths`"),
        "expected image_paths guidance in initial gg_team_manage description"
    );
    assert!(
        initial_description.contains("optional `use_existing_worktree`"),
        "expected use_existing_worktree guidance in initial gg_team_manage description"
    );
    assert!(
        initial_description.contains("optional `creator_compaction_subscription`"),
        "expected creator_compaction_subscription guidance in initial gg_team_manage description"
    );
    assert!(
        initial_description.contains("defaults to `auto`"),
        "expected default auto guidance in initial gg_team_manage description"
    );
    assert!(
        initial_description.contains("set `unsubscribed`"),
        "expected unsubscribed guidance in initial gg_team_manage description"
    );
    assert!(
        initial_description.contains("Available model_preset values: claude-sonnet-5, gpt-5."),
        "tools/list should include model preset values from capabilities; got {initial_description}"
    );
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
    let initial_model_preset_schema_json = serde_json::to_string(initial_model_preset_schema)?;
    assert!(
        initial_model_preset_schema_json.contains("claude-sonnet-5"),
        "expected model_preset enum to include claude-sonnet-5"
    );
    assert!(
        initial_model_preset_schema_json.contains("gpt-5"),
        "expected model_preset enum to include gpt-5"
    );

    let result = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            arguments: json!({
                "team_id": "team_integration",
            })
            .as_object()
            .cloned(),
            task: None,
        })
        .await?;
    assert_eq!(result.is_error, Some(false));

    assert!(
        capabilities_calls.load(Ordering::SeqCst) >= 1,
        "capabilities endpoint should remain callable while handling tools/call"
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
        description.contains("Available model_preset values: claude-sonnet-5, gpt-5."),
        "expected dynamic model_preset values after tools/call; got {description}"
    );

    let _ = service.cancel().await?;
    gateway_handle.abort();
    Ok(())
}
