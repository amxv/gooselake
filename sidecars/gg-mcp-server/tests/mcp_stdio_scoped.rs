use axum::{
    Router,
    extract::Json,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use rmcp::{
    ServiceExt,
    model::CallToolRequestParams,
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde_json::{Value, json};
use std::net::Ipv4Addr;

mod support;
use support::{capabilities_stub, extract_json_payload, mcp_server_command, stub_gateway_state};

async fn scoped_model_invoke_stub(
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|header| header.to_str().ok()),
        Some("Bearer scoped-integration-credential")
    );
    assert!(
        headers
            .get("x-gg-tool-invocation-id")
            .and_then(|header| header.to_str().ok())
            .is_some_and(|id| id.starts_with("mcp_"))
    );
    assert!(
        body.get("caller_agent_id").is_none(),
        "caller identity must never be model request JSON"
    );
    assert!(
        body.get("invocation_id").is_none(),
        "trusted invocation id belongs in header"
    );
    assert_eq!(body["tool_name"], "gg_message");
    assert_eq!(body["args"]["mode"], "direct");
    (
        StatusCode::OK,
        Json(json!({"ok":true,"result":{"model_scoped":true}})),
    )
}

#[tokio::test]
async fn stdio_scoped_model_gateway_exposes_only_three_tools_and_forwards_trusted_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let state = stub_gateway_state("scoped-integration-credential", Vec::new());
    let app = Router::new()
        .route("/v2/mcp/capabilities", get(capabilities_stub))
        .route("/v2/mcp/invoke", post(scoped_model_invoke_stub))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("scoped gateway");
    });
    let url = format!("http://{addr}/v2/mcp");
    let service = ()
        .serve(TokioChildProcess::new(mcp_server_command().configure(
            |command| {
                command.env("GG_MCP_GATEWAY_URL", url);
                command.env("GG_MCP_GATEWAY_TOKEN", "scoped-integration-credential");
                command.env("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID", "1");
                command.env("GG_MCP_CALLER_AGENT_ID", "sess_scoped_model");
            },
        ))?)
        .await?;
    let mut names = service
        .peer()
        .list_tools(None)
        .await?
        .tools
        .into_iter()
        .map(|tool| tool.name.into_owned())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["gg_message", "gg_process", "gg_team"]);

    let direct = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_message".into(),
            task: None,
            arguments: json!({"mode":"direct","agent_id":"sess_other","message":"hello"})
                .as_object()
                .cloned(),
        })
        .await?;
    assert_eq!(direct.is_error, Some(false));
    let payload = extract_json_payload(&direct.content)?;
    assert_eq!(payload["result"]["model_scoped"], true);

    let legacy = service
        .peer()
        .call_tool(CallToolRequestParams {
            meta: None,
            name: "gg_team_status".into(),
            task: None,
            arguments: Some(serde_json::Map::new()),
        })
        .await?;
    assert_eq!(legacy.is_error, Some(true));
    let payload = extract_json_payload(&legacy.content)?;
    assert_eq!(payload["error"]["code"], "bad_request");
    let _ = service.cancel().await?;
    handle.abort();
    Ok(())
}
