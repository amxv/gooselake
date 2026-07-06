use super::*;
use runtime_core::ToolGateway;

#[tokio::test]
async fn team_status_gateway_invoke_returns_member_rows() {
    let (gateway, runtime, team_comms, temp_dir) = build_team_gateway_fixture().await;
    let (sessions, team_id) =
        create_team_gateway_sessions(&runtime, &team_comms, &temp_dir, 2).await;
    runtime
        .send_turn(
            sessions[0].id.as_str(),
            runtime_core::SendTurnInput {
                input: vec![json!({ "type": "text", "text": "collect usage" })],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await
        .expect("send turn");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let response = gateway
        .invoke_tool(runtime_core::ToolInvokeRequest {
            namespace: Some("gg_team".to_string()),
            tool_name: GG_TEAM_STATUS.to_string(),
            caller_session_id: sessions[0].id.clone(),
            invocation_id: None,
            args: json!({ "team_id": team_id }),
        })
        .await
        .expect("status invoke");

    assert_eq!(response.get("ok").and_then(Value::as_bool), Some(true));
}
