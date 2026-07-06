use super::*;

#[test]
fn gateway_namespace_validation_accepts_gg_team_tools() {
    assert!(crate::gateway::namespace_matches_tool(
        "gg_team",
        GG_TEAM_STATUS
    ));
    assert!(crate::gateway::namespace_matches_tool(
        " gg_team ",
        GG_TEAM_MESSAGE
    ));
    assert!(!crate::gateway::namespace_matches_tool(
        "gg_process",
        GG_TEAM_STATUS
    ));
    assert!(!crate::gateway::namespace_matches_tool(
        "unsupported",
        GG_TEAM_STATUS
    ));
}

#[tokio::test]
async fn gateway_capabilities_include_team_tools_when_enabled() {
    let gateway = build_test_tool_gateway(TeamMcpPolicy {
        enabled: true,
        non_lead_can_add_members: true,
        non_lead_can_remove_members: false,
    })
    .await;

    let capabilities = gateway.capabilities().await.expect("capabilities");
    let result = capabilities.get("result").expect("result");
    assert_eq!(
        result.get("ggTeamEnabled").and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        result
            .get("ggTeamManagePermissions")
            .and_then(|value| value.get("nonLeadCanAddMembers"))
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        result
            .get("ggTeamManagePermissions")
            .and_then(|value| value.get("nonLeadCanRemoveMembers"))
            .and_then(Value::as_bool),
        Some(false)
    );
    let namespaces = result
        .get("supportedNamespaces")
        .and_then(Value::as_array)
        .expect("namespaces");
    assert!(namespaces.iter().any(|value| value == "gg_team"));
    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .expect("tools");
    assert!(tools.iter().any(|value| value == GG_TEAM_STATUS));
    assert!(tools.iter().any(|value| value == GG_TEAM_MESSAGE));
    assert!(tools.iter().any(|value| value == GG_TEAM_MANAGE));
}

#[tokio::test]
async fn gateway_capabilities_omit_team_tools_when_disabled() {
    let gateway = build_test_tool_gateway(TeamMcpPolicy {
        enabled: false,
        non_lead_can_add_members: true,
        non_lead_can_remove_members: true,
    })
    .await;

    let capabilities = gateway.capabilities().await.expect("capabilities");
    let result = capabilities.get("result").expect("result");
    assert_eq!(
        result.get("ggTeamEnabled").and_then(Value::as_bool),
        Some(false)
    );
    let namespaces = result
        .get("supportedNamespaces")
        .and_then(Value::as_array)
        .expect("namespaces");
    assert!(!namespaces.iter().any(|value| value == "gg_team"));
    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .expect("tools");
    assert!(!tools.iter().any(|value| value == GG_TEAM_STATUS));
    assert!(!tools.iter().any(|value| value == GG_TEAM_MESSAGE));
    assert!(!tools.iter().any(|value| value == GG_TEAM_MANAGE));
}

#[tokio::test]
async fn gateway_rejects_disabled_team_tool_with_feature_disabled() {
    let gateway = build_test_tool_gateway(TeamMcpPolicy {
        enabled: false,
        non_lead_can_add_members: false,
        non_lead_can_remove_members: false,
    })
    .await;

    let response = gateway
        .invoke_tool(runtime_core::ToolInvokeRequest {
            namespace: Some("gg_team".to_string()),
            tool_name: GG_TEAM_STATUS.to_string(),
            caller_session_id: "sess_caller".to_string(),
            invocation_id: None,
            args: json!({ "team_id": "team_1" }),
        })
        .await
        .expect("invoke");
    assert_eq!(response.get("ok").and_then(Value::as_bool), Some(false));
    assert_eq!(
        response
            .get("error")
            .and_then(|value| value.get("code"))
            .and_then(Value::as_str),
        Some("feature_disabled")
    );
}
