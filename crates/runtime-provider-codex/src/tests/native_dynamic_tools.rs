use super::*;

#[derive(Default)]
struct RecordingNativeGateway {
    calls: tokio::sync::Mutex<Vec<runtime_core::ToolInvokeRequest>>,
}

#[async_trait::async_trait]
impl runtime_core::ToolGateway for RecordingNativeGateway {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn capabilities(&self) -> Result<Value, RuntimeError> {
        Ok(json!({"result":{"tools":["gg_team","gg_message","gg_process"]}}))
    }

    async fn invoke_tool(
        &self,
        request: runtime_core::ToolInvokeRequest,
    ) -> Result<Value, RuntimeError> {
        self.calls.lock().await.push(request);
        Ok(json!({"ok":true,"result":{"message_id":"msg_native_1"}}))
    }
}

#[tokio::test]
async fn native_codex_dynamic_gg_tools_bind_caller_and_replace_mcp_registration() {
    let temp = tempfile::tempdir().expect("temp");
    let (provider, log_path) = fake_provider(&temp, true);
    let gateway = std::sync::Arc::new(RecordingNativeGateway::default());
    let provider_gateway: std::sync::Arc<dyn runtime_core::ToolGateway> = gateway.clone();
    provider
        .attach_native_gg_gateway(&provider_gateway)
        .expect("bind native gateway");
    assert!(
        provider
            .attach_native_gg_gateway(&provider_gateway)
            .is_err(),
        "duplicate binding must fail"
    );
    provider
        .create_session_with_policy(legacy_create("sess_native"))
        .await
        .expect("create native session");

    let staged = std::fs::read_to_string(
        temp.path()
            .join("codex-home/runtime-sessions/sess_native/config.toml"),
    )
    .expect("staged home config");
    assert!(
        !staged.contains("mcp_servers.gg"),
        "native GG must not duplicate MCP tools"
    );
    assert!(
        !staged.contains("GG_MCP_GATEWAY_TOKEN"),
        "native Codex must not receive the scoped MCP bearer"
    );
    let calls = read_log(&log_path);
    let start = calls
        .iter()
        .find(|line| line["method"] == "thread/start")
        .unwrap();
    assert_eq!(
        start["params"]["dynamicTools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["gg_team", "gg_message", "gg_process"]
    );

    let first_ack = provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_native".into(),
            turn_id: "logical-native".into(),
            input: vec![json!({"type":"text","text":"native-gg-tool"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
            turn_settings: Default::default(),
        })
        .await
        .expect("send native turn");
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_native".into(),
            turn_id: "logical-native".into(),
            timeout_ms: Some(20_000),
        })
        .await
        .expect("native completion");
    assert_eq!(result.status, ProviderTurnStatus::Completed);
    let requests = gateway.calls.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].caller_session_id, "sess_native");
    assert_eq!(requests[0].tool_name, "gg_message");
    assert_eq!(
        requests[0].args,
        json!({"mode":"broadcast","message":"native greeting"})
    );
    assert!(requests[0]
        .invocation_id
        .as_ref()
        .is_some_and(|id| id.starts_with("codex_")));
    drop(requests);
    let native_result = read_log(&log_path)
        .into_iter()
        .find(|line| line["id"] == "native-gg-rpc-1" && line.get("method").is_none())
        .expect("native JSON-RPC tool response");
    assert_eq!(native_result["result"]["success"], true);
    let envelope: Value = serde_json::from_str(
        native_result["result"]["contentItems"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(envelope["result"]["message_id"], "msg_native_1");

    // An old native tool RPC may be replayed on the same persistent Codex
    // thread after a later turn starts. It must not execute with the newer
    // turn's authority just because some logical turn is currently active.
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_native".into(),
            turn_id: "logical-next".into(),
            input: vec![json!({"type":"text","text":"interrupt"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
            turn_settings: Default::default(),
        })
        .await
        .expect("start next turn");
    let thread_id = provider
        .inner
        .sessions
        .read()
        .await
        .get("sess_native")
        .unwrap()
        .provider_session_ref
        .clone();
    crate::dynamic_tools::dispatch_native_tool_call(
        &provider.inner,
        "sess_native",
        &json!({
            "id":"stale-native-rpc",
            "method":"item/tool/call",
            "params":{
                "threadId":thread_id,
                "turnId":first_ack.provider_native_turn_id.unwrap(),
                "tool":"gg_message",
                "itemId":"old-item-replayed",
                "arguments":{"mode":"broadcast","message":"must never be sent"}
            }
        }),
    )
    .await;
    let rejected = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(response) = read_log(&log_path)
                .into_iter()
                .find(|line| line["id"] == "stale-native-rpc" && line.get("method").is_none())
            {
                break response;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stale native tool must be rejected");
    assert_eq!(rejected["result"]["success"], false);
    let rejected_envelope: Value = serde_json::from_str(
        rejected["result"]["contentItems"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(rejected_envelope["error"]["code"], "invalid_turn");
    assert_eq!(gateway.calls.lock().await.len(), 1);
    provider
        .interrupt_turn(ProviderInterruptTurnRequest {
            runtime_session_id: "sess_native".into(),
            turn_id: "logical-next".into(),
        })
        .await
        .expect("stop second turn");
}
