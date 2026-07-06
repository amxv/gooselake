use std::fs;
use std::time::Duration;

use runtime_core::RuntimeProvider;
use serde_json::{json, Value};

use crate::{AcpProvider, AcpProviderConfig};

use super::support::FakeAgentHarness;

#[tokio::test]
async fn close_session_shuts_down_idle_connection() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();

    let created = provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_idle_close".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");
    assert!(created.provider_session_ref.starts_with("sess_"));
    assert!(provider.inner.connection.lock().await.is_some());

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: "sess_idle_close".to_string(),
            reason: Some("idle close".to_string()),
        })
        .await
        .expect("close");

    assert!(provider.inner.connection.lock().await.is_none());
}

#[tokio::test]
async fn close_session_succeeds_after_connection_death_without_respawn() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();

    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_close_dead_connection".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_close_dead_connection".to_string(),
            turn_id: "turn_close_dead_connection".to_string(),
            input: vec![json!({"type":"text","text":"crash now"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");

    let _ = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_close_dead_connection".to_string(),
            turn_id: "turn_close_dead_connection".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait after crash");

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: "sess_close_dead_connection".to_string(),
            reason: Some("cleanup after crash".to_string()),
        })
        .await
        .expect("close after connection death");

    assert!(provider.inner.connection.lock().await.is_none());
    assert!(!provider
        .inner
        .sessions
        .read()
        .await
        .contains_key("sess_close_dead_connection"));
}

#[tokio::test]
async fn close_session_ignores_close_rpc_timeout_after_local_cleanup() {
    let harness = FakeAgentHarness::new("hang_close");
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        provider_dir: harness.provider_dir.clone(),
        command: Some("python3".to_string()),
        args: vec![harness.script_path.display().to_string()],
        request_timeout_secs: 1,
        ..AcpProviderConfig::default()
    });

    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_hang_close".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: "sess_hang_close".to_string(),
            reason: Some("close timeout".to_string()),
        })
        .await
        .expect("close should still succeed");

    assert!(provider.inner.connection.lock().await.is_none());
    assert!(!provider
        .inner
        .sessions
        .read()
        .await
        .contains_key("sess_hang_close"));
}

#[tokio::test]
async fn close_session_ignores_close_rpc_error_after_local_cleanup() {
    let harness = FakeAgentHarness::new("error_close");
    let provider = harness.provider();

    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_error_close".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: "sess_error_close".to_string(),
            reason: Some("close error".to_string()),
        })
        .await
        .expect("close should still succeed");

    assert!(provider.inner.connection.lock().await.is_none());
    assert!(!provider
        .inner
        .sessions
        .read()
        .await
        .contains_key("sess_error_close"));
}

#[tokio::test]
async fn create_session_failure_during_initialize_cleans_up_connection_and_child() {
    let harness = FakeAgentHarness::new("hang_initialize");
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        provider_dir: harness.provider_dir.clone(),
        command: Some("python3".to_string()),
        args: vec![harness.script_path.display().to_string()],
        request_timeout_secs: 1,
        ..AcpProviderConfig::default()
    });

    let error = provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_hang_initialize".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect_err("initialize timeout should fail");
    assert!(error
        .to_string()
        .contains("timed out waiting for acp response to initialize"));

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(provider.inner.connection.lock().await.is_none());

    let pid = fs::read_to_string(harness.pid_path.as_path())
        .expect("pid file")
        .trim()
        .parse::<u32>()
        .expect("pid");
    let alive = std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .expect("kill -0 status")
        .success();
    assert!(!alive, "initialize-timeout child should have been reaped");
}

#[tokio::test]
async fn bad_protocol_initialize_cleans_up_connection_and_child() {
    let harness = FakeAgentHarness::new("bad_protocol");
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        provider_dir: harness.provider_dir.clone(),
        command: Some("python3".to_string()),
        args: vec![harness.script_path.display().to_string()],
        ..AcpProviderConfig::default()
    });

    let error = provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_bad_protocol".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect_err("bad protocol should fail");
    assert!(error.to_string().contains("acp protocol version mismatch"));

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(provider.inner.connection.lock().await.is_none());
}

#[tokio::test]
async fn real_adapter_contract_handles_malformed_agent_output() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_malformed".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_malformed".to_string(),
            turn_id: "turn_malformed".to_string(),
            input: vec![json!({"type":"text","text":"malformed response"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_malformed".to_string(),
            turn_id: "turn_malformed".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait");
    assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
    assert!(result
        .error
        .as_ref()
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .is_some_and(|message| message.contains("malformed JSON-RPC")));
}

#[tokio::test]
async fn real_adapter_contract_handles_process_death() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_crash".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_crash".to_string(),
            turn_id: "turn_crash".to_string(),
            input: vec![json!({"type":"text","text":"crash now"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_crash".to_string(),
            turn_id: "turn_crash".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait");
    assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
    assert!(result
        .error
        .as_ref()
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .is_some_and(|message| message.contains("connection closed")));
}

#[tokio::test]
async fn real_adapter_contract_preserves_ordered_updates_in_terminal_usage() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_tooling".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_tooling".to_string(),
            turn_id: "turn_tooling".to_string(),
            input: vec![json!({"type":"text","text":"tooling please"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_tooling".to_string(),
            turn_id: "turn_tooling".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait");
    assert_eq!(result.status, runtime_core::ProviderTurnStatus::Completed);
    let usage = result.usage.expect("usage");
    assert_eq!(
        usage.get("last_message").and_then(Value::as_str),
        Some("First line.\n\nSecond line.")
    );
    assert_eq!(
        usage.get("assistant_text").and_then(Value::as_str),
        Some("First line.\n\nSecond line.")
    );
    assert_eq!(
        usage.pointer("/usage_update/used").and_then(Value::as_i64),
        Some(11)
    );
    let tool_calls = usage
        .get("tool_calls")
        .and_then(Value::as_array)
        .expect("tool calls");
    assert_eq!(tool_calls.len(), 1);
    assert_eq!(
        tool_calls[0].get("toolCallId").and_then(Value::as_str),
        Some("tool_1")
    );
    assert_eq!(
        tool_calls[0].get("sessionUpdate").and_then(Value::as_str),
        Some("tool_call_update")
    );
}

#[tokio::test]
async fn real_adapter_contract_stages_runtime_approval_before_execution() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_approval".to_string(),
            model: None,
            cwd: None,
            permission_mode: Some("require_approval".to_string()),
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "turn_approval".to_string(),
            input: vec![json!({"type":"text","text":"split please"})],
            expected_turn_id: None,
            permission_mode: Some("require_approval".to_string()),
            approval_id: Some("apr_1".to_string()),
        })
        .await
        .expect("send");

    provider
        .respond_approval(runtime_core::ProviderApprovalResponseRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "turn_approval".to_string(),
            approval_id: "apr_1".to_string(),
            decision: "accept".to_string(),
            payload: None,
        })
        .await
        .expect("respond");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "turn_approval".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait");
    assert_eq!(result.status, runtime_core::ProviderTurnStatus::Completed);
}

#[tokio::test]
async fn real_adapter_contract_declined_runtime_approval_interrupts_turn() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_decline".to_string(),
            model: None,
            cwd: None,
            permission_mode: Some("require_approval".to_string()),
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_decline".to_string(),
            turn_id: "turn_decline".to_string(),
            input: vec![json!({"type":"text","text":"will not run"})],
            expected_turn_id: None,
            permission_mode: Some("require_approval".to_string()),
            approval_id: Some("apr_decline".to_string()),
        })
        .await
        .expect("send");

    provider
        .respond_approval(runtime_core::ProviderApprovalResponseRequest {
            runtime_session_id: "sess_decline".to_string(),
            turn_id: "turn_decline".to_string(),
            approval_id: "apr_decline".to_string(),
            decision: "decline".to_string(),
            payload: None,
        })
        .await
        .expect("respond");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_decline".to_string(),
            turn_id: "turn_decline".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait");
    assert_eq!(result.status, runtime_core::ProviderTurnStatus::Interrupted);
}
