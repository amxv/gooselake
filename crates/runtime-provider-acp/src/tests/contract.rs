use runtime_core::RuntimeProvider;
use serde_json::{json, Value};

use crate::{AcpProvider, AcpProviderConfig};

use super::support::{expected_gg_mcp_server, FakeAgentHarness};

#[tokio::test]
async fn metadata_reports_acp_provider_identity() {
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        command: Some("python3".to_string()),
        ..AcpProviderConfig::default()
    });

    let metadata = provider.metadata();
    assert_eq!(metadata.kind.as_str(), "acp");
    assert_eq!(metadata.display_name, "ACP");
    assert!(metadata.enabled);
}

#[tokio::test]
async fn healthcheck_creates_provider_runtime_directories() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();

    provider.healthcheck().await.expect("healthcheck");

    assert!(harness.provider_dir.is_dir());
    assert!(harness.provider_dir.join("instances").is_dir());
    assert!(harness.provider_dir.join("sessions").is_dir());
}

#[tokio::test]
async fn healthcheck_rejects_disabled_provider() {
    let provider = AcpProvider::new(AcpProviderConfig::default());
    let error = provider.healthcheck().await.expect_err("disabled");
    assert_eq!(error.to_string(), "bootstrap error: acp provider disabled");
}

#[tokio::test]
async fn list_models_is_empty_for_session_scoped_acp_selection() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();

    let models = provider.list_models().await.expect("models");
    assert!(models.is_empty());
}

#[tokio::test]
async fn auth_status_is_clear_about_unconfigured_state() {
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        ..AcpProviderConfig::default()
    });

    let status = provider.auth_status().await.expect("auth status");
    assert!(!status.authenticated);
    assert_eq!(status.mode.as_deref(), Some("not_configured"));
    assert!(status
        .detail
        .as_deref()
        .is_some_and(|detail| detail.contains("command is not configured")));
}

#[test]
fn default_config_matches_phase_two_server_contract() {
    let config = AcpProviderConfig::default();
    assert!(!config.enabled);
    assert_eq!(config.max_instances, 4);
    assert_eq!(config.max_sessions_per_instance, 4);
    assert!(config.command.is_none());
    assert_eq!(config.transport, "stdio");
    assert_eq!(config.request_timeout_secs, 30);
    assert_eq!(config.wait_timeout_secs, 300);
    assert!(config.gg_mcp_enabled);
    assert_eq!(config.gg_mcp_server_name, "gg");
    assert_eq!(config.gg_mcp_command, "gg-mcp-server");
    assert!(config.gg_mcp_args.is_empty());
    assert!(config.gg_mcp_enable_process_tools);
}

#[tokio::test]
async fn healthcheck_rejects_non_stdio_transport() {
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        command: Some("python3".to_string()),
        transport: "http".to_string(),
        ..AcpProviderConfig::default()
    });

    let error = provider
        .healthcheck()
        .await
        .expect_err("unsupported transport");
    assert_eq!(
        error.to_string(),
        "configuration error: acp transport 'http' is unsupported; expected stdio"
    );
}

#[tokio::test]
async fn lifecycle_methods_require_configured_command() {
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        ..AcpProviderConfig::default()
    });

    let error = provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_acp_test".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect_err("missing command");
    assert_eq!(
        error.to_string(),
        "configuration error: acp command is not configured"
    );
}

#[tokio::test]
async fn real_adapter_contract_create_send_wait_resume_and_close() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();

    let created = provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_real_1".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");
    assert!(created.provider_session_ref.starts_with("sess_"));

    let ack = provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_real_1".to_string(),
            turn_id: "turn_real_1".to_string(),
            input: vec![json!({"type":"text","text":"split please"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");
    assert_eq!(ack.turn_id, "turn_real_1");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_real_1".to_string(),
            turn_id: "turn_real_1".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait");
    assert_eq!(result.status, runtime_core::ProviderTurnStatus::Completed);
    let usage = result.usage.expect("usage");
    assert_eq!(
        usage.get("last_message").and_then(Value::as_str),
        Some("Hello world")
    );

    let resumed = provider
        .resume_session(runtime_core::ProviderResumeSessionRequest {
            runtime_session_id: "sess_real_2".to_string(),
            provider_session_ref: created.provider_session_ref.clone(),
            canonical_provider_session_ref: created.canonical_provider_session_ref.clone(),
            cwd: None,
            metadata: None,
        })
        .await
        .expect("resume");
    assert_eq!(resumed.provider_session_ref, created.provider_session_ref);

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: "sess_real_2".to_string(),
            reason: Some("test close".to_string()),
        })
        .await
        .expect("close");
}

#[tokio::test]
async fn create_and_resume_include_expected_gg_mcp_server_shape() {
    let harness = FakeAgentHarness::new("normal");
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        provider_dir: harness.provider_dir.clone(),
        command: Some("python3".to_string()),
        args: vec![harness.script_path.display().to_string()],
        gg_mcp_enabled: true,
        gg_mcp_server_name: "gg".to_string(),
        gg_mcp_command: "gg-mcp-server".to_string(),
        gg_mcp_args: vec!["--stdio".to_string()],
        gg_mcp_enable_process_tools: true,
        gg_mcp_gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
        gg_mcp_gateway_token: Some("acp-token".to_string()),
        ..AcpProviderConfig::default()
    });

    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_create".to_string(),
            model: None,
            cwd: Some("/tmp/create".to_string()),
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");
    provider
        .resume_session(runtime_core::ProviderResumeSessionRequest {
            runtime_session_id: "sess_resume".to_string(),
            provider_session_ref: "sess_1".to_string(),
            canonical_provider_session_ref: Some("sess_1".to_string()),
            cwd: Some("/tmp/resume".to_string()),
            metadata: None,
        })
        .await
        .expect("resume");

    let server = provider
        .build_gg_mcp_server_config("sess_create")
        .expect("gg mcp server config");
    assert_eq!(
        server,
        expected_gg_mcp_server(
            "sess_create",
            true,
            Some("http://127.0.0.1:8787/v1/mcp"),
            Some("acp-token")
        )
    );
}

#[test]
fn gg_mcp_server_config_can_disable_process_tools_without_disabling_team_tools() {
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        command: Some("agent".to_string()),
        gg_mcp_enabled: true,
        gg_mcp_server_name: "gg".to_string(),
        gg_mcp_command: "gg-mcp-server".to_string(),
        gg_mcp_args: vec!["--stdio".to_string()],
        gg_mcp_enable_process_tools: false,
        gg_mcp_gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
        gg_mcp_gateway_token: Some("acp-token".to_string()),
        ..AcpProviderConfig::default()
    });

    let server = provider
        .build_gg_mcp_server_config("sess-team-only")
        .expect("gg mcp server config");
    assert_eq!(server["name"].as_str(), Some("gg"));
    assert_eq!(server["command"].as_str(), Some("gg-mcp-server"));
    let env = server["env"].as_array().expect("env array");
    assert!(env.iter().any(|entry| {
        entry["name"].as_str() == Some("GG_MCP_ENABLE_PROCESS_TOOLS")
            && entry["value"].as_str() == Some("0")
    }));
    assert!(env.iter().any(|entry| {
        entry["name"].as_str() == Some("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID")
            && entry["value"].as_str() == Some("1")
    }));
    assert!(env.iter().any(|entry| {
        entry["name"].as_str() == Some("GG_MCP_CALLER_AGENT_ID")
            && entry["value"].as_str() == Some("sess-team-only")
    }));
    assert!(env.iter().any(|entry| {
        entry["name"].as_str() == Some("GG_MCP_GATEWAY_URL")
            && entry["value"].as_str() == Some("http://127.0.0.1:8787/v1/mcp")
    }));
}

#[tokio::test]
async fn real_adapter_contract_load_based_resume_is_supported() {
    let harness = FakeAgentHarness::new("load_only");
    let provider = harness.provider();

    let created = provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_load_1".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    let resumed = provider
        .resume_session(runtime_core::ProviderResumeSessionRequest {
            runtime_session_id: "sess_load_2".to_string(),
            provider_session_ref: created.provider_session_ref.clone(),
            canonical_provider_session_ref: created.canonical_provider_session_ref.clone(),
            cwd: None,
            metadata: None,
        })
        .await
        .expect("load resume");
    assert_eq!(resumed.provider_session_ref, created.provider_session_ref);
}

#[tokio::test]
async fn real_adapter_contract_interrupts_active_prompt() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_interrupt".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_interrupt".to_string(),
            turn_id: "turn_interrupt".to_string(),
            input: vec![json!({"type":"text","text":"sleep now"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    provider
        .interrupt_turn(runtime_core::ProviderInterruptTurnRequest {
            runtime_session_id: "sess_interrupt".to_string(),
            turn_id: "turn_interrupt".to_string(),
        })
        .await
        .expect("interrupt");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_interrupt".to_string(),
            turn_id: "turn_interrupt".to_string(),
            timeout_ms: Some(5_000),
        })
        .await
        .expect("wait");
    assert_eq!(result.status, runtime_core::ProviderTurnStatus::Interrupted);
}

#[tokio::test]
async fn real_adapter_contract_fails_permission_requests_clearly() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_permission".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_permission".to_string(),
            turn_id: "turn_permission".to_string(),
            input: vec![json!({"type":"text","text":"permission please"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_permission".to_string(),
            turn_id: "turn_permission".to_string(),
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
        .is_some_and(|message| message.contains("request_permission")));
}

#[tokio::test]
async fn real_adapter_contract_fails_permission_request_id_collisions_clearly() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_permission_collision".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    provider
        .send_turn(runtime_core::ProviderSendTurnRequest {
            runtime_session_id: "sess_permission_collision".to_string(),
            turn_id: "turn_permission_collision".to_string(),
            input: vec![json!({"type":"text","text":"permission collision please"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send");

    let result = provider
        .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
            runtime_session_id: "sess_permission_collision".to_string(),
            turn_id: "turn_permission_collision".to_string(),
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
        .is_some_and(|message| message.contains("request_permission")));
}

#[tokio::test]
async fn real_adapter_contract_maps_non_happy_stop_reasons_to_failed() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_stop_reasons".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create");

    for (turn_id, prompt_text, stop_reason) in [
        ("turn_refusal", "refusal please", "refusal"),
        ("turn_max_tokens", "max tokens please", "max_tokens"),
        ("turn_max_turns", "max turns please", "max_turn_requests"),
    ] {
        provider
            .send_turn(runtime_core::ProviderSendTurnRequest {
                runtime_session_id: "sess_stop_reasons".to_string(),
                turn_id: turn_id.to_string(),
                input: vec![json!({"type":"text","text":prompt_text})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
            })
            .await
            .expect("send");

        let result = provider
            .wait_for_turn(runtime_core::ProviderWaitTurnRequest {
                runtime_session_id: "sess_stop_reasons".to_string(),
                turn_id: turn_id.to_string(),
                timeout_ms: Some(5_000),
            })
            .await
            .expect("wait");
        assert_eq!(result.status, runtime_core::ProviderTurnStatus::Failed);
        let usage = result.usage.expect("usage");
        assert_eq!(
            usage.get("stop_reason").and_then(Value::as_str),
            Some(stop_reason)
        );
    }
}

#[tokio::test]
async fn create_session_enforces_configured_capacity() {
    let harness = FakeAgentHarness::new("normal");
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        provider_dir: harness.provider_dir.clone(),
        command: Some("python3".to_string()),
        args: vec![harness.script_path.display().to_string()],
        max_instances: 1,
        max_sessions_per_instance: 1,
        ..AcpProviderConfig::default()
    });

    provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_capacity_1".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create first");

    let error = provider
        .create_session(runtime_core::ProviderCreateSessionRequest {
            runtime_session_id: "sess_capacity_2".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect_err("capacity exceeded");
    assert!(error
        .to_string()
        .contains("acp session capacity exceeded (1 total sessions"));
}

#[tokio::test]
async fn concurrent_create_session_enforces_configured_capacity() {
    let harness = FakeAgentHarness::new("slow_create");
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        provider_dir: harness.provider_dir.clone(),
        command: Some("python3".to_string()),
        args: vec![harness.script_path.display().to_string()],
        max_instances: 1,
        max_sessions_per_instance: 1,
        ..AcpProviderConfig::default()
    });

    let provider_a = provider.clone();
    let provider_b = provider.clone();

    let create_a = tokio::spawn(async move {
        provider_a
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_capacity_a".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
    });
    let create_b = tokio::spawn(async move {
        provider_b
            .create_session(runtime_core::ProviderCreateSessionRequest {
                runtime_session_id: "sess_capacity_b".to_string(),
                model: None,
                cwd: None,
                permission_mode: None,
                metadata: None,
            })
            .await
    });

    let result_a = create_a.await.expect("task a");
    let result_b = create_b.await.expect("task b");
    let successes = [result_a.is_ok(), result_b.is_ok()]
        .into_iter()
        .filter(|value| *value)
        .count();
    let failures = [result_a, result_b]
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();

    assert_eq!(successes, 1, "exactly one concurrent create should succeed");
    assert_eq!(
        failures.len(),
        1,
        "exactly one concurrent create should fail"
    );
    assert!(failures[0]
        .to_string()
        .contains("acp session capacity exceeded (1 total sessions"));
}
