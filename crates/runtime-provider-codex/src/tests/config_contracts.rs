use super::*;

#[tokio::test]
async fn resume_rejects_missing_cwd_evidence_and_mismatched_canonical_ref() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);

    let mismatched = provider
        .resume_session_with_policy(
            ProviderResumeSessionPolicyRequest::legacy_compatible(
                "sess_canonical_mismatch".to_string(),
                "thread-native-1".to_string(),
                Some("different-thread".to_string()),
                Some("gpt-6-luna".to_string()),
                Some("/workspace/fake".to_string()),
                Some("workspace_write".to_string()),
                None,
                None,
            )
            .expect("resume policy"),
        )
        .await
        .expect_err("mismatched canonical ref must fail closed");
    assert!(
        matches!(mismatched, RuntimeError::ProtocolViolation(message) if message.contains("canonical provider session ref"))
    );

    let missing_cwd = provider
        .resume_session_with_policy(
            ProviderResumeSessionPolicyRequest::legacy_compatible(
                "sess_missing_cwd_evidence".to_string(),
                "thread-native-1".to_string(),
                Some("thread-native-1".to_string()),
                Some("gpt-6-luna".to_string()),
                Some("/workspace/missing-evidence".to_string()),
                Some("workspace_write".to_string()),
                None,
                None,
            )
            .expect("resume policy"),
        )
        .await
        .expect_err("resume without top-level cwd evidence must fail closed");
    assert!(
        matches!(missing_cwd, RuntimeError::ProtocolViolation(message) if message.contains("cwd evidence"))
    );
}

#[tokio::test]
async fn healthcheck_fails_fast_when_app_server_command_is_missing() {
    let temp = tempfile::tempdir().expect("temp dir");
    let provider = CodexProvider::new(CodexProviderConfig {
        enabled: true,
        home_dir: temp.path().join("codex-home"),
        command: temp.path().join("missing-codex").display().to_string(),
        app_server_args: vec!["app-server".to_string()],
        request_timeout_ms: 250,
        max_transports: 1,
        max_sessions_per_transport: 1,
        gg_mcp: CodexGgMcpConfig::default(),
    });
    let error = provider
        .healthcheck()
        .await
        .expect_err("missing app-server must fail");
    assert_eq!(
        error.provider_dispatch_code(),
        Some("codex_app_server_unavailable")
    );
}

#[tokio::test]
async fn codex_model_catalog_exposes_supported_models_and_reasoning_levels() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp_dir, false);
    let models = provider.list_models().await.expect("list models");

    let reasoning_levels = vec!["low", "medium", "high", "xhigh", "max"];
    let expected = [
        ("gpt-6-astra", "GPT 6 Astra", reasoning_levels.as_slice()),
        ("gpt-6.1-sol", "GPT 6.1 Sol", reasoning_levels.as_slice()),
        ("gpt-6-luna", "GPT 6 Luna", reasoning_levels.as_slice()),
    ];

    assert_eq!(models.len(), expected.len());
    for (model_id, display_name, reasoning_levels) in expected {
        let model = models
            .iter()
            .find(|model| model.id == model_id)
            .expect("expected codex model");
        assert_eq!(model.display_name, display_name);
        assert_eq!(model.reasoning_levels, reasoning_levels);
    }
}

#[test]
fn copy_auth_file_stages_into_runtime_home() {
    let source_dir = tempfile::tempdir().expect("source dir");
    let destination_dir = tempfile::tempdir().expect("destination dir");
    let source_auth = source_dir.path().join("auth.json");
    std::fs::write(source_auth.as_path(), "{\"token\":\"x\"}").expect("write source");
    let copied =
        copy_codex_auth_file(source_auth.as_path(), destination_dir.path()).expect("copy auth");
    assert!(copied.exists());
    assert!(std::fs::read_to_string(copied)
        .expect("read copied")
        .contains("token"));
}

#[test]
fn permission_modes_use_current_thread_and_turn_protocol_shapes() {
    let mut thread = json!({});
    apply_thread_permission_mode(&mut thread, Some("full_auto"));
    assert_eq!(thread["approvalPolicy"], "on-request");
    assert_eq!(thread["sandbox"], "workspace-write");
    assert!(thread.get("sandboxPolicy").is_none());

    let mut turn = json!({});
    apply_turn_permission_mode(&mut turn, Some("full_auto"));
    assert_eq!(turn["approvalPolicy"], "on-request");
    assert_eq!(turn["sandboxPolicy"], json!({"type":"workspaceWrite"}));
    assert!(turn.get("sandbox").is_none());

    let mut read_only = json!({});
    apply_turn_permission_mode(&mut read_only, Some("read-only"));
    assert_eq!(read_only["sandboxPolicy"], json!({"type":"readOnly"}));

    let mut dangerous = json!({});
    apply_turn_permission_mode(&mut dangerous, Some("danger_full_access"));
    assert_eq!(dangerous["approvalPolicy"], "never");
    assert_eq!(
        dangerous["sandboxPolicy"],
        json!({"type":"dangerFullAccess"})
    );
}

#[test]
fn codex_gg_mcp_config_includes_gateway_and_caller_identity() {
    let rendered = format_codex_gg_mcp_config(
        &CodexGgMcpConfig {
            enabled: true,
            server_name: "gg".to_string(),
            command: "/opt/gg-runtime/sidecars/gg-mcp-server/gg-mcp-server".to_string(),
            args: vec!["--stdio".to_string()],
            enable_process_tools: true,
            gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
            gateway_token: Some("codex-token".to_string()),
        },
        "sess_codex",
    );

    assert!(rendered.contains("[mcp_servers.gg]"));
    assert!(rendered.contains("command = \"/opt/gg-runtime/sidecars/gg-mcp-server/gg-mcp-server\""));
    assert!(rendered.contains("args = [\"--stdio\"]"));
    assert!(rendered.contains("GG_MCP_ENABLE_PROCESS_TOOLS = \"1\""));
    assert!(rendered.contains("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID = \"1\""));
    assert!(rendered.contains("GG_MCP_CALLER_AGENT_ID = \"sess_codex\""));
    assert!(rendered.contains("GG_MCP_GATEWAY_URL = \"http://127.0.0.1:8787/v1/mcp\""));
    assert!(rendered.contains("GG_MCP_GATEWAY_TOKEN = \"codex-token\""));
}

#[test]
fn codex_gg_mcp_config_can_disable_process_tools_without_hiding_team_server() {
    let rendered = format_codex_gg_mcp_config(
        &CodexGgMcpConfig {
            enable_process_tools: false,
            ..CodexGgMcpConfig::default()
        },
        "sess_codex",
    );
    assert!(rendered.contains("[mcp_servers.gg]"));
    assert!(rendered.contains("GG_MCP_ENABLE_PROCESS_TOOLS = \"0\""));
    assert!(rendered.contains("GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID = \"1\""));
}
