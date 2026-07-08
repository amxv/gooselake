use std::path::{Path, PathBuf};

use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderCreateSessionRequest,
    ProviderSendTurnRequest, RuntimeError, RuntimeProvider,
};

use crate::mcp_config::format_codex_gg_mcp_config;
use crate::{copy_codex_auth_file, CodexGgMcpConfig, CodexProvider, CodexProviderConfig};

#[test]
fn approval_decision_normalization_matches_core_contract() {
    assert_eq!(
        ApprovalDecision::parse("Accept").expect("mixed-case accept"),
        ApprovalDecision::Accept
    );
    assert!(ApprovalDecision::parse("maybe").is_err());
}

#[test]
fn build_turn_prompt_collects_text_items() {
    let prompt = CodexProvider::build_turn_prompt(&[
        serde_json::json!({"type":"text","text":"first"}),
        serde_json::json!({"text":"second"}),
    ]);
    assert!(prompt.contains("first"));
    assert!(prompt.contains("second"));
}

#[test]
fn build_turn_command_args_places_output_flag_under_exec() {
    let args = CodexProvider::build_turn_command_args(
        Path::new("/tmp/last.txt"),
        "runtime:session",
        Some("gpt-5.4"),
        Some("full_auto"),
        "hello",
    );
    let args = args
        .iter()
        .map(|value| value.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert_eq!(args.first().map(String::as_str), Some("exec"));
    assert!(args.contains(&"-o".to_string()));
    assert_eq!(
        args,
        vec![
            "exec",
            "--json",
            "--skip-git-repo-check",
            "-o",
            "/tmp/last.txt",
            "-m",
            "gpt-5.4",
            "--full-auto",
            "hello",
        ]
    );
}

#[test]
fn build_turn_command_args_resume_includes_resume_subcommand_and_provider_ref() {
    let args = CodexProvider::build_turn_command_args(
        Path::new("/tmp/last.txt"),
        "provider-session-123",
        None,
        Some("danger-full-access"),
        "continue",
    );
    let args = args
        .iter()
        .map(|value| value.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert_eq!(args.first().map(String::as_str), Some("exec"));
    assert_eq!(args.get(1).map(String::as_str), Some("resume"));
    assert!(args.contains(&"provider-session-123".to_string()));
    assert_eq!(
        args,
        vec![
            "exec",
            "resume",
            "--json",
            "--skip-git-repo-check",
            "-o",
            "/tmp/last.txt",
            "--dangerously-bypass-approvals-and-sandbox",
            "provider-session-123",
            "continue",
        ]
    );
}

#[test]
fn provider_new_absolutizes_relative_home_dir() {
    let provider = CodexProvider::new(CodexProviderConfig {
        enabled: true,
        home_dir: PathBuf::from("tmp/relative-codex-home"),
        max_transports: 1,
        max_sessions_per_transport: 1,
        gg_mcp: CodexGgMcpConfig::default(),
    });
    assert!(
        provider.inner.config.home_dir.is_absolute(),
        "expected codex home dir to be absolute"
    );
}

#[tokio::test]
async fn codex_model_catalog_exposes_gpt_reasoning_levels() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let provider = CodexProvider::new(CodexProviderConfig {
        enabled: true,
        home_dir: temp_dir.path().to_path_buf(),
        max_transports: 1,
        max_sessions_per_transport: 1,
        gg_mcp: CodexGgMcpConfig::default(),
    });
    let models = provider.list_models().await.expect("list models");
    let expected = vec!["low", "medium", "high", "extra-high"];

    for model_id in ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini"] {
        let model = models
            .iter()
            .find(|model| model.id == model_id)
            .expect("expected codex model");
        assert_eq!(model.reasoning_levels, expected);
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
    let content = std::fs::read_to_string(copied).expect("read copied");
    assert!(content.contains("token"));
}

#[tokio::test]
async fn accepted_approval_launch_failure_keeps_pending_turn_retryable() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let missing_cwd = temp_dir.path().join("missing-cwd");
    let provider = CodexProvider::new(CodexProviderConfig {
        enabled: true,
        home_dir: temp_dir.path().join("codex-home"),
        max_transports: 1,
        max_sessions_per_transport: 1,
        gg_mcp: CodexGgMcpConfig::default(),
    });

    provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess_retry".to_string(),
            model: Some("gpt-5.4-mini".to_string()),
            cwd: Some(missing_cwd.display().to_string()),
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create session");

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_retry".to_string(),
            turn_id: "turn_retry".to_string(),
            input: vec![serde_json::json!({"type":"text","text":"needs approval"})],
            expected_turn_id: None,
            permission_mode: Some("require_approval".to_string()),
            approval_id: Some("apr_retry".to_string()),
        })
        .await
        .expect("send pending approval turn");

    let first = provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "sess_retry".to_string(),
            turn_id: "turn_retry".to_string(),
            approval_id: "apr_retry".to_string(),
            decision: "accept".to_string(),
            payload: None,
        })
        .await;
    assert!(matches!(first, Err(RuntimeError::Io(_))));
    {
        let sessions = provider.inner.sessions.read().await;
        let session = sessions.get("sess_retry").expect("session");
        assert!(session.pending_approvals.contains_key("apr_retry"));
        assert!(!session.active_turns.contains_key("turn_retry"));
    }

    // Retry should fail the same way (launch failure), not with provider-side approval NotFound.
    let second = provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "sess_retry".to_string(),
            turn_id: "turn_retry".to_string(),
            approval_id: "apr_retry".to_string(),
            decision: "Accept".to_string(),
            payload: None,
        })
        .await;
    assert!(matches!(second, Err(RuntimeError::Io(_))));
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
    assert!(rendered.contains("[mcp_servers.gg.env]"));
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
