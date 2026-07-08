use super::*;

#[tokio::test]
async fn claude_model_catalog_exposes_family_reasoning_levels() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    let models = provider.list_models().await.expect("list models");
    let full_levels = vec!["low", "medium", "high", "extra-high", "max"];

    for model_id in ["claude-sonnet-5", "claude-opus-4-8", "claude-fable-5"] {
        let model = models
            .iter()
            .find(|model| model.id == model_id)
            .expect("expected claude model");
        assert_eq!(model.reasoning_levels, full_levels);
    }

    let haiku = models
        .iter()
        .find(|model| model.id == "claude-haiku-4-5")
        .expect("expected haiku model");
    assert!(haiku.reasoning_levels.is_empty());
}

#[tokio::test]
async fn real_adapter_contract_covers_create_resume_send_interrupt_approval_wait_and_close() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());

    let created = provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-create".to_string(),
            model: Some("claude-sonnet-5".to_string()),
            cwd: Some("/tmp/project".to_string()),
            permission_mode: Some("default".to_string()),
            metadata: None,
        })
        .await
        .expect("create session");
    assert_eq!(created.runtime_session_id, "sess-create");
    assert_eq!(created.provider_session_ref, "provider-session-1");

    let ack = provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess-create".to_string(),
            turn_id: "runtime-turn-1".to_string(),
            input: vec![serde_json::json!({
                "type": "text",
                "text": "hello"
            })],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send turn");
    assert_eq!(ack.runtime_session_id, "sess-create");
    assert_eq!(ack.turn_id, "runtime-turn-1");

    provider
        .interrupt_turn(ProviderInterruptTurnRequest {
            runtime_session_id: "sess-create".to_string(),
            turn_id: ack.turn_id.clone(),
        })
        .await
        .expect("interrupt turn");

    provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "sess-create".to_string(),
            turn_id: ack.turn_id.clone(),
            approval_id: "approval-1".to_string(),
            decision: "accept".to_string(),
            payload: Some(serde_json::json!({
                "type": "text",
                "text": "updated"
            })),
        })
        .await
        .expect("respond approval");

    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess-create".to_string(),
            turn_id: ack.turn_id.clone(),
            timeout_ms: Some(500),
        })
        .await
        .expect("wait for turn");
    assert_eq!(result.runtime_session_id, "sess-create");
    assert_eq!(result.turn_id, "runtime-turn-1");
    assert_eq!(result.status, ProviderTurnStatus::Completed);

    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-create".to_string(),
            reason: Some("contract_complete".to_string()),
        })
        .await
        .expect("close session");

    let resumed = provider
        .resume_session(ProviderResumeSessionRequest {
            runtime_session_id: "sess-resume".to_string(),
            provider_session_ref: "provider-session-upstream".to_string(),
            canonical_provider_session_ref: Some("canonical-upstream".to_string()),
            cwd: Some("/tmp/resumed".to_string()),
            metadata: None,
        })
        .await
        .expect("resume session");
    assert_eq!(resumed.runtime_session_id, "sess-resume");
    assert_eq!(resumed.provider_session_ref, "provider-session-upstream");
    assert_eq!(
        resumed.canonical_provider_session_ref.as_deref(),
        Some("canonical-upstream")
    );

    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-resume".to_string(),
            reason: Some("contract_complete".to_string()),
        })
        .await
        .expect("close resumed session");

    let requests = harness.read_requests();
    assert_eq!(requests_for_method(&requests, "session.create").len(), 1);
    assert_eq!(requests_for_method(&requests, "session.resume").len(), 1);
    assert_eq!(requests_for_method(&requests, "session.send").len(), 1);
    assert_eq!(requests_for_method(&requests, "session.interrupt").len(), 1);
    assert_eq!(
        requests_for_method(&requests, "session.approval.respond").len(),
        1
    );
    assert_eq!(requests_for_method(&requests, "session.wait").len(), 1);
    assert_eq!(requests_for_method(&requests, "session.close").len(), 2);
}

#[tokio::test]
async fn create_and_resume_include_expected_gg_mcp_server_shape() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig {
        enabled: true,
        server_name: "gg".to_string(),
        command: "gg-mcp-server".to_string(),
        args: vec!["--stdio".to_string()],
        enable_process_tools: true,
        gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
        gateway_token: Some("bridge-token".to_string()),
    });

    provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-create".to_string(),
            model: None,
            cwd: Some("/tmp/create".to_string()),
            permission_mode: Some("default".to_string()),
            metadata: None,
        })
        .await
        .expect("create session");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-create".to_string(),
            reason: Some("done".to_string()),
        })
        .await
        .expect("close create session");

    provider
        .resume_session(ProviderResumeSessionRequest {
            runtime_session_id: "sess-resume".to_string(),
            provider_session_ref: "provider-resume".to_string(),
            canonical_provider_session_ref: Some("canonical-resume".to_string()),
            cwd: Some("/tmp/resume".to_string()),
            metadata: None,
        })
        .await
        .expect("resume session");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-resume".to_string(),
            reason: Some("done".to_string()),
        })
        .await
        .expect("close resume session");

    let requests = harness.read_requests();
    let create = requests_for_method(&requests, "session.create");
    let resume = requests_for_method(&requests, "session.resume");
    assert_eq!(create.len(), 1);
    assert_eq!(resume.len(), 1);

    let create_gg_mcp = create[0]
        .get("params")
        .and_then(|params| params.get("ggMcpServer"))
        .expect("create request should include ggMcpServer");
    assert_eq!(
        create_gg_mcp,
        &expected_gg_mcp_config(
            "gg",
            "gg-mcp-server",
            &["--stdio"],
            "sess-create",
            true,
            Some("http://127.0.0.1:8787/v1/mcp"),
            Some("bridge-token"),
        )
    );

    let resume_gg_mcp = resume[0]
        .get("params")
        .and_then(|params| params.get("ggMcpServer"))
        .expect("resume request should include ggMcpServer");
    assert_eq!(
        resume_gg_mcp,
        &expected_gg_mcp_config(
            "gg",
            "gg-mcp-server",
            &["--stdio"],
            "sess-resume",
            true,
            Some("http://127.0.0.1:8787/v1/mcp"),
            Some("bridge-token"),
        )
    );
}

#[test]
fn gg_mcp_server_config_can_disable_process_tools_without_disabling_team_tools() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig {
        enabled: true,
        server_name: "gg".to_string(),
        command: "gg-mcp-server".to_string(),
        args: vec!["--stdio".to_string()],
        enable_process_tools: false,
        gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
        gateway_token: Some("bridge-token".to_string()),
    });

    let server = provider.build_gg_mcp_server_session_config("sess-team-only");
    assert_eq!(server["serverName"].as_str(), Some("gg"));
    assert_eq!(server["callerAgentId"].as_str(), Some("sess-team-only"));
    assert_eq!(
        server["env"]["GG_MCP_ENABLE_PROCESS_TOOLS"].as_str(),
        Some("0")
    );
    assert_eq!(
        server["env"]["GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID"].as_str(),
        Some("1")
    );
    assert_eq!(
        server["env"]["GG_MCP_GATEWAY_URL"].as_str(),
        Some("http://127.0.0.1:8787/v1/mcp")
    );
}

#[tokio::test]
async fn create_and_resume_retry_with_gg_mcp_when_bridge_requires_it() {
    let harness = FakeClaudeBridgeHarness::new("require_gg_mcp");
    let provider = harness.provider(ClaudeGgMcpConfig {
        enabled: false,
        server_name: "gg".to_string(),
        command: "gg-mcp-server".to_string(),
        args: vec!["--stdio".to_string()],
        enable_process_tools: false,
        gateway_url: Some("http://127.0.0.1:8787/v1/mcp".to_string()),
        gateway_token: Some("bridge-token".to_string()),
    });

    provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-create".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create with compatibility retry");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-create".to_string(),
            reason: None,
        })
        .await
        .expect("close create");

    provider
        .resume_session(ProviderResumeSessionRequest {
            runtime_session_id: "sess-resume".to_string(),
            provider_session_ref: "provider-resume".to_string(),
            canonical_provider_session_ref: Some("canonical-resume".to_string()),
            cwd: None,
            metadata: None,
        })
        .await
        .expect("resume with compatibility retry");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-resume".to_string(),
            reason: None,
        })
        .await
        .expect("close resume");

    let requests = harness.read_requests();
    let create = requests_for_method(&requests, "session.create");
    let resume = requests_for_method(&requests, "session.resume");
    assert_eq!(create.len(), 2, "create should be retried once");
    assert_eq!(resume.len(), 2, "resume should be retried once");

    assert!(
        create[0]
            .get("params")
            .and_then(|params| params.get("ggMcpServer"))
            .is_none(),
        "initial create call should omit ggMcpServer when disabled",
    );
    assert!(
        create[1]
            .get("params")
            .and_then(|params| params.get("ggMcpServer"))
            .is_some(),
        "retry create call should include ggMcpServer",
    );

    assert!(
        resume[0]
            .get("params")
            .and_then(|params| params.get("ggMcpServer"))
            .is_none(),
        "initial resume call should omit ggMcpServer when disabled",
    );
    assert!(
        resume[1]
            .get("params")
            .and_then(|params| params.get("ggMcpServer"))
            .is_some(),
        "retry resume call should include ggMcpServer",
    );
}

#[tokio::test]
async fn runtime_manager_recovers_send_turn_after_bridge_session_not_found() {
    let harness = FakeClaudeBridgeHarness::new("send_not_found_once");
    let provider = Arc::new(harness.provider(ClaudeGgMcpConfig::default()));

    let mut registry = ProviderRegistry::new();
    registry.register(provider).expect("register provider");

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize sqlite store");

    let manager = Arc::new(
        RuntimeSessionManager::new(store, Arc::new(registry), 256)
            .expect("construct runtime session manager"),
    );

    let session = manager
        .create_session(CreateSessionInput {
            provider: ProviderKind::Claude,
            model: Some("claude-sonnet-5".to_string()),
            cwd: Some("/tmp/runtime".to_string()),
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create runtime session");

    let send = manager
        .send_turn(
            session.id.as_str(),
            SendTurnInput {
                input: vec![serde_json::json!({
                    "type": "text",
                    "text": "recover this turn"
                })],
                expected_turn_id: None,
                permission_mode: None,
            },
        )
        .await
        .expect("send turn should recover after session_not_found");
    assert_eq!(send.status, "in_progress");

    wait_for_ready_session(&manager, session.id.as_str()).await;

    let requests = harness.read_requests();
    assert_eq!(requests_for_method(&requests, "session.send").len(), 2);
    assert_eq!(requests_for_method(&requests, "session.resume").len(), 1);
}

#[tokio::test]
#[ignore = "requires local Claude auth sources at ~/.claude/.credentials.json and ~/.gg/claude/.claude.json (or ~/.claude.json fallback)"]
async fn ignored_real_claude_smoke_with_standalone_bridge() {
    let home_dir = std::env::var_os("HOME")
        .map(PathBuf::from)
        .expect("HOME must be set for Claude smoke");
    let credentials_source_path = std::env::var("GG_CLAUDE_SMOKE_CREDENTIALS_SOURCE")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir.join(".claude").join(".credentials.json"));
    let config_source_path = std::env::var("GG_CLAUDE_SMOKE_CONFIG_SOURCE")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let gg_claude_config = home_dir.join(".gg").join("claude").join(".claude.json");
            if gg_claude_config.exists() {
                gg_claude_config
            } else {
                home_dir.join(".claude.json")
            }
        });
    assert!(
        credentials_source_path.exists(),
        "Claude smoke credentials source path must exist: {}",
        credentials_source_path.display()
    );
    assert!(
        config_source_path.exists(),
        "Claude smoke config source path must exist: {}",
        config_source_path.display()
    );

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let config_dir = temp_dir.path().join("claude-config");
    std::fs::create_dir_all(config_dir.as_path()).expect("create smoke config dir");
    let runtime_home = temp_dir.path().join("home");
    let runtime_credentials_path = runtime_home.join(".claude").join(".credentials.json");
    std::fs::create_dir_all(
        runtime_credentials_path
            .parent()
            .expect("runtime credentials parent"),
    )
    .expect("create runtime credentials dir");
    std::fs::copy(
        credentials_source_path.as_path(),
        runtime_credentials_path.as_path(),
    )
    .expect("stage Claude credentials into runtime-managed home");
    std::fs::copy(
        config_source_path.as_path(),
        config_dir.join(".claude.json"),
    )
    .expect("stage Claude config into runtime-managed config dir");

    let provider = ClaudeProvider::new(ClaudeProviderConfig {
        enabled: true,
        config_dir,
        bridge_command: standalone_claude_bridge_command_path()
            .display()
            .to_string(),
        bridge_args: Vec::new(),
        max_bridges: 1,
        max_sessions_per_bridge: 1,
        request_timeout_ms: 30_000,
        default_wait_timeout_ms: 120_000,
        heartbeat_interval_ms: 30_000,
        heartbeat_failure_threshold: 3,
        gg_mcp: ClaudeGgMcpConfig {
            enabled: false,
            ..ClaudeGgMcpConfig::default()
        },
        bridge_env: BTreeMap::new(),
    });

    let created = provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "smoke-claude-runtime-session".to_string(),
            model: Some("claude-sonnet-5".to_string()),
            cwd: Some(
                std::env::current_dir()
                    .expect("current dir")
                    .display()
                    .to_string(),
            ),
            permission_mode: Some("default".to_string()),
            metadata: None,
        })
        .await
        .expect("create real Claude session");

    let ack = provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: created.runtime_session_id.clone(),
            turn_id: "smoke-turn-1".to_string(),
            input: vec![serde_json::json!({
                "type": "text",
                "text": "Return exactly: smoke_ok"
            })],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send real Claude turn");

    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: created.runtime_session_id.clone(),
            turn_id: ack.turn_id,
            timeout_ms: Some(120_000),
        })
        .await
        .expect("wait for real Claude turn");
    assert_eq!(result.status, ProviderTurnStatus::Completed);

    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: created.runtime_session_id,
            reason: Some("smoke_complete".to_string()),
        })
        .await
        .expect("close real Claude smoke session");
}
