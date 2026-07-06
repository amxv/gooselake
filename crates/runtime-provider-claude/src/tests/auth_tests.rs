use super::*;

#[tokio::test]
async fn auth_lifecycle_is_runtime_managed() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let bridge_home = temp_dir.path().join("bridge-home");
    let bridge_config_dir = temp_dir.path().join("bridge-config");
    std::fs::create_dir_all(bridge_home.as_path()).expect("create bridge home dir");
    std::fs::create_dir_all(bridge_config_dir.as_path()).expect("create bridge config dir");
    let mut bridge_env = BTreeMap::new();
    bridge_env.insert("HOME".to_string(), bridge_home.display().to_string());
    bridge_env.insert(
        "CLAUDE_CONFIG_DIR".to_string(),
        bridge_config_dir.display().to_string(),
    );
    let provider = ClaudeProvider::new(ClaudeProviderConfig {
        enabled: true,
        config_dir: temp_dir.path().join("claude"),
        bridge_command: "does-not-matter".to_string(),
        bridge_args: Vec::new(),
        max_bridges: 1,
        max_sessions_per_bridge: 1,
        request_timeout_ms: 100,
        default_wait_timeout_ms: 100,
        heartbeat_interval_ms: 10_000,
        heartbeat_failure_threshold: 3,
        gg_mcp: ClaudeGgMcpConfig::default(),
        bridge_env,
    });

    let initial = provider.auth_status().await.expect("auth status");
    assert!(!initial.authenticated);

    let with_key = provider
        .auth_set_api_key("sk-ant-test".to_string())
        .await
        .expect("set api key");
    assert!(with_key.authenticated);
    assert_eq!(with_key.mode.as_deref(), Some("api_key"));
    assert!(provider.api_key_path().exists());

    let with_auth_bundle = provider
        .auth_import_json(serde_json::json!({
            "credentials_json": {
                "claudeAiOauth": {
                    "accessToken": "abc",
                    "refreshToken": "abc"
                }
            },
            "config_json": {
                "projects": {}
            }
        }))
        .await
        .expect("import auth bundle");
    assert!(with_auth_bundle.authenticated);
    assert_eq!(with_auth_bundle.mode.as_deref(), Some("claude_code_oauth"));
    assert!(provider.claude_credentials_path().exists());
    assert!(provider.claude_config_path().exists());

    let logged_out = provider.auth_logout().await.expect("logout");
    assert!(!logged_out.authenticated);
    assert!(!provider.claude_credentials_path().exists());
    assert!(!provider.claude_config_path().exists());
    assert!(!provider.api_key_path().exists());
}

#[tokio::test]
async fn auth_status_uses_host_machine_bridge_oauth_by_default() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let canonical_home = temp_dir.path().join("canonical-home");
    let canonical_config = temp_dir.path().join("canonical-config");
    let canonical_credentials = canonical_home.join(".claude").join(".credentials.json");
    std::fs::create_dir_all(
        canonical_credentials
            .parent()
            .expect("canonical credentials parent"),
    )
    .expect("create canonical credentials dir");
    std::fs::create_dir_all(canonical_config.as_path()).expect("create canonical config dir");
    std::fs::write(
        canonical_credentials.as_path(),
        r#"{"claudeAiOauth":{"accessToken":"bridge-only","refreshToken":"bridge-only"}}"#,
    )
    .expect("write canonical credentials");
    std::fs::write(canonical_config.join(".claude.json"), r#"{"projects":{}}"#)
        .expect("write canonical config");

    let mut bridge_env = BTreeMap::new();
    bridge_env.insert("HOME".to_string(), canonical_home.display().to_string());
    bridge_env.insert(
        "CLAUDE_CONFIG_DIR".to_string(),
        canonical_config.display().to_string(),
    );

    let provider = ClaudeProvider::new(ClaudeProviderConfig {
        enabled: true,
        config_dir: temp_dir.path().join("runtime-claude-config"),
        bridge_command: "does-not-matter".to_string(),
        bridge_args: Vec::new(),
        max_bridges: 1,
        max_sessions_per_bridge: 1,
        request_timeout_ms: 100,
        default_wait_timeout_ms: 100,
        heartbeat_interval_ms: 10_000,
        heartbeat_failure_threshold: 3,
        gg_mcp: ClaudeGgMcpConfig::default(),
        bridge_env,
    });

    let status = provider.auth_status().await.expect("auth status");
    assert!(status.authenticated, "host-mode auth status should be true");
    assert_eq!(status.mode.as_deref(), Some("claude_code_oauth"));
    let detail = status.detail.unwrap_or_default();
    assert!(
        detail.contains("bridge_credentials_present=true"),
        "expected detail to surface bridge credential visibility"
    );
    assert!(
        detail.contains("auth_mode=host_machine"),
        "expected host-mode detail annotation"
    );
}

#[tokio::test]
async fn runtime_managed_mode_allows_explicit_host_bridge_overrides() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let canonical_home = temp_dir.path().join("canonical-home");
    let canonical_config = temp_dir.path().join("canonical-config");
    let canonical_credentials = canonical_home.join(".claude").join(".credentials.json");
    std::fs::create_dir_all(
        canonical_credentials
            .parent()
            .expect("canonical credentials parent"),
    )
    .expect("create canonical credentials dir");
    std::fs::create_dir_all(canonical_config.as_path()).expect("create canonical config dir");
    std::fs::write(
        canonical_credentials.as_path(),
        r#"{"claudeAiOauth":{"accessToken":"bridge-only","refreshToken":"bridge-only"}}"#,
    )
    .expect("write canonical credentials");
    std::fs::write(canonical_config.join(".claude.json"), r#"{"projects":{}}"#)
        .expect("write canonical config");

    let mut bridge_env = BTreeMap::new();
    bridge_env.insert("HOME".to_string(), canonical_home.display().to_string());
    bridge_env.insert(
        "CLAUDE_CONFIG_DIR".to_string(),
        canonical_config.display().to_string(),
    );
    bridge_env.insert(
        "GG_CLAUDE_AUTH_MODE".to_string(),
        "runtime_managed".to_string(),
    );

    let provider = ClaudeProvider::new(ClaudeProviderConfig {
        enabled: true,
        config_dir: temp_dir.path().join("runtime-claude-config"),
        bridge_command: "does-not-matter".to_string(),
        bridge_args: Vec::new(),
        max_bridges: 1,
        max_sessions_per_bridge: 1,
        request_timeout_ms: 100,
        default_wait_timeout_ms: 100,
        heartbeat_interval_ms: 10_000,
        heartbeat_failure_threshold: 3,
        gg_mcp: ClaudeGgMcpConfig::default(),
        bridge_env,
    });
    let status = provider.auth_status().await.expect("auth status");
    assert!(
        status.authenticated,
        "runtime-managed mode should accept explicit bridge HOME/CLAUDE_CONFIG_DIR overrides"
    );
    let detail = status.detail.unwrap_or_default();
    assert!(
        detail.contains("auth_mode=runtime_managed"),
        "expected runtime-managed mode detail annotation"
    );
    assert!(
        detail.contains("bridge_override_active=true"),
        "expected explicit override detail annotation"
    );
}

#[tokio::test]
async fn bridge_spawn_defaults_to_host_machine_home_and_config_resolution() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());

    provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-default-env".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create session");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-default-env".to_string(),
            reason: None,
        })
        .await
        .expect("close session");

    let spawn_env = harness.read_spawn_env();
    let expected_home_display = harness.home_dir.display().to_string();
    let expected_config_display = harness.config_dir.display().to_string();
    assert_eq!(
        spawn_env.get("HOME").and_then(Value::as_str),
        Some(expected_home_display.as_str())
    );
    assert_eq!(
        spawn_env.get("CLAUDE_CONFIG_DIR").and_then(Value::as_str),
        Some(expected_config_display.as_str())
    );
    assert_eq!(
        spawn_env
            .get("CLAUDE_CODE_OAUTH_TOKEN_PRESENT")
            .and_then(Value::as_bool),
        Some(false)
    );
}

#[tokio::test]
async fn bridge_spawn_allows_explicit_passthrough_home_and_config_overrides() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let passthrough_home = harness
        .config_dir
        .parent()
        .expect("provider dir")
        .join("passthrough-home");
    let passthrough_config = harness
        .config_dir
        .parent()
        .expect("provider dir")
        .join("passthrough-config");
    let passthrough_credentials = passthrough_home.join(".claude").join(".credentials.json");
    std::fs::create_dir_all(
        passthrough_credentials
            .parent()
            .expect("passthrough credentials parent"),
    )
    .expect("create passthrough credentials dir");
    std::fs::create_dir_all(passthrough_config.as_path()).expect("create passthrough config dir");
    std::fs::write(
            passthrough_credentials.as_path(),
            r#"{"claudeAiOauth":{"accessToken":"passthrough-token","refreshToken":"passthrough-token"}}"#,
        )
        .expect("write passthrough credentials fixture");
    std::fs::write(
        passthrough_config.join(".claude.json"),
        r#"{"oauthAccount":{"emailAddress":"passthrough@example.com"}}"#,
    )
    .expect("write passthrough config fixture");
    let mut bridge_env = BTreeMap::new();
    bridge_env.insert("HOME".to_string(), passthrough_home.display().to_string());
    bridge_env.insert(
        "CLAUDE_CONFIG_DIR".to_string(),
        passthrough_config.display().to_string(),
    );
    let provider = harness.provider_with_bridge_env(ClaudeGgMcpConfig::default(), bridge_env);

    provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-override-env".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create session");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-override-env".to_string(),
            reason: None,
        })
        .await
        .expect("close session");

    let spawn_env = harness.read_spawn_env();
    let passthrough_home_display = passthrough_home.display().to_string();
    let passthrough_config_display = passthrough_config.display().to_string();
    assert_eq!(
        spawn_env.get("HOME").and_then(Value::as_str),
        Some(passthrough_home_display.as_str())
    );
    assert_eq!(
        spawn_env.get("CLAUDE_CONFIG_DIR").and_then(Value::as_str),
        Some(passthrough_config_display.as_str())
    );
}

#[tokio::test]
async fn bridge_spawn_does_not_export_oauth_token_by_default() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());

    provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-oauth-env".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create session");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-oauth-env".to_string(),
            reason: None,
        })
        .await
        .expect("close session");

    let spawn_env = harness.read_spawn_env();
    assert_eq!(
        spawn_env
            .get("CLAUDE_CODE_OAUTH_TOKEN_PRESENT")
            .and_then(Value::as_bool),
        Some(false)
    );
}

#[tokio::test]
async fn bridge_spawn_exports_oauth_token_when_explicitly_forced() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let mut bridge_env = BTreeMap::new();
    bridge_env.insert(
        "GG_CLAUDE_BRIDGE_FORCE_OAUTH_TOKEN".to_string(),
        "1".to_string(),
    );
    let provider = harness.provider_with_bridge_env(ClaudeGgMcpConfig::default(), bridge_env);

    provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-forced-oauth-env".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("create session");
    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-forced-oauth-env".to_string(),
            reason: None,
        })
        .await
        .expect("close session");

    let spawn_env = harness.read_spawn_env();
    assert_eq!(
        spawn_env
            .get("CLAUDE_CODE_OAUTH_TOKEN_PRESENT")
            .and_then(Value::as_bool),
        Some(true)
    );
}

#[tokio::test]
async fn bridge_spawn_fails_fast_when_runtime_credentials_missing() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let credentials_path = harness.home_dir.join(".claude").join(".credentials.json");
    std::fs::remove_file(credentials_path.as_path()).expect("remove runtime credentials fixture");
    let provider = harness.provider(ClaudeGgMcpConfig::default());

    let result = provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-missing-credentials".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await;

    let error = result.expect_err("create session should fail when credentials are missing");
    let rendered = format!("{error}");
    assert!(
        rendered.contains("Claude OAuth credentials file missing"),
        "expected missing-runtime-credentials fail-fast message, got: {rendered}"
    );
    assert!(
        rendered.contains("/v1/providers/claude/auth/import-json"),
        "expected import-json route guidance, got: {rendered}"
    );
    assert!(
        rendered.contains("/v1/providers/claude/auth/import-file"),
        "expected import-file route guidance, got: {rendered}"
    );
}

#[tokio::test]
async fn bridge_spawn_fails_fast_when_runtime_config_is_malformed() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let config_path = harness.config_dir.join(".claude.json");
    std::fs::write(config_path.as_path(), "{not-valid-json")
        .expect("write malformed config fixture");
    let provider = harness.provider(ClaudeGgMcpConfig::default());

    let result = provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-malformed-config".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await;

    let error = result.expect_err("create session should fail on malformed config json");
    let rendered = format!("{error}");
    assert!(
        rendered.contains("is not valid JSON"),
        "expected malformed-runtime-config fail-fast message, got: {rendered}"
    );
}

#[tokio::test]
async fn bridge_spawn_explicit_api_key_override_bypasses_oauth_preflight() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let credentials_path = harness.home_dir.join(".claude").join(".credentials.json");
    let config_path = harness.config_dir.join(".claude.json");
    std::fs::remove_file(credentials_path.as_path()).expect("remove runtime credentials fixture");
    std::fs::write(config_path.as_path(), "{not-valid-json")
        .expect("write malformed config fixture");

    let mut bridge_env = BTreeMap::new();
    bridge_env.insert(
        "ANTHROPIC_API_KEY".to_string(),
        "sk-ant-test-bypass".to_string(),
    );
    let provider = harness.provider_with_bridge_env(ClaudeGgMcpConfig::default(), bridge_env);

    let session = provider
        .create_session(ProviderCreateSessionRequest {
            runtime_session_id: "sess-api-key-bypass".to_string(),
            model: None,
            cwd: None,
            permission_mode: None,
            metadata: None,
        })
        .await
        .expect("api key should bypass oauth preflight");
    assert_eq!(session.runtime_session_id, "sess-api-key-bypass");

    provider
        .close_session(ProviderCloseSessionRequest {
            runtime_session_id: "sess-api-key-bypass".to_string(),
            reason: None,
        })
        .await
        .expect("close session");
}
