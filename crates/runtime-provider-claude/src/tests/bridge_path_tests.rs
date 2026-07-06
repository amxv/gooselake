use super::*;

#[test]
fn claude_auth_paths_use_split_credentials_and_env_config_override() {
    let home_dir = PathBuf::from("/home/alice");
    let config_dir = PathBuf::from("/runtime/claude-config");
    let resolved = resolve_claude_auth_paths(Some(config_dir.clone()), Some(home_dir.clone()))
        .expect("resolved auth paths");

    assert_eq!(
        resolved.credentials_path,
        home_dir.join(".claude").join(".credentials.json")
    );
    assert_eq!(resolved.config_path, config_dir.join(".claude.json"));
    assert_eq!(resolved.config_dir, Some(config_dir));
    assert_eq!(
        resolved.config_source,
        ClaudeConfigResolutionSource::EnvOverride
    );
}

#[test]
fn claude_auth_paths_use_gg_fallback_when_no_override_exists() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let home_dir = temp_dir.path().join("home");
    let gg_claude = home_dir.join(".gg").join("claude");
    std::fs::create_dir_all(&gg_claude).expect("create gg claude dir");
    let resolved =
        resolve_claude_auth_paths(None, Some(home_dir.clone())).expect("resolved auth paths");

    assert_eq!(
        resolved.credentials_path,
        home_dir.join(".claude").join(".credentials.json")
    );
    assert_eq!(resolved.config_path, gg_claude.join(".claude.json"));
    assert_eq!(resolved.config_dir, Some(gg_claude));
    assert_eq!(
        resolved.config_source,
        ClaudeConfigResolutionSource::GgFallback
    );
}

#[test]
fn claude_auth_paths_fall_back_to_upstream_defaults() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let home_dir = temp_dir.path().join("home");
    std::fs::create_dir_all(&home_dir).expect("create home dir");
    let resolved =
        resolve_claude_auth_paths(None, Some(home_dir.clone())).expect("resolved auth paths");

    assert_eq!(
        resolved.credentials_path,
        home_dir.join(".claude").join(".credentials.json")
    );
    assert_eq!(resolved.config_path, home_dir.join(".claude.json"));
    assert_eq!(resolved.config_dir, None);
    assert_eq!(
        resolved.config_source,
        ClaudeConfigResolutionSource::UpstreamDefault
    );
}

#[test]
fn lane_index_is_stable_for_same_key() {
    let key = "session:test";
    let lane_count = 16;
    let first = stdout_worker_lane_index(key, lane_count);
    let second = stdout_worker_lane_index(key, lane_count);
    assert_eq!(first, second);
    assert!(first < lane_count);
}

#[test]
fn standalone_gg_mcp_server_command_path_is_branch_owned() {
    let executable = PathBuf::from("/opt/gg-runtime/bin/gg-runtime-server");
    let path = sidecar_command_path_from_executable(
        executable.as_path(),
        "gg-mcp-server",
        "gg-mcp-server",
    );
    assert_eq!(
        path,
        PathBuf::from("/opt/gg-runtime/sidecars/gg-mcp-server/gg-mcp-server")
    );
}

#[test]
fn standalone_claude_bridge_command_path_is_branch_owned() {
    let executable = PathBuf::from("/opt/gg-runtime/bin/gg-runtime-server");
    let path = sidecar_command_path_from_executable(
        executable.as_path(),
        "claude-bridge",
        "claude-bridge",
    );
    assert_eq!(
        path,
        PathBuf::from("/opt/gg-runtime/sidecars/claude-bridge/claude-bridge")
    );
}

#[test]
fn sidecar_paths_resolve_from_runtime_install_layout_not_source_tree() {
    let executable = PathBuf::from("/opt/gg-runtime/bin/gg-runtime-server");
    let claude_path = sidecar_command_path_for_executable_with_workspace_roots(
        executable.as_path(),
        "claude-bridge",
        "claude-bridge",
        "claude-bridge-dev",
        &[],
    );
    let gg_mcp_path = sidecar_command_path_for_executable_with_workspace_roots(
        executable.as_path(),
        "gg-mcp-server",
        "gg-mcp-server",
        "gg-mcp-server-dev",
        &[],
    );
    assert_eq!(
        claude_path,
        PathBuf::from("/opt/gg-runtime/sidecars/claude-bridge/claude-bridge")
    );
    assert_eq!(
        gg_mcp_path,
        PathBuf::from("/opt/gg-runtime/sidecars/gg-mcp-server/gg-mcp-server")
    );
    assert!(!claude_path.ends_with("src/main.ts"));
    assert!(!gg_mcp_path.to_string_lossy().contains("Cargo.toml"));
}

#[test]
fn workspace_root_is_detected_for_cargo_test_binary_paths() {
    let executable =
        PathBuf::from("/repo/worktree/target/debug/deps/runtime_provider_claude-abcdef");
    let workspace_root =
        workspace_root_from_target_binary_path(executable.as_path()).expect("workspace root");
    assert_eq!(workspace_root, PathBuf::from("/repo/worktree"));
}

#[test]
fn sidecar_paths_fallback_to_workspace_sidecars_for_target_binaries() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let repo_root = temp_dir.path();
    let target_bin = repo_root
        .join("target")
        .join("debug")
        .join("deps")
        .join("runtime-provider-claude-test-bin");
    std::fs::create_dir_all(target_bin.parent().expect("parent")).expect("create target dir");
    std::fs::write(target_bin.as_path(), b"").expect("write target bin placeholder");

    let claude_launcher = repo_root
        .join("sidecars")
        .join("claude-bridge")
        .join("bin")
        .join("claude-bridge-dev");
    let gg_mcp_launcher = repo_root
        .join("sidecars")
        .join("gg-mcp-server")
        .join("bin")
        .join("gg-mcp-server-dev");
    std::fs::create_dir_all(claude_launcher.parent().expect("claude parent"))
        .expect("create claude sidecar dir");
    std::fs::create_dir_all(gg_mcp_launcher.parent().expect("gg mcp parent"))
        .expect("create gg mcp sidecar dir");
    std::fs::write(claude_launcher.as_path(), b"").expect("write claude launcher placeholder");
    std::fs::write(gg_mcp_launcher.as_path(), b"").expect("write gg mcp launcher placeholder");

    let resolved_claude = sidecar_command_path_for_executable(
        target_bin.as_path(),
        "claude-bridge",
        "claude-bridge",
        "claude-bridge-dev",
    );
    let resolved_gg_mcp = sidecar_command_path_for_executable(
        target_bin.as_path(),
        "gg-mcp-server",
        "gg-mcp-server",
        "gg-mcp-server-dev",
    );

    assert_eq!(
        resolved_claude,
        repo_root
            .join("sidecars")
            .join("claude-bridge")
            .join("bin")
            .join("claude-bridge-dev")
    );
    assert_eq!(
        resolved_gg_mcp,
        repo_root
            .join("sidecars")
            .join("gg-mcp-server")
            .join("bin")
            .join("gg-mcp-server-dev")
    );
}

#[test]
fn sidecar_paths_fallback_to_workspace_roots_for_cargo_cache_binaries() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let repo_root = temp_dir.path().join("workspace");
    let cargo_cache_bin = temp_dir
        .path()
        .join("cargo-build")
        .join("debug")
        .join("deps")
        .join("runtime-server-test-bin");
    std::fs::create_dir_all(cargo_cache_bin.parent().expect("parent"))
        .expect("create cache bin dir");
    std::fs::write(cargo_cache_bin.as_path(), b"").expect("write cache bin placeholder");

    let claude_launcher = repo_root
        .join("sidecars")
        .join("claude-bridge")
        .join("bin")
        .join("claude-bridge-dev");
    std::fs::create_dir_all(claude_launcher.parent().expect("claude parent"))
        .expect("create claude launcher dir");
    std::fs::write(claude_launcher.as_path(), b"").expect("write claude launcher placeholder");

    let resolved_claude = sidecar_command_path_for_executable_with_workspace_roots(
        cargo_cache_bin.as_path(),
        "claude-bridge",
        "claude-bridge",
        "claude-bridge-dev",
        &[repo_root.clone()],
    );
    assert_eq!(resolved_claude, claude_launcher);
}

#[test]
fn map_bridge_error_preserves_not_found_and_protocol_categories() {
    let session_not_found = map_bridge_error(&serde_json::json!({
        "code": "SESSION_NOT_FOUND",
        "message": "session is gone",
    }));
    assert!(matches!(session_not_found, RuntimeError::NotFound(_)));

    let turn_not_found = map_bridge_error(&serde_json::json!({
        "code": "TURN_NOT_FOUND",
        "message": "turn is gone",
    }));
    assert!(matches!(turn_not_found, RuntimeError::NotFound(_)));

    let approval_not_found = map_bridge_error(&serde_json::json!({
        "code": "APPROVAL_NOT_FOUND",
        "message": "approval is gone",
    }));
    assert!(matches!(approval_not_found, RuntimeError::NotFound(_)));

    let protocol = map_bridge_error(&serde_json::json!({
        "code": "PROTOCOL_VIOLATION",
        "message": "bad payload",
    }));
    assert!(matches!(protocol, RuntimeError::ProtocolViolation(_)));

    let bad_request = map_bridge_error(&serde_json::json!({
        "code": "BAD_REQUEST",
        "message": "invalid field",
    }));
    assert!(matches!(bad_request, RuntimeError::InvalidState(_)));
}

#[test]
fn merge_assistant_text_into_usage_sets_last_message() {
    let merged = merge_assistant_text_into_usage(
        Some(serde_json::json!({
            "inputTokens": 10,
            "outputTokens": 20
        })),
        Some("hello from claude".to_string()),
    )
    .expect("merged usage");
    assert_eq!(merged["last_message"], "hello from claude");
    assert_eq!(merged["assistant_text"], "hello from claude");
}

#[test]
fn merge_assistant_text_into_usage_creates_usage_when_missing() {
    let merged = merge_assistant_text_into_usage(None, Some("terminal text only".to_string()))
        .expect("merged usage");
    assert_eq!(merged["last_message"], "terminal text only");
    assert_eq!(merged["assistant_text"], "terminal text only");
}
