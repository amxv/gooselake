use super::*;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{
    map_bridge_error, merge_assistant_text_into_usage, resolve_claude_auth_paths,
    ClaudeConfigResolutionSource,
};
use crate::bridge::stdout_worker_lane_index;
use crate::paths::{
    sidecar_command_path_for_executable, sidecar_command_path_for_executable_with_workspace_roots,
    sidecar_command_path_from_executable, workspace_root_from_target_binary_path,
};
use runtime_core::{
    CreateSessionInput, ProviderApprovalResponseRequest, ProviderCloseSessionRequest,
    ProviderCreateSessionRequest, ProviderInterruptTurnRequest, ProviderKind, ProviderRegistry,
    ProviderResumeSessionRequest, ProviderSendTurnRequest, ProviderTurnStatus,
    ProviderWaitTurnRequest, RuntimeError, RuntimeProvider, RuntimeSessionManager, RuntimeStore,
    SendTurnInput,
};
use runtime_store_sqlite::{SqliteRuntimeStore, SqliteStoreConfig};
use serde_json::Value;

mod auth_tests;
mod bridge_path_tests;
mod contract_tests;

const FAKE_BRIDGE_SCRIPT: &str = r#"#!/usr/bin/env python3
import json
import os
import sys

MISSING_GG_MCP = "Missing ggMcpServer config for SDK mode session"

scenario = os.environ.get("FAKE_BRIDGE_SCENARIO", "normal").strip()
log_path = os.environ.get("FAKE_BRIDGE_REQUEST_LOG", "").strip()
env_log_path = os.environ.get("FAKE_BRIDGE_ENV_LOG", "").strip()
state = {
  "next_session": 1,
  "next_turn": 1,
  "send_calls": 0,
}

def log_request(payload):
  if not log_path:
    return
  with open(log_path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload, sort_keys=True))
    handle.write("\n")

def write_env_snapshot():
  if not env_log_path:
    return
  with open(env_log_path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps({
      "HOME": os.environ.get("HOME"),
      "CLAUDE_CONFIG_DIR": os.environ.get("CLAUDE_CONFIG_DIR"),
      "CLAUDE_CODE_OAUTH_TOKEN_PRESENT": bool(os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "").strip()),
    }, sort_keys=True))

write_env_snapshot()

def emit(payload):
  sys.stdout.write(json.dumps(payload))
  sys.stdout.write("\n")
  sys.stdout.flush()

def emit_ok(rpc_id, result):
  emit({
    "id": rpc_id,
    "result": result,
  })

def emit_error(rpc_id, code, message, details=None):
  emit({
    "id": rpc_id,
    "error": {
      "code": code,
      "message": message,
      "details": details,
    },
  })

def requires_gg_mcp(params):
  return scenario == "require_gg_mcp" and "ggMcpServer" not in params

for raw_line in sys.stdin:
  line = raw_line.strip()
  if not line:
    continue

  try:
    request = json.loads(line)
  except Exception:
    continue

  log_request(request)
  rpc_id = request.get("id", "")
  method = request.get("method", "")
  params = request.get("params", {})
  if not isinstance(params, dict):
    emit_error(rpc_id, "BAD_REQUEST", "params must be an object")
    continue

  if method == "bridge.ping":
    emit_ok(rpc_id, {"ok": True})
    continue

  if method == "session.create":
    if requires_gg_mcp(params):
      emit_error(
        rpc_id,
        "BAD_REQUEST",
        MISSING_GG_MCP,
        {"reason": "missing_gg_mcp_server"}
      )
      continue
    session_index = state["next_session"]
    state["next_session"] = session_index + 1
    session_id = f"bridge-session-{session_index}"
    emit_ok(
      rpc_id,
      {
        "sessionId": session_id,
        "providerSessionRef": f"provider-session-{session_index}",
        "claudeCanonicalSessionRef": f"canonical-session-{session_index}",
      },
    )
    continue

  if method == "session.resume":
    if requires_gg_mcp(params):
      emit_error(
        rpc_id,
        "BAD_REQUEST",
        MISSING_GG_MCP,
        {"reason": "missing_gg_mcp_server"}
      )
      continue
    session_index = state["next_session"]
    state["next_session"] = session_index + 1
    session_id = f"bridge-session-resume-{session_index}"
    provider_session_ref = params.get("providerSessionRef") or params.get("sessionId") or f"provider-resume-{session_index}"
    canonical_ref = params.get("claudeCanonicalSessionRef")
    emit_ok(
      rpc_id,
      {
        "sessionId": session_id,
        "providerSessionRef": provider_session_ref,
        "claudeCanonicalSessionRef": canonical_ref or f"canonical-resume-{session_index}",
      },
    )
    continue

  if method == "session.send":
    if scenario == "send_not_found_once" and state["send_calls"] == 0:
      state["send_calls"] = 1
      emit_error(
        rpc_id,
        "SESSION_NOT_FOUND",
        "bridge session was recycled",
        {"sessionId": params.get("sessionId")},
      )
      continue
    state["send_calls"] = state["send_calls"] + 1
    turn_index = state["next_turn"]
    state["next_turn"] = turn_index + 1
    emit_ok(
      rpc_id,
      {
        "turnId": f"bridge-turn-{turn_index}",
      },
    )
    continue

  if method == "session.interrupt":
    emit_ok(rpc_id, {"ok": True})
    continue

  if method == "session.approval.respond":
    if params.get("approvalId") == "missing-approval":
      emit_error(
        rpc_id,
        "APPROVAL_NOT_FOUND",
        "approval was not found",
        {"approvalId": "missing-approval"},
      )
      continue
    emit_ok(rpc_id, {"ok": True})
    continue

  if method == "session.wait":
    turn_id = params.get("turnId") or "unknown-turn"
    emit_ok(
      rpc_id,
      {
        "turnId": turn_id,
        "status": "completed",
        "usage": {"output_tokens": 1},
      },
    )
    continue

  if method == "session.close":
    emit_ok(rpc_id, {"ok": True})
    continue

  emit_error(
    rpc_id,
    "BAD_REQUEST",
    f"unsupported fake-bridge method: {method}",
  )
"#;

struct FakeClaudeBridgeHarness {
    _temp_dir: tempfile::TempDir,
    script_path: PathBuf,
    request_log_path: PathBuf,
    env_log_path: PathBuf,
    home_dir: PathBuf,
    config_dir: PathBuf,
    scenario: String,
}

impl FakeClaudeBridgeHarness {
    fn new(scenario: &str) -> Self {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let script_path = temp_dir.path().join("fake_claude_bridge.py");
        let request_log_path = temp_dir.path().join("bridge-requests.jsonl");
        let env_log_path = temp_dir.path().join("bridge-env.json");
        let home_dir = temp_dir.path().join("home");
        let config_dir = temp_dir.path().join("claude-config");
        let credentials_path = home_dir.join(".claude").join(".credentials.json");
        let config_path = config_dir.join(".claude.json");

        let mut script_file =
            std::fs::File::create(script_path.as_path()).expect("create fake bridge script");
        script_file
            .write_all(FAKE_BRIDGE_SCRIPT.as_bytes())
            .expect("write fake bridge script");
        std::fs::create_dir_all(
            credentials_path
                .parent()
                .expect("credentials parent should resolve"),
        )
        .expect("create fake bridge credentials dir");
        std::fs::create_dir_all(config_dir.as_path()).expect("create fake bridge config dir");
        std::fs::write(
            credentials_path.as_path(),
            r#"{"claudeAiOauth":{"accessToken":"fixture-token","refreshToken":"fixture-token"}}"#,
        )
        .expect("write fake bridge credentials fixture");
        std::fs::write(
            config_path.as_path(),
            r#"{"oauthAccount":{"emailAddress":"fixture@example.com"}}"#,
        )
        .expect("write fake bridge config fixture");

        Self {
            _temp_dir: temp_dir,
            script_path,
            request_log_path,
            env_log_path,
            home_dir,
            config_dir,
            scenario: scenario.to_string(),
        }
    }

    fn provider_with_bridge_env(
        &self,
        gg_mcp: ClaudeGgMcpConfig,
        extra_bridge_env: BTreeMap<String, String>,
    ) -> ClaudeProvider {
        let mut bridge_env = BTreeMap::new();
        bridge_env.insert("FAKE_BRIDGE_SCENARIO".to_string(), self.scenario.clone());
        bridge_env.insert(
            "FAKE_BRIDGE_REQUEST_LOG".to_string(),
            self.request_log_path.display().to_string(),
        );
        bridge_env.insert(
            "FAKE_BRIDGE_ENV_LOG".to_string(),
            self.env_log_path.display().to_string(),
        );
        bridge_env.insert("HOME".to_string(), self.home_dir.display().to_string());
        bridge_env.insert(
            "CLAUDE_CONFIG_DIR".to_string(),
            self.config_dir.display().to_string(),
        );
        for (key, value) in extra_bridge_env {
            bridge_env.insert(key, value);
        }

        ClaudeProvider::new(ClaudeProviderConfig {
            enabled: true,
            config_dir: self.config_dir.clone(),
            bridge_command: "python3".to_string(),
            bridge_args: vec![self.script_path.display().to_string()],
            max_bridges: 1,
            max_sessions_per_bridge: 8,
            request_timeout_ms: 2_000,
            default_wait_timeout_ms: 5_000,
            heartbeat_interval_ms: 120_000,
            heartbeat_failure_threshold: 3,
            gg_mcp,
            bridge_env,
        })
    }

    fn provider(&self, gg_mcp: ClaudeGgMcpConfig) -> ClaudeProvider {
        self.provider_with_bridge_env(gg_mcp, BTreeMap::new())
    }

    fn read_requests(&self) -> Vec<Value> {
        let content = match std::fs::read_to_string(self.request_log_path.as_path()) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
            Err(error) => panic!("read fake bridge request log: {error}"),
        };
        content
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect()
    }

    fn read_spawn_env(&self) -> Value {
        let content =
            std::fs::read_to_string(self.env_log_path.as_path()).expect("read fake bridge env log");
        serde_json::from_str(content.as_str()).expect("parse fake bridge env log")
    }
}

fn requests_for_method<'a>(requests: &'a [Value], method: &str) -> Vec<&'a Value> {
    requests
        .iter()
        .filter(|request| {
            request
                .get("method")
                .and_then(Value::as_str)
                .is_some_and(|value| value == method)
        })
        .collect()
}

fn expected_gg_mcp_config(
    server_name: &str,
    command: &str,
    args: &[&str],
    runtime_session_id: &str,
    enable_process_tools: bool,
    gateway_url: Option<&str>,
    gateway_token: Option<&str>,
) -> Value {
    let mut env = serde_json::Map::new();
    env.insert(
        "GG_MCP_ENABLE_PROCESS_TOOLS".to_string(),
        Value::String(if enable_process_tools {
            "1".to_string()
        } else {
            "0".to_string()
        }),
    );
    env.insert(
        "GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID".to_string(),
        Value::String("1".to_string()),
    );
    env.insert(
        "GG_MCP_CALLER_AGENT_ID".to_string(),
        Value::String(runtime_session_id.to_string()),
    );
    if let Some(url) = gateway_url {
        env.insert(
            "GG_MCP_GATEWAY_URL".to_string(),
            Value::String(url.to_string()),
        );
    }
    if let Some(token) = gateway_token {
        env.insert(
            "GG_MCP_GATEWAY_TOKEN".to_string(),
            Value::String(token.to_string()),
        );
    }
    if let Some(home) = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        env.insert(
            "HOME".to_string(),
            Value::String(home.display().to_string()),
        );
    }
    if let Some(cargo_home) = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        env.insert(
            "CARGO_HOME".to_string(),
            Value::String(cargo_home.display().to_string()),
        );
    }
    if let Some(rustup_home) = std::env::var_os("RUSTUP_HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        env.insert(
            "RUSTUP_HOME".to_string(),
            Value::String(rustup_home.display().to_string()),
        );
    }

    serde_json::json!({
        "serverName": server_name,
        "callerAgentId": runtime_session_id,
        "command": command,
        "args": args,
        "env": env,
    })
}

async fn wait_for_ready_session(manager: &Arc<RuntimeSessionManager>, session_id: &str) {
    for _ in 0..20 {
        let session = manager
            .get_session(session_id)
            .await
            .expect("runtime session should exist");
        if session.status == "ready" && session.active_turn_id.is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let session = manager
        .get_session(session_id)
        .await
        .expect("runtime session should exist");
    panic!(
        "session {session_id} did not become ready in time (status={}, active_turn_id={:?})",
        session.status, session.active_turn_id
    );
}
