use std::path::PathBuf;
use std::time::Duration;

use runtime_core::{
    provider_harness_text, ApprovalDecision, ProviderApprovalResponseRequest,
    ProviderCompactSessionOutcome, ProviderCompactSessionRequest,
    ProviderCreateSessionPolicyRequest, ProviderHardForkEditRerunRequest,
    ProviderInterruptTurnRequest, ProviderKind, ProviderResumeSessionPolicyRequest,
    ProviderRuntimeEvent, ProviderSendTurnRequest, ProviderSessionPreferences,
    ProviderSkillDiscoveryRequest, ProviderThinkingEffort, ProviderTurnStatus,
    ProviderWaitTurnRequest, ProviderWorkspaceRebindRequest, RuntimeError, RuntimeProvider,
};
use serde_json::{json, Value};

use crate::mcp_config::format_codex_gg_mcp_config;
use crate::protocol::{
    apply_thread_permission_mode, apply_turn_permission_mode, build_native_input,
};
use crate::{copy_codex_auth_file, CodexGgMcpConfig, CodexProvider, CodexProviderConfig};

const FAKE_APP_SERVER: &str = r#"
import json
import sys

log_path = sys.argv[1]
state_path = sys.argv[2]
pending_approvals = {}
pending_interrupts = {}
fail_next_rollback = False

try:
    with open(state_path, "r", encoding="utf-8") as handle:
        state = json.load(handle)
except (FileNotFoundError, json.JSONDecodeError):
    state = {"turn_counter": 0, "threads": {}}

def save_state():
    with open(state_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle)

def ensure_thread(thread_id, cwd="/workspace/fake", model="gpt-6-luna"):
    return state["threads"].setdefault(thread_id, {
        "id": thread_id,
        "cwd": cwd,
        "model": model,
        "turns": []
    })

def find_turn(thread_id, turn_id):
    thread = ensure_thread(thread_id)
    for turn in thread["turns"]:
        if turn.get("id") == turn_id:
            return turn
    return None

def emit(value):
    sys.stdout.write(json.dumps(value) + "\n")
    sys.stdout.flush()

def log(value):
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(value) + "\n")

def finish(thread_id, turn_id, status="completed"):
    turn = find_turn(thread_id, turn_id)
    if turn is not None:
        turn["status"] = status
        turn["error"] = None
        turn["tokenUsage"] = {
            "total": {"totalTokens": 25},
            "modelContextWindow": 100
        }
        save_state()
    emit({
        "method": "thread/tokenUsage/updated",
        "params": {
            "threadId": thread_id,
            "turnId": turn_id,
            "tokenUsage": {
                "total": {
                    "totalTokens": 25,
                    "inputTokens": 15,
                    "cachedInputTokens": 0,
                    "cacheWriteInputTokens": 0,
                    "outputTokens": 10,
                    "reasoningOutputTokens": 0
                },
                "last": {
                    "totalTokens": 25,
                    "inputTokens": 15,
                    "cachedInputTokens": 0,
                    "cacheWriteInputTokens": 0,
                    "outputTokens": 10,
                    "reasoningOutputTokens": 0
                },
                "modelContextWindow": 100
            }
        }
    })
    if status == "completed":
        emit({
            "method": "item/completed",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "item": {
                    "type": "agentMessage",
                    "id": "agent-message-" + turn_id,
                    "text": "fake complete",
                    "phase": None,
                    "memoryCitation": None,
                    "delivery": None,
                    "questions": None
                }
            }
        })
    emit({
        "method": "turn/completed",
        "params": {
            "threadId": thread_id,
            "turn": {
                "id": turn_id,
                "items": [],
                "status": status,
                "error": None
            }
        }
    })

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    message = json.loads(raw)
    log(message)
    method = message.get("method")
    request_id = message.get("id")

    if method is None:
        key = str(request_id)
        pending = pending_approvals.pop(key, None)
        if pending is not None:
            finish(pending[0], pending[1])
        continue

    params = message.get("params") or {}
    if method == "initialize":
        emit({"id": request_id, "result": {"userAgent": "fake-codex/1"}})
    elif method == "initialized":
        continue
    elif method == "thread/start":
        cwd = params.get("cwd", "/workspace/fake")
        model = params.get("model", "gpt-6-luna")
        ensure_thread("thread-native-1", cwd, model)
        save_state()
        emit({
            "id": request_id,
            "result": {
                "thread": {
                    "id": "thread-native-1",
                    "cwd": cwd,
                    "model": model
                },
                "cwd": cwd
            }
        })
    elif method == "thread/resume":
        if fail_next_rollback:
            fail_next_rollback = False
            emit({
                "id": request_id,
                "error": {"code": -32002, "message": "fake rollback failure"}
            })
            continue
        cwd = params.get("cwd", "/workspace/fake")
        if "reject-destination" in cwd:
            emit({
                "id": request_id,
                "error": {"code": -32001, "message": "fake destination rejection"}
            })
            continue
        if "unrecoverable-destination" in cwd:
            fail_next_rollback = True
            emit({
                "id": request_id,
                "error": {"code": -32001, "message": "fake unrecoverable destination rejection"}
            })
            continue
        model = params.get("model", "gpt-6-luna")
        thread = ensure_thread(params["threadId"], cwd, model)
        thread["cwd"] = cwd
        thread["model"] = model
        save_state()
        response = {
            "id": request_id,
            "result": {
                "thread": {
                    "id": params["threadId"],
                    "cwd": cwd,
                    "model": model
                },
                "cwd": cwd
            }
        }
        if "missing-evidence" in cwd:
            del response["result"]["cwd"]
        emit(response)
    elif method == "thread/unsubscribe":
        emit({"id": request_id, "result": {"status": "unsubscribed"}})
    elif method == "thread/read":
        thread = ensure_thread(params["threadId"])
        emit({"id": request_id, "result": {"thread": thread}})
    elif method == "turn/start":
        state["turn_counter"] += 1
        turn_counter = state["turn_counter"]
        turn_id = "turn-native-" + str(turn_counter)
        thread_id = params["threadId"]
        thread = ensure_thread(thread_id)
        thread["turns"].append({
            "id": turn_id,
            "items": [],
            "status": "inProgress",
            "error": None
        })
        save_state()
        text = " ".join(
            item.get("text", "")
            for item in params.get("input", [])
            if isinstance(item, dict)
        )
        response = {
            "id": request_id,
            "result": {
                "turn": {
                    "id": turn_id,
                    "items": [],
                    "status": "inProgress",
                    "error": None
                }
            }
        }
        if "ambiguous-start" in text:
            continue
        elif "model-capacity" in text:
            thread["turns"] = [turn for turn in thread["turns"] if turn.get("id") != turn_id]
            save_state()
            emit({
                "id": request_id,
                "error": {"code": -32000, "message": "Selected model is at capacity."}
            })
        elif "malformed-notification" in text:
            emit(response)
            sys.stdout.write("not-json\n")
            sys.stdout.flush()
        elif "server-close" in text:
            emit(response)
            sys.exit(0)
        elif "read-reconcile" in text:
            turn = find_turn(thread_id, turn_id)
            turn["status"] = "completed"
            turn["tokenUsage"] = {
                "total": {"totalTokens": 25},
                "modelContextWindow": 100
            }
            save_state()
            emit(response)
        elif "out-of-order" in text:
            finish(thread_id, turn_id)
            emit(response)
        elif "duplicate-terminal" in text:
            emit(response)
            finish(thread_id, turn_id)
            finish(thread_id, turn_id)
        elif "permission-approval" in text:
            emit(response)
            rpc_id = "permission-rpc-" + str(turn_counter)
            pending_approvals[rpc_id] = (thread_id, turn_id)
            emit({
                "id": rpc_id,
                "method": "item/permissions/requestApproval",
                "params": {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": "permission-item-" + str(turn_counter),
                    "approvalId": "permission-native-" + str(turn_counter),
                    "cwd": "/tmp/fake",
                    "permissions": {"network": {"enabled": True}},
                    "reason": "fake permission approval",
                    "startedAtMs": 1
                }
            })
        elif "approval" in text:
            emit(response)
            rpc_id = "approval-rpc-" + str(turn_counter)
            pending_approvals[rpc_id] = (thread_id, turn_id)
            emit({
                "id": rpc_id,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": "command-item-" + str(turn_counter),
                    "approvalId": "approval-native-" + str(turn_counter),
                    "reason": "fake approval",
                    "command": "echo fake"
                }
            })
        elif "interrupt" in text:
            emit(response)
            pending_interrupts[turn_id] = thread_id
        else:
            emit(response)
            finish(thread_id, turn_id)
    elif method == "turn/interrupt":
        emit({"id": request_id, "result": {}})
        turn_id = params["turnId"]
        thread_id = pending_interrupts.pop(turn_id, params["threadId"])
        finish(thread_id, turn_id, "interrupted")
    elif method == "thread/compact/start":
        emit({"id": request_id, "result": {}})
        emit({
            "method": "thread/compacted",
            "params": {"threadId": params["threadId"]}
        })
    elif method == "thread/fork":
        source = ensure_thread(params["threadId"])
        child = {
            "id": "thread-fork-1",
            "cwd": params.get("cwd", source.get("cwd", "/workspace/fake")),
            "model": params.get("model", source.get("model", "gpt-6-luna")),
            "turns": list(source.get("turns", []))
        }
        state["threads"]["thread-fork-1"] = child
        save_state()
        emit({
            "id": request_id,
            "result": {
                "thread": {
                    "id": "thread-fork-1",
                    "cwd": child["cwd"],
                    "model": child["model"]
                }
            }
        })
    elif method == "thread/revert":
        thread = ensure_thread(params["threadId"])
        before = params["beforeTurnId"]
        ids = [turn.get("id") for turn in thread["turns"]]
        if before in ids:
            thread["turns"] = thread["turns"][:ids.index(before)]
            save_state()
        emit({"id": request_id, "result": {"thread": {"id": params["threadId"]}}})
    elif method == "skills/list":
        cwd = (params.get("cwds") or ["/workspace/fake"])[0]
        emit({
            "id": request_id,
            "result": {
                "data": [{
                    "cwd": cwd,
                    "skills": [{
                        "name": "sweep",
                        "path": "/skills/sweep/SKILL.md",
                        "interface": {
                            "displayName": "Sweep",
                            "shortDescription": "Inspect a codebase deeply",
                            "argumentHint": "<repo>"
                        }
                    }, {
                        "name": "disabled-skill",
                        "description": "must not surface",
                        "path": "/skills/disabled/SKILL.md",
                        "enabled": False
                    }],
                    "errors": []
                }]
            }
        })
    else:
        if request_id is not None:
            emit({"id": request_id, "error": {"code": -32601, "message": "unknown method " + method}})
"#;

fn fake_provider(temp: &tempfile::TempDir, gg_mcp_enabled: bool) -> (CodexProvider, PathBuf) {
    fake_provider_with_timeout(temp, gg_mcp_enabled, 10_000)
}

fn fake_provider_with_timeout(
    temp: &tempfile::TempDir,
    gg_mcp_enabled: bool,
    request_timeout_ms: u64,
) -> (CodexProvider, PathBuf) {
    let script_path = temp.path().join("fake_codex_app_server.py");
    let log_path = temp.path().join("app-server.log");
    let state_path = temp.path().join("app-server-state.json");
    std::fs::write(&script_path, FAKE_APP_SERVER).expect("write fake app-server");
    let provider = CodexProvider::new(CodexProviderConfig {
        enabled: true,
        home_dir: temp.path().join("codex-home"),
        command: "python3".to_string(),
        app_server_args: vec![
            "-u".to_string(),
            script_path.display().to_string(),
            log_path.display().to_string(),
            state_path.display().to_string(),
        ],
        request_timeout_ms,
        max_transports: 1,
        max_sessions_per_transport: 2,
        gg_mcp: CodexGgMcpConfig {
            enabled: gg_mcp_enabled,
            command: "/bin/true".to_string(),
            ..CodexGgMcpConfig::default()
        },
    });
    (provider, log_path)
}

fn read_log(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect()
}

mod failure_contracts;
mod live_acceptance;

fn legacy_create(runtime_session_id: &str) -> ProviderCreateSessionPolicyRequest {
    ProviderCreateSessionPolicyRequest::legacy_compatible(
        runtime_session_id.to_string(),
        Some("gpt-6-luna".to_string()),
        Some("/workspace/fake".to_string()),
        Some("workspace_write".to_string()),
        None,
    )
    .expect("legacy create policy")
}

#[test]
fn approval_decision_normalization_matches_core_contract() {
    assert_eq!(
        ApprovalDecision::parse("Accept").expect("mixed-case accept"),
        ApprovalDecision::Accept
    );
    assert!(ApprovalDecision::parse("maybe").is_err());
}

#[test]
fn structured_input_is_native_and_unknown_shapes_are_never_flattened() {
    let input = build_native_input(&[
        json!({"type":"text","text":"hello"}),
        json!({"type":"local_image","path":"/tmp/image.png"}),
        json!({"type":"skill","name":"sweep","path":"/skills/sweep/SKILL.md"}),
    ])
    .expect("native input");
    assert_eq!(input[0], json!({"type":"text","text":"hello"}));
    assert_eq!(
        input[1],
        json!({"type":"localImage","path":"/tmp/image.png"})
    );
    assert_eq!(
        input[2],
        json!({"type":"skill","name":"sweep","path":"/skills/sweep/SKILL.md"})
    );

    let error = build_native_input(&[json!({"type":"mystery","value":42})])
        .expect_err("unknown shape must fail rather than flatten");
    assert!(matches!(error, RuntimeError::ProtocolViolation(_)));
    assert!(error.to_string().contains("refusing to flatten"));
}

#[test]
fn provider_new_absolutizes_relative_home_dir() {
    let provider = CodexProvider::new(CodexProviderConfig {
        enabled: true,
        home_dir: PathBuf::from("tmp/relative-codex-home"),
        command: "codex".to_string(),
        app_server_args: vec!["app-server".to_string()],
        request_timeout_ms: 2_000,
        max_transports: 1,
        max_sessions_per_transport: 1,
        gg_mcp: CodexGgMcpConfig::default(),
    });
    assert!(provider.inner.config.home_dir.is_absolute());
}

#[tokio::test]
async fn fake_app_server_proves_create_structured_turn_mapping_context_and_harness() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, log_path) = fake_provider(&temp, true);
    let mut create = legacy_create("sess_core");
    create.launch_policy.system_prompt = Some("custom system instruction".to_string());
    create.current_preferences = ProviderSessionPreferences {
        thinking_effort: Some(ProviderThinkingEffort::High),
    };

    let created = provider
        .create_session_with_policy(create)
        .await
        .expect("create session");
    assert_eq!(created.provider_session_ref, "thread-native-1");
    assert_eq!(
        created.canonical_provider_session_ref.as_deref(),
        Some("thread-native-1")
    );

    let session_config = temp
        .path()
        .join("codex-home/runtime-sessions/sess_core/config.toml");
    let session_config = std::fs::read_to_string(session_config).expect("session config");
    assert!(session_config.contains("GG_MCP_CALLER_AGENT_ID = \"sess_core\""));

    let ack = provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_core".to_string(),
            turn_id: "logical-turn-1".to_string(),
            input: vec![
                json!({"type":"text","text":"hello native protocol"}),
                json!({"type":"localImage","path":"/tmp/fake-image.png"}),
                json!({"type":"skill","name":"sweep","path":"/skills/sweep/SKILL.md"}),
            ],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send turn");
    assert_eq!(
        ack.provider_native_turn_id.as_deref(),
        Some("turn-native-1")
    );

    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_core".to_string(),
            turn_id: "logical-turn-1".to_string(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("wait turn");
    assert_eq!(result.status, ProviderTurnStatus::Completed);
    assert_eq!(result.usage.as_ref().unwrap()["modelContextWindow"], 100);
    assert_eq!(
        result.usage.as_ref().unwrap()["last_message"],
        "fake complete"
    );
    assert_eq!(
        result.usage.as_ref().unwrap()["assistant_text"],
        "fake complete"
    );

    let context = provider
        .observe_context_limit("sess_core")
        .await
        .expect("context observation");
    assert_eq!(context.model_context_window, 100);
    assert_eq!(context.last_total_tokens, 25);
    assert_eq!(context.remaining_percentage, 75);

    let messages = read_log(&log_path);
    let start = messages
        .iter()
        .find(|message| message["method"] == "thread/start")
        .expect("thread/start request");
    let instructions = start["params"]["developerInstructions"]
        .as_str()
        .expect("developer instructions");
    assert!(instructions.contains("custom system instruction"));
    let harness = provider_harness_text(ProviderKind::Codex)
        .expect("harness")
        .expect("Codex harness");
    assert!(instructions.contains(harness.as_str()));

    let turn = messages
        .iter()
        .find(|message| message["method"] == "turn/start")
        .expect("turn/start request");
    assert_eq!(turn["params"]["effort"], "high");
    assert_eq!(
        turn["params"]["sandboxPolicy"],
        json!({"type":"workspaceWrite"})
    );
    assert!(turn["params"].get("sandbox").is_none());
    let types = turn["params"]["input"]
        .as_array()
        .expect("turn input")
        .iter()
        .map(|item| item["type"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(types, vec!["text", "localImage", "skill"]);
}

#[tokio::test]
async fn fake_app_server_proves_provider_approval_and_interrupt_lifecycle() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, log_path) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_approval"))
        .await
        .expect("create session");
    let mut events = provider.subscribe_events().expect("provider events");

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-approval".to_string(),
            input: vec![json!({"type":"text","text":"please request approval"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send approval turn");

    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("approval event timeout")
        .expect("approval event channel");
    let approval_ref = match event {
        ProviderRuntimeEvent::ApprovalRequested {
            runtime_session_id,
            turn_id,
            provider_approval_ref,
            tool_call_id,
            ..
        } => {
            assert_eq!(runtime_session_id, "sess_approval");
            assert_eq!(turn_id, "logical-approval");
            assert_eq!(tool_call_id.as_deref(), Some("command-item-1"));
            provider_approval_ref
        }
        ProviderRuntimeEvent::TurnOutcomeUnknown { code, message, .. } => {
            panic!("unexpected unknown outcome while waiting for approval: {code}: {message}")
        }
        ProviderRuntimeEvent::PermissionObserved { .. } => {
            panic!("unexpected permission observation while waiting for approval")
        }
        ProviderRuntimeEvent::ContextCompactionObserved { .. } => {
            panic!("unexpected compaction observation while waiting for approval")
        }
    };
    assert_eq!(approval_ref, "approval-native-1");

    provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-approval".to_string(),
            approval_id: approval_ref,
            decision: "accept".to_string(),
            payload: None,
        })
        .await
        .expect("respond approval");
    let approved = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-approval".to_string(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("wait approved turn");
    assert_eq!(approved.status, ProviderTurnStatus::Completed);

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-permission-approval".to_string(),
            input: vec![json!({"type":"text","text":"permission-approval"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send permission approval turn");

    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("permission approval event timeout")
        .expect("permission approval event channel");
    let permission_approval_ref = match event {
        ProviderRuntimeEvent::ApprovalRequested {
            runtime_session_id,
            turn_id,
            provider_approval_ref,
            tool_call_id,
            ..
        } => {
            assert_eq!(runtime_session_id, "sess_approval");
            assert_eq!(turn_id, "logical-permission-approval");
            assert_eq!(tool_call_id.as_deref(), Some("permission-item-2"));
            provider_approval_ref
        }
        ProviderRuntimeEvent::TurnOutcomeUnknown { code, message, .. } => {
            panic!(
                "unexpected unknown outcome while waiting for permission approval: {code}: {message}"
            )
        }
        ProviderRuntimeEvent::PermissionObserved { .. } => {
            panic!("unexpected permission observation while waiting for permission approval")
        }
        ProviderRuntimeEvent::ContextCompactionObserved { .. } => {
            panic!("unexpected compaction observation while waiting for permission approval")
        }
    };
    assert_eq!(permission_approval_ref, "permission-native-2");

    provider
        .respond_approval(ProviderApprovalResponseRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-permission-approval".to_string(),
            approval_id: permission_approval_ref,
            decision: "decline".to_string(),
            payload: None,
        })
        .await
        .expect("decline permission approval");
    let permission_turn = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-permission-approval".to_string(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("wait permission approval turn");
    assert_eq!(permission_turn.status, ProviderTurnStatus::Completed);

    let messages = read_log(&log_path);
    let permission_response = messages
        .iter()
        .find(|message| message["id"] == "permission-rpc-2" && message.get("method").is_none())
        .expect("permission approval response");
    assert_eq!(permission_response["result"]["permissions"], json!({}));
    assert_eq!(permission_response["result"]["scope"], "turn");
    assert!(permission_response.get("error").is_none());

    let ack = provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-interrupt".to_string(),
            input: vec![json!({"type":"text","text":"interrupt this turn"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send interrupt turn");
    assert_eq!(
        ack.provider_native_turn_id.as_deref(),
        Some("turn-native-3")
    );
    provider
        .interrupt_turn(ProviderInterruptTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-interrupt".to_string(),
        })
        .await
        .expect("interrupt turn");
    let interrupted = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_approval".to_string(),
            turn_id: "logical-interrupt".to_string(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("wait interrupted turn");
    assert_eq!(interrupted.status, ProviderTurnStatus::Interrupted);

    let messages = read_log(&log_path);
    assert!(messages.iter().any(|message| {
        message["id"] == "approval-rpc-1" && message["result"]["decision"] == "accept"
    }));
    assert!(messages
        .iter()
        .any(|message| message["method"] == "turn/interrupt"));
}

#[tokio::test]
async fn fake_app_server_buffers_out_of_order_terminal_notification_until_native_mapping_exists() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_out_of_order"))
        .await
        .expect("create session");
    let ack = provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_out_of_order".to_string(),
            turn_id: "logical-out-of-order".to_string(),
            input: vec![json!({"type":"text","text":"out-of-order completion"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send turn");
    assert_eq!(
        ack.provider_native_turn_id.as_deref(),
        Some("turn-native-1")
    );
    let result = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_out_of_order".to_string(),
            turn_id: "logical-out-of-order".to_string(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("wait out-of-order turn");
    assert_eq!(result.status, ProviderTurnStatus::Completed);
}

#[tokio::test]
async fn fake_app_server_proves_compaction_rebind_hard_fork_and_restart_resume() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_controls"))
        .await
        .expect("create session");
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_controls".to_string(),
            turn_id: "logical-target".to_string(),
            input: vec![json!({"type":"text","text":"historical turn"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("send historical turn");
    provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_controls".to_string(),
            turn_id: "logical-target".to_string(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("wait historical turn");

    let compact = provider
        .compact_session(ProviderCompactSessionRequest {
            runtime_session_id: "sess_controls".to_string(),
        })
        .await
        .expect("compact session");
    assert_eq!(compact, ProviderCompactSessionOutcome::Accepted);

    let rebind = provider
        .rebind_workspace(ProviderWorkspaceRebindRequest {
            runtime_session_id: "sess_controls".to_string(),
            cwd: "/tmp/rebound-workspace".to_string(),
        })
        .await
        .expect("rebind workspace");
    assert_eq!(rebind.cwd, "/tmp/rebound-workspace");
    assert_eq!(
        rebind.provider_session_ref.as_deref(),
        Some("thread-native-1")
    );

    let forked = provider
        .hard_fork_edit_rerun(ProviderHardForkEditRerunRequest {
            runtime_session_id: "sess_controls".to_string(),
            target_turn_id: "logical-target".to_string(),
            edited_input: vec![json!({"type":"text","text":"edited replay"})],
        })
        .await
        .expect("hard fork");
    assert_eq!(forked.provider_session_ref, "thread-fork-1");
    assert_eq!(
        forked.canonical_provider_session_ref.as_deref(),
        Some("thread-fork-1")
    );

    provider
        .close_session(runtime_core::ProviderCloseSessionRequest {
            runtime_session_id: "sess_controls".to_string(),
            reason: Some("restart test".to_string()),
        })
        .await
        .expect("close first provider session");

    let (restarted, _) = fake_provider(&temp, false);
    let resumed = restarted
        .resume_session_with_policy(
            ProviderResumeSessionPolicyRequest::legacy_compatible(
                "sess_controls".to_string(),
                "thread-fork-1".to_string(),
                Some("thread-fork-1".to_string()),
                Some("gpt-6-luna".to_string()),
                Some("/tmp/rebound-workspace".to_string()),
                Some("workspace_write".to_string()),
                None,
                None,
            )
            .expect("resume policy"),
        )
        .await
        .expect("resume after restart");
    assert_eq!(resumed.provider_session_ref, "thread-fork-1");
}

#[tokio::test]
async fn fake_app_server_proves_skill_discovery_and_aggregate_capacity() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, _) = fake_provider(&temp, false);
    let skills = provider
        .list_skills(ProviderSkillDiscoveryRequest {
            cwd: Some("/workspace/fake".to_string()),
            force_refresh: true,
            ..ProviderSkillDiscoveryRequest::default()
        })
        .await
        .expect("list skills");
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name, "sweep");
    assert_eq!(skills[0].display_name.as_deref(), Some("Sweep"));
    assert_eq!(skills[0].description, "Inspect a codebase deeply");
    assert_eq!(skills[0].argument_hint.as_deref(), Some("<repo>"));

    provider
        .create_session_with_policy(legacy_create("sess_capacity_1"))
        .await
        .expect("first capacity session");
    provider
        .create_session_with_policy(legacy_create("sess_capacity_2"))
        .await
        .expect("second capacity session");
    let third = provider
        .create_session_with_policy(legacy_create("sess_capacity_3"))
        .await
        .expect_err("aggregate capacity should reject third session");
    assert_eq!(
        third.provider_dispatch_code(),
        Some("codex_capacity_exhausted")
    );
}

mod config_contracts;
