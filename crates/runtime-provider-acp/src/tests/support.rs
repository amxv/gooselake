use std::fs;
use std::path::PathBuf;

use serde_json::{json, Value};

use crate::{AcpProvider, AcpProviderConfig};

pub(super) struct FakeAgentHarness {
    pub(super) _temp_dir: tempfile::TempDir,
    pub(super) provider_dir: PathBuf,
    pub(super) script_path: PathBuf,
    pub(super) pid_path: PathBuf,
}

impl FakeAgentHarness {
    pub(super) fn new(mode: &str) -> Self {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let script_path = temp_dir.path().join("fake_acp_agent.py");
        let pid_path = temp_dir.path().join("fake_acp_agent.pid");
        fs::write(script_path.as_path(), fake_agent_script(mode)).expect("write script");
        Self {
            provider_dir: temp_dir.path().join("provider"),
            script_path,
            pid_path,
            _temp_dir: temp_dir,
        }
    }

    pub(super) fn provider(&self) -> AcpProvider {
        AcpProvider::new(AcpProviderConfig {
            enabled: true,
            provider_dir: self.provider_dir.clone(),
            command: Some("python3".to_string()),
            args: vec![self.script_path.display().to_string()],
            ..AcpProviderConfig::default()
        })
    }
}

fn fake_agent_script(mode: &str) -> String {
    format!(
        r#"#!/usr/bin/env python3
import json
import os
import sys
import time

MODE = {mode:?}
PID_PATH = os.path.join(os.path.dirname(__file__), "fake_acp_agent.pid")
SESSIONS = {{}}
PENDING_PROMPTS = {{}}
PENDING_PERMISSIONS = {{}}
NEXT_SESSION_ID = 1

with open(PID_PATH, "w", encoding="utf-8") as pid_file:
    pid_file.write(str(os.getpid()))

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

def prompt_text(prompt):
    parts = []
    for block in prompt:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
        else:
            parts.append(json.dumps(block))
    return " ".join(part.strip() for part in parts if part).strip()

def finish_prompt(session_id, request_id, stop_reason, texts):
    for index, text in enumerate(texts):
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "msg_shared" if index < 2 else f"msg_{{index}}",
                    "content": {{
                        "type": "text",
                        "text": text
                    }}
                }}
            }}
        }})
    send({{
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {{
            "sessionId": session_id,
            "update": {{
                "sessionUpdate": "usage_update",
                "used": 7,
                "size": 64,
                "cost": {{
                    "amount": 0.01,
                    "currency": "USD"
                }}
            }}
        }}
    }})
    send({{
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {{
            "stopReason": stop_reason
        }}
    }})

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    msg = json.loads(line)

    if "method" not in msg and "id" in msg:
        permission = None
        for session_id, value in list(PENDING_PERMISSIONS.items()):
            if value["permission_id"] == msg["id"]:
                permission = (session_id, value)
                break
        if permission is not None:
            session_id, value = permission
            PENDING_PERMISSIONS.pop(session_id, None)
            finish_prompt(session_id, value["prompt_request_id"], "cancelled", ["Permission request was cancelled."])
        continue

    method = msg.get("method")
    if method == "initialize":
        if MODE == "hang_initialize":
            continue
        result = {{
            "protocolVersion": 1,
            "agentCapabilities": {{
                "loadSession": True,
                "sessionCapabilities": {{
                    "close": {{}}
                }}
            }},
            "authMethods": []
        }}
        if MODE == "bad_protocol":
            result["protocolVersion"] = 999
        if MODE != "load_only":
            result["agentCapabilities"]["sessionCapabilities"]["resume"] = {{}}
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": result}})
    elif method == "session/new":
        if MODE == "slow_create":
            time.sleep(0.2)
        session_id = f"sess_{{NEXT_SESSION_ID}}"
        NEXT_SESSION_ID += 1
        SESSIONS[session_id] = {{}}
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{"sessionId": session_id}}}})
    elif method == "session/resume":
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
    elif method == "session/load":
        session_id = msg["params"]["sessionId"]
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "history_1",
                    "content": {{
                        "type": "text",
                        "text": "Loaded history."
                    }}
                }}
            }}
        }})
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": None}})
    elif method == "session/prompt":
        session_id = msg["params"]["sessionId"]
        request_id = msg["id"]
        text = prompt_text(msg["params"].get("prompt", []))
        if "malformed" in text:
            sys.stdout.write("{{not-json\n")
            sys.stdout.flush()
            continue
        if "crash" in text:
            os._exit(9)
        if "permission collision" in text:
            permission_id = request_id
            PENDING_PERMISSIONS[session_id] = {{
                "permission_id": permission_id,
                "prompt_request_id": request_id
            }}
            send({{
                "jsonrpc": "2.0",
                "id": permission_id,
                "method": "session/request_permission",
                "params": {{
                    "sessionId": session_id,
                    "toolCall": {{
                        "toolCallId": "call_permission_collision"
                    }},
                    "options": [
                        {{
                            "optionId": "allow-once",
                            "name": "Allow once",
                            "kind": "allow_once"
                        }}
                    ]
                }}
            }})
            continue
        if "permission" in text:
            permission_id = request_id + 1000 if isinstance(request_id, int) else 1000
            PENDING_PERMISSIONS[session_id] = {{
                "permission_id": permission_id,
                "prompt_request_id": request_id
            }}
            send({{
                "jsonrpc": "2.0",
                "id": permission_id,
                "method": "session/request_permission",
                "params": {{
                    "sessionId": session_id,
                    "toolCall": {{
                        "toolCallId": "call_permission"
                    }},
                    "options": [
                        {{
                            "optionId": "allow-once",
                            "name": "Allow once",
                            "kind": "allow_once"
                        }},
                        {{
                            "optionId": "reject-once",
                            "name": "Reject",
                            "kind": "reject_once"
                        }}
                    ]
                }}
            }})
            continue
        if "sleep" in text:
            PENDING_PROMPTS[session_id] = {{
                "request_id": request_id
            }}
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "sleep_1",
                        "content": {{
                            "type": "text",
                            "text": "Starting long task..."
                        }}
                    }}
                }}
            }})
            continue
        if "split" in text:
            finish_prompt(session_id, request_id, "end_turn", ["Hello ", "world"])
            continue
        if "refusal" in text:
            finish_prompt(session_id, request_id, "refusal", ["Refused."])
            continue
        if "max tokens" in text:
            finish_prompt(session_id, request_id, "max_tokens", ["Stopped for token limit."])
            continue
        if "max turns" in text:
            finish_prompt(session_id, request_id, "max_turn_requests", ["Stopped for turn limit."])
            continue
        if "tooling" in text:
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "msg_tool_1",
                        "content": {{
                            "type": "text",
                            "text": "First line."
                        }}
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "tool_call",
                        "toolCallId": "tool_1",
                        "toolName": "gg_ping"
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "tool_1",
                        "status": "completed"
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "msg_tool_2",
                        "content": {{
                            "type": "text",
                            "text": "Second line."
                        }}
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{
                    "sessionId": session_id,
                    "update": {{
                        "sessionUpdate": "usage_update",
                        "used": 11,
                        "size": 128,
                        "cost": {{
                            "amount": 0.02,
                            "currency": "USD"
                        }}
                    }}
                }}
            }})
            send({{
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {{
                    "stopReason": "end_turn"
                }}
            }})
            continue
        finish_prompt(session_id, request_id, "end_turn", [f"Echo: {{text}}"])
    elif method == "session/cancel":
        session_id = msg["params"]["sessionId"]
        pending = PENDING_PROMPTS.pop(session_id, None)
        if pending is not None:
            finish_prompt(session_id, pending["request_id"], "cancelled", ["Cancelled by client."])
    elif method == "session/close":
        session_id = msg["params"]["sessionId"]
        if MODE == "hang_close":
            continue
        if MODE == "error_close":
            send({{
                "jsonrpc": "2.0",
                "id": msg["id"],
                "error": {{
                    "code": -32001,
                    "message": "close failed"
                }}
            }})
            continue
        PENDING_PROMPTS.pop(session_id, None)
        PENDING_PERMISSIONS.pop(session_id, None)
        SESSIONS.pop(session_id, None)
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
"#
    )
}

pub(super) fn expected_gg_mcp_server(
    runtime_session_id: &str,
    enable_process_tools: bool,
    gateway_url: Option<&str>,
    gateway_token: Option<&str>,
) -> Value {
    let mut env = vec![
        json!({
            "name": "GG_MCP_ENABLE_PROCESS_TOOLS",
            "value": if enable_process_tools { "1" } else { "0" },
        }),
        json!({
            "name": "GG_MCP_REQUIRE_TOOL_CALLER_AGENT_ID",
            "value": "1",
        }),
        json!({
            "name": "GG_MCP_CALLER_AGENT_ID",
            "value": runtime_session_id,
        }),
    ];
    if let Some(url) = gateway_url {
        env.push(json!({
            "name": "GG_MCP_GATEWAY_URL",
            "value": url,
        }));
    }
    if let Some(token) = gateway_token {
        env.push(json!({
            "name": "GG_MCP_GATEWAY_TOKEN",
            "value": token,
        }));
    }
    if let Some(home) = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        env.push(json!({
            "name": "HOME",
            "value": home.display().to_string(),
        }));
    }
    if let Some(cargo_home) = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        env.push(json!({
            "name": "CARGO_HOME",
            "value": cargo_home.display().to_string(),
        }));
    }

    json!({
        "name": "gg",
        "command": "gg-mcp-server",
        "args": ["--stdio"],
        "env": env,
    })
}
