use super::*;

pub(super) struct FakeAcpAgentScript {
    _temp_dir: tempfile::TempDir,
    pub(super) script_path: PathBuf,
    pub(super) log_path: PathBuf,
}

pub(super) fn fake_acp_agent_with_request_log() -> FakeAcpAgentScript {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let script_path = temp_dir.path().join("fake_acp_http_agent.py");
    let log_path = temp_dir.path().join("acp-agent-requests.jsonl");
    fs::write(
        script_path.as_path(),
        format!(
            r#"#!/usr/bin/env python3
import json
import os
import sys

LOG_PATH = {log_path:?}
NEXT_SESSION_ID = 1

def write_log(obj):
    with open(LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj) + "\n")

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    msg = json.loads(line)
    write_log(msg)
    method = msg.get("method")
    if method == "initialize":
        send({{
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {{
                "protocolVersion": 1,
                "agentCapabilities": {{
                    "loadSession": True,
                    "sessionCapabilities": {{
                        "close": {{}},
                        "resume": {{}}
                    }}
                }},
                "authMethods": []
            }}
        }})
    elif method == "session/new":
        session_id = f"sess_{{NEXT_SESSION_ID}}"
        NEXT_SESSION_ID += 1
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{"sessionId": session_id}}}})
    elif method == "session/resume":
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
    elif method == "session/prompt":
        session_id = msg["params"]["sessionId"]
        prompt = msg["params"].get("prompt", [])
        prompt_text = " ".join(
            block.get("text", "")
            for block in prompt
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{
                "sessionId": session_id,
                "update": {{
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "msg_http_1",
                    "content": {{
                        "type": "text",
                        "text": "Echo: "
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
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "msg_http_1",
                    "content": {{
                        "type": "text",
                        "text": prompt_text
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
                    "used": 3,
                    "size": 16
                }}
            }}
        }})
        send({{
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {{
                "stopReason": "end_turn"
            }}
        }})
    elif method == "session/close":
        send({{"jsonrpc": "2.0", "id": msg["id"], "result": {{}}}})
"#
        ),
    )
    .expect("write fake acp agent");
    FakeAcpAgentScript {
        _temp_dir: temp_dir,
        script_path,
        log_path,
    }
}

pub(super) fn read_logged_jsonl(path: &Path) -> Vec<serde_json::Value> {
    let contents = fs::read_to_string(path).expect("read jsonl");
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl line"))
        .collect()
}
