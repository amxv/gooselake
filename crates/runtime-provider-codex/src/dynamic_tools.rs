//! Provider-native Codex GG tools. MCP projections are maintained separately
//! by gg-mcp-server, but the canonical semantic authority is the runtime
//! gateway: neither provider accepts model-supplied caller/workspace identity.

use crate::state::CodexProviderInner;
use runtime_core::ToolInvokeRequest;
use serde_json::{json, Value};
use std::sync::Arc;

fn mode(name: &str, properties: Value, required: &[&str]) -> Value {
    let mut merged = properties.as_object().cloned().unwrap_or_default();
    merged.insert("mode".into(), json!({"type":"string","const":name}));
    json!({
        "type":"object", "properties":merged,
        "required":std::iter::once("mode").chain(required.iter().copied()).collect::<Vec<_>>(),
        "additionalProperties":false
    })
}

fn image_paths() -> Value {
    json!({"type":"array","maxItems":8,"items":{"type":"string","minLength":1}})
}

fn gg_team() -> Value {
    json!({
        "name":"gg_team",
        "description":"Inspect or coordinate members of your current workspace. Status includes aliases, activity, context remaining and eligible worktrees. Add an agent with optional onboarding; remove active members; or assign an eligible managed worktree. Caller and workspace are authenticated by the host.",
        "inputSchema":{"oneOf":[
            mode("status",json!({}),&[]),
            mode("add",json!({
                "title":{"type":"string","minLength":1},
                "prompt":{"type":"string","minLength":1},
                "image_paths":image_paths(),
                "model_preset":{"type":"string","minLength":1},
                "worktree_name":{"type":"string","minLength":1},
                "creator_compaction_subscription":{"type":"string","enum":["auto","unsubscribed"]}
            }),&[]),
            mode("remove",json!({
                "remove_agent_ids":{"type":"array","minItems":1,"maxItems":100,"items":{"type":"string","minLength":1}},
                "preserve_worktrees":{"type":"boolean"}
            }),&["remove_agent_ids"]),
            mode("assign_worktree",json!({
                "assign_worktree":{"type":"object","properties":{
                    "agent_id":{"type":"string","minLength":1},
                    "worktree_name":{"type":"string","minLength":1},
                    "preserve_previous_worktree":{"type":"boolean"}
                },"required":["agent_id","worktree_name"],"additionalProperties":false}
            }),&["assign_worktree"])
        ]}
    })
}

fn gg_message() -> Value {
    json!({
        "name":"gg_message",
        "description":"Send a direct message to an agent in any workspace using its durable alias or opaque agent ID, or broadcast to active members of your own workspace. Trusted sender identity is supplied by the host. Image paths are ordered native attachments; unsupported providers reject them explicitly.",
        "inputSchema":{"oneOf":[
            mode("direct",json!({
                "agent_id":{"type":"string","minLength":1},
                "message":{"type":"string","minLength":1},
                "image_paths":image_paths()
            }),&["agent_id","message"]),
            mode("broadcast",json!({
                "message":{"type":"string","minLength":1},
                "image_paths":image_paths()
            }),&["message"])
        ]}
    })
}

fn gg_process() -> Value {
    json!({
        "name":"gg_process",
        "description":"Durably schedule a command, list active workspace processes, inspect one process, or cancel a process you own. Completion is pushed automatically; do not poll. Status includes authoritative stdout/stderr paths.",
        "inputSchema":{"oneOf":[
            mode("run",json!({
                "command":{"type":"string","minLength":1},
                "cwd":{"type":"string","minLength":1},
                "timeout_ms":{"type":"integer","minimum":0}
            }),&["command"]),
            mode("list",json!({}),&[]),
            mode("status",json!({"process_id":{"type":"string","minLength":1}}),&["process_id"]),
            mode("cancel",json!({"process_id":{"type":"string","minLength":1}}),&["process_id"])
        ]}
    })
}

/// Only the gateway's actually available semantic tools are projected into
/// Codex thread/start. Never advertise disabled functions as failing stubs.
pub(super) fn specs_for_gateway_capabilities(capabilities: &Value) -> Result<Vec<Value>, String> {
    let available = capabilities
        .pointer("/result/tools")
        .and_then(Value::as_array)
        .ok_or("canonical gateway capabilities missing result.tools")?;
    let allowed = available
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    Ok([
        ("gg_team", gg_team()),
        ("gg_message", gg_message()),
        ("gg_process", gg_process()),
    ]
    .into_iter()
    .filter(|(name, _)| allowed.contains(name))
    .map(|(_, spec)| spec)
    .collect())
}

fn error(code: &str, message: &str) -> Value {
    json!({"ok":false,"error":{"code":code,"message":message,"retryable":false}})
}

/// A provider-generated item/call ID is stable across native transport replay.
/// Derive a bounded correlation key without accepting a model-authored key in
/// arguments or leaking any runtime secret into model-visible results.
fn trusted_invocation_id(
    agent: &str,
    native_turn: &str,
    tool: &str,
    rpc_id: &Value,
    params: &Value,
) -> String {
    let item = params
        .get("callId")
        .or_else(|| params.get("call_id"))
        .or_else(|| params.get("toolCallId"))
        .or_else(|| params.get("tool_call_id"))
        .or_else(|| params.get("itemId"))
        .or_else(|| params.get("item_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| rpc_id.to_string());
    let input = format!("{agent}\0{native_turn}\0{tool}\0{item}");
    let digest = ring::digest::digest(&ring::digest::SHA256, input.as_bytes());
    let mut output = String::from("codex_");
    for byte in digest.as_ref() {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

pub(super) async fn dispatch_native_tool_call(
    inner: &Arc<CodexProviderInner>,
    agent: &str,
    message: &Value,
) {
    let Some(rpc_id) = message.get("id").cloned() else {
        return;
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let (transport, thread_id) = {
        let sessions = inner.sessions.read().await;
        let Some(session) = sessions.get(agent) else {
            return;
        };
        (
            Arc::clone(&session.transport),
            session.provider_session_ref.clone(),
        )
    };
    let bound = inner
        .native_tool_gateway
        .read()
        .ok()
        .and_then(|guard| guard.as_ref().and_then(|weak| weak.0.upgrade()));
    let inner = Arc::clone(inner);
    let agent = agent.to_string();
    // Each native tool RPC is resolved independently. Do not block the event
    // router while process/team effects run; turn completion continues to flow.
    tokio::spawn(async move {
        let tool = params.get("tool").and_then(Value::as_str).unwrap_or("");
        let native_turn = params.get("turnId").and_then(Value::as_str).unwrap_or("");
        let args = params.get("arguments").cloned().unwrap_or(Value::Null);
        // A stale tool call from an older native turn on the same persistent
        // thread must never execute inside a newer active logical turn. The
        // app-server may send the call *before* turn/start responds, so wait
        // briefly for the authoritative mapping instead of either trusting
        // merely `active_turn_id.is_some()` or rejecting valid early calls.
        let turn_bound = if native_turn.trim().is_empty()
            || params.get("threadId").and_then(Value::as_str) != Some(thread_id.as_str())
        {
            false
        } else {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let notified = inner.native_turn_binding_notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let binding = {
                        let sessions = inner.sessions.read().await;
                        sessions.get(&agent).map(|session| {
                            let active = session.active_turn_id.as_deref();
                            if active.is_none() || session.provider_session_ref != thread_id {
                                return Some(false);
                            }
                            session
                                .native_to_logical_turns
                                .get(native_turn)
                                .map(|logical| active == Some(logical.as_str()))
                        })
                    };
                    match binding {
                        Some(Some(is_current)) => break is_current,
                        None => break false,
                        Some(None) => notified.await,
                    }
                }
            })
            .await
            .unwrap_or(false)
        };
        let result = if !turn_bound {
            error(
                "invalid_turn",
                "Codex native tool call does not belong to the current active session turn",
            )
        } else if !matches!(tool, "gg_team" | "gg_message" | "gg_process") {
            error("unknown_tool", "Unknown provider-native GG tool")
        } else if !args.is_object()
            || serde_json::to_vec(&args).map_or(true, |bytes| bytes.len() > 64 * 1024)
        {
            error(
                "bad_request",
                "GG tool arguments must be an object of at most 64 KiB",
            )
        } else if let Some(gateway) = bound {
            match gateway.capabilities().await {
                Ok(caps)
                    if specs_for_gateway_capabilities(&caps)
                        .is_ok_and(|specs| specs.iter().any(|spec| spec["name"] == tool)) =>
                {
                    let invocation =
                        trusted_invocation_id(&agent, native_turn, tool, &rpc_id, &params);
                    match gateway.invoke_tool(ToolInvokeRequest {
                        namespace:Some(tool.to_string()),tool_name:tool.to_string(),
                        caller_session_id:agent,invocation_id:Some(invocation),args
                    }).await {
                        Ok(envelope)=>envelope,
                        Err(_)=>error("outcome_unresolved",
                            "GG tool outcome cannot be established; inspect authoritative status instead of retrying")
                    }
                }
                Ok(_) => error("feature_disabled", "GG tool is disabled for this runtime"),
                Err(_) => error(
                    "backend_unavailable",
                    "GG tool capabilities cannot be resolved",
                ),
            }
        } else {
            error("backend_unavailable", "GG native gateway is unavailable")
        };
        let ok = result.get("ok").and_then(Value::as_bool) == Some(true);
        let text = serde_json::to_string(&result).unwrap_or_else(|_| {
            error("internal_error", "GG tool result encoding failed").to_string()
        });
        let _ = transport
            .respond(
                rpc_id,
                json!({
                    "success":ok,"contentItems":[{"type":"inputText","text":text}]
                }),
            )
            .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_projection_is_three_strict_mode_tools_and_gates_disabled_processes() {
        let capabilities = json!({"result":{"tools":["gg_team","gg_message","gg_process"]}});
        let specs = specs_for_gateway_capabilities(&capabilities).expect("specs");
        assert_eq!(
            specs
                .iter()
                .map(|spec| spec["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["gg_team", "gg_message", "gg_process"]
        );
        for spec in &specs {
            let modes = spec["inputSchema"]["oneOf"].as_array().unwrap();
            assert!(!modes.is_empty());
            for mode in modes {
                assert_eq!(mode["additionalProperties"], false);
                assert_eq!(mode["required"][0], "mode");
                assert!(mode["properties"].get("workspace_id").is_none());
                assert!(mode["properties"].get("caller_agent_id").is_none());
            }
        }
        let disabled =
            specs_for_gateway_capabilities(&json!({"result":{"tools":["gg_team","gg_message"]}}))
                .unwrap();
        assert_eq!(disabled.len(), 2);
        assert!(specs_for_gateway_capabilities(&json!({})).is_err());
    }
}
