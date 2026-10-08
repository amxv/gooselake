use std::collections::HashSet;
use std::sync::Arc;

use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderRuntimeEvent, RuntimeError,
};
use serde_json::{json, Value};

use crate::connection::AcpConnection;
use crate::protocol::message_id_key;
use crate::provider::AcpProvider;
use crate::state::{AcpNativePermission, AcpPermissionOption};

impl AcpProvider {
    /// Never block the stdio reader on a human decision: the durable manager
    /// receives the event, while the exact JSON-RPC request remains pending.
    pub(super) async fn handle_native_permission_request(
        &self,
        connection: &Arc<AcpConnection>,
        message: &Value,
    ) {
        let session_ref = message
            .get("params")
            .and_then(|p| p.get("sessionId"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let rpc_id = message
            .get("id")
            .filter(|id| message_id_key(&json!({"id":id})).is_some())
            .cloned();
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let options = parse_permission_options(&params);
        let tool_call_id = params
            .get("toolCall")
            .and_then(|tool| tool.get("toolCallId"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let accepted = if let (Some(rpc_id), Ok(options), Some(tool_call_id)) =
            (rpc_id.as_ref(), options, tool_call_id)
        {
            self.admit_native_permission(
                connection,
                session_ref,
                rpc_id.clone(),
                &params,
                options,
                tool_call_id,
            )
            .await
        } else {
            false
        };
        if accepted {
            return;
        }

        // Malformed, ambiguous, or unrouteable requests fail closed. A native
        // caller is never authorized merely because it names a session ID.
        if let Some(rpc_id) = rpc_id {
            let _ = cancel_request(connection, rpc_id).await;
        } else {
            // A JSON-RPC request without an ID cannot be answered. Tear down
            // the ambiguous transport rather than leave the ACP child blocked.
            connection.shutdown(true).await;
        }
        let _ = self
            .fail_permission_request(session_ref, connection.instance_id)
            .await;
    }

    async fn admit_native_permission(
        &self,
        connection: &Arc<AcpConnection>,
        session_ref: &str,
        rpc_id: Value,
        params: &Value,
        options: Vec<AcpPermissionOption>,
        tool_call_id: String,
    ) -> bool {
        let mut sessions = self.inner.sessions.write().await;
        let matches = sessions
            .iter()
            .filter(|(_, session)| {
                !session_ref.is_empty()
                    && session.provider_session_ref == session_ref
                    && session.connection_id == Some(connection.instance_id)
                    && session.active_turn.is_some()
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return false;
        }
        let runtime_id = matches[0].clone();
        let Some(session) = sessions.get_mut(&runtime_id) else {
            return false;
        };
        let Some(active) = session.active_turn.as_ref() else {
            return false;
        };
        let turn_id = active.runtime_turn_id.clone();
        let Some(rpc_key) = message_id_key(&json!({"id":rpc_id})) else {
            return false;
        };
        if rpc_key.len() > 128 {
            return false;
        }
        // JSON-RPC IDs are unique only among outstanding requests. ACP agents
        // may legitimately reuse one after a turn completes; include the
        // logical turn to avoid deduplicating an unrelated later approval.
        let approval_ref = format!(
            "acp:{}:{}:{}:{}",
            connection.instance_id, runtime_id, turn_id, rpc_key
        );
        if let Some(previous) = session.pending_native_permissions.get(&approval_ref) {
            return previous.turn_id == turn_id && previous.request == *params;
        }
        let pending = AcpNativePermission {
            connection_id: connection.instance_id,
            rpc_id,
            turn_id: turn_id.clone(),
            request: params.clone(),
            options,
        };
        session
            .pending_native_permissions
            .insert(approval_ref.clone(), pending);
        let published = self
            .inner
            .provider_events
            .send(ProviderRuntimeEvent::ApprovalRequested {
                runtime_session_id: runtime_id.clone(),
                turn_id,
                provider_approval_ref: approval_ref.clone(),
                tool_call_id: Some(tool_call_id),
                request: params.clone(),
            });
        if published.is_err() {
            session.pending_native_permissions.remove(&approval_ref);
            return false;
        }
        true
    }

    /// Returns false only for a legacy runtime pre-dispatch gate. The manager
    /// supplies the original provider approval ref for native approvals.
    pub(super) async fn resolve_native_permission(
        &self,
        req: &ProviderApprovalResponseRequest,
    ) -> Result<bool, RuntimeError> {
        // Reject invalid decisions before consuming the pending native RPC.
        // A caller must be able to correct the decision and retry without
        // orphaning a tool permission request inside the ACP child.
        let decision = ApprovalDecision::parse(req.decision.as_str())?;
        let _response_guard = self.inner.permission_response_gate.lock().await;
        let permission = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions.get(&req.runtime_session_id).ok_or_else(|| {
                RuntimeError::NotFound(format!("ACP session {}", req.runtime_session_id))
            })?;
            let Some(pending) = session.pending_native_permissions.get(&req.approval_id) else {
                return Ok(false);
            };
            if pending.turn_id != req.turn_id {
                return Err(RuntimeError::ProtocolViolation(
                    "ACP native approval turn mismatch".into(),
                ));
            }
            pending.clone()
        };
        // Validate the exact offered option and decision before any response
        // reaches the subprocess. Local validation errors are retryable without
        // changing durable manager approval state or issuing a native reply.
        let outcome =
            choose_permission_outcome(&permission.options, decision, req.payload.as_ref())
                .map_err(|error| {
                    RuntimeError::provider_not_dispatched(
                        "invalid_acp_permission_option",
                        error.to_string(),
                    )
                })?;
        let connection = self.current_connection().await.ok_or_else(|| {
            RuntimeError::provider_dispatch_unknown(
                "acp_approval_connection_lost",
                "ACP native permission connection is unavailable",
            )
        })?;
        if connection.instance_id != permission.connection_id {
            return Err(RuntimeError::provider_dispatch_unknown(
                "acp_approval_connection_changed",
                "ACP native permission belongs to a different stdio connection",
            ));
        }
        connection
            .write_message(&json!({
                "jsonrpc": "2.0", "id": permission.rpc_id,
                "result": { "outcome": outcome }
            }))
            .await
            .map_err(|error| {
                RuntimeError::provider_dispatch_unknown(
                    "acp_approval_response_unknown",
                    error.to_string(),
                )
            })?;
        if let Some(session) = self
            .inner
            .sessions
            .write()
            .await
            .get_mut(&req.runtime_session_id)
        {
            session.pending_native_permissions.remove(&req.approval_id);
        }
        Ok(true)
    }

    pub(super) async fn cancel_native_permissions(
        &self,
        runtime_session_id: &str,
        turn_id: Option<&str>,
    ) {
        let _response_guard = self.inner.permission_response_gate.lock().await;
        let pending = {
            let mut sessions = self.inner.sessions.write().await;
            let Some(session) = sessions.get_mut(runtime_session_id) else {
                return;
            };
            let mut pending = Vec::new();
            session.pending_native_permissions.retain(|_, permission| {
                if turn_id.is_none_or(|turn_id| permission.turn_id == turn_id) {
                    pending.push(permission.clone());
                    false
                } else {
                    true
                }
            });
            pending
        };
        let Some(connection) = self.current_connection().await else {
            return;
        };
        for permission in pending {
            if permission.connection_id == connection.instance_id {
                let _ = cancel_request(&connection, permission.rpc_id).await;
            }
        }
    }
}

fn parse_permission_options(params: &Value) -> Result<Vec<AcpPermissionOption>, RuntimeError> {
    let list = params
        .get("options")
        .and_then(Value::as_array)
        .filter(|list| !list.is_empty() && list.len() <= 32)
        .ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "ACP permission options must be nonempty and bounded".into(),
            )
        })?;
    let mut seen = HashSet::new();
    list.iter()
        .map(|option| {
            let id = option
                .get("optionId")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation("ACP option missing optionId".into())
                })?;
            let kind = option.get("kind").and_then(Value::as_str).unwrap_or("");
            if id.len() > 128
                || kind.len() > 64
                || !seen.insert(id.to_string())
                || !(matches!(
                    kind,
                    "allow_once" | "allow_always" | "reject_once" | "reject_always"
                ) || kind.starts_with('_'))
            {
                return Err(RuntimeError::ProtocolViolation(
                    "ACP permission options contain duplicate or unsupported selection".into(),
                ));
            }
            Ok(AcpPermissionOption {
                option_id: id.to_string(),
                kind: kind.to_string(),
            })
        })
        .collect()
}

fn choose_permission_outcome(
    options: &[AcpPermissionOption],
    decision: ApprovalDecision,
    payload: Option<&Value>,
) -> Result<Value, RuntimeError> {
    let requested = match payload.and_then(|p| p.get("optionId")) {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.as_str()),
        Some(_) => {
            return Err(RuntimeError::InvalidState(
                "ACP approval optionId must be a nonblank offered string".into(),
            ))
        }
        None => None,
    };
    let selected = match requested {
        Some(id) => options.iter().find(|option| option.option_id == id).ok_or_else(|| {
            RuntimeError::InvalidState("ACP approval optionId was not offered by the agent".into())
        })?,
        None => match decision {
            ApprovalDecision::Accept => options.iter().find(|option| option.kind == "allow_once").ok_or_else(|| RuntimeError::InvalidState("ACP approval has no one-shot allow option; explicitly select a durable permission option".into()))?,
            ApprovalDecision::Decline => options.iter().find(|option| option.kind == "reject_once")
                .ok_or_else(|| RuntimeError::InvalidState("ACP approval has no one-shot reject option".into()))?,
        },
    };
    let accepted = match selected.kind.as_str() {
        "allow_once" | "allow_always" => true,
        "reject_once" | "reject_always" => false,
        _ => {
            return Err(RuntimeError::InvalidState(
                "ACP approval option kind has no safe known decision semantics".into(),
            ))
        }
    };
    if accepted != (decision == ApprovalDecision::Accept) {
        return Err(RuntimeError::InvalidState(
            "ACP approval decision and optionId disagree".into(),
        ));
    }
    Ok(json!({"outcome":"selected", "optionId":selected.option_id}))
}

async fn cancel_request(connection: &AcpConnection, rpc_id: Value) -> Result<(), RuntimeError> {
    connection
        .write_message(&json!({
            "jsonrpc":"2.0", "id":rpc_id, "result":{"outcome":{"outcome":"cancelled"}}
        }))
        .await
}
