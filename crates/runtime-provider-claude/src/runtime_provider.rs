use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use runtime_core::{
    ApprovalDecision, ProviderApprovalResponseRequest, ProviderAuthStatus,
    ProviderCloseSessionRequest, ProviderCreateSessionRequest, ProviderInterruptTurnRequest,
    ProviderKind, ProviderMetadata, ProviderModel, ProviderResumeSessionRequest,
    ProviderSendTurnRequest, ProviderSession, ProviderTurnAck, ProviderTurnResult,
    ProviderWaitTurnRequest, RuntimeError, RuntimeProvider,
};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

use crate::auth::{
    claude_smoke_debug_enabled, extract_assistant_text, extract_turn_status,
    is_missing_gg_mcp_server_bad_request, merge_assistant_text_into_usage,
    parse_claude_auth_import_payload,
};
use crate::bridge::send_bridge_request;
use crate::provider::{ClaudeProvider, ClaudeSessionHandle};

#[async_trait]
impl RuntimeProvider for ClaudeProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Claude
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: ProviderKind::Claude,
            display_name: "Claude".to_string(),
            enabled: self.inner.config.enabled,
        }
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        self.ensure_provider_enabled().await
    }

    async fn list_models(&self) -> Result<Vec<ProviderModel>, RuntimeError> {
        if !self.inner.config.enabled {
            return Ok(Vec::new());
        }
        Ok(vec![
            ProviderModel {
                id: "claude-sonnet-5".to_string(),
                display_name: "Claude Sonnet 5".to_string(),
                reasoning_levels: Vec::new(),
            },
            ProviderModel {
                id: "claude-opus-4-8".to_string(),
                display_name: "Claude Opus 4.8".to_string(),
                reasoning_levels: Vec::new(),
            },
            ProviderModel {
                id: "claude-fable-5".to_string(),
                display_name: "Claude Fable 5".to_string(),
                reasoning_levels: Vec::new(),
            },
            ProviderModel {
                id: "claude-haiku-4-5".to_string(),
                display_name: "Claude Haiku 4.5".to_string(),
                reasoning_levels: Vec::new(),
            },
        ])
    }

    async fn auth_status(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        self.provider_auth_status_internal().await
    }

    async fn auth_set_api_key(&self, api_key: String) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let trimmed = api_key.trim();
        if trimmed.is_empty() {
            return Err(RuntimeError::InvalidState(
                "Claude API key cannot be empty".to_string(),
            ));
        }
        self.write_api_key(trimmed).await?;
        self.recycle_after_live_auth_change().await;
        self.provider_auth_status_internal().await
    }

    async fn auth_import_json(&self, auth_json: Value) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let import_payload = parse_claude_auth_import_payload(auth_json)?;
        if let Some(credentials_json) = import_payload.credentials_json.as_ref() {
            self.write_oauth_credentials_json(credentials_json).await?;
        }
        if let Some(config_json) = import_payload.config_json.as_ref() {
            self.write_claude_config_json(config_json).await?;
        }
        self.recycle_after_live_auth_change().await;
        self.provider_auth_status_internal().await
    }

    async fn auth_import_json_text(
        &self,
        auth_json_text: String,
    ) -> Result<ProviderAuthStatus, RuntimeError> {
        let parsed = serde_json::from_str::<Value>(auth_json_text.trim()).map_err(|error| {
            RuntimeError::InvalidState(format!("Claude auth_json_text must be valid JSON: {error}"))
        })?;
        self.auth_import_json(parsed).await
    }

    async fn auth_logout(&self) -> Result<ProviderAuthStatus, RuntimeError> {
        self.ensure_provider_enabled().await?;

        let credentials_path = self.claude_credentials_path();
        if let Err(error) = tokio::fs::remove_file(credentials_path.as_path()).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(RuntimeError::Io(format!(
                    "failed removing Claude credentials file {}: {error}",
                    credentials_path.display()
                )));
            }
        }
        let config_path = self.claude_config_path();
        if let Err(error) = tokio::fs::remove_file(config_path.as_path()).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(RuntimeError::Io(format!(
                    "failed removing Claude config file {}: {error}",
                    config_path.display()
                )));
            }
        }

        let api_key_path = self.api_key_path();
        if let Err(error) = tokio::fs::remove_file(api_key_path.as_path()).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(RuntimeError::Io(format!(
                    "failed removing Claude API key {}: {error}",
                    api_key_path.display()
                )));
            }
        }

        self.recycle_after_live_auth_change().await;
        self.provider_auth_status_internal().await
    }

    async fn create_session(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let bridge = self.acquire_bridge_for_new_session().await?;
        let mut create_params = serde_json::json!({
            "cwd": req.cwd,
            "model": req.model,
            "permissionMode": req.permission_mode,
        });
        let configure_gg_mcp_server = self.inner.config.gg_mcp.enabled;
        if configure_gg_mcp_server {
            create_params["ggMcpServer"] =
                self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str());
        }

        let response = match send_bridge_request(
            &self.inner,
            &bridge,
            "session.create",
            create_params.clone(),
            self.inner.config.request_timeout_ms,
        )
        .await
        {
            Ok(response) => response,
            Err(error)
                if !configure_gg_mcp_server && is_missing_gg_mcp_server_bad_request(&error) =>
            {
                create_params["ggMcpServer"] =
                    self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str());
                send_bridge_request(
                    &self.inner,
                    &bridge,
                    "session.create",
                    create_params,
                    self.inner.config.request_timeout_ms,
                )
                .await?
            }
            Err(error) => return Err(error),
        };

        let bridge_session_id = response
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "session.create response missing sessionId".to_string(),
                )
            })?;

        let provider_session_ref = response
            .get("providerSessionRef")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| bridge_session_id.clone());
        let canonical_provider_session_ref = response
            .get("claudeCanonicalSessionRef")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        let session = Arc::new(ClaudeSessionHandle {
            runtime_session_id: req.runtime_session_id.clone(),
            bridge_session_id,
            provider_session_ref: RwLock::new(provider_session_ref.clone()),
            canonical_provider_session_ref: RwLock::new(canonical_provider_session_ref.clone()),
            bridge,
            active_turn_id: RwLock::new(None),
            bridge_turn_by_runtime_turn: Mutex::new(BTreeMap::new()),
            runtime_turn_by_bridge_turn: Mutex::new(BTreeMap::new()),
            completed_turns: Mutex::new(BTreeMap::new()),
        });
        self.insert_session(req.runtime_session_id.as_str(), session)
            .await?;

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref,
            canonical_provider_session_ref,
        })
    }

    async fn resume_session(
        &self,
        req: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.ensure_provider_enabled().await?;
        let _ = self.remove_session(req.runtime_session_id.as_str()).await;
        let bridge = self.acquire_bridge_for_new_session().await?;
        let mut resume_params = serde_json::json!({
            "sessionId": req.provider_session_ref,
            "providerSessionRef": req.provider_session_ref,
            "claudeCanonicalSessionRef": req.canonical_provider_session_ref,
            "cwd": req.cwd,
        });
        let configure_gg_mcp_server = self.inner.config.gg_mcp.enabled;
        if configure_gg_mcp_server {
            if let Some(object) = resume_params.as_object_mut() {
                object.insert(
                    "ggMcpServer".to_string(),
                    self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str()),
                );
            }
        }

        let response = match send_bridge_request(
            &self.inner,
            &bridge,
            "session.resume",
            resume_params.clone(),
            self.inner.config.request_timeout_ms,
        )
        .await
        {
            Ok(response) => response,
            Err(error)
                if !configure_gg_mcp_server && is_missing_gg_mcp_server_bad_request(&error) =>
            {
                if let Some(object) = resume_params.as_object_mut() {
                    object.insert(
                        "ggMcpServer".to_string(),
                        self.build_gg_mcp_server_session_config(req.runtime_session_id.as_str()),
                    );
                }
                send_bridge_request(
                    &self.inner,
                    &bridge,
                    "session.resume",
                    resume_params,
                    self.inner.config.request_timeout_ms,
                )
                .await?
            }
            Err(error) => return Err(error),
        };

        let bridge_session_id = response
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "session.resume response missing sessionId".to_string(),
                )
            })?;

        let provider_session_ref = response
            .get("providerSessionRef")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| bridge_session_id.clone());
        let canonical_provider_session_ref = response
            .get("claudeCanonicalSessionRef")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or(req.canonical_provider_session_ref);

        let session = Arc::new(ClaudeSessionHandle {
            runtime_session_id: req.runtime_session_id.clone(),
            bridge_session_id,
            provider_session_ref: RwLock::new(provider_session_ref.clone()),
            canonical_provider_session_ref: RwLock::new(canonical_provider_session_ref.clone()),
            bridge,
            active_turn_id: RwLock::new(None),
            bridge_turn_by_runtime_turn: Mutex::new(BTreeMap::new()),
            runtime_turn_by_bridge_turn: Mutex::new(BTreeMap::new()),
            completed_turns: Mutex::new(BTreeMap::new()),
        });
        self.insert_session(req.runtime_session_id.as_str(), session)
            .await?;

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref,
            canonical_provider_session_ref,
        })
    }

    async fn send_turn(
        &self,
        req: ProviderSendTurnRequest,
    ) -> Result<ProviderTurnAck, RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let runtime_turn_id = req.turn_id.clone();

        let result = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.send",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "input": req.input,
                "expectedTurnId": req.expected_turn_id,
            }),
            self.inner.config.request_timeout_ms,
        )
        .await?;

        let bridge_turn_id = result
            .get("turnId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| runtime_turn_id.clone());

        {
            let mut bridge_turn_by_runtime_turn = session.bridge_turn_by_runtime_turn.lock().await;
            bridge_turn_by_runtime_turn.insert(runtime_turn_id.clone(), bridge_turn_id.clone());
        }
        {
            let mut runtime_turn_by_bridge_turn = session.runtime_turn_by_bridge_turn.lock().await;
            runtime_turn_by_bridge_turn.insert(bridge_turn_id, runtime_turn_id.clone());
        }

        {
            let mut active_turn_id = session.active_turn_id.write().await;
            *active_turn_id = Some(runtime_turn_id.clone());
        }

        Ok(ProviderTurnAck {
            runtime_session_id: req.runtime_session_id,
            turn_id: runtime_turn_id,
        })
    }

    async fn interrupt_turn(&self, req: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let bridge_turn_id = self
            .resolve_bridge_turn_id(&session, req.turn_id.as_str())
            .await;
        let _ = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.interrupt",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "turnId": bridge_turn_id,
            }),
            self.inner.config.request_timeout_ms,
        )
        .await?;
        Ok(())
    }

    async fn respond_approval(
        &self,
        req: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let decision = ApprovalDecision::parse(req.decision.as_str())?;
        let decision = match decision {
            ApprovalDecision::Accept => "accept",
            ApprovalDecision::Decline => "decline",
        };
        let bridge_turn_id = self
            .resolve_bridge_turn_id(&session, req.turn_id.as_str())
            .await;

        let mut payload = serde_json::json!({
            "sessionId": session.bridge_session_id,
            "turnId": bridge_turn_id,
            "approvalId": req.approval_id,
            "decision": decision,
        });
        if let Some(updated_input) = req.payload {
            if let Some(object) = payload.as_object_mut() {
                object.insert("updatedInput".to_string(), updated_input);
            }
        }

        let _ = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.approval.respond",
            payload,
            self.inner.config.request_timeout_ms,
        )
        .await?;
        Ok(())
    }

    async fn wait_for_turn(
        &self,
        req: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        let session = self.get_session(req.runtime_session_id.as_str()).await?;
        let runtime_turn_id = req.turn_id.clone();

        if let Some(result) = {
            let completed_turns = session.completed_turns.lock().await;
            completed_turns.get(runtime_turn_id.as_str()).cloned()
        } {
            return Ok(result);
        }

        let timeout_ms = req
            .timeout_ms
            .unwrap_or(self.inner.config.default_wait_timeout_ms as u64)
            .max(1);
        let transport_timeout_ms =
            timeout_ms.saturating_add(self.inner.config.request_timeout_ms.max(1));
        let bridge_turn_id = self
            .resolve_bridge_turn_id(&session, runtime_turn_id.as_str())
            .await;
        if claude_smoke_debug_enabled() {
            eprintln!(
                "[claude-provider] session.wait start runtime_session_id={} runtime_turn_id={} bridge_turn_id={} timeout_ms={} transport_timeout_ms={}",
                req.runtime_session_id,
                runtime_turn_id,
                bridge_turn_id,
                timeout_ms,
                transport_timeout_ms
            );
        }

        let result = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.wait",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "turnId": bridge_turn_id,
                "timeoutMs": timeout_ms,
            }),
            transport_timeout_ms,
        )
        .await?;
        if claude_smoke_debug_enabled() {
            eprintln!(
                "[claude-provider] session.wait returned runtime_session_id={} turn_id={} payload={}",
                req.runtime_session_id, runtime_turn_id, result
            );
        }

        let bridge_turn_id = result
            .get("turnId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation("session.wait response missing turnId".to_string())
            })?;
        let resolved_turn_id = self
            .resolve_runtime_turn_id(&session, bridge_turn_id.as_str())
            .await;
        // Bridge completion events can arrive before session.wait resolves and
        // clear the runtime/bridge turn map. Preserve the runtime turn id from
        // the wait request when the map lookup falls back to the bridge id.
        let turn_id = if resolved_turn_id == bridge_turn_id && runtime_turn_id != bridge_turn_id {
            runtime_turn_id.clone()
        } else {
            resolved_turn_id
        };
        let status = extract_turn_status(result.get("status"));
        let assistant_text = extract_assistant_text(result.get("assistant_text"))
            .or_else(|| extract_assistant_text(result.get("assistantText")));

        let turn_result = ProviderTurnResult {
            runtime_session_id: req.runtime_session_id,
            turn_id: turn_id.clone(),
            status,
            usage: merge_assistant_text_into_usage(result.get("usage").cloned(), assistant_text),
            error: result.get("error").cloned(),
        };

        {
            let mut completed_turns = session.completed_turns.lock().await;
            completed_turns.insert(turn_id.clone(), turn_result.clone());
        }
        {
            let mut active_turn_id = session.active_turn_id.write().await;
            if active_turn_id.as_deref() == Some(turn_id.as_str()) {
                *active_turn_id = None;
            }
        }
        {
            let mut bridge_turn_by_runtime_turn = session.bridge_turn_by_runtime_turn.lock().await;
            if let Some(bridge_turn_id) = bridge_turn_by_runtime_turn.remove(turn_id.as_str()) {
                let mut runtime_turn_by_bridge_turn =
                    session.runtime_turn_by_bridge_turn.lock().await;
                runtime_turn_by_bridge_turn.remove(bridge_turn_id.as_str());
            }
        }

        Ok(turn_result)
    }

    async fn close_session(&self, req: ProviderCloseSessionRequest) -> Result<(), RuntimeError> {
        let Some(session) = self.remove_session(req.runtime_session_id.as_str()).await else {
            return Ok(());
        };

        let _ = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.close",
            serde_json::json!({
                "sessionId": session.bridge_session_id,
                "reason": req.reason,
            }),
            self.inner.config.request_timeout_ms,
        )
        .await;

        self.shutdown_bridges_if_idle().await;

        Ok(())
    }
}
