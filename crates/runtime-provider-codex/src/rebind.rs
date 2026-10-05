use std::path::PathBuf;
use std::sync::Arc;

use runtime_core::{ProviderWorkspaceRebindEvidence, ProviderWorkspaceRebindRequest, RuntimeError};
use serde_json::{json, Value};

use crate::protocol::extract_thread_id;
use crate::provider::CodexProvider;
use crate::transport::CodexTransport;

impl CodexProvider {
    pub(super) async fn rebind_workspace_with_evidence(
        &self,
        req: ProviderWorkspaceRebindRequest,
    ) -> Result<ProviderWorkspaceRebindEvidence, RuntimeError> {
        let destination_cwd = req.cwd.trim();
        if destination_cwd.is_empty() {
            return Err(RuntimeError::InvalidState(
                "Codex workspace rebind requires a non-empty destination cwd".to_string(),
            ));
        }

        let (transport, thread_id, old_cwd, model, developer_instructions) = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            if session.active_turn_id.is_some() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::Conflict(format!(
                    "cannot rebind busy Codex session {}",
                    req.runtime_session_id
                )));
            }
            (
                Arc::clone(&session.transport),
                session.provider_session_ref.clone(),
                session.cwd.clone(),
                session.model.clone(),
                session.developer_instructions.clone(),
            )
        };

        let unsubscribe = transport
            .request("thread/unsubscribe", json!({"threadId": thread_id}))
            .await?;
        let unsubscribe_status = unsubscribe
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(
            unsubscribe_status,
            "unsubscribed" | "notSubscribed" | "notLoaded"
        ) {
            return Err(RuntimeError::ProtocolViolation(format!(
                "Codex thread/unsubscribe returned unacknowledged status {unsubscribe_status:?}"
            )));
        }

        let destination = Self::resume_rebind_target(
            &transport,
            thread_id.as_str(),
            destination_cwd,
            model.as_deref(),
            developer_instructions.as_str(),
        )
        .await;

        let effective_cwd = match destination {
            Ok(response) => {
                match Self::validate_rebind_response(&response, thread_id.as_str(), destination_cwd)
                {
                    Ok(cwd) => cwd,
                    Err(error) => {
                        return self
                            .rollback_rebind(
                                &req.runtime_session_id,
                                &transport,
                                thread_id.as_str(),
                                old_cwd.as_deref(),
                                model.as_deref(),
                                developer_instructions.as_str(),
                                error,
                            )
                            .await;
                    }
                }
            }
            Err(error) => {
                return self
                    .rollback_rebind(
                        &req.runtime_session_id,
                        &transport,
                        thread_id.as_str(),
                        old_cwd.as_deref(),
                        model.as_deref(),
                        developer_instructions.as_str(),
                        error,
                    )
                    .await;
            }
        };

        if let Some(session) = self
            .inner
            .sessions
            .write()
            .await
            .get_mut(req.runtime_session_id.as_str())
        {
            session.cwd = Some(effective_cwd.clone());
        }

        Ok(ProviderWorkspaceRebindEvidence {
            runtime_session_id: req.runtime_session_id,
            cwd: effective_cwd,
            provider_session_ref: Some(thread_id),
        })
    }

    async fn resume_rebind_target(
        transport: &Arc<CodexTransport>,
        thread_id: &str,
        cwd: &str,
        model: Option<&str>,
        developer_instructions: &str,
    ) -> Result<Value, RuntimeError> {
        let mut params = json!({
            "threadId": thread_id,
            "cwd": cwd,
            "developerInstructions": developer_instructions,
        });
        if let Some(model) = model.map(str::trim).filter(|value| !value.is_empty()) {
            params["model"] = json!(model);
        }
        transport.request("thread/resume", params).await
    }

    fn validate_rebind_response(
        response: &Value,
        expected_thread_id: &str,
        expected_cwd: &str,
    ) -> Result<String, RuntimeError> {
        let returned_thread = extract_thread_id(response).ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "Codex workspace rebind response missing thread.id".to_string(),
            )
        })?;
        let effective_cwd = response
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(
                    "Codex workspace rebind response missing required top-level cwd evidence"
                        .to_string(),
                )
            })?;
        if returned_thread != expected_thread_id
            || !canonical_paths_equal(expected_cwd, effective_cwd.as_str())
        {
            return Err(RuntimeError::ProtocolViolation(format!(
                "Codex workspace rebind returned mismatched evidence (expected thread={expected_thread_id}, cwd={expected_cwd}; actual thread={returned_thread}, cwd={effective_cwd})"
            )));
        }
        Ok(effective_cwd)
    }

    async fn rollback_rebind(
        &self,
        runtime_session_id: &str,
        transport: &Arc<CodexTransport>,
        thread_id: &str,
        old_cwd: Option<&str>,
        model: Option<&str>,
        developer_instructions: &str,
        destination_error: RuntimeError,
    ) -> Result<ProviderWorkspaceRebindEvidence, RuntimeError> {
        let Some(old_cwd) = old_cwd else {
            return Err(RuntimeError::provider_dispatch_unknown(
                "reassignment_recovery_required",
                format!(
                    "Codex workspace rebind failed and no previous cwd exists for verified rollback: {destination_error}"
                ),
            ));
        };

        if let Ok(response) =
            Self::resume_rebind_target(transport, thread_id, old_cwd, model, developer_instructions)
                .await
        {
            if Self::validate_rebind_response(&response, thread_id, old_cwd).is_ok() {
                if let Some(session) = self
                    .inner
                    .sessions
                    .write()
                    .await
                    .get_mut(runtime_session_id)
                {
                    session.cwd = Some(old_cwd.to_string());
                }
                return Err(destination_error);
            }
        }

        Err(RuntimeError::provider_dispatch_unknown(
            "reassignment_recovery_required",
            format!(
                "Codex workspace rebind failed and the previous binding could not be verified: {destination_error}"
            ),
        ))
    }
}

pub(super) fn canonical_paths_equal(left: &str, right: &str) -> bool {
    let canonicalize =
        |value: &str| std::fs::canonicalize(value).unwrap_or_else(|_| PathBuf::from(value));
    canonicalize(left) == canonicalize(right)
}
