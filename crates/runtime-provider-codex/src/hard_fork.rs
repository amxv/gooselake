use std::collections::HashSet;
use std::sync::Arc;

use runtime_core::{ProviderHardForkEditRerunRequest, ProviderSession, RuntimeError};
use serde_json::json;

use crate::protocol::{build_native_input, extract_thread_id, turn_ids_from_thread_read};
use crate::provider::CodexProvider;

impl CodexProvider {
    pub(super) async fn hard_fork_edit_rerun_verified(
        &self,
        req: ProviderHardForkEditRerunRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        build_native_input(req.edited_input.as_slice())?;
        let (transport, source_thread_id, native_target, model, cwd, developer_instructions) = {
            let sessions = self.inner.sessions.read().await;
            let session = sessions
                .get(req.runtime_session_id.as_str())
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
                })?;
            if session.active_turn_id.is_some() || !session.pending_approvals.is_empty() {
                return Err(RuntimeError::Conflict(format!(
                    "cannot hard-fork busy Codex session {}",
                    req.runtime_session_id
                )));
            }
            let native_target = session
                .logical_to_native_turns
                .get(req.target_turn_id.as_str())
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "native turn for historical logical turn {}",
                        req.target_turn_id
                    ))
                })?;
            (
                Arc::clone(&session.transport),
                session.provider_session_ref.clone(),
                native_target,
                session.model.clone(),
                session.cwd.clone(),
                session.developer_instructions.clone(),
            )
        };

        let source = transport
            .request(
                "thread/read",
                json!({"threadId": source_thread_id, "includeTurns": true}),
            )
            .await?;
        let source_turn_ids = turn_ids_from_thread_read(&source)?;
        let target_index = source_turn_ids
            .iter()
            .position(|turn_id| turn_id == &native_target)
            .ok_or_else(|| {
                RuntimeError::ProtocolViolation(format!(
                    "Codex source thread {source_thread_id} does not contain target turn {native_target}"
                ))
            })?;

        let mut fork_params = json!({
            "threadId": source_thread_id,
            "developerInstructions": developer_instructions,
        });
        if let Some(model) = model.as_deref() {
            fork_params["model"] = json!(model);
        }
        if let Some(cwd) = cwd.as_deref() {
            fork_params["cwd"] = json!(cwd);
        }
        let fork_result = transport.request("thread/fork", fork_params).await?;
        let child_thread_id = extract_thread_id(&fork_result).ok_or_else(|| {
            RuntimeError::ProtocolViolation(
                "Codex thread/fork response missing thread.id".to_string(),
            )
        })?;
        if child_thread_id == source_thread_id {
            return Err(RuntimeError::ProtocolViolation(
                "Codex thread/fork returned source thread identity".to_string(),
            ));
        }

        transport
            .request(
                "thread/revert",
                json!({"threadId": child_thread_id, "beforeTurnId": native_target}),
            )
            .await?;
        let child = transport
            .request(
                "thread/read",
                json!({"threadId": child_thread_id, "includeTurns": true}),
            )
            .await?;
        let child_turn_ids = turn_ids_from_thread_read(&child)?;
        let expected_child_turns = &source_turn_ids[..target_index];
        if child_turn_ids.as_slice() != expected_child_turns {
            return Err(RuntimeError::ProtocolViolation(format!(
                "Codex hard-fork history verification failed (expected prefix {expected_child_turns:?}, actual {child_turn_ids:?})"
            )));
        }

        let rolled_back_native_ids = source_turn_ids[target_index..]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(req.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("codex session {}", req.runtime_session_id))
            })?;
        let rolled_back_logical_ids = session
            .native_to_logical_turns
            .iter()
            .filter_map(|(native, logical)| {
                rolled_back_native_ids
                    .contains(native)
                    .then_some(logical.clone())
            })
            .collect::<HashSet<_>>();

        session
            .native_to_logical_turns
            .retain(|native, _| !rolled_back_native_ids.contains(native));
        session
            .logical_to_native_turns
            .retain(|logical, _| !rolled_back_logical_ids.contains(logical));
        session
            .completed_turns
            .retain(|logical, _| !rolled_back_logical_ids.contains(logical));
        session
            .waiters
            .retain(|logical, _| !rolled_back_logical_ids.contains(logical));
        session
            .pending_approvals
            .retain(|_, approval| !rolled_back_native_ids.contains(&approval.native_turn_id));
        session
            .last_messages
            .retain(|native, _| !rolled_back_native_ids.contains(native));
        session
            .usage_by_native_turn
            .retain(|native, _| !rolled_back_native_ids.contains(native));
        session
            .pending_terminal_by_native
            .retain(|native, _| !rolled_back_native_ids.contains(native));
        session.provider_session_ref = child_thread_id.clone();
        session.canonical_provider_session_ref = Some(child_thread_id.clone());
        session.active_turn_id = None;
        session.model_context_window = None;
        session.last_total_tokens = None;

        Ok(ProviderSession {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: child_thread_id.clone(),
            canonical_provider_session_ref: Some(child_thread_id),
        })
    }
}
