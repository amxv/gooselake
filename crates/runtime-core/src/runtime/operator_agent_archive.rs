use std::sync::Arc;

use crate::{
    OperationActor, OperatorAgentArchiveAdmission, OperatorAgentArchiveCommand,
    OperatorAgentArchiveReviewCommand, OperatorAgentArchiveReviewInput,
    OperatorAgentArchiveReviewResponse, ProviderCloseSessionRequest,
    ProviderRetirementCloseProofKind, RuntimeError, WorkspaceAgentLifecycleState,
    WorkspaceAgentRecord, WorkspaceLifecycleState,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// /v2 operator archival never interprets a successful SQLite lifecycle
    /// update as proof of provider closure. An immutable provider-close intent
    /// is durable first; only strongly verified native evidence may join the
    /// archive, session closure, lead change and operation receipt in one SQL
    /// transaction. Unknown effects are never sent again on the same key.
    pub async fn archive_workspace_agent_operator(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
        reason: Option<&str>,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        let workspace_id = workspace_id.trim();
        let agent_id = agent_id.trim();
        let reason = reason
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let request = serde_json::json!({
            "workspace_id":workspace_id, "agent_id":agent_id,
            "action":"archive", "reason":reason,
        });
        let hash = crate::normalized_json_hash(&request)?;
        let _mutation = self.session_policy_mutation_lock.lock().await;

        if let Some(replay) = self.store.replay_operator_agent_archive(
            &actor,
            &idempotency_key,
            workspace_id,
            agent_id,
            &hash,
        )? {
            return replay.into_replay_result();
        }
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let repository_lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
        let _repository_guard = repository_lock.read().await;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active
            || self
                .store
                .workspace_retirement_preflight(workspace_id)?
                .blockers
                .iter()
                .any(|b| {
                    matches!(
                        b.code.as_str(),
                        "retirement_in_progress" | "workspace_not_active"
                    )
                })
        {
            return Err(RuntimeError::Conflict(format!(
                "workspace {workspace_id} is inactive or frozen for retirement"
            )));
        }
        self.fence_unresolved_operator_turn_native_effect(agent_id)?;
        if self.store.unresolved_session_edit_rerun(agent_id)?
            || self.store.unresolved_session_compaction(agent_id)?
            || self.store.unresolved_workspace_agent_rebind(agent_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has unresolved historical provider work and cannot be archived".into(),
            ));
        }
        let agent = self.get_workspace_agent(workspace_id, agent_id)?;
        let session = self.get_session(agent_id).await?;
        let provider = if agent.lifecycle_state == WorkspaceAgentLifecycleState::Active {
            Some(
                self.providers
                    .get_enabled(agent.recreation_policy.provider)
                    .ok_or_else(|| {
                        RuntimeError::ProviderNotRegistered(
                            agent.recreation_policy.provider.as_str().to_owned(),
                        )
                    })?,
            )
        } else {
            None
        };
        if session.active_turn_id.is_some() {
            return Err(RuntimeError::Conflict(
                "agent has an active logical turn".into(),
            ));
        }
        let native_ref = agent
            .provider_session_ref
            .clone()
            .or_else(|| session.provider_session_ref.clone())
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "workspace agent {agent_id} has no provider session identity"
                ))
            })?;
        let command = OperatorAgentArchiveCommand {
            operation_id: format!("op_archive_{:032x}", rand::random::<u128>()),
            actor,
            idempotency_key,
            workspace_id: workspace_id.into(),
            agent_id: agent_id.into(),
            reason,
            expected_revision: agent.revision,
            provider: agent.recreation_policy.provider.as_str().into(),
            provider_session_ref: native_ref,
            canonical_provider_session_ref: agent
                .canonical_provider_session_ref
                .clone()
                .or_else(|| session.canonical_provider_session_ref.clone()),
            session_updated_at: session.updated_at,
            normalized_request: request,
            normalized_request_hash: hash,
            requested_at: now_ms(),
        };
        let admission = self.store.admit_operator_agent_archive(&command)?;
        if !matches!(admission, OperatorAgentArchiveAdmission::Execute { .. }) {
            return admission.into_replay_result();
        }
        let provider = provider.ok_or_else(|| {
            RuntimeError::operation_unresolved(
                &command.operation_id,
                "archive provider vanished after durable intent admission",
            )
        })?;
        let evidence = provider
            .close_session_for_retirement(ProviderCloseSessionRequest {
                runtime_session_id: agent_id.into(),
                reason: Some("workspace_agent_archived".into()),
            })
            .await;
        let evidence = match evidence {
            Ok(evidence)
                if evidence.runtime_session_id == agent_id
                    && evidence.provider_session_ref == command.provider_session_ref
                    && matches!(
                        evidence.proof_kind,
                        ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged
                            | ProviderRetirementCloseProofKind::NativeProcessTerminated
                    ) =>
            {
                evidence
            }
            Ok(_) => {
                return self.unresolved_archive_after_native(
                    &command.operation_id,
                    "provider returned a mismatched or unproven native closure acknowledgement",
                )
            }
            Err(error) => {
                return self.unresolved_archive_after_native(
                    &command.operation_id,
                    &format!("provider close has unknown native outcome: {error}"),
                )
            }
        };

        let changed_at = now_ms().max(session.updated_at.saturating_add(1));
        let mut closed = session;
        closed.status = "closed".into();
        closed.closed_at = Some(changed_at);
        closed.failure_code = None;
        closed.failure_message = None;
        closed.updated_at = changed_at;
        let archived = match self
            .store
            .complete_operator_agent_archive(&command, &closed, &evidence, changed_at)
        {
            Ok(record) => record,
            Err(error) => {
                return self.unresolved_archive_after_native(
                    &command.operation_id,
                    &format!(
                        "provider closure verified but atomic archival receipt failed: {error}"
                    ),
                )
            }
        };
        self.sessions.write().await.insert(agent_id.into(), closed);
        Ok((archived, command.operation_id))
    }

    fn unresolved_archive_after_native<T>(
        &self,
        operation_id: &str,
        reason: &str,
    ) -> Result<T, RuntimeError> {
        let mark = self
            .store
            .mark_operator_agent_archive_uncertain(operation_id, reason, now_ms());
        let message = match mark {
            Ok(()) => {
                format!("{reason}; independently verify the native close before evidence review")
            }
            Err(error) => format!("{reason}; quarantine persistence also failed: {error}"),
        };
        Err(RuntimeError::operation_unresolved(operation_id, message))
    }

    pub async fn resolve_operator_agent_archive_review(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
        operation_id: &str,
        input: OperatorAgentArchiveReviewInput,
        actor: OperationActor,
    ) -> Result<OperatorAgentArchiveReviewResponse, RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        // This is evidence-only. No native close or resume is performed by
        // review; a not-closed result is fenced pending exact startup resume.
        let result = self.store.resolve_operator_agent_archive_review(
            &OperatorAgentArchiveReviewCommand {
                operation_id: operation_id.trim().into(),
                workspace_id: workspace_id.trim().into(),
                agent_id: agent_id.trim().into(),
                actor,
                input,
                reviewed_at: now_ms(),
            },
        )?;
        let hydrated = self.store.hydrate_runtime_state()?;
        if let Some(session) = hydrated.sessions.into_iter().find(|row| row.id == agent_id) {
            self.sessions.write().await.insert(agent_id.into(), session);
        }
        Ok(result)
    }
}
