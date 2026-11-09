use std::sync::Arc;

use crate::{
    OperationActor, OperatorAgentRestoreAdmission, OperatorAgentRestoreCommand,
    OperatorAgentRestoreReviewCommand, OperatorAgentRestoreReviewInput,
    OperatorAgentRestoreReviewResponse, ProviderResumeSessionPolicyRequest, RuntimeError,
    WorkspaceAgentLifecycleState, WorkspaceAgentRecord, WorkspaceLifecycleState,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// Every public /v2 provider-backed restore has an immutable native intent
    /// committed before calling resume_session_with_policy. A retry replays an
    /// exact SQLite receipt or returns the unresolved operation coordinate; it
    /// never invokes a second provider restore on the original key.
    pub async fn restore_workspace_agent_operator(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<(WorkspaceAgentRecord, String), RuntimeError> {
        let workspace_id = workspace_id.trim();
        let agent_id = agent_id.trim();
        let request = serde_json::json!({
            "workspace_id":workspace_id, "agent_id":agent_id, "action":"restore",
        });
        let hash = crate::normalized_json_hash(&request)?;
        let _mutation = self.session_policy_mutation_lock.lock().await;

        // Exact replay must precede changed workspace/agent state or a missing
        // provider. The original receipt remains useful after restoration,
        // retirement or a service restart.
        if let Some(replay) = self.store.replay_operator_agent_restore(
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
        let repo_lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
        let _repo_guard = repo_lock.read().await;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active
            || self
                .store
                .workspace_retirement_preflight(workspace_id)?
                .blockers
                .iter()
                .any(|blocker| {
                    matches!(
                        blocker.code.as_str(),
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
                "agent has unresolved historical provider work and cannot be restored".into(),
            ));
        }
        let agent = self.get_workspace_agent(workspace_id, agent_id)?;
        let session = self.get_session(agent_id).await?;
        let provider = if agent.lifecycle_state == WorkspaceAgentLifecycleState::Archived {
            Some(
                self.providers
                    .get_enabled(agent.recreation_policy.provider)
                    .ok_or_else(|| {
                        RuntimeError::ProviderNotRegistered(
                            agent.recreation_policy.provider.as_str().to_string(),
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
        let native_identity = agent
            .provider_session_ref
            .clone()
            .or_else(|| session.provider_session_ref.clone())
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "workspace agent {agent_id} has no provider session identity"
                ))
            })?;
        let command = OperatorAgentRestoreCommand {
            operation_id: format!("op_restore_{:032x}", rand::random::<u128>()),
            actor,
            idempotency_key,
            workspace_id: workspace_id.into(),
            agent_id: agent_id.into(),
            expected_revision: agent.revision,
            provider: agent.recreation_policy.provider.as_str().into(),
            provider_session_ref: native_identity.clone(),
            canonical_provider_session_ref: agent
                .canonical_provider_session_ref
                .clone()
                .or_else(|| session.canonical_provider_session_ref.clone()),
            session_updated_at: session.updated_at,
            normalized_request: request,
            normalized_request_hash: hash,
            requested_at: now_ms(),
        };
        let admitted = self.store.admit_operator_agent_restore(&command)?;
        if !matches!(admitted, OperatorAgentRestoreAdmission::Execute { .. }) {
            return admitted.into_replay_result();
        }
        // An executing restore necessarily passed archived-state and enabled
        // provider preflight before SQLite admitted its first native effect.
        let provider = provider.ok_or_else(|| {
            RuntimeError::operation_unresolved(
                &command.operation_id,
                "provider was not preflighted for the admitted native restore",
            )
        })?;
        let native = provider
            .resume_session_with_policy(ProviderResumeSessionPolicyRequest {
                runtime_session_id: agent_id.into(),
                provider_session_ref: native_identity,
                canonical_provider_session_ref: command.canonical_provider_session_ref.clone(),
                cwd: Some(agent.recreation_policy.authoritative_cwd.clone()),
                model: agent.recreation_policy.model.clone(),
                launch_policy: agent.recreation_policy.launch_policy(),
                current_preferences: agent.recreation_policy.current_preferences.clone(),
                metadata: Some(agent.metadata.clone()),
            })
            .await;
        let native = match native {
            Ok(resumed)
                if resumed.runtime_session_id == agent_id
                    && !resumed.provider_session_ref.trim().is_empty() =>
            {
                resumed
            }
            Ok(_) => {
                return self.unresolved_restore_after_native(
                    &command.operation_id,
                    "provider returned invalid native restore identity",
                )
            }
            Err(error) => {
                return self.unresolved_restore_after_native(
                    &command.operation_id,
                    &format!("provider native restore returned an unproven outcome: {error}"),
                )
            }
        };

        let changed_at = now_ms().max(session.updated_at.saturating_add(1));
        let mut ready = session;
        ready.provider = agent.recreation_policy.provider.as_str().into();
        ready.provider_session_ref = Some(native.provider_session_ref);
        ready.canonical_provider_session_ref = native.canonical_provider_session_ref;
        ready.cwd = Some(agent.recreation_policy.authoritative_cwd.clone());
        ready.model = agent.recreation_policy.model.clone();
        ready.permission_mode = agent.recreation_policy.permission_intent.resolved_mode();
        ready.system_prompt = agent.recreation_policy.system_prompt.clone();
        ready.status = "ready".into();
        ready.closed_at = None;
        ready.failure_code = None;
        ready.failure_message = None;
        ready.updated_at = changed_at;
        let restored = match self
            .store
            .complete_operator_agent_restore(&command, &ready, changed_at)
        {
            Ok(record) => record,
            Err(error) => {
                return self.unresolved_restore_after_native(
                    &command.operation_id,
                    &format!("provider resumed but atomic agent restore receipt failed: {error}"),
                )
            }
        };
        self.sessions.write().await.insert(agent_id.into(), ready);
        Ok((restored, command.operation_id))
    }

    fn unresolved_restore_after_native<T>(
        &self,
        operation_id: &str,
        reason: &str,
    ) -> Result<T, RuntimeError> {
        let mark = self
            .store
            .mark_operator_agent_restore_uncertain(operation_id, reason, now_ms());
        let message = match mark {
            Ok(()) => {
                format!("{reason}; independently inspect the native attachment before reviewing")
            }
            Err(error) => format!("{reason}; quarantine persistence also failed: {error}"),
        };
        Err(RuntimeError::operation_unresolved(operation_id, message))
    }

    pub async fn resolve_operator_agent_restore_review(
        self: &Arc<Self>,
        workspace_id: &str,
        agent_id: &str,
        operation_id: &str,
        input: OperatorAgentRestoreReviewInput,
        actor: OperationActor,
    ) -> Result<OperatorAgentRestoreReviewResponse, RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        // Review is evidence-only: never communicate with the provider from
        // this endpoint. On applied resolution, a *new* startup will verify
        // native reattachment before clearing its durable recovery gate.
        let result = self.store.resolve_operator_agent_restore_review(
            &OperatorAgentRestoreReviewCommand {
                operation_id: operation_id.trim().into(),
                workspace_id: workspace_id.trim().into(),
                agent_id: agent_id.trim().into(),
                actor,
                input,
                reviewed_at: now_ms(),
            },
        )?;
        // Reflect the durable review fence in this live process. Even when
        // replaying an old review, do not leave the in-memory session 'closed'
        // while the authoritative restored agent awaits startup reattachment.
        let hydrated = self.store.hydrate_runtime_state()?;
        if let Some(session) = hydrated.sessions.into_iter().find(|row| row.id == agent_id) {
            self.sessions
                .write()
                .await
                .insert(agent_id.to_string(), session);
        }
        Ok(result)
    }
}
