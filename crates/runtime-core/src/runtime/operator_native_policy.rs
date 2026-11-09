use std::sync::Arc;

use serde_json::Value;

use crate::{
    OperationActor, OperatorNativePolicyAdmission, OperatorNativePolicyCommand,
    OperatorNativePolicyKind, OperatorNativePolicyReviewCommand, OperatorNativePolicyReviewInput,
    OperatorNativePolicyReviewResponse, ProviderCapabilitySupport, ProviderKind,
    ProviderPermissionIntent, ProviderPermissionMutationRequest, ProviderPermissionMutationResult,
    ProviderSessionPreferences, ProviderSessionPreferencesMutationRequest,
    ProviderSessionPreferencesMutationResult, RuntimeError,
};

use super::helpers::now_ms;
use super::workspace_agents::validate_claude_mutable_permission;
use super::RuntimeSessionManager;

#[derive(Clone)]
enum OperatorPolicySelection {
    Preferences(ProviderSessionPreferences),
    Permission(ProviderPermissionIntent),
}

impl OperatorPolicySelection {
    fn kind(&self) -> OperatorNativePolicyKind {
        match self {
            Self::Preferences(_) => OperatorNativePolicyKind::Preferences,
            Self::Permission(_) => OperatorNativePolicyKind::Permission,
        }
    }

    fn normalized_request(&self, agent_id: &str, revision: u64) -> Value {
        match self {
            Self::Preferences(value) => serde_json::json!({
                "agent_id":agent_id,"kind":self.kind().as_str(),
                "expected_revision":revision,"current_preferences":value,
            }),
            Self::Permission(value) => serde_json::json!({
                "agent_id":agent_id,"kind":self.kind().as_str(),
                "expected_revision":revision,"permission_intent":value,
            }),
        }
    }
}

impl RuntimeSessionManager {
    /// A manual outcome assertion never re-invokes the native provider. Both
    /// the original key and any later model work consume the reconciled durable
    /// result only after an explicit provider reattachment on next startup.
    pub async fn resolve_operator_native_policy_review(
        self: &Arc<Self>,
        agent_id: &str,
        operation_id: &str,
        input: OperatorNativePolicyReviewInput,
        actor: OperationActor,
    ) -> Result<OperatorNativePolicyReviewResponse, RuntimeError> {
        let _guard = self.session_policy_mutation_lock.lock().await;
        let command = OperatorNativePolicyReviewCommand {
            operation_id: operation_id.to_string(),
            agent_id: agent_id.to_string(),
            actor,
            input,
            reviewed_at: now_ms(),
        };
        let result = self.store.resolve_operator_native_policy_review(&command)?;
        // The SQL review and resource release are one commit. Refresh the
        // active projection while still serialized against other native work.
        let hydrated = self.store.hydrate_runtime_state()?;
        if let Some(session) = hydrated.sessions.into_iter().find(|row| row.id == agent_id) {
            self.sessions
                .write()
                .await
                .insert(agent_id.to_string(), session);
        }
        Ok(result)
    }

    pub async fn mutate_workspace_agent_preferences_operator(
        self: &Arc<Self>,
        request: ProviderSessionPreferencesMutationRequest,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<(ProviderSessionPreferencesMutationResult, String), RuntimeError> {
        let expected_revision = request.expected_revision.ok_or_else(|| {
            RuntimeError::InvalidState("operator native preferences require a revision".into())
        })?;
        let (receipt, operation_id) = self
            .mutate_operator_native_policy(
                &request.runtime_session_id,
                expected_revision,
                OperatorPolicySelection::Preferences(request.current_preferences),
                actor,
                idempotency_key,
            )
            .await?;
        let parsed = serde_json::from_value(receipt).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "invalid exact provider preference receipt: {error}"
            ))
        })?;
        Ok((parsed, operation_id))
    }

    pub async fn mutate_workspace_agent_permission_operator(
        self: &Arc<Self>,
        request: ProviderPermissionMutationRequest,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<(ProviderPermissionMutationResult, String), RuntimeError> {
        let expected_revision = request.expected_revision.ok_or_else(|| {
            RuntimeError::InvalidState("operator native permission requires a revision".into())
        })?;
        let (receipt, operation_id) = self
            .mutate_operator_native_policy(
                &request.runtime_session_id,
                expected_revision,
                OperatorPolicySelection::Permission(request.permission_intent),
                actor,
                idempotency_key,
            )
            .await?;
        let parsed = serde_json::from_value(receipt).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "invalid exact provider permission receipt: {error}"
            ))
        })?;
        Ok((parsed, operation_id))
    }

    /// All native and durable phases run while the policy mutation lock is
    /// held. Replay checks run *before* reading mutable revision or provider
    /// status; a lost response still returns the original successful receipt.
    async fn mutate_operator_native_policy(
        self: &Arc<Self>,
        agent_id: &str,
        expected_revision: u64,
        selection: OperatorPolicySelection,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<(Value, String), RuntimeError> {
        let normalized_request = selection.normalized_request(agent_id, expected_revision);
        let normalized_request_hash = crate::normalized_json_hash(&normalized_request)?;
        let _guard = self.session_policy_mutation_lock.lock().await;
        if let Some(replay) = self.store.replay_operator_native_policy(
            &actor,
            &idempotency_key,
            agent_id,
            selection.kind(),
            &normalized_request_hash,
        )? {
            return replay.into_replay_result();
        }

        let agent = self.get_active_workspace_agent_by_id(agent_id)?;
        if agent.revision != expected_revision {
            return Err(RuntimeError::Conflict(format!(
                "workspace agent {} revision conflict: expected {expected_revision}, current {}",
                agent.agent_id, agent.revision
            )));
        }
        if let Some(operation_id) = self.store.retiring_operation_for_session(agent_id)? {
            return Err(RuntimeError::Conflict(format!(
                "policy mutation is frozen by workspace retirement {operation_id}"
            )));
        }
        if self.store.unresolved_workspace_agent_rebind(agent_id)?
            || self.store.unresolved_session_compaction(agent_id)?
            || self.store.unresolved_session_edit_rerun(agent_id)?
        {
            return Err(RuntimeError::Conflict(
                "provider attachment has unresolved native work".into(),
            ));
        }
        self.fence_unresolved_operator_turn_native_effect(agent_id)?;
        let provider_kind = agent.recreation_policy.provider;
        let provider = self
            .providers
            .get(provider_kind)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider_kind.as_str().into()))?;
        let capabilities = provider.capabilities();
        match &selection {
            OperatorPolicySelection::Preferences(_)
                if !matches!(provider_kind, ProviderKind::Claude | ProviderKind::Codex)
                    || capabilities.session_preferences != ProviderCapabilitySupport::Supported =>
            {
                return Err(RuntimeError::Unsupported(
                    "provider does not support durable mutable session preferences".into(),
                ));
            }
            OperatorPolicySelection::Permission(intent) => {
                if provider_kind != ProviderKind::Claude
                    || capabilities.permission_mutation != ProviderCapabilitySupport::Supported
                {
                    return Err(RuntimeError::Unsupported(
                        "provider does not support durable mutable permissions".into(),
                    ));
                }
                validate_claude_mutable_permission(intent)?;
            }
            _ => {}
        }
        let mut session = self.get_session(agent_id).await?;
        if session.active_turn_id.is_some()
            || matches!(session.status.as_str(), "closed" | "failed")
        {
            return Err(RuntimeError::InvalidState(
                "provider policy cannot change during an active or closed session".into(),
            ));
        }
        if session.provider_session_ref.is_none()
            || session.provider != provider_kind.as_str()
            || session.cwd.as_deref() != Some(agent.recreation_policy.authoritative_cwd.as_str())
        {
            return Err(RuntimeError::Conflict(
                "provider policy requires the exact persisted native attachment".into(),
            ));
        }

        let command = OperatorNativePolicyCommand {
            operation_id: format!("op_policy_{:032x}", rand::random::<u128>()),
            actor,
            idempotency_key,
            workspace_id: agent.workspace_id.clone(),
            agent_id: agent.agent_id.clone(),
            kind: selection.kind(),
            expected_revision,
            provider: session.provider.clone(),
            provider_session_ref: session.provider_session_ref.clone(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            session_updated_at: session.updated_at,
            normalized_request,
            normalized_request_hash,
            requested_at: now_ms(),
        };
        match self.store.admit_operator_native_policy(&command)? {
            OperatorNativePolicyAdmission::Execute { .. } => {}
            replay => return replay.into_replay_result(),
        }

        // Deliberately no rollback or automatic retry after a provider was
        // invoked. Its error may be a post-effect transport failure.
        let native = match &selection {
            OperatorPolicySelection::Preferences(value) => provider
                .mutate_session_preferences(ProviderSessionPreferencesMutationRequest {
                    runtime_session_id: agent_id.into(),
                    expected_revision: None,
                    current_preferences: value.clone(),
                })
                .await
                .and_then(|observed| {
                    if observed.current_preferences != *value {
                        return Err(RuntimeError::Conflict(
                            "provider acknowledged a different native preference than requested"
                                .into(),
                        ));
                    }
                    Ok(())
                }),
            OperatorPolicySelection::Permission(value) => provider
                .mutate_session_permission(ProviderPermissionMutationRequest {
                    runtime_session_id: agent_id.into(),
                    expected_revision: None,
                    permission_intent: value.clone(),
                })
                .await
                .and_then(|observed| {
                    if observed.permission_intent != *value {
                        return Err(RuntimeError::Conflict(
                            "provider acknowledged a different native permission than requested"
                                .into(),
                        ));
                    }
                    Ok(())
                }),
        };
        if native.is_err() {
            let _ = self.store.mark_operator_native_policy_uncertain(
                &command.operation_id,
                "provider returned without verified native policy receipt",
                now_ms(),
            );
            return Err(RuntimeError::operation_unresolved(
                command.operation_id,
                "provider policy outcome may have changed natively; inspect the exact operation",
            ));
        }

        let mut policy = agent.recreation_policy;
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| RuntimeError::InvalidState("agent revision overflow".into()))?;
        let exact = match selection {
            OperatorPolicySelection::Preferences(value) => {
                policy.current_preferences = value.clone();
                serde_json::to_value(ProviderSessionPreferencesMutationResult {
                    revision: next_revision,
                    current_preferences: value,
                })
            }
            OperatorPolicySelection::Permission(value) => {
                policy.permission_intent = value.clone();
                session.permission_mode = value.resolved_mode();
                serde_json::to_value(ProviderPermissionMutationResult {
                    revision: next_revision,
                    permission_intent: value,
                })
            }
        }
        .map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid provider policy receipt: {error}"))
        })?;
        let at_ms = now_ms();
        session.updated_at = at_ms;
        if self
            .store
            .complete_operator_native_policy(&command, &session, &policy, &exact, at_ms)
            .is_err()
        {
            let _ = self.store.mark_operator_native_policy_uncertain(
                &command.operation_id,
                "provider acknowledged but durable policy receipt did not commit",
                now_ms(),
            );
            return Err(RuntimeError::operation_unresolved(
                command.operation_id,
                "provider acknowledged policy change, but the durable receipt is unresolved",
            ));
        }
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session);
        Ok((exact, command.operation_id))
    }
}
