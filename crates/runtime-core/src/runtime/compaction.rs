use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;

use crate::{
    ProviderCapabilitySupport, ProviderCompactSessionOutcome, ProviderCompactSessionRequest,
    ProviderKind, ProviderRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
    SessionCompactionRun, WorkspaceAgentLifecycleState,
};

use super::{helpers::now_ms, RuntimeSessionManager};

impl RuntimeSessionManager {
    pub fn session_compaction_run(
        &self,
        agent_id: &str,
        run_id: &str,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        self.store
            .get_workspace_agent_by_id(agent_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
        let run = self
            .store
            .get_session_compaction(run_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("compaction run {run_id}")))?;
        if run.agent_id != agent_id {
            return Err(RuntimeError::NotFound(format!("compaction run {run_id}")));
        }
        Ok(run)
    }

    /// A trusted operator can settle a quarantined native outcome only after
    /// verifying it externally. This never redispatches the provider request.
    pub fn resolve_compaction_review(
        &self,
        agent_id: &str,
        run_id: &str,
        phase: &str,
        evidence: &str,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        self.session_compaction_run(agent_id, run_id)?;
        self.store
            .resolve_session_compaction_review(run_id, phase, evidence, now_ms())
    }

    /// A single-flight, crash-safe manager operation. Provider accepted means
    /// only that native compaction started; follow-up observations settle it.
    pub async fn compact_workspace_agent(
        self: &Arc<Self>,
        agent_id: &str,
        idempotency_key: Option<String>,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        let _guard = self.session_policy_mutation_lock.lock().await;
        if let Some(key) = idempotency_key.as_deref() {
            if key.trim().is_empty() || key.len() > 128 {
                return Err(RuntimeError::InvalidState(
                    "Idempotency-Key must be 1-128 nonblank characters".into(),
                ));
            }
            if let Some(existing) = self.store.get_session_compaction_by_key(agent_id, key)? {
                return Ok(existing);
            }
        }
        let agent = self.get_active_workspace_agent_by_id(agent_id)?;
        if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active {
            return Err(RuntimeError::Conflict("agent is archived".into()));
        }
        let session = self.get_session(agent_id).await?;
        if session.status != "ready" || session.active_turn_id.is_some() {
            return Err(RuntimeError::Conflict(
                "manual compaction requires a ready idle agent".into(),
            ));
        }
        if self.store.unresolved_workspace_agent_rebind(agent_id)? {
            return Err(RuntimeError::Conflict(
                "cannot compact during unresolved workspace rebinding".into(),
            ));
        }
        let provider = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation("unknown provider for session compaction".into())
        })?;
        if provider != agent.recreation_policy.provider {
            return Err(RuntimeError::ProtocolViolation(
                "agent/provider authority mismatch during compaction".into(),
            ));
        }
        let adapter = self
            .providers
            .get(provider)
            .ok_or_else(|| RuntimeError::ProviderNotRegistered(provider.as_str().into()))?;
        if adapter.capabilities().manual_compact != ProviderCapabilitySupport::Supported {
            return Err(RuntimeError::Unsupported(format!(
                "provider {} does not support manual compaction",
                provider.as_str()
            )));
        }
        let provider_session_ref = session.provider_session_ref.clone().ok_or_else(|| {
            RuntimeError::InvalidState("compaction requires a native provider attachment".into())
        })?;
        let now = now_ms();
        let run = SessionCompactionRun {
            run_id: self.allocate_id("cmp", provider.as_str()),
            agent_id: agent_id.to_owned(),
            provider,
            provider_session_ref,
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            agent_revision: agent.revision,
            expected_session_updated_at: session.updated_at,
            idempotency_key,
            phase: "intended".into(),
            outcome: None,
            error_code: None,
            review_evidence: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.store.begin_session_compaction(&run)?;
        // Subscribe before native dispatch to preserve an observation that
        // arrives ahead of the provider's synchronous acceptance response.
        let events = adapter.subscribe_events();
        let native_request = ProviderCompactSessionRequest {
            runtime_session_id: agent_id.into(),
        };
        let native_result = match adapter.compact_session(native_request.clone()).await {
            Err(RuntimeError::NotFound(_)) => {
                // The local provider adapter may have been recreated. Resuming
                // its exact durable policy is safe; repeating an ambiguous
                // native compact request is not.
                let resume = self.provider_resume_request_for_session(
                    &session,
                    run.provider_session_ref.clone(),
                    run.canonical_provider_session_ref.clone(),
                );
                match resume {
                    Ok(resume) => match adapter.resume_session_with_policy(resume).await {
                        Ok(observed)
                            if observed.runtime_session_id == agent_id
                                && observed.provider_session_ref == run.provider_session_ref
                                && observed.canonical_provider_session_ref
                                    == run.canonical_provider_session_ref =>
                        {
                            adapter.compact_session(native_request).await
                        }
                        Ok(_) => Err(RuntimeError::ProtocolViolation(
                            "compaction resume changed native provider session identity".into(),
                        )),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            }
            result => result,
        };
        let (phase, outcome, error_code) = match &native_result {
            Ok(ProviderCompactSessionOutcome::Accepted) => (
                "accepted",
                Some(ProviderCompactSessionOutcome::Accepted),
                None,
            ),
            Ok(ProviderCompactSessionOutcome::NotPerformed) => (
                "not_performed",
                Some(ProviderCompactSessionOutcome::NotPerformed),
                None,
            ),
            Err(_) => (
                "manual_review",
                None,
                Some("native_compaction_outcome_unknown"),
            ),
        };
        let persisted = self.store.transition_session_compaction(
            &run.run_id,
            phase,
            outcome,
            error_code,
            now_ms(),
        )?;
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                agent_id,
                Some(agent_id),
                None,
                "compaction.request_state_changed",
                RuntimeEventCriticality::Critical,
                serde_json::json!({"run_id": run.run_id, "phase": persisted.phase}),
            )
            .await;
        match native_result {
            Ok(ProviderCompactSessionOutcome::Accepted) => {
                if let Some(events) = events {
                    self.spawn_compaction_observer(persisted.run_id.clone(), agent_id.into(), events);
                    Ok(persisted)
                } else {
                    // An adapter that accepts compaction but cannot surface
                    // terminal events must not leave an unresolvable forever-
                    // accepted request in the durable manager state.
                    self.store.transition_session_compaction(
                        &persisted.run_id,
                        "manual_review",
                        None,
                        Some("provider_compaction_observer_unavailable"),
                        now_ms(),
                    )
                }
            }
            Ok(ProviderCompactSessionOutcome::NotPerformed) => Ok(persisted),
            Err(error) => Err(RuntimeError::provider_dispatch_unknown(
                "native_compaction_outcome_unknown",
                format!(
                    "manual compaction {} may have reached the provider; inspect its durable status instead of resubmitting: {error}",
                    persisted.run_id
                ),
            )),
        }
    }

    fn spawn_compaction_observer(
        self: &Arc<Self>,
        run_id: String,
        agent_id: String,
        mut events: broadcast::Receiver<ProviderRuntimeEvent>,
    ) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let observation = tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    match events.recv().await {
                        Ok(ProviderRuntimeEvent::ContextCompactionObserved {
                            runtime_session_id,
                            phase,
                            turn_id,
                            trigger,
                            ..
                        }) if runtime_session_id == agent_id
                            && (turn_id.is_none() || trigger.as_deref() == Some("manual")) =>
                        {
                            match phase.to_ascii_lowercase().as_str() {
                                "completed" => return "completed",
                                "failed" | "cancelled" => return "failed",
                                _ => {}
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => return "manual_review",
                        Err(broadcast::error::RecvError::Closed) => return "manual_review",
                        _ => {}
                    }
                }
            })
            .await
            .unwrap_or("manual_review");
            let outcome =
                (observation == "completed").then_some(ProviderCompactSessionOutcome::Accepted);
            let error_code =
                (observation != "completed").then_some("compaction_evidence_missing_or_failed");
            if let Ok(settled) = manager.store.transition_session_compaction(
                &run_id,
                observation,
                outcome,
                error_code,
                now_ms(),
            ) {
                let _ = manager
                    .append_event(
                        RuntimeEventScope::Session,
                        &agent_id,
                        Some(&agent_id),
                        None,
                        "compaction.observed",
                        RuntimeEventCriticality::Critical,
                        serde_json::json!({"run_id": run_id, "phase": settled.phase}),
                    )
                    .await;
                if observation == "completed" {
                    let _ = manager.refresh_session_context_limit(&agent_id, None).await;
                }
            }
        });
    }
}
