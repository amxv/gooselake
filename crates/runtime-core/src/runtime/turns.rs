use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::MutexGuard;

use crate::{
    ApprovalRecord, OperationActor, OperatorUserTurnCommand, OperatorUserTurnReplay,
    PersistedUserInputSnapshot, ProviderDispatchOutcome, ProviderKind, ProviderRuntimeEvent,
    ProviderSendTurnRequest, ProviderTurnStatus, ProviderWaitTurnRequest, RuntimeError,
    RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope, TurnAdmissionRecord,
    TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
    TurnRecord,
};

use super::helpers::now_ms;
use super::turn_dispatch_transitions::is_valid_dispatch_transition;
use super::{RuntimeSessionManager, SendTurnAccepted, SendTurnInput};

impl RuntimeSessionManager {
    /// Unknown native user-turn dispatch holds an agent-level durable claim.
    /// Other provider-native mutations must honor it too: a stale in-memory
    /// `active_turn_id` cannot establish that the original turn is finished.
    pub(super) fn fence_unresolved_operator_turn_native_effect(
        &self,
        agent_id: &str,
    ) -> Result<(), RuntimeError> {
        if let Some(operation_id) = self
            .store
            .unresolved_operator_user_turn_for_agent(agent_id)?
        {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "an operator turn has unresolved native dispatch; reconcile its operation before changing this provider attachment",
            ));
        }
        Ok(())
    }

    pub async fn send_turn(
        self: &Arc<Self>,
        session_id: &str,
        input: SendTurnInput,
    ) -> Result<SendTurnAccepted, RuntimeError> {
        let policy_mutation_guard = self.session_policy_mutation_lock.lock().await;
        self.send_turn_under_policy_guard(
            session_id,
            input,
            None,
            None,
            None,
            policy_mutation_guard,
        )
        .await
    }

    /// `/v2` user turns share the canonical admission and provider dispatch
    /// path, but first claim the trusted operator's retry key alongside that
    /// exact logical turn. A retry never re-enters provider dispatch.
    pub async fn send_operator_user_turn(
        self: &Arc<Self>,
        session_id: &str,
        input: SendTurnInput,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<(SendTurnAccepted, String), RuntimeError> {
        let normalized_request = serde_json::json!({
            "agent_id": session_id,
            "input": input.input,
            "expected_turn_id": input.expected_turn_id,
            "permission_mode": input.permission_mode,
            "turn_settings": input.turn_settings,
        });
        let normalized_request_hash = crate::normalized_json_hash(&normalized_request)?;
        let policy_guard = self.session_policy_mutation_lock.lock().await;
        // Replay precedes active-session, retirement and even workspace
        // existence checks. A previously accepted turn must remain replayable
        // after it finishes or its agent is archived.
        if let Some(replay) = self.store.operator_user_turn_replay(
            &actor,
            &idempotency_key,
            session_id,
            &normalized_request_hash,
        )? {
            return replay.into_result();
        }
        let agent = self.get_active_workspace_agent_by_id(session_id)?;
        // Selecting a *new* persistent thinking preference invokes provider
        // work before ordinary turn admission. A keyed operator turn cannot
        // safely perform that separate effect without its own journal. Require
        // preselection via the revisioned policy endpoint instead of claiming
        // this key protects an unjournaled native preference mutation.
        if let Some(effort) = input.turn_settings.thinking_effort {
            if agent.recreation_policy.current_preferences.thinking_effort != Some(effort) {
                return Err(RuntimeError::Unsupported(
                    "keyed operator turns require the selected thinking effort to be the agent's current preference; update the agent preference separately first".into(),
                ));
            }
        }
        let command = OperatorUserTurnCommand {
            operation_id: format!("op_turn_{:032x}", rand::random::<u128>()),
            agent_id: agent.agent_id,
            workspace_id: agent.workspace_id,
            actor: actor.clone(),
            idempotency_key: idempotency_key.clone(),
            normalized_request,
            normalized_request_hash: normalized_request_hash.clone(),
            requested_at: now_ms(),
        };

        let result = self
            .send_turn_under_policy_guard(
                session_id,
                input,
                None,
                None,
                Some(&command),
                policy_guard,
            )
            .await;
        match result {
            Ok(accepted) => {
                self.store
                    .complete_operator_user_turn(&command.operation_id, &accepted, now_ms())
                    .map_err(|_| RuntimeError::operation_unresolved(
                        command.operation_id.clone(),
                        "provider accepted the original logical turn but exact operator receipt persistence is unresolved",
                    ))?;
                Ok((accepted, command.operation_id))
            }
            Err(error) => {
                // Only an operation actually admitted by this caller can be
                // settled. A preflight failure leaves no journal and a race
                // against another runtime must not rewrite its native effect.
                let admitted = self.store.operator_user_turn_replay(
                    &actor,
                    &idempotency_key,
                    session_id,
                    &normalized_request_hash,
                )?;
                if !matches!(
                    admitted.as_ref(),
                    Some(OperatorUserTurnReplay::Unresolved { operation_id })
                        if operation_id == &command.operation_id
                ) {
                    return Err(error);
                }
                if let RuntimeError::ProviderDispatch {
                    code,
                    outcome: "not_dispatched",
                    message,
                } = &error
                {
                    self.store
                        .fail_operator_user_turn_not_dispatched(
                            &command.operation_id,
                            code,
                            message,
                            now_ms(),
                        )
                        .map_err(|_| RuntimeError::operation_unresolved(
                            command.operation_id.clone(),
                            "original native dispatch was not performed but its failure receipt could not be saved",
                        ))?;
                    return Err(RuntimeError::OperatorTurnNotDispatched {
                        operation_id: command.operation_id,
                        code: code.clone(),
                        message: message.clone(),
                    });
                }
                let _ = self.store.mark_operator_user_turn_uncertain(
                    &command.operation_id,
                    &error.to_string(),
                    now_ms(),
                );
                Err(RuntimeError::operation_unresolved(
                    command.operation_id,
                    format!("original turn dispatch or its receipt is unresolved: {error}"),
                ))
            }
        }
    }

    pub(super) async fn send_turn_under_policy_guard(
        self: &Arc<Self>,
        session_id: &str,
        input: SendTurnInput,
        authorized_edit_id: Option<&str>,
        authorized_notice_id: Option<&str>,
        operator_command: Option<&OperatorUserTurnCommand>,
        policy_mutation_guard: MutexGuard<'_, ()>,
    ) -> Result<SendTurnAccepted, RuntimeError> {
        if let Some(operation_id) = self.store.retiring_operation_for_session(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "session {session_id} is frozen by workspace retirement {operation_id}"
            )));
        }
        if authorized_edit_id.is_some_and(|id| input.correlation_id.as_deref() != Some(id)) {
            return Err(RuntimeError::ProtocolViolation(
                "edit replacement input correlation does not match durable edit operation".into(),
            ));
        }
        let session = self.get_session(session_id).await?;
        if self.store.unresolved_workspace_agent_rebind(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "session {session_id} has an unresolved workspace rebind; inspect its operation before submitting another turn"
            )));
        }
        if self.store.unresolved_session_compaction(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "session {session_id} has unresolved provider compaction; inspect the compaction operation before sending another turn"
            )));
        }
        if authorized_edit_id.is_none() && self.store.unresolved_session_edit_rerun(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "session {session_id} has unresolved native edit-and-rerun; inspect its operation"
            )));
        }
        // A prior keyed operator request may already have called the native
        // provider, even when a stale session projection appears idle. Fence
        // it BEFORE a new user's thinking selection mutates provider policy.
        if let Some(operation_id) = self
            .store
            .unresolved_operator_user_turn_for_agent(session_id)?
        {
            if operator_command.map(|command| command.operation_id.as_str())
                != Some(operation_id.as_str())
            {
                return Err(RuntimeError::operation_unresolved(
                    operation_id,
                    "an earlier operator model turn has unresolved native dispatch; inspect its operation before sending another turn",
                ));
            }
        }
        if session.status == "closed" || session.status == "failed" {
            return Err(RuntimeError::InvalidState(format!(
                "session {session_id} is not writable in status {}",
                session.status
            )));
        }
        if session.active_turn_id.is_some() {
            return Err(RuntimeError::InvalidState(format!(
                "session {session_id} already has an active turn"
            )));
        }
        self.ensure_workspace_onboarding_before_turn(session_id, &input, authorized_notice_id)?;
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let turn_settings = input.turn_settings.clone();
        self.validate_turn_settings(provider_kind, &turn_settings)?;
        let session = if operator_command.is_some() {
            // The operator path verified this choice before any native effect;
            // its matching durable policy is rechecked transactionally below.
            session
        } else {
            self.persist_selected_turn_thinking(session_id, turn_settings.thinking_effort, session)
                .await?
        };
        let turn_id = self.allocate_id("turn", provider_kind.as_str());
        let now = now_ms();
        let effective_permission_mode = input
            .permission_mode
            .clone()
            .or_else(|| session.permission_mode.clone());
        let requires_approval = effective_permission_mode.as_deref() == Some("require_approval");
        let approval_id = if requires_approval {
            Some(self.allocate_id("apr", provider_kind.as_str()))
        } else {
            None
        };
        let projection_source = input.projection_source.unwrap_or_default();
        let user_input_snapshot = input
            .user_input_snapshot
            .clone()
            .unwrap_or_else(|| PersistedUserInputSnapshot::from_input(&input.input));
        let source = match projection_source {
            TurnInputProjectionSource::UserVisible => "user",
            TurnInputProjectionSource::AgentMessageDeliveryTransport => {
                "agent_message_delivery_transport"
            }
            TurnInputProjectionSource::AutomationContext => "automation_context",
        };
        let turn = TurnRecord {
            id: turn_id.clone(),
            session_id: session_id.to_string(),
            provider_turn_ref: None,
            status: "admitted".to_string(),
            input: Value::Array(input.input.clone()),
            source: Some(source.to_string()),
            started_at: Some(now),
            completed_at: None,
            usage: None,
            error: None,
        };
        let mut updated_session = session.clone();
        updated_session.status = "turn_admitted".to_string();
        updated_session.active_turn_id = Some(turn_id.clone());
        updated_session.updated_at = now;
        let approval = approval_id.as_ref().map(|approval_id| ApprovalRecord {
            id: approval_id.clone(),
            session_id: session_id.to_string(),
            turn_id: turn_id.clone(),
            origin: "runtime_pre_dispatch_policy".to_string(),
            tool_call_id: None,
            provider_approval_ref: None,
            status: "pending".to_string(),
            request: serde_json::json!({
                "reason": "manual approval required before provider execution",
            }),
            response: None,
            created_at: now,
            resolved_at: None,
        });
        let admission = TurnAdmissionRecord {
            turn_id: turn_id.clone(),
            session_id: session_id.to_string(),
            provider: provider_kind.as_str().to_string(),
            projection_source,
            user_input_snapshot,
            dispatch_policy: TurnDispatchPolicySnapshot {
                permission_mode: effective_permission_mode.clone(),
                pre_dispatch_approval_required: requires_approval,
                turn_settings: turn_settings.clone(),
            },
            correlation: TurnCorrelationState {
                expected_turn_id: input.expected_turn_id.clone(),
                correlation_id: input.correlation_id.clone(),
            },
            dispatch_state: TurnDispatchState::Pending,
            provider_native_turn_id: None,
            dispatch_error: None,
            admitted_at: now,
            updated_at: now,
        };

        if let Some(command) = operator_command {
            self.store.admit_operator_user_turn(
                command,
                &admission,
                &turn,
                &updated_session,
                approval.as_ref(),
            )?;
        } else {
            self.store
                .admit_turn(&admission, &turn, &updated_session, approval.as_ref())?;
        }
        {
            let mut turns = self.turns.write().await;
            turns.insert(turn_id.clone(), turn.clone());
        }
        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.to_string(), updated_session.clone());
        }
        if let Some(approval) = approval.clone() {
            {
                let mut approvals = self.approvals.write().await;
                approvals.insert(approval.id.clone(), approval);
            }
        }
        self.turn_admissions
            .write()
            .await
            .insert(turn_id.clone(), admission);
        drop(policy_mutation_guard);
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id.as_str()),
            "turn.admitted",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "projection_source": projection_source.as_str(),
                "requires_approval": requires_approval,
            }),
        )
        .await?;

        if requires_approval {
            let approval_id =
                approval_id.expect("approval id must exist when approval is required");
            {
                let mut turns = self.turns.write().await;
                let turn = turns
                    .get_mut(&turn_id)
                    .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
                turn.status = "waiting_for_approval".to_string();
                self.store.upsert_turn(turn)?;
            }
            {
                let mut sessions = self.sessions.write().await;
                let session = sessions
                    .get_mut(session_id)
                    .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
                session.status = "waiting_for_approval".to_string();
                session.updated_at = now_ms();
                self.store.upsert_session(session)?;
            }
            self.append_event(
                RuntimeEventScope::Session,
                session_id,
                Some(session_id),
                Some(turn_id.as_str()),
                "approval.requested",
                RuntimeEventCriticality::Critical,
                serde_json::json!({
                    "approval_id": approval_id,
                    "origin": "runtime_pre_dispatch_policy",
                }),
            )
            .await?;
            return Ok(SendTurnAccepted {
                session_id: session_id.to_string(),
                turn_id,
                status: "waiting_for_approval".to_string(),
            });
        }
        self.update_turn_dispatch_authority(
            turn_id.as_str(),
            TurnDispatchState::Dispatching,
            None,
            None,
        )
        .await?;

        let provider_send_input = ProviderSendTurnRequest {
            runtime_session_id: session_id.to_string(),
            turn_id: turn_id.clone(),
            input: input.input,
            expected_turn_id: input.expected_turn_id,
            permission_mode: effective_permission_mode,
            approval_id: approval_id.clone(),
            turn_settings,
        };
        let (ack, provider_events) = match self
            .dispatch_send_turn_with_resume_fallback(provider_kind, provider_send_input, &session)
            .await
        {
            Ok((ack, provider_events))
                if ack.runtime_session_id == session_id && ack.turn_id == turn_id =>
            {
                (ack, provider_events)
            }
            Ok((ack, _)) => {
                let error = RuntimeError::provider_dispatch_unknown(
                    "ack_identity_mismatch",
                    format!(
                        "provider send_turn acknowledgement mismatch (expected_session={session_id}, expected_turn={turn_id}, actual_session={}, actual_turn={})",
                        ack.runtime_session_id, ack.turn_id
                    ),
                );
                self.mark_dispatch_unknown(session_id, turn_id.as_str(), &error)
                    .await?;
                return Err(error);
            }
            Err(error)
                if error.provider_dispatch_outcome() == ProviderDispatchOutcome::NotDispatched =>
            {
                self.mark_not_dispatched(session_id, turn_id.as_str(), &error)
                    .await?;
                return Err(error);
            }
            Err(error) => {
                let unknown = RuntimeError::provider_dispatch_unknown(
                    error.provider_dispatch_code().unwrap_or("provider_error"),
                    error.to_string(),
                );
                self.mark_dispatch_unknown(session_id, turn_id.as_str(), &unknown)
                    .await?;
                return Err(unknown);
            }
        };

        self.update_turn_dispatch_authority(
            turn_id.as_str(),
            TurnDispatchState::Dispatched,
            ack.provider_native_turn_id.clone(),
            None,
        )
        .await?;

        {
            let mut turns = self.turns.write().await;
            let turn = turns
                .get_mut(&turn_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
            turn.status = "in_progress".to_string();
            self.store.upsert_turn(turn)?;
        }
        {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
            session.status = "turn_running".to_string();
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id.as_str()),
            "turn.started",
            RuntimeEventCriticality::Critical,
            serde_json::json!({}),
        )
        .await?;
        self.spawn_wait_for_turn(
            provider_kind,
            session_id.to_string(),
            turn_id.clone(),
            provider_events,
        );

        Ok(SendTurnAccepted {
            session_id: session_id.to_string(),
            turn_id,
            status: "in_progress".to_string(),
        })
    }

    pub fn replay_session_events(
        &self,
        session_id: &str,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.store.list_runtime_events(
            Some((RuntimeEventScope::Session, session_id)),
            after_seq,
            limit.max(1),
        )
    }

    pub(super) async fn update_turn_dispatch_authority(
        &self,
        turn_id: &str,
        state: TurnDispatchState,
        provider_native_turn_id: Option<String>,
        dispatch_error: Option<Value>,
    ) -> Result<TurnAdmissionRecord, RuntimeError> {
        let current = self
            .turn_admissions
            .read()
            .await
            .get(turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn admission {turn_id}")))?;
        if !is_valid_dispatch_transition(current.dispatch_state, state) {
            return Err(RuntimeError::ProtocolViolation(format!(
                "invalid provider dispatch transition for turn {turn_id}: {} -> {}",
                current.dispatch_state.as_str(),
                state.as_str()
            )));
        }
        if let Some(provider_native_turn_id) = provider_native_turn_id.as_deref() {
            if provider_native_turn_id.trim().is_empty()
                || provider_native_turn_id.trim() != provider_native_turn_id
            {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "provider-native turn id for {turn_id} must be non-empty and trimmed"
                )));
            }
            if !matches!(
                state,
                TurnDispatchState::Dispatched | TurnDispatchState::Unknown
            ) {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "provider-native turn id for {turn_id} cannot be attached in dispatch state {}",
                    state.as_str()
                )));
            }
            if let Some(existing) = current.provider_native_turn_id.as_deref() {
                if existing != provider_native_turn_id {
                    return Err(RuntimeError::ProtocolViolation(format!(
                        "logical turn {turn_id} cannot be remapped from provider-native turn {existing} to {provider_native_turn_id}"
                    )));
                }
            }
        }
        let mut updated = current;
        updated.dispatch_state = state;
        if provider_native_turn_id.is_some() {
            updated.provider_native_turn_id = provider_native_turn_id;
        }
        updated.dispatch_error = dispatch_error;
        updated.updated_at = now_ms();
        self.store.upsert_turn_admission(&updated)?;
        self.turn_admissions
            .write()
            .await
            .insert(turn_id.to_string(), updated.clone());
        Ok(updated)
    }

    pub(super) async fn mark_not_dispatched(
        &self,
        session_id: &str,
        turn_id: &str,
        error: &RuntimeError,
    ) -> Result<(), RuntimeError> {
        self.update_turn_dispatch_authority(
            turn_id,
            TurnDispatchState::NotDispatched,
            None,
            Some(serde_json::json!({
                "code": error.provider_dispatch_code(),
                "message": error.to_string(),
            })),
        )
        .await?;
        {
            let mut turns = self.turns.write().await;
            let turn = turns
                .get_mut(turn_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
            turn.status = "failed".to_string();
            turn.completed_at = Some(now_ms());
            turn.error = Some(serde_json::json!({
                "message": error.to_string(),
                "provider_dispatch_outcome": "not_dispatched",
            }));
            self.store.upsert_turn(turn)?;
        }
        {
            let mut approvals = self.approvals.write().await;
            for approval in approvals.values_mut().filter(|approval| {
                approval.session_id == session_id
                    && approval.turn_id == turn_id
                    && approval.status == "pending"
            }) {
                approval.status = "cancelled".to_string();
                approval.response = Some(serde_json::json!({
                    "reason": "provider dispatch was proven not to have occurred",
                }));
                approval.resolved_at = Some(now_ms());
                self.store.upsert_approval(approval)?;
            }
        }
        {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
            if session.active_turn_id.as_deref() == Some(turn_id) {
                session.active_turn_id = None;
            }
            if !matches!(session.status.as_str(), "closed" | "failed") {
                session.status = "ready".to_string();
            }
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "turn.failed",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "error": error.to_string(),
                "provider_dispatch_outcome": "not_dispatched",
            }),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn mark_dispatch_unknown(
        &self,
        session_id: &str,
        turn_id: &str,
        error: &RuntimeError,
    ) -> Result<(), RuntimeError> {
        self.update_turn_dispatch_authority(
            turn_id,
            TurnDispatchState::Unknown,
            None,
            Some(serde_json::json!({
                "code": error.provider_dispatch_code(),
                "message": error.to_string(),
            })),
        )
        .await?;
        {
            let mut turns = self.turns.write().await;
            let turn = turns
                .get_mut(turn_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
            turn.status = "dispatch_unknown".to_string();
            turn.error = Some(serde_json::json!({
                "message": error.to_string(),
                "provider_dispatch_outcome": "unknown",
            }));
            self.store.upsert_turn(turn)?;
        }
        {
            let mut sessions = self.sessions.write().await;
            let session = sessions
                .get_mut(session_id)
                .ok_or_else(|| RuntimeError::NotFound(format!("session {session_id}")))?;
            session.status = "turn_recovery_required".to_string();
            session.updated_at = now_ms();
            self.store.upsert_session(session)?;
        }
        self.append_event(
            RuntimeEventScope::Session,
            session_id,
            Some(session_id),
            Some(turn_id),
            "turn.dispatch_unknown",
            RuntimeEventCriticality::Critical,
            serde_json::json!({
                "error": error.to_string(),
                "provider_dispatch_outcome": "unknown",
            }),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn dispatch_send_turn_with_resume_fallback(
        &self,
        provider_kind: ProviderKind,
        request: ProviderSendTurnRequest,
        session: &crate::SessionRecord,
    ) -> Result<
        (
            crate::ProviderTurnAck,
            Option<broadcast::Receiver<ProviderRuntimeEvent>>,
        ),
        RuntimeError,
    > {
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::provider_not_dispatched(
                "provider_not_registered",
                format!("provider '{}' is not registered", provider_kind.as_str()),
            )
        })?;
        let provider_events = provider.subscribe_events();
        match provider.send_turn(request.clone()).await {
            Ok(ack) => Ok((ack, provider_events)),
            Err(error)
                if error.provider_dispatch_outcome() == ProviderDispatchOutcome::NotDispatched
                    && error.provider_dispatch_code() == Some("session_not_found") =>
            {
                let provider_session_ref =
                    session.provider_session_ref.clone().ok_or_else(|| {
                        RuntimeError::provider_not_dispatched(
                            "provider_session_unavailable",
                            format!(
                                "provider session {} was not found and cannot be resumed",
                                request.runtime_session_id
                            ),
                        )
                    })?;
                let resume_request = self
                    .provider_resume_request_for_session(
                        session,
                        provider_session_ref,
                        session.canonical_provider_session_ref.clone(),
                    )
                    .map_err(|resume_error| {
                        RuntimeError::provider_not_dispatched(
                            "provider_resume_failed",
                            resume_error.to_string(),
                        )
                    })?;
                provider
                    .resume_session_with_policy(resume_request)
                    .await
                    .map_err(|resume_error| {
                        RuntimeError::provider_not_dispatched(
                            "provider_resume_failed",
                            resume_error.to_string(),
                        )
                    })?;
                provider
                    .send_turn(request)
                    .await
                    .map(|ack| (ack, provider_events))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn spawn_wait_for_turn(
        self: &Arc<Self>,
        provider: ProviderKind,
        session_id: String,
        turn_id: String,
        provider_events: Option<broadcast::Receiver<ProviderRuntimeEvent>>,
    ) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let provider_adapter = match manager.providers.get(provider) {
                Some(provider_adapter) => provider_adapter,
                None => return,
            };
            let wait = provider_adapter.wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                timeout_ms: None,
            });
            tokio::pin!(wait);
            let mut provider_events =
                provider_events.or_else(|| provider_adapter.subscribe_events());
            let result = loop {
                let Some(events) = provider_events.as_mut() else {
                    break wait.await;
                };
                tokio::select! {
                    result = &mut wait => break result,
                    event = events.recv() => {
                        match event {
                            Ok(event) => match manager
                                .record_provider_side_event(
                                    session_id.as_str(),
                                    turn_id.as_str(),
                                    event,
                                )
                                .await
                            {
                                Ok(Some(ProviderRuntimeEvent::TurnOutcomeUnknown {
                                    runtime_session_id,
                                    turn_id: event_turn_id,
                                    code,
                                    message,
                                })) if runtime_session_id == session_id && event_turn_id == turn_id => {
                                    let _ = manager
                                        .mark_provider_event_stream_unknown(
                                            session_id.as_str(),
                                            turn_id.as_str(),
                                            code.as_str(),
                                            message,
                                        )
                                        .await;
                                    return;
                                }
                                Ok(_) => {}
                                Err(_) => return,
                            },
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                let _ = manager
                                    .mark_provider_event_stream_unknown(
                                        session_id.as_str(),
                                        turn_id.as_str(),
                                        "provider_event_stream_lagged",
                                        format!("provider event stream skipped {skipped} events"),
                                    )
                                    .await;
                                return;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                let _ = manager
                                    .mark_provider_event_stream_unknown(
                                        session_id.as_str(),
                                        turn_id.as_str(),
                                        "provider_event_stream_closed",
                                        "provider event stream closed before turn completion".to_string(),
                                    )
                                    .await;
                                return;
                            }
                        }
                    }
                }
            };
            match result {
                Ok(turn_result) => {
                    // Some providers (Claude SDK) discover their canonical
                    // session ID only during the first turn. Re-read it at
                    // completion even if the bridge notification raced with
                    // the wait response, before clearing the active turn.
                    if let Err(error) = manager
                        .verify_provider_session_identity(
                            provider_adapter.as_ref(),
                            session_id.as_str(),
                            turn_id.as_str(),
                        )
                        .await
                    {
                        let _ = manager
                            .mark_provider_event_stream_unknown(
                                session_id.as_str(),
                                turn_id.as_str(),
                                "provider_session_identity_unverified",
                                error.to_string(),
                            )
                            .await;
                        return;
                    }
                    let has_provider_usage = matches!(
                        turn_result.status,
                        ProviderTurnStatus::Completed | ProviderTurnStatus::Interrupted
                    ) && turn_result.usage.is_some();
                    let applied = manager.apply_terminal_result(turn_result).await;
                    if let Err(error) = &applied {
                        if std::env::var("GG_CLAUDE_SMOKE_DEBUG")
                            .ok()
                            .map(|value| value.trim() == "1")
                            .unwrap_or(false)
                        {
                            eprintln!(
                                "[runtime-core] failed to apply terminal turn result for session_id={} turn_id={}: {}",
                                session_id, turn_id, error
                            );
                        }
                    } else if matches!(applied, Ok(true)) && has_provider_usage {
                        manager
                            .record_terminal_context_limit(session_id.as_str(), turn_id.as_str())
                            .await;
                    }
                }
                Err(error) => {
                    if let Some(code) = error.provider_dispatch_code() {
                        let _ = manager
                            .mark_provider_event_stream_unknown(
                                session_id.as_str(),
                                turn_id.as_str(),
                                code,
                                error.to_string(),
                            )
                            .await;
                    } else {
                        let _ = manager
                            .apply_terminal_failure(session_id.as_str(), turn_id.as_str(), error)
                            .await;
                    }
                }
            }
        });
    }
}
