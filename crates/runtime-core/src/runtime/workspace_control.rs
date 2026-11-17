use std::sync::Arc;

use crate::{
    authorize_workspace_membership_mutation, OperationDetails, OperationPhase, ProcessKillRequest,
    ProcessManager, ProviderCloseSessionRequest, ProviderKind, RuntimeError,
    WorkspaceAgentCreateRequest, WorkspaceAgentRecord, WorkspaceInterruptAdmission,
    WorkspaceInterruptCommand, WorkspaceInterruptResponse, WorkspaceMembershipAction,
    WorkspaceMembershipPolicy, WorkspaceRetirementAdmission, WorkspaceRetirementCommand,
    WorkspaceRetirementResponse, WorkspaceRetirementTurnStep,
};

use super::helpers::now_ms;
use super::RuntimeSessionManager;

impl RuntimeSessionManager {
    /// Drive a durable retirement through its next safe boundary. Reissuing
    /// the same operator identity/key/input resumes from persisted effects,
    /// never blindly replays unknown provider/native requests. A nonterminal
    /// response remains a frozen operation and can be inspected separately.
    pub async fn advance_workspace_retirement(
        &self,
        command: &WorkspaceRetirementCommand,
        processes: &dyn ProcessManager,
    ) -> Result<OperationDetails, RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        let workspace = self
            .store
            .get_workspace(&command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", command.workspace_id)))?;
        // Serialize legacy/in-memory native creation windows that use the
        // shared repository lock before admitting the durable SQLite fence.
        let repository_lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
        let admission = {
            let _freeze = repository_lock.write().await;
            self.store.admit_workspace_retirement(command)?
        };
        let plan = match admission {
            WorkspaceRetirementAdmission::Replay(receipt) => {
                return self.retirement_operation(&receipt.operation_id)
            }
            WorkspaceRetirementAdmission::New(plan)
            | WorkspaceRetirementAdmission::Resume(plan) => plan,
        };
        let operation_id = plan.operation_id.as_str();

        self.store
            .settle_passive_workspace_retirement(operation_id, now_ms())?;
        if self.retirement_operation(operation_id)?.operation.phase == OperationPhase::ManualReview
        {
            return self.retirement_operation(operation_id);
        }
        let mut pending_native_turns = false;
        for turn_id in &plan.turn_ids {
            let step = self
                .store
                .progress_workspace_retirement_turn(operation_id, turn_id, now_ms())
                .map_err(|error| {
                    self.admitted_legacy_guard_error(
                        operation_id,
                        "workspace_turn_interrupt",
                        turn_id,
                        error,
                    )
                })?;
            match step {
                WorkspaceRetirementTurnStep::Finalized => {
                    self.sync_retirement_settled_turn(turn_id).await?;
                }
                WorkspaceRetirementTurnStep::Waiting => pending_native_turns = true,
                WorkspaceRetirementTurnStep::NeedsReview => {
                    self.store.quarantine_workspace_retirement_interrupt(
                        operation_id,
                        turn_id,
                        "native_turn_outcome_unresolved",
                        now_ms(),
                    )?;
                    return self.retirement_operation(operation_id);
                }
                WorkspaceRetirementTurnStep::Interrupt {
                    session_id,
                    turn_id,
                } => match self
                    .interrupt_retirement_turn_under_policy_guard(
                        &session_id,
                        &turn_id,
                        operation_id,
                    )
                    .await
                {
                    Ok(()) => {
                        self.store.acknowledge_workspace_retirement_interrupt(
                            operation_id,
                            &turn_id,
                            now_ms(),
                        )?;
                        let settled = self
                            .store
                            .progress_workspace_retirement_turn(operation_id, &turn_id, now_ms())
                            .map_err(|error| {
                                self.admitted_legacy_guard_error(
                                    operation_id,
                                    "workspace_turn_interrupt",
                                    &turn_id,
                                    error,
                                )
                            })?;
                        if settled == WorkspaceRetirementTurnStep::Finalized {
                            self.sync_retirement_settled_turn(&turn_id).await?;
                        } else {
                            pending_native_turns = true;
                        }
                    }
                    Err(error) => {
                        self.store.quarantine_workspace_retirement_interrupt(
                            operation_id,
                            &turn_id,
                            "native_interrupt_unverified",
                            now_ms(),
                        )?;
                        tracing::warn!(operation_id, turn_id, error=%error,
                                "retirement turn interruption requires reconciliation");
                        return self.retirement_operation(operation_id);
                    }
                },
            }
        }

        for process_id in &plan.managed_process_ids {
            let details = self.retirement_operation(operation_id)?;
            let unsettled = details.effects.iter().any(|effect| {
                effect.effect_kind == "workspace_process_settle"
                    && effect.target_id == *process_id
                    && effect.phase == "intended"
            });
            if !unsettled {
                continue;
            }
            if let Err(error) = processes
                .kill_process(ProcessKillRequest {
                    process_id: process_id.clone(),
                    caller_session_id: None,
                    reason: Some("workspace_retirement".into()),
                })
                .await
            {
                tracing::warn!(operation_id, process_id, error=%error,
                    "retirement managed process cancellation remains unresolved");
            }
        }
        self.store
            .settle_passive_workspace_retirement(operation_id, now_ms())?;

        // Closing a provider session before process/turn authority settles can
        // strand completion/interrupt injections. Wait for terminal effects.
        let current = self.retirement_operation(operation_id)?;
        let process_or_turn_outstanding = pending_native_turns
            || current.effects.iter().any(|effect| {
                matches!(
                    effect.effect_kind.as_str(),
                    "workspace_turn_interrupt" | "workspace_process_settle"
                ) && effect.phase != "finalized"
            });
        if process_or_turn_outstanding || current.operation.phase == OperationPhase::ManualReview {
            return Ok(current);
        }

        for agent_id in &plan.agent_ids {
            let current = self.retirement_operation(operation_id)?;
            let pending = current.effects.iter().any(|effect| {
                effect.effect_kind == "workspace_provider_close"
                    && effect.target_id == *agent_id
                    && effect.phase == "intended"
            });
            if !pending {
                continue;
            }
            let attempt = self
                .store
                .claim_workspace_retirement_native_close(operation_id, agent_id, now_ms())
                .map_err(|error| {
                    self.admitted_legacy_guard_error(
                        operation_id,
                        "workspace_provider_close",
                        agent_id,
                        error,
                    )
                })?;
            let adapter =
                ProviderKind::from_str(&attempt.provider).and_then(|kind| self.providers.get(kind));
            let outcome = match adapter {
                Some(adapter) => {
                    adapter
                        .close_session_for_retirement(ProviderCloseSessionRequest {
                            runtime_session_id: agent_id.clone(),
                            reason: Some("workspace_retirement".into()),
                        })
                        .await
                }
                None => Err(RuntimeError::ProviderNotRegistered(
                    attempt.provider.clone(),
                )),
            };
            match outcome {
                Ok(evidence) => {
                    let mut turns = self.turns.write().await;
                    let mut sessions = self.sessions.write().await;
                    if let Err(error) = self.store.finalize_workspace_retirement_native_close(
                        &attempt,
                        &evidence,
                        now_ms(),
                    ) {
                        self.store.quarantine_workspace_retirement_native_close(
                            &attempt,
                            "native_close_receipt_unverified",
                            now_ms(),
                        )?;
                        tracing::warn!(operation_id, agent_id, error=%error,
                            "native close completed but its durable receipt needs reconciliation");
                        return self.retirement_operation(operation_id);
                    }
                    // Native close has already committed. Refresh only this
                    // attachment and its turns, never unrelated sessions.
                    let (closed, affected_turns) = self
                        .store
                        .get_persisted_session_with_turns(agent_id)?
                        .ok_or_else(|| {
                            RuntimeError::NotFound(format!("closed session {agent_id}"))
                        })?;
                    sessions.insert(agent_id.clone(), closed);
                    for turn in affected_turns {
                        turns.insert(turn.id.clone(), turn);
                    }
                }
                Err(error) => {
                    self.store.quarantine_workspace_retirement_native_close(
                        &attempt,
                        "native_close_unverified",
                        now_ms(),
                    )?;
                    tracing::warn!(operation_id, agent_id, error=%error,
                        "retirement native provider close requires reconciliation");
                    return self.retirement_operation(operation_id);
                }
            }
        }

        self.store
            .settle_passive_workspace_retirement(operation_id, now_ms())?;
        let _ = self
            .store
            .finalize_settled_workspace_retirement(operation_id, now_ms())?;
        self.retirement_operation(operation_id)
    }

    fn retirement_operation(&self, operation_id: &str) -> Result<OperationDetails, RuntimeError> {
        self.store
            .get_operation(operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("retirement operation {operation_id}")))
    }

    async fn sync_retirement_settled_turn(&self, turn_id: &str) -> Result<(), RuntimeError> {
        // The terminal receipt already committed. Never hydrate unrelated
        // historical sessions/approvals/admissions just to project this turn.
        let Some(projection) = self.store.get_persisted_retirement_settled_turn(turn_id)? else {
            return Ok(());
        };
        self.turns
            .write()
            .await
            .insert(projection.turn.id.clone(), projection.turn);
        if let Some(session) = projection.session {
            self.sessions
                .write()
                .await
                .insert(session.id.clone(), session);
        }
        let mut approvals = self.approvals.write().await;
        for approval in projection.approvals {
            approvals.insert(approval.id.clone(), approval);
        }
        drop(approvals);
        if let Some(admission) = projection.admission {
            self.turn_admissions
                .write()
                .await
                .insert(turn_id.into(), admission);
        }
        Ok(())
    }

    /// Wait for native workspace-agent admission and managed worktree creation
    /// to finish before trusting the durable no-obligations snapshot.
    pub async fn retire_quiescent_workspace(
        &self,
        command: &WorkspaceRetirementCommand,
    ) -> Result<WorkspaceRetirementResponse, RuntimeError> {
        let workspace = self
            .store
            .get_workspace(&command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", command.workspace_id)))?;
        let repository_lock = crate::repository_lifecycle_lock(&workspace.canonical_root).await;
        let _repository_guard = repository_lock.write().await;
        self.store.retire_empty_workspace(command)
    }

    pub fn authorize_workspace_membership(
        &self,
        workspace_id: &str,
        caller_agent_id: &str,
        action: WorkspaceMembershipAction,
        policy: WorkspaceMembershipPolicy,
    ) -> Result<(), RuntimeError> {
        let workspace_id = workspace_id.trim();
        let workspace = self
            .store
            .get_workspace(workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let active_members = self.store.list_workspace_agents(
            workspace_id,
            Some(crate::WorkspaceAgentLifecycleState::Active),
        )?;
        authorize_workspace_membership_mutation(
            &workspace,
            &active_members,
            caller_agent_id,
            action,
            policy,
        )
    }

    pub async fn create_workspace_agent_as_member(
        self: &Arc<Self>,
        _workspace_id: &str,
        _request: WorkspaceAgentCreateRequest,
        _caller_agent_id: &str,
        _policy: WorkspaceMembershipPolicy,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "raw member creation is unsupported; use the scoped member creation journal".into(),
        ))
    }

    pub async fn archive_workspace_agent_as_member(
        self: &Arc<Self>,
        _workspace_id: &str,
        _agent_id: &str,
        _caller_agent_id: &str,
        _reason: Option<&str>,
        _policy: WorkspaceMembershipPolicy,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "raw member archival is unsupported; use the scoped member archival journal".into(),
        ))
    }

    pub async fn interrupt_workspace(
        &self,
        command: WorkspaceInterruptCommand,
    ) -> Result<WorkspaceInterruptResponse, RuntimeError> {
        let plan = match self.store.begin_workspace_interrupt(&command)? {
            WorkspaceInterruptAdmission::Replay(response) => return Ok(response),
            WorkspaceInterruptAdmission::Execute(plan) => plan,
        };
        let unresolved = |error: RuntimeError| {
            RuntimeError::operation_unresolved_with_legacy_recovery(
            &plan.operation_id,
            format!("workspace interrupt requires inspection; native retry is not authorized: {error}"),
            error.legacy_native_recovery(),
        )
        };

        for target in plan.targets {
            if let Err(error) = self.store.mark_workspace_interrupt_started(
                &plan.operation_id,
                &target.agent_id,
                &target.turn_id,
                now_ms(),
            ) {
                self.preserve_legacy_native_recovery(
                    &plan.operation_id,
                    "workspace_turn_interrupt",
                    &target.agent_id,
                    error.legacy_native_recovery(),
                )?;
                return Err(unresolved(error));
            }
            match self
                .interrupt_turn_for_operation(
                    target.agent_id.as_str(),
                    target.turn_id.as_str(),
                    &plan.operation_id,
                )
                .await
            {
                Ok(()) => self
                    .store
                    .finalize_workspace_interrupt_effect(
                        &plan.operation_id,
                        &target.agent_id,
                        &target.turn_id,
                        true,
                        "interrupt_requested",
                        now_ms(),
                    )
                    .map_err(&unresolved)?,
                Err(RuntimeError::InvalidState(_)) | Err(RuntimeError::NotFound(_)) => {
                    self.store
                        .finalize_workspace_interrupt_effect(
                            &plan.operation_id,
                            &target.agent_id,
                            &target.turn_id,
                            false,
                            "turn_no_longer_active",
                            now_ms(),
                        )
                        .map_err(&unresolved)?;
                }
                Err(error) => {
                    self.preserve_legacy_native_recovery(
                        &plan.operation_id,
                        "workspace_turn_interrupt",
                        &target.agent_id,
                        error.legacy_native_recovery(),
                    )?;
                    self.store
                        .mark_workspace_interrupt_uncertain(
                            &plan.operation_id,
                            &target.agent_id,
                            &target.turn_id,
                            &serde_json::json!({"message": error.to_string()}),
                            now_ms(),
                        )
                        .map_err(&unresolved)?;
                    return Err(unresolved(error));
                }
            }
        }

        self.store
            .complete_workspace_interrupt(&plan.operation_id, now_ms())
            .map_err(unresolved)
    }
}
