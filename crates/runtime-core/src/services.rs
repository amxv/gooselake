#[path = "services_team_comms_trait.rs"]
mod team_comms_trait;
#[path = "services_team_models.rs"]
mod team_models;
#[path = "services_worktree_models.rs"]
mod worktree_models;
#[path = "services_worktree_trait.rs"]
mod worktree_trait;
pub use team_comms_trait::*;
pub use team_models::*;
pub use worktree_models::*;
pub use worktree_trait::*;

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    AgentDeliveryRecord, AgentMessageRecord, AgentToolMutationAdmission, AgentToolMutationCommand,
    ApprovalRecord, CompactionObservationReceipt, LegacyWorkspaceMigrationApplyCommand,
    LegacyWorkspaceMigrationApplyResponse, LegacyWorkspaceMigrationResolutionCommand,
    LegacyWorkspaceMigrationResolutionResponse, LegacyWorkspaceMigrationStatus,
    ManagedProcessAdmission, ManagedProcessRecord, ManagedProcessTerminalUpdate,
    ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent, OperationDetails,
    ProcessCompletionUpdate, ProcessRecord, ProcessSchedulerSettings,
    ProviderCompactionObservation, ProviderWorkspaceRebindEvidence, RuntimeError,
    RuntimeEventRecord, RuntimeEventScope, RuntimeHydratedState, SessionCompactionRun,
    SessionContextLimitSnapshot, SessionEditRerunOperation, SessionRecord, TeamDeliveryRecord,
    TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord, TeamOperationJournalRecord,
    TeamRecord, TurnAdmissionRecord, TurnRecord, WorkspaceAgentLifecycleState,
    WorkspaceAgentRebindOperation, WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy,
    WorkspaceInterruptAdmission, WorkspaceInterruptCommand, WorkspaceInterruptResponse,
    WorkspaceLeadTransitionCommand, WorkspaceLeadTransitionResponse, WorkspaceRecord,
    WorkspaceRegisterCommand, WorkspaceRegisterResponse, WorkspaceRetirementCommand,
    WorkspaceRetirementResponse,
};
use crate::{
    TeamHookApproval, TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase,
    TeamHookEffectReceipt, TeamHookEffectRecord, TeamHookEventName, TeamHookPreAddAdmission,
    TeamHookPreAddContext,
};
use crate::{WorkspaceAgentOnboardingRequest, WorkspaceSystemNoticeRecord};

#[async_trait]
pub trait RuntimeStore: Send + Sync {
    /// Transactionally snapshot approved pre-add hooks against a trusted
    /// reserved agent identity before any shell or provider work can start.
    fn admit_pre_add_team_hooks(
        &self,
        _request: &TeamHookPreAddAdmission,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        Ok(Vec::new())
    }
    /// Operator-only execution capability. No project file can grant itself
    /// shell authority, and a missing grant means disabled.
    fn get_team_hook_approval(
        &self,
        _workspace_id: &str,
    ) -> Result<TeamHookApproval, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team-hook approval store unavailable".into(),
        ))
    }

    fn update_team_hook_approval(
        &self,
        _update: &TeamHookApprovalUpdate,
    ) -> Result<TeamHookApproval, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team-hook approval store unavailable".into(),
        ))
    }

    fn list_team_hook_effects(
        &self,
        _workspace_id: &str,
        _limit: usize,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        Ok(Vec::new())
    }

    /// Startup/retry work queues are derived from durable immutable intents,
    /// not the current project configuration or in-memory event listeners.
    fn list_dispatchable_team_hook_effects(
        &self,
        _limit: usize,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        Ok(Vec::new())
    }

    fn list_unapplied_team_hook_effects(
        &self,
        _limit: usize,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        Ok(Vec::new())
    }

    fn apply_team_hook_effect_output(
        &self,
        _effect: &TeamHookEffectRecord,
        _at_ms: i64,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable hook context application is unavailable".into(),
        ))
    }

    fn admit_team_hook_effect(
        &self,
        _admission: &TeamHookEffectAdmission,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team-hook effect admission unavailable".into(),
        ))
    }

    fn get_team_hook_effect(
        &self,
        _workspace_id: &str,
        _parent_event_id: &str,
        _event: TeamHookEventName,
        _hook_name: &str,
    ) -> Result<Option<TeamHookEffectRecord>, RuntimeError> {
        Ok(None)
    }

    fn claim_team_hook_effect(
        &self,
        _workspace_id: &str,
        _parent_event_id: &str,
        _event: TeamHookEventName,
        _hook_name: &str,
        _at_ms: i64,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team-hook effect claim unavailable".into(),
        ))
    }

    fn finish_team_hook_effect(
        &self,
        _workspace_id: &str,
        _parent_event_id: &str,
        _event: TeamHookEventName,
        _hook_name: &str,
        _outcome: TeamHookEffectPhase,
        _receipt: TeamHookEffectReceipt,
        _at_ms: i64,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "team-hook effect receipt unavailable".into(),
        ))
    }

    /// Reconcile ambiguous shell dispatch at startup. A started hook is never
    /// blindly retried because the child might already have modified state.
    fn quarantine_started_team_hook_effects(&self, _at_ms: i64) -> Result<usize, RuntimeError> {
        Ok(0)
    }

    /// Admission is committed before any native spawn, archive, or worktree
    /// mutation. A previously admitted but unreceipted call fails closed.
    fn begin_agent_tool_mutation(
        &self,
        _command: &AgentToolMutationCommand,
    ) -> Result<AgentToolMutationAdmission, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable model-tool mutation admission unavailable".into(),
        ))
    }

    fn finish_agent_tool_mutation(
        &self,
        _operation_id: &str,
        _exact_result: &Value,
        _finished_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable model-tool mutation receipt unavailable".into(),
        ))
    }

    fn begin_session_edit_rerun(
        &self,
        _operation: &SessionEditRerunOperation,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "edit-and-rerun persistence unavailable".into(),
        ))
    }

    fn get_session_edit_rerun(
        &self,
        _operation_id: &str,
    ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
        Ok(None)
    }

    fn get_session_edit_rerun_by_key(
        &self,
        _agent_id: &str,
        _idempotency_key: &str,
    ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
        Ok(None)
    }

    fn unresolved_session_edit_rerun(&self, _agent_id: &str) -> Result<bool, RuntimeError> {
        Ok(false)
    }

    fn advance_session_edit_rerun(
        &self,
        _operation_id: &str,
        _phase: &str,
        _child: Option<&crate::ProviderSession>,
        _replacement_turn_id: Option<&str>,
        _error_code: Option<&str>,
        _now: i64,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "edit-and-rerun transition unavailable".into(),
        ))
    }

    fn recover_session_edit_reruns(
        &self,
        _now: i64,
    ) -> Result<Vec<SessionEditRerunOperation>, RuntimeError> {
        Ok(Vec::new())
    }

    fn resolve_session_edit_rerun_review(
        &self,
        _operation_id: &str,
        _evidence: &str,
        _now: i64,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "edit-and-rerun review unavailable".into(),
        ))
    }

    fn begin_session_compaction(
        &self,
        _run: &SessionCompactionRun,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "session compaction persistence unavailable".into(),
        ))
    }

    fn transition_session_compaction(
        &self,
        _run_id: &str,
        _phase: &str,
        _outcome: Option<crate::ProviderCompactSessionOutcome>,
        _error_code: Option<&str>,
        _updated_at_ms: i64,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "session compaction persistence unavailable".into(),
        ))
    }

    fn get_session_compaction(
        &self,
        _run_id: &str,
    ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
        Ok(None)
    }

    fn observe_provider_compaction(
        &self,
        _observation: &ProviderCompactionObservation,
    ) -> Result<CompactionObservationReceipt, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "provider compaction observation persistence unavailable".into(),
        ))
    }

    fn get_session_compaction_by_key(
        &self,
        _agent_id: &str,
        _key: &str,
    ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
        Ok(None)
    }

    fn unresolved_session_compaction(&self, _agent_id: &str) -> Result<bool, RuntimeError> {
        Ok(false)
    }

    fn recover_session_compactions(
        &self,
        _now_ms: i64,
    ) -> Result<Vec<SessionCompactionRun>, RuntimeError> {
        Ok(Vec::new())
    }

    fn resolve_session_compaction_review(
        &self,
        _run_id: &str,
        _phase: &str,
        _evidence: &str,
        _now_ms: i64,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "compaction manual review unavailable".into(),
        ))
    }

    async fn initialize(&self) -> Result<(), RuntimeError>;

    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError>;

    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError>;

    fn register_workspace(
        &self,
        _command: &WorkspaceRegisterCommand,
    ) -> Result<WorkspaceRegisterResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_workspace(&self, _workspace_id: &str) -> Result<Option<WorkspaceRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn retire_empty_workspace(
        &self,
        _command: &WorkspaceRetirementCommand,
    ) -> Result<WorkspaceRetirementResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "atomic retirement of quiescent workspaces is unavailable".to_string(),
        ))
    }

    fn transition_workspace_lead(
        &self,
        _command: &WorkspaceLeadTransitionCommand,
    ) -> Result<WorkspaceLeadTransitionResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace lead authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn begin_workspace_interrupt(
        &self,
        _command: &WorkspaceInterruptCommand,
    ) -> Result<WorkspaceInterruptAdmission, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn mark_workspace_interrupt_started(
        &self,
        _operation_id: &str,
        _agent_id: &str,
        _turn_id: &str,
        _changed_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn finalize_workspace_interrupt_effect(
        &self,
        _operation_id: &str,
        _agent_id: &str,
        _turn_id: &str,
        _interrupted: bool,
        _reason: &str,
        _changed_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn mark_workspace_interrupt_uncertain(
        &self,
        _operation_id: &str,
        _agent_id: &str,
        _turn_id: &str,
        _error: &Value,
        _changed_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn complete_workspace_interrupt(
        &self,
        _operation_id: &str,
        _completed_at: i64,
    ) -> Result<WorkspaceInterruptResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace interrupt authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn create_workspace_agent(
        &self,
        _session: &SessionRecord,
        _agent: &WorkspaceAgentRecord,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn create_workspace_agent_with_onboarding(
        &self,
        _session: &SessionRecord,
        _agent: &WorkspaceAgentRecord,
        _onboarding: &WorkspaceAgentOnboardingRequest,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "atomic workspace onboarding admission unavailable".into(),
        ))
    }

    fn create_workspace_agent_with_hook_context(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
        onboarding: &WorkspaceAgentOnboardingRequest,
        hooks: Option<&TeamHookPreAddContext>,
    ) -> Result<(), RuntimeError> {
        if hooks.is_some() {
            return Err(RuntimeError::Unsupported(
                "atomic pre-add hook context admission unavailable".into(),
            ));
        }
        self.create_workspace_agent_with_onboarding(session, agent, onboarding)
    }

    fn list_workspace_system_notices(
        &self,
        _limit: usize,
    ) -> Result<Vec<WorkspaceSystemNoticeRecord>, RuntimeError> {
        Ok(Vec::new())
    }

    /// Page in immutable creation order, including notices whose recipients
    /// are busy or whose admitted provider turn is awaiting reconciliation.
    fn list_workspace_system_notices_after(
        &self,
        after: Option<&WorkspaceSystemNoticeRecord>,
        limit: usize,
    ) -> Result<Vec<WorkspaceSystemNoticeRecord>, RuntimeError> {
        if after.is_some() {
            return Err(RuntimeError::Unsupported(
                "workspace system notice cursor pagination unavailable".into(),
            ));
        }
        self.list_workspace_system_notices(limit)
    }

    /// Only the oldest open notice for a recipient may be newly admitted.
    /// Keyset fairness must not reorder one agent's lifecycle context.
    fn is_workspace_system_notice_head(
        &self,
        _notice: &WorkspaceSystemNoticeRecord,
    ) -> Result<bool, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace system notice recipient ordering unavailable".into(),
        ))
    }

    fn get_workspace_system_notice(
        &self,
        _notice_id: &str,
    ) -> Result<Option<WorkspaceSystemNoticeRecord>, RuntimeError> {
        Ok(None)
    }

    fn get_workspace_notice_policy(
        &self,
        _workspace_id: &str,
    ) -> Result<crate::WorkspaceNoticePolicyRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace notice policy unavailable".into(),
        ))
    }

    fn update_workspace_notice_policy(
        &self,
        _workspace_id: &str,
        _update: &crate::WorkspaceNoticePolicyUpdate,
    ) -> Result<crate::WorkspaceNoticePolicyRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace notice policy unavailable".into(),
        ))
    }

    fn transition_workspace_system_notice(
        &self,
        _notice_id: &str,
        _from_state: &str,
        _to_state: &str,
        _turn_id: Option<&str>,
        _error: Option<&str>,
        _now: i64,
    ) -> Result<bool, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable workspace notice transitions unavailable".into(),
        ))
    }

    fn list_workspace_agents(
        &self,
        _workspace_id: &str,
        _lifecycle: Option<WorkspaceAgentLifecycleState>,
    ) -> Result<Vec<WorkspaceAgentRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_workspace_agent(
        &self,
        _workspace_id: &str,
        _agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_workspace_agent_by_id(
        &self,
        _agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        Ok(None)
    }

    fn get_workspace_agent_by_alias(
        &self,
        _alias: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        Ok(None)
    }

    fn record_session_context_limit(
        &self,
        _snapshot: &SessionContextLimitSnapshot,
        _expected_session_updated_at: i64,
    ) -> Result<bool, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable provider context snapshots are not implemented by this runtime store".into(),
        ))
    }

    fn get_session_context_limit(
        &self,
        _agent_id: &str,
    ) -> Result<Option<SessionContextLimitSnapshot>, RuntimeError> {
        Ok(None)
    }

    fn begin_workspace_agent_rebind(
        &self,
        _operation: &WorkspaceAgentRebindOperation,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable workspace rebind is not implemented".into(),
        ))
    }

    fn finalize_workspace_agent_rebind(
        &self,
        _operation_id: &str,
        _updated_session: &SessionRecord,
        _updated_policy: &WorkspaceAgentRecreationPolicy,
        _evidence: &ProviderWorkspaceRebindEvidence,
        _changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable workspace rebind is not implemented".into(),
        ))
    }

    fn classify_workspace_agent_rebind(
        &self,
        _operation_id: &str,
        _phase: &str,
        _error_code: &str,
        _changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable workspace rebind is not implemented".into(),
        ))
    }

    fn get_workspace_agent_rebind(
        &self,
        _operation_id: &str,
    ) -> Result<Option<WorkspaceAgentRebindOperation>, RuntimeError> {
        Ok(None)
    }

    fn record_workspace_agent_rebind_cleanup(
        &self,
        _operation_id: &str,
        _observed_status: &str,
        _changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable previous-worktree cleanup recording is not implemented".into(),
        ))
    }

    fn get_workspace_agent_rebind_by_key(
        &self,
        _workspace_id: &str,
        _agent_id: &str,
        _idempotency_key: &str,
    ) -> Result<Option<WorkspaceAgentRebindOperation>, RuntimeError> {
        Ok(None)
    }

    fn unresolved_workspace_agent_rebind(&self, _agent_id: &str) -> Result<bool, RuntimeError> {
        Ok(false)
    }

    fn set_workspace_agent_lifecycle(
        &self,
        _session: &SessionRecord,
        _agent_id: &str,
        _lifecycle: WorkspaceAgentLifecycleState,
        _archive_reason: Option<&str>,
        _changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn set_workspace_agent_lifecycle_with_initiator(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        lifecycle: WorkspaceAgentLifecycleState,
        archive_reason: Option<&str>,
        changed_at: i64,
        _initiator_agent_id: Option<&str>,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.set_workspace_agent_lifecycle(session, agent_id, lifecycle, archive_reason, changed_at)
    }

    fn compare_and_set_workspace_agent_recreation_policy(
        &self,
        _session: &SessionRecord,
        _agent_id: &str,
        _expected_revision: u64,
        _recreation_policy: &crate::WorkspaceAgentRecreationPolicy,
        _changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace agent recreation-policy mutation is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn get_operation(&self, _operation_id: &str) -> Result<Option<OperationDetails>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "durable operation authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn persist_workspace_migration_preview(
        &self,
        _status: &LegacyWorkspaceMigrationStatus,
    ) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn workspace_migration_status(&self) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn apply_workspace_migration(
        &self,
        _command: &LegacyWorkspaceMigrationApplyCommand,
    ) -> Result<LegacyWorkspaceMigrationApplyResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn resolve_workspace_migration_subject(
        &self,
        _command: &LegacyWorkspaceMigrationResolutionCommand,
    ) -> Result<LegacyWorkspaceMigrationResolutionResponse, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "workspace migration authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError>;

    fn admit_turn(
        &self,
        _admission: &TurnAdmissionRecord,
        _turn: &TurnRecord,
        _session: &SessionRecord,
        _approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "atomic turn admission is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_turn_admission(&self, _record: &TurnAdmissionRecord) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn list_turn_admissions(&self) -> Result<Vec<TurnAdmissionRecord>, RuntimeError> {
        Ok(Vec::new())
    }

    fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError>;

    fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError>;

    fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError>;

    fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError>;

    fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError>;

    fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError>;

    fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError>;

    fn insert_agent_message_with_deliveries(
        &self,
        _message: &AgentMessageRecord,
        _deliveries: &[AgentDeliveryRecord],
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent message persistence is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_agent_delivery(&self, _record: &AgentDeliveryRecord) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "agent delivery persistence is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError>;
    fn managed_worktree_revision(&self, _worktree_id: &str) -> Result<u64, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed worktree revision unavailable".into(),
        ))
    }
    fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError>;
    fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError>;
    fn admit_managed_process(
        &self,
        _admission: &ManagedProcessAdmission,
    ) -> Result<ManagedProcessRecord, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process admission authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn list_managed_processes(&self) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn get_managed_process(
        &self,
        _process_id: &str,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn claim_managed_processes(
        &self,
        _settings: &ProcessSchedulerSettings,
        _claimed_at: i64,
    ) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process scheduler authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn mark_managed_process_running(
        &self,
        _process_id: &str,
        _claim_generation: i64,
        _pid: i64,
        _os_start_identity: &str,
        _started_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process launch authority is not implemented by this runtime store".to_string(),
        ))
    }

    fn requeue_managed_process_claim(
        &self,
        _process_id: &str,
        _claim_generation: i64,
        _updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process requeue authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn terminalize_managed_process(
        &self,
        _process_id: &str,
        _update: &ManagedProcessTerminalUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process terminal authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn update_managed_process_capture_progress(
        &self,
        _process_id: &str,
        _stream: &str,
        _captured_bytes: i64,
        _truncated: bool,
        _updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process capture progress is not implemented by this runtime store".to_string(),
        ))
    }

    fn cancel_queued_managed_process(
        &self,
        _process_id: &str,
        _owner_session_id: Option<&str>,
        _canceled_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process cancellation authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn request_managed_process_cancel(
        &self,
        _process_id: &str,
        _owner_session_id: Option<&str>,
        _updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process cancellation authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn update_managed_process_completion(
        &self,
        _process_id: &str,
        _update: &ProcessCompletionUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process completion authority is not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn load_or_initialize_process_scheduler_settings(
        &self,
        _defaults: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process scheduler settings are not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn replace_process_scheduler_settings(
        &self,
        _settings: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process scheduler settings are not implemented by this runtime store"
                .to_string(),
        ))
    }

    fn reorder_queued_managed_process(
        &self,
        _process_id: &str,
        _before_process_id: Option<&str>,
        _after_process_id: Option<&str>,
        _updated_at: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "managed-process queue reorder is not implemented by this runtime store".to_string(),
        ))
    }

    fn upsert_team_operation_journal(
        &self,
        record: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError>;

    fn append_team_operation_diagnostic(
        &self,
        operation_id: Option<&str>,
        team_id: Option<&str>,
        code: &str,
        message: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError>;

    fn list_team_operation_journal(
        &self,
        team_id: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError>;

    fn list_team_operation_diagnostics(
        &self,
        team_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError>;

    fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError>;
}

#[async_trait]
pub trait ToolGateway: Send + Sync {
    async fn healthcheck(&self) -> Result<(), RuntimeError>;

    async fn invoke_tool(&self, request: ToolInvokeRequest) -> Result<Value, RuntimeError>;

    async fn capabilities(&self) -> Result<Value, RuntimeError>;
}
