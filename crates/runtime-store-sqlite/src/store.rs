use std::path::Path;

use async_trait::async_trait;
use runtime_core::{
    AgentDeliveryRecord, AgentMessageRecord, ApprovalRecord, LegacyWorkspaceMigrationApplyCommand,
    LegacyWorkspaceMigrationApplyResponse, LegacyWorkspaceMigrationResolutionCommand,
    LegacyWorkspaceMigrationResolutionResponse, LegacyWorkspaceMigrationStatus,
    ManagedProcessAdmission, ManagedProcessRecord, ManagedProcessTerminalUpdate,
    ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent, OperationDetails,
    ProcessCompletionUpdate, ProcessRecord, ProcessSchedulerSettings,
    ProviderWorkspaceRebindEvidence, RuntimeError, RuntimeEventRecord, RuntimeEventScope,
    RuntimeHydratedState, RuntimeStore, SessionContextLimitSnapshot, SessionRecord,
    TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord,
    TeamOperationJournalRecord, TeamRecord, TurnAdmissionRecord, TurnRecord,
    WorkspaceAgentLifecycleState, WorkspaceAgentRebindOperation, WorkspaceAgentRecord,
    WorkspaceAgentRecreationPolicy, WorkspaceInterruptAdmission, WorkspaceInterruptCommand,
    WorkspaceInterruptResponse, WorkspaceLeadTransitionCommand, WorkspaceLeadTransitionResponse,
    WorkspaceRecord, WorkspaceRegisterCommand, WorkspaceRegisterResponse,
};
use serde_json::Value;

use crate::db::{db_error, open_connection};
use crate::{SqliteRuntimeRepository, SqliteStoreConfig};

#[derive(Debug)]
pub struct SqliteRuntimeStore {
    config: SqliteStoreConfig,
    repository: SqliteRuntimeRepository,
}

impl SqliteRuntimeStore {
    pub fn new(config: SqliteStoreConfig) -> Self {
        let repository = SqliteRuntimeRepository::new(config.database_path.clone());
        Self { config, repository }
    }

    pub fn database_path(&self) -> &Path {
        &self.config.database_path
    }

    pub fn repository(&self) -> &SqliteRuntimeRepository {
        &self.repository
    }

    pub fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError> {
        self.repository.hydrate_runtime_state()
    }

    async fn ensure_parent_dir(path: &Path) -> Result<(), RuntimeError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl RuntimeStore for SqliteRuntimeStore {
    fn record_session_context_limit(
        &self,
        snapshot: &SessionContextLimitSnapshot,
        expected_session_updated_at: i64,
    ) -> Result<bool, RuntimeError> {
        self.repository
            .record_session_context_limit(snapshot, expected_session_updated_at)
    }

    fn get_session_context_limit(
        &self,
        agent_id: &str,
    ) -> Result<Option<SessionContextLimitSnapshot>, RuntimeError> {
        self.repository.get_session_context_limit(agent_id)
    }

    async fn initialize(&self) -> Result<(), RuntimeError> {
        Self::ensure_parent_dir(self.database_path()).await?;
        self.repository.initialize_schema()
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        let connection = open_connection(self.database_path())?;
        let _: i64 = connection
            .query_row("SELECT 1", [], |row| row.get(0))
            .map_err(|error| db_error("sqlite healthcheck query failed", error))?;
        Ok(())
    }

    fn append_runtime_event(
        &self,
        event: &NewRuntimeEvent,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        self.repository.append_runtime_event(event)
    }

    fn list_runtime_events(
        &self,
        scope: Option<(RuntimeEventScope, &str)>,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<RuntimeEventRecord>, RuntimeError> {
        self.repository.list_runtime_events(scope, after_seq, limit)
    }

    fn register_workspace(
        &self,
        command: &WorkspaceRegisterCommand,
    ) -> Result<WorkspaceRegisterResponse, RuntimeError> {
        self.repository.register_workspace(command)
    }

    fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, RuntimeError> {
        self.repository.list_workspaces()
    }

    fn get_workspace(&self, workspace_id: &str) -> Result<Option<WorkspaceRecord>, RuntimeError> {
        self.repository.get_workspace(workspace_id)
    }

    fn transition_workspace_lead(
        &self,
        command: &WorkspaceLeadTransitionCommand,
    ) -> Result<WorkspaceLeadTransitionResponse, RuntimeError> {
        self.repository.transition_workspace_lead(command)
    }

    fn begin_workspace_interrupt(
        &self,
        command: &WorkspaceInterruptCommand,
    ) -> Result<WorkspaceInterruptAdmission, RuntimeError> {
        self.repository.begin_workspace_interrupt(command)
    }

    fn mark_workspace_interrupt_started(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        self.repository.mark_workspace_interrupt_started(
            operation_id,
            agent_id,
            turn_id,
            changed_at,
        )
    }

    fn finalize_workspace_interrupt_effect(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        interrupted: bool,
        reason: &str,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        self.repository.finalize_workspace_interrupt_effect(
            operation_id,
            agent_id,
            turn_id,
            interrupted,
            reason,
            changed_at,
        )
    }

    fn mark_workspace_interrupt_uncertain(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        error: &Value,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        self.repository.mark_workspace_interrupt_uncertain(
            operation_id,
            agent_id,
            turn_id,
            error,
            changed_at,
        )
    }

    fn complete_workspace_interrupt(
        &self,
        operation_id: &str,
        completed_at: i64,
    ) -> Result<WorkspaceInterruptResponse, RuntimeError> {
        self.repository
            .complete_workspace_interrupt(operation_id, completed_at)
    }

    fn create_workspace_agent(
        &self,
        session: &SessionRecord,
        agent: &WorkspaceAgentRecord,
    ) -> Result<(), RuntimeError> {
        self.repository.create_workspace_agent(session, agent)
    }

    fn begin_workspace_agent_rebind(
        &self,
        operation: &WorkspaceAgentRebindOperation,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        self.repository.begin_workspace_agent_rebind(operation)
    }

    fn finalize_workspace_agent_rebind(
        &self,
        operation_id: &str,
        session: &SessionRecord,
        policy: &WorkspaceAgentRecreationPolicy,
        evidence: &ProviderWorkspaceRebindEvidence,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        self.repository.finalize_workspace_agent_rebind(
            operation_id,
            session,
            policy,
            evidence,
            changed_at,
        )
    }

    fn classify_workspace_agent_rebind(
        &self,
        operation_id: &str,
        phase: &str,
        error_code: &str,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        self.repository
            .classify_workspace_agent_rebind(operation_id, phase, error_code, changed_at)
    }

    fn get_workspace_agent_rebind(
        &self,
        operation_id: &str,
    ) -> Result<Option<WorkspaceAgentRebindOperation>, RuntimeError> {
        self.repository.get_workspace_agent_rebind(operation_id)
    }

    fn record_workspace_agent_rebind_cleanup(
        &self,
        operation_id: &str,
        observed_status: &str,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        self.repository.record_workspace_agent_rebind_cleanup(
            operation_id,
            observed_status,
            changed_at,
        )
    }

    fn get_workspace_agent_rebind_by_key(
        &self,
        workspace_id: &str,
        agent_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<WorkspaceAgentRebindOperation>, RuntimeError> {
        self.repository
            .get_workspace_agent_rebind_by_key(workspace_id, agent_id, idempotency_key)
    }

    fn unresolved_workspace_agent_rebind(&self, agent_id: &str) -> Result<bool, RuntimeError> {
        self.repository.unresolved_workspace_agent_rebind(agent_id)
    }

    fn list_workspace_agents(
        &self,
        workspace_id: &str,
        lifecycle: Option<WorkspaceAgentLifecycleState>,
    ) -> Result<Vec<WorkspaceAgentRecord>, RuntimeError> {
        self.repository
            .list_workspace_agents(workspace_id, lifecycle)
    }

    fn get_workspace_agent(
        &self,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        self.repository.get_workspace_agent(workspace_id, agent_id)
    }

    fn get_workspace_agent_by_id(
        &self,
        agent_id: &str,
    ) -> Result<Option<WorkspaceAgentRecord>, RuntimeError> {
        self.repository.get_workspace_agent_by_id(agent_id)
    }

    fn set_workspace_agent_lifecycle(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        lifecycle: WorkspaceAgentLifecycleState,
        archive_reason: Option<&str>,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.repository.set_workspace_agent_lifecycle(
            session,
            agent_id,
            lifecycle,
            archive_reason,
            changed_at,
        )
    }

    fn compare_and_set_workspace_agent_recreation_policy(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        expected_revision: u64,
        recreation_policy: &WorkspaceAgentRecreationPolicy,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.repository
            .compare_and_set_workspace_agent_recreation_policy(
                session,
                agent_id,
                expected_revision,
                recreation_policy,
                changed_at,
            )
    }

    fn get_operation(&self, operation_id: &str) -> Result<Option<OperationDetails>, RuntimeError> {
        self.repository.get_operation(operation_id)
    }

    fn persist_workspace_migration_preview(
        &self,
        status: &LegacyWorkspaceMigrationStatus,
    ) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        self.repository.persist_workspace_migration_preview(status)
    }

    fn workspace_migration_status(&self) -> Result<LegacyWorkspaceMigrationStatus, RuntimeError> {
        self.repository.workspace_migration_status()
    }

    fn apply_workspace_migration(
        &self,
        command: &LegacyWorkspaceMigrationApplyCommand,
    ) -> Result<LegacyWorkspaceMigrationApplyResponse, RuntimeError> {
        self.repository.apply_workspace_migration(command)
    }

    fn resolve_workspace_migration_subject(
        &self,
        command: &LegacyWorkspaceMigrationResolutionCommand,
    ) -> Result<LegacyWorkspaceMigrationResolutionResponse, RuntimeError> {
        self.repository.resolve_workspace_migration_subject(command)
    }

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_session(record)
    }

    fn admit_turn(
        &self,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        self.repository
            .admit_turn(admission, turn, session, approval)
    }

    fn upsert_turn_admission(&self, record: &TurnAdmissionRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_turn_admission(record)
    }

    fn list_turn_admissions(&self) -> Result<Vec<TurnAdmissionRecord>, RuntimeError> {
        self.repository.list_turn_admissions()
    }

    fn upsert_turn(&self, record: &TurnRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_turn(record)
    }

    fn upsert_approval(&self, record: &ApprovalRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_approval(record)
    }

    fn upsert_team(&self, record: &TeamRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team(record)
    }

    fn upsert_team_member(&self, record: &TeamMemberRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team_member(record)
    }

    fn delete_team_member(&self, team_id: &str, agent_id: &str) -> Result<(), RuntimeError> {
        self.repository.delete_team_member(team_id, agent_id)
    }

    fn upsert_team_message(&self, record: &TeamMessageRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team_message(record)
    }

    fn upsert_team_delivery(&self, record: &TeamDeliveryRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_team_delivery(record)
    }

    fn insert_agent_message_with_deliveries(
        &self,
        message: &AgentMessageRecord,
        deliveries: &[AgentDeliveryRecord],
    ) -> Result<(), RuntimeError> {
        self.repository
            .insert_agent_message_with_deliveries(message, deliveries)
    }

    fn upsert_agent_delivery(&self, record: &AgentDeliveryRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_agent_delivery(record)
    }

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_managed_worktree(record)
    }

    fn managed_worktree_revision(&self, worktree_id: &str) -> Result<u64, RuntimeError> {
        self.repository.managed_worktree_revision(worktree_id)
    }

    fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        self.repository.upsert_managed_worktree_claim(record)
    }

    fn upsert_process(&self, record: &ProcessRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_process(record)
    }

    fn admit_managed_process(
        &self,
        admission: &ManagedProcessAdmission,
    ) -> Result<ManagedProcessRecord, RuntimeError> {
        self.repository.admit_managed_process(admission)
    }

    fn list_managed_processes(&self) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        self.repository.list_managed_processes()
    }

    fn get_managed_process(
        &self,
        process_id: &str,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository.get_managed_process(process_id)
    }

    fn claim_managed_processes(
        &self,
        settings: &ProcessSchedulerSettings,
        claimed_at: i64,
    ) -> Result<Vec<ManagedProcessRecord>, RuntimeError> {
        self.repository
            .claim_managed_processes(settings, claimed_at)
    }

    fn mark_managed_process_running(
        &self,
        process_id: &str,
        claim_generation: i64,
        pid: i64,
        os_start_identity: &str,
        started_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository.mark_managed_process_running(
            process_id,
            claim_generation,
            pid,
            os_start_identity,
            started_at,
        )
    }

    fn requeue_managed_process_claim(
        &self,
        process_id: &str,
        claim_generation: i64,
        updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository
            .requeue_managed_process_claim(process_id, claim_generation, updated_at)
    }

    fn terminalize_managed_process(
        &self,
        process_id: &str,
        update: &ManagedProcessTerminalUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository
            .terminalize_managed_process(process_id, update)
    }

    fn update_managed_process_capture_progress(
        &self,
        process_id: &str,
        stream: &str,
        captured_bytes: i64,
        truncated: bool,
        updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository.update_managed_process_capture_progress(
            process_id,
            stream,
            captured_bytes,
            truncated,
            updated_at,
        )
    }

    fn cancel_queued_managed_process(
        &self,
        process_id: &str,
        owner_session_id: Option<&str>,
        canceled_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository
            .cancel_queued_managed_process(process_id, owner_session_id, canceled_at)
    }

    fn request_managed_process_cancel(
        &self,
        process_id: &str,
        owner_session_id: Option<&str>,
        updated_at: i64,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository
            .request_managed_process_cancel(process_id, owner_session_id, updated_at)
    }

    fn update_managed_process_completion(
        &self,
        process_id: &str,
        update: &ProcessCompletionUpdate,
    ) -> Result<Option<ManagedProcessRecord>, RuntimeError> {
        self.repository
            .update_managed_process_completion(process_id, update)
    }

    fn load_or_initialize_process_scheduler_settings(
        &self,
        defaults: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        self.repository
            .load_or_initialize_process_scheduler_settings(defaults)
    }

    fn replace_process_scheduler_settings(
        &self,
        settings: &ProcessSchedulerSettings,
    ) -> Result<ProcessSchedulerSettings, RuntimeError> {
        self.repository.replace_process_scheduler_settings(settings)
    }

    fn reorder_queued_managed_process(
        &self,
        process_id: &str,
        before_process_id: Option<&str>,
        after_process_id: Option<&str>,
        updated_at: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        self.repository.reorder_queued_managed_process(
            process_id,
            before_process_id,
            after_process_id,
            updated_at,
        )
    }

    fn upsert_team_operation_journal(
        &self,
        record: &TeamOperationJournalRecord,
    ) -> Result<(), RuntimeError> {
        self.repository.upsert_team_operation_journal(record)
    }

    fn append_team_operation_diagnostic(
        &self,
        operation_id: Option<&str>,
        team_id: Option<&str>,
        code: &str,
        message: &str,
        payload: &Value,
        created_at: i64,
    ) -> Result<TeamOperationDiagnosticRecord, RuntimeError> {
        self.repository.append_team_operation_diagnostic(
            operation_id,
            team_id,
            code,
            message,
            payload,
            created_at,
        )
    }

    fn list_team_operation_journal(
        &self,
        team_id: Option<&str>,
    ) -> Result<Vec<TeamOperationJournalRecord>, RuntimeError> {
        self.repository.list_team_operation_journal(team_id)
    }

    fn list_team_operation_diagnostics(
        &self,
        team_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> Result<Vec<TeamOperationDiagnosticRecord>, RuntimeError> {
        self.repository
            .list_team_operation_diagnostics(team_id, operation_id)
    }

    fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError> {
        self.repository.hydrate_runtime_state()
    }
}
