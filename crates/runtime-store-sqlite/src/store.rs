use std::path::Path;

use async_trait::async_trait;
use runtime_core::{
    ApprovalRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, NewRuntimeEvent,
    ProcessRecord, RuntimeError, RuntimeEventRecord, RuntimeEventScope, RuntimeHydratedState,
    RuntimeSourceBootstrap, RuntimeStore, SessionRecord, TeamDeliveryRecord, TeamMemberRecord,
    TeamMessageRecord, TeamOperationDiagnosticRecord, TeamOperationJournalRecord, TeamRecord,
    TurnRecord,
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

    fn source_bootstrap(&self) -> Result<RuntimeSourceBootstrap, RuntimeError> {
        self.repository.source_bootstrap()
    }

    fn upsert_session(&self, record: &SessionRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_session(record)
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

    fn upsert_managed_worktree(&self, record: &ManagedWorktreeRecord) -> Result<(), RuntimeError> {
        self.repository.upsert_managed_worktree(record)
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
