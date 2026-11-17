//! Mutable cleanup observations retain the original immutable rebind receipt.
use super::*;
use runtime_core::{NewRuntimeEvent, RuntimeEventCriticality, RuntimeEventScope};

impl SqliteRuntimeRepository {
    pub fn record_workspace_agent_rebind_cleanup(
        &self,
        operation_id: &str,
        observed_status: &str,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        const COMPLETED: &[&str] = &["deleted", "retained_by_policy", "skipped_live_claims"];
        const RETRYABLE: &[&str] = &[
            "skipped_external_worktree",
            "skipped_unverified_native",
            "skipped_dirty_worktree",
            "skipped_unmerged_branch",
            "skipped_live_binding",
            "cleanup_failed",
            "cleanup_error",
        ];
        if !COMPLETED.contains(&observed_status) && !RETRYABLE.contains(&observed_status) {
            return Err(RuntimeError::ProtocolViolation(
                "unrecognized native previous-worktree cleanup outcome".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting previous-worktree cleanup update", error))?;
        let mut operation = get(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("worktree rebind {operation_id}")))?;
        if operation.phase != "completed"
            || !operation.cleanup_previous_worktree
            || operation.previous_worktree_id.is_none()
        {
            return Err(RuntimeError::Conflict(
                "native cleanup requires an already verified rebind with explicit cleanup intent"
                    .into(),
            ));
        }
        if operation.previous_cleanup_status.as_deref() != Some("pending") {
            return Ok(operation);
        }
        let previous = operation.clone();
        if COMPLETED.contains(&observed_status) {
            operation.previous_cleanup_status = Some(observed_status.to_string());
            operation.previous_cleanup_diagnostic = None;
        } else {
            operation.previous_cleanup_diagnostic = Some(observed_status.to_string());
        }
        if operation.previous_cleanup_status == previous.previous_cleanup_status
            && operation.previous_cleanup_diagnostic == previous.previous_cleanup_diagnostic
        {
            return Ok(previous);
        }
        operation.updated_at = changed_at;
        tx.execute(
            "UPDATE workspace_agent_rebinds
             SET record_json = ?2, updated_at = ?3
             WHERE operation_id = ?1 AND phase = 'completed'",
            params![operation_id, encode(&operation)?, changed_at],
        )
        .map_err(|error| db_error("failed recording previous-worktree cleanup outcome", error))?;
        journal_cleanup_observation(&tx, &previous, &operation)?;
        if get(&tx, operation_id)?.as_ref() != Some(&operation) {
            return Err(RuntimeError::Conflict(
                "rebind cleanup projection changed during its event receipt".into(),
            ));
        }
        tx.commit().map_err(|error| {
            db_error("failed committing previous-worktree cleanup outcome", error)
        })?;
        Ok(operation)
    }
}

fn journal_cleanup_observation(
    tx: &Transaction<'_>,
    previous: &WorkspaceAgentRebindOperation,
    operation: &WorkspaceAgentRebindOperation,
) -> Result<(), RuntimeError> {
    let prior_sequence: i64 = tx.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM runtime_events WHERE scope='workspace' AND scope_id=?1",
        [&operation.workspace_id], |row| row.get(0),
    ).map_err(|error| db_error("failed reading cleanup observation sequence", error))?;
    let sequence = prior_sequence
        .checked_add(1)
        .filter(|next| *next > 0)
        .ok_or_else(|| {
            RuntimeError::InvalidState("cleanup observation event sequence exhausted".into())
        })?;
    let expected = NewRuntimeEvent {
        event_id: format!("{}:cleanup_observation:{sequence}", operation.operation_id),
        scope: RuntimeEventScope::Workspace,
        scope_id: operation.workspace_id.clone(),
        session_id: Some(operation.agent_id.clone()),
        team_id: None,
        turn_id: None,
        kind: "workspace.rebind_cleanup_observed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({
            "operation_id": operation.operation_id,
            "workspace_id": operation.workspace_id,
            "agent_id": operation.agent_id,
            "worktree_id": operation.previous_worktree_id,
            "previous_status": previous.previous_cleanup_status,
            "previous_diagnostic": previous.previous_cleanup_diagnostic,
            "status": operation.previous_cleanup_status,
            "diagnostic": operation.previous_cleanup_diagnostic,
        }),
        provider: None,
        provider_seq: None,
        created_at: operation.updated_at,
    };
    let event = crate::repository::insert_runtime_event_in_transaction(tx, &expected)?;
    if event.event_id != expected.event_id
        || event.seq != sequence
        || event.scope != expected.scope
        || event.scope_id != expected.scope_id
        || event.session_id != expected.session_id
        || event.team_id != expected.team_id
        || event.turn_id != expected.turn_id
        || event.kind != expected.kind
        || event.criticality != expected.criticality
        || event.payload != expected.payload
        || event.provider != expected.provider
        || event.provider_seq != expected.provider_seq
        || event.created_at != expected.created_at
    {
        return Err(RuntimeError::Conflict(
            "cleanup observation lacks its exact Critical event".into(),
        ));
    }
    Ok(())
}
