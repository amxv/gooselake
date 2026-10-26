use std::collections::HashSet;

use runtime_core::{
    prepare_team_hook_approval_mutation, team_hook_fingerprint, OperationActor, OperationPhase,
    OperationRecord, OperationTransitionRecord, RuntimeError, TeamHookApproval,
    TeamHookApprovalMutationCommand, TeamHookApprovalMutationResponse, TeamHookApprovalUpdate,
    TeamHookResolvedEntry, TeamHookSelection,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::operation_by_idempotency;
use crate::SqliteRuntimeRepository;

const HOOK_APPROVAL_KIND: &str = "workspace_hook_approval_update";

impl SqliteRuntimeRepository {
    pub fn get_team_hook_approval(
        &self,
        workspace_id: &str,
    ) -> Result<TeamHookApproval, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = ?1)",
                [workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking hook approval workspace", error))?;
        if !exists {
            return Err(RuntimeError::NotFound(format!("workspace {workspace_id}")));
        }
        let existing: Option<(i64, String, i64)> = connection
            .query_row(
                "SELECT revision, approved_hooks_json, approved_at_ms
                 FROM workspace_hook_approvals WHERE workspace_id = ?1",
                [workspace_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading hook approval", error))?;
        approval_from_row(workspace_id, existing)
    }

    /// This mutation must only be called by the authenticated operator path.
    /// Store exact resolved commands, not user-submitted shell strings.
    pub fn update_team_hook_approval(
        &self,
        update: &TeamHookApprovalUpdate,
    ) -> Result<TeamHookApproval, RuntimeError> {
        // Trusted internal callers use the same durable authority. The
        // operator endpoint additionally supplies an HTTP replay key.
        let selections = update
            .hooks
            .iter()
            .map(|hook| TeamHookSelection {
                event_name: hook.event_name,
                name: hook.name.clone(),
                fingerprint: team_hook_fingerprint(hook),
            })
            .collect();
        let mut command = prepare_team_hook_approval_mutation(
            &update.workspace_id,
            update.expected_revision,
            selections,
            OperationActor::operator("runtime_store_compat"),
            None,
        )?;
        command.requested_at_ms = update.approved_at_ms;
        Ok(self
            .update_team_hook_approval_idempotent(&command, &update.hooks)?
            .approval)
    }

    /// Resolve a prior terminal operator receipt without consulting mutable
    /// effective hook configuration or the current CAS revision.
    pub fn replay_team_hook_approval_mutation(
        &self,
        command: &TeamHookApprovalMutationCommand,
    ) -> Result<Option<TeamHookApprovalMutationResponse>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        replay_hook_approval(&connection, command)
    }

    pub fn update_team_hook_approval_idempotent(
        &self,
        command: &TeamHookApprovalMutationCommand,
        hooks: &[TeamHookResolvedEntry],
    ) -> Result<TeamHookApprovalMutationResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting hook approval transaction", error))?;
        if let Some(response) = replay_hook_approval(&transaction, command)? {
            return Ok(response);
        }
        let mut seen = HashSet::new();
        if hooks.len() != command.selections.len()
            || hooks.len() > 6 * 32
            || hooks
                .iter()
                .zip(&command.selections)
                .any(|(hook, selection)| {
                    hook.name.is_empty()
                        || hook.name.len() > 128
                        || hook.command.is_empty()
                        || hook.command.len() > 4096
                        || !(100..=120_000).contains(&hook.timeout_ms)
                        || !seen.insert((hook.event_name, hook.name.clone()))
                        || hook.event_name != selection.event_name
                        || hook.name != selection.name
                        || team_hook_fingerprint(hook) != selection.fingerprint
                })
        {
            return Err(RuntimeError::InvalidState(
                "invalid or duplicate hook approval snapshot".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: HOOK_APPROVAL_KIND.into(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at_ms,
            updated_at: command.requested_at_ms,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: None,
            created_at: command.requested_at_ms,
        })?;
        let claim = authority.acquire_claim(
            &command.operation_id,
            "workspace_hook_approval",
            &command.workspace_id,
            command.requested_at_ms,
        )?;
        let approval = write_hook_approval_tx(
            &transaction,
            &TeamHookApprovalUpdate {
                workspace_id: command.workspace_id.clone(),
                expected_revision: command.expected_revision,
                hooks: hooks.to_vec(),
                approved_at_ms: command.requested_at_ms,
            },
        )?;
        let response = TeamHookApprovalMutationResponse {
            operation_id: command.operation_id.clone(),
            approval,
        };
        let receipt = serde_json::to_value(&response).map_err(|error| {
            RuntimeError::Bootstrap(format!(
                "failed encoding exact hook approval receipt: {error}"
            ))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&command.workspace_id),
            &receipt,
            None,
            None,
            command.requested_at_ms,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({"revision": response.approval.revision})),
            created_at: command.requested_at_ms,
        })?;
        authority.release_claim(&claim)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing durable hook approval", error))?;
        Ok(response)
    }
}

fn replay_hook_approval(
    connection: &Connection,
    command: &TeamHookApprovalMutationCommand,
) -> Result<Option<TeamHookApprovalMutationResponse>, RuntimeError> {
    let Some(existing) = operation_by_idempotency(
        connection,
        command.actor.kind,
        &command.actor.identifier,
        command.idempotency_key.as_deref(),
    )?
    else {
        return Ok(None);
    };
    if existing.kind != HOOK_APPROVAL_KIND
        || existing.normalized_request_hash != command.normalized_request_hash
    {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key already belongs to operation {} with different input",
            existing.operation_id
        )));
    }
    let receipt = existing.exact_terminal_result.ok_or_else(|| {
        RuntimeError::Conflict(format!(
            "hook approval operation {} is not terminal; review before retry",
            existing.operation_id
        ))
    })?;
    serde_json::from_value(receipt).map(Some).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "failed decoding exact hook approval receipt: {error}"
        ))
    })
}

fn write_hook_approval_tx(
    transaction: &Transaction<'_>,
    update: &TeamHookApprovalUpdate,
) -> Result<TeamHookApproval, RuntimeError> {
    let lifecycle: Option<String> = transaction
        .query_row(
            "SELECT lifecycle_state FROM workspaces WHERE workspace_id = ?1",
            [&update.workspace_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading hook approval workspace", error))?;
    match lifecycle.as_deref() {
        None => {
            return Err(RuntimeError::NotFound(format!(
                "workspace {}",
                update.workspace_id
            )))
        }
        Some("active") => {}
        _ => {
            return Err(RuntimeError::Conflict(
                "cannot approve hooks for a retired workspace".into(),
            ))
        }
    }
    let frozen: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id = ?1)",
            [&update.workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking hook approval retirement fence", error))?;
    if frozen {
        return Err(RuntimeError::Conflict(
            "cannot approve hooks while the workspace is retiring".into(),
        ));
    }
    let existing: Option<(i64, String, i64)> = transaction
        .query_row(
            "SELECT revision, approved_hooks_json, approved_at_ms
                 FROM workspace_hook_approvals WHERE workspace_id = ?1",
            [&update.workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| db_error("failed reading previous hook approval", error))?;
    let current = approval_from_row(&update.workspace_id, existing)?;
    if current.revision != update.expected_revision {
        return Err(RuntimeError::Conflict(format!(
            "hook approval revision changed from {} to {}",
            update.expected_revision, current.revision
        )));
    }
    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| RuntimeError::InvalidState("hook approval revision overflow".into()))?;
    transaction
            .execute(
                "INSERT INTO workspace_hook_approvals (workspace_id,revision,approved_hooks_json,approved_at_ms)
                 VALUES (?1,?2,?3,?4)
                 ON CONFLICT(workspace_id) DO UPDATE SET revision=excluded.revision,
                   approved_hooks_json=excluded.approved_hooks_json,approved_at_ms=excluded.approved_at_ms",
                params![
                    update.workspace_id,
                    i64::try_from(revision).map_err(|_| RuntimeError::InvalidState("hook approval revision overflow".into()))?,
                    json_to_string(&serde_json::to_value(&update.hooks)
                        .map_err(|error| RuntimeError::Bootstrap(format!("invalid hook snapshot: {error}")))?)?,
                    update.approved_at_ms,
                ],
            )
            .map_err(|error| db_error("failed recording hook approval", error))?;
    Ok(TeamHookApproval {
        workspace_id: update.workspace_id.clone(),
        revision,
        hooks: update.hooks.clone(),
        approved_at_ms: Some(update.approved_at_ms),
    })
}

fn approval_from_row(
    workspace_id: &str,
    row: Option<(i64, String, i64)>,
) -> Result<TeamHookApproval, RuntimeError> {
    let Some((revision, hooks_json, approved_at_ms)) = row else {
        return Ok(TeamHookApproval {
            workspace_id: workspace_id.into(),
            revision: 0,
            hooks: Vec::new(),
            approved_at_ms: None,
        });
    };
    let hooks =
        serde_json::from_str::<Vec<TeamHookResolvedEntry>>(&hooks_json).map_err(|error| {
            RuntimeError::Bootstrap(format!("corrupt hook approval snapshot: {error}"))
        })?;
    Ok(TeamHookApproval {
        workspace_id: workspace_id.into(),
        revision: u64::try_from(revision)
            .map_err(|_| RuntimeError::Bootstrap("negative hook approval revision".into()))?,
        hooks,
        approved_at_ms: Some(approved_at_ms),
    })
}
