use runtime_core::{
    prepare_workspace_notice_policy_mutation, OperationActor, OperationPhase, OperationRecord,
    OperationTransitionRecord, RuntimeError, WorkspaceNoticePolicy,
    WorkspaceNoticePolicyMutationCommand, WorkspaceNoticePolicyMutationResponse,
    WorkspaceNoticePolicyRecord, WorkspaceNoticePolicyUpdate, WorkspaceSystemNoticeRecord,
};
use rusqlite::{params, types::Type, OptionalExtension, Row, Transaction, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::operation_by_idempotency;
use crate::SqliteRuntimeRepository;

const SELECT_NOTICE: &str = "SELECT notice_id, workspace_id, recipient_agent_id, kind,
    correlation_id, input_json, state, turn_id, attempt_count, last_error,
    created_at_ms, updated_at_ms FROM workspace_system_notices";
const NOTICE_POLICY_MUTATION_KIND: &str = "workspace_notice_policy_update";

fn write_notice_policy_tx(
    tx: &Transaction<'_>,
    workspace_id: &str,
    update: &WorkspaceNoticePolicyUpdate,
) -> Result<WorkspaceNoticePolicyRecord, RuntimeError> {
    let lifecycle: Option<String> = tx
        .query_row(
            "SELECT lifecycle_state FROM workspaces WHERE workspace_id = ?1",
            [workspace_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading notice policy workspace lifecycle", error))?;
    let frozen: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id = ?1)",
            [workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading notice policy retirement fence", error))?;
    if lifecycle.is_none() {
        return Err(RuntimeError::NotFound(format!("workspace {workspace_id}")));
    }
    if lifecycle.as_deref() != Some("active") || frozen {
        return Err(RuntimeError::Conflict(
            "cannot update notice policy while the workspace is retired or retiring".into(),
        ));
    }
    let current = workspace_notice_policy_tx(tx, workspace_id)?;
    if current.revision != update.expected_revision {
        return Err(RuntimeError::Conflict(format!(
            "workspace notice policy revision mismatch: expected {}, actual {}",
            update.expected_revision, current.revision,
        )));
    }
    let json = json_to_string(&serde_json::json!(update.policy))?;
    let next_revision = current.revision.checked_add(1).ok_or_else(|| {
        RuntimeError::InvalidState("workspace notice policy revision overflow".into())
    })?;
    tx.execute(
        "INSERT INTO workspace_notice_policies (workspace_id, revision, policy_json)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(workspace_id) DO UPDATE
         SET revision = excluded.revision, policy_json = excluded.policy_json",
        params![workspace_id, next_revision, json],
    )
    .map_err(|error| db_error("failed saving workspace notice policy", error))?;
    Ok(WorkspaceNoticePolicyRecord {
        workspace_id: workspace_id.to_string(),
        revision: next_revision,
        policy: update.policy.clone(),
    })
}

pub(crate) fn workspace_notice_policy_tx(
    tx: &Transaction<'_>,
    workspace_id: &str,
) -> Result<WorkspaceNoticePolicyRecord, RuntimeError> {
    let exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = ?1)",
            [workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed resolving workspace notice policy owner", error))?;
    if !exists {
        return Err(RuntimeError::NotFound(format!("workspace {workspace_id}")));
    }
    let row: Option<(i64, String)> = tx
        .query_row(
            "SELECT revision, policy_json FROM workspace_notice_policies WHERE workspace_id = ?1",
            [workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| db_error("failed loading workspace notice policy", error))?;
    let (revision, policy) = match row {
        Some((revision, json)) => (
            u64::try_from(revision)
                .map_err(|_| RuntimeError::Bootstrap("negative notice policy revision".into()))?,
            serde_json::from_str::<WorkspaceNoticePolicy>(&json).map_err(|error| {
                RuntimeError::Bootstrap(format!("corrupt workspace notice policy: {error}"))
            })?,
        ),
        None => (0, WorkspaceNoticePolicy::default()),
    };
    Ok(WorkspaceNoticePolicyRecord {
        workspace_id: workspace_id.to_string(),
        revision,
        policy,
    })
}

pub(crate) fn workspace_active_lead_tx(
    tx: &Transaction<'_>,
    workspace_id: &str,
) -> Result<Option<(String, String)>, RuntimeError> {
    tx.query_row(
        "SELECT a.session_id, a.identity_alias
         FROM workspaces w JOIN workspace_agents a ON a.session_id = w.lead_agent_id
         WHERE w.workspace_id = ?1 AND a.lifecycle_state = 'active'",
        [workspace_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(|error| db_error("failed snapshotting workspace lead notice recipient", error))
}

pub(crate) fn queue_workspace_lifecycle_notice_tx(
    tx: &Transaction<'_>,
    workspace_id: &str,
    recipient_agent_id: &str,
    event_id: &str,
    kind: &str,
    text: String,
    created_at_ms: i64,
) -> Result<(), RuntimeError> {
    let notice = WorkspaceSystemNoticeRecord::lifecycle(
        workspace_id,
        recipient_agent_id,
        event_id,
        kind,
        text,
        created_at_ms,
    );
    SqliteRuntimeRepository::insert_workspace_system_notice_tx(tx, &notice)
}

/// The lead receives teammate changes except their own membership changes and
/// actions they initiated. The active lead and policy are snapshotted in the
/// *same transaction* as the membership mutation, never queried afterward.
#[allow(clippy::too_many_arguments)]
pub(crate) fn queue_membership_lead_notice_tx(
    tx: &Transaction<'_>,
    workspace_id: &str,
    affected_agent_id: &str,
    affected_alias: &str,
    initiator_agent_id: Option<&str>,
    event_id: &str,
    action: &str,
    now: i64,
) -> Result<(), RuntimeError> {
    if !workspace_notice_policy_tx(tx, workspace_id)?
        .policy
        .membership_to_lead
    {
        return Ok(());
    }
    let Some((lead_id, _)) = workspace_active_lead_tx(tx, workspace_id)? else {
        return Ok(());
    };
    if lead_id == affected_agent_id || initiator_agent_id == Some(lead_id.as_str()) {
        return Ok(());
    }
    let initiator_alias: Option<String> = initiator_agent_id
        .map(|id| {
            tx.query_row(
                "SELECT identity_alias FROM workspace_agents WHERE session_id = ?1 AND workspace_id = ?2",
                params![id, workspace_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed resolving membership notice initiator", error))
        })
        .transpose()?
        .flatten();
    let actor = initiator_alias.as_deref().unwrap_or("User");
    let workspace_name: String = tx
        .query_row(
            "SELECT display_name FROM workspaces WHERE workspace_id = ?1",
            [workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading workspace membership notice name", error))?;
    queue_workspace_lifecycle_notice_tx(
        tx,
        workspace_id,
        &lead_id,
        event_id,
        action,
        format!("{actor} {action} {affected_alias} in workspace team \"{workspace_name}\"."),
        now,
    )
}

fn notice_from_row(row: &Row<'_>) -> rusqlite::Result<WorkspaceSystemNoticeRecord> {
    let input_json: String = row.get(5)?;
    let input = serde_json::from_str(&input_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(error))
    })?;
    let attempts: i64 = row.get(8)?;
    let attempt_count = u32::try_from(attempts).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(8, Type::Integer, Box::new(error))
    })?;
    Ok(WorkspaceSystemNoticeRecord {
        notice_id: row.get(0)?,
        workspace_id: row.get(1)?,
        recipient_agent_id: row.get(2)?,
        kind: row.get(3)?,
        correlation_id: row.get(4)?,
        input,
        state: row.get(6)?,
        turn_id: row.get(7)?,
        attempt_count,
        last_error: row.get(9)?,
        created_at_ms: row.get(10)?,
        updated_at_ms: row.get(11)?,
    })
}

impl SqliteRuntimeRepository {
    pub fn get_workspace_notice_policy(
        &self,
        workspace_id: &str,
    ) -> Result<WorkspaceNoticePolicyRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed opening workspace notice policy read", error))?;
        workspace_notice_policy_tx(&tx, workspace_id)
    }

    pub fn update_workspace_notice_policy(
        &self,
        workspace_id: &str,
        update: &WorkspaceNoticePolicyUpdate,
    ) -> Result<WorkspaceNoticePolicyRecord, RuntimeError> {
        // Older internal callers share the same transactional operation
        // authority; only the HTTP path supplies a replayable client key.
        let command = prepare_workspace_notice_policy_mutation(
            workspace_id,
            update.clone(),
            OperationActor::operator("runtime_store_compat"),
            None,
        )?;
        Ok(self
            .update_workspace_notice_policy_idempotent(&command)?
            .policy)
    }

    pub fn update_workspace_notice_policy_idempotent(
        &self,
        command: &WorkspaceNoticePolicyMutationCommand,
    ) -> Result<WorkspaceNoticePolicyMutationResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed opening durable notice policy transaction", error))?;
        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            command.idempotency_key.as_deref(),
        )? {
            if existing.kind != NOTICE_POLICY_MUTATION_KIND
                || existing.normalized_request_hash != command.normalized_request_hash
            {
                return Err(RuntimeError::Conflict(format!(
                    "Idempotency-Key already belongs to operation {} with different input",
                    existing.operation_id
                )));
            }
            let exact = existing.exact_terminal_result.ok_or_else(|| {
                RuntimeError::Conflict(format!(
                    "notice policy operation {} has no terminal result; review before retry",
                    existing.operation_id
                ))
            })?;
            let response = serde_json::from_value(exact).map_err(|error| {
                RuntimeError::Bootstrap(format!(
                    "failed decoding exact notice policy mutation receipt: {error}"
                ))
            })?;
            tx.commit().map_err(|error| {
                db_error("failed committing notice policy mutation replay", error)
            })?;
            return Ok(response);
        }

        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: NOTICE_POLICY_MUTATION_KIND.into(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: None,
            created_at: command.requested_at,
        })?;
        let claim = authority.acquire_claim(
            &command.operation_id,
            "workspace_notice_policy",
            &command.workspace_id,
            command.requested_at,
        )?;
        let policy = write_notice_policy_tx(&tx, &command.workspace_id, &command.update)?;
        let response = WorkspaceNoticePolicyMutationResponse {
            operation_id: command.operation_id.clone(),
            policy,
        };
        let exact = serde_json::to_value(&response).map_err(|error| {
            RuntimeError::Bootstrap(format!("failed encoding notice policy result: {error}"))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&command.workspace_id),
            &exact,
            None,
            None,
            command.requested_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({"revision": response.policy.revision})),
            created_at: command.requested_at,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing durable notice policy mutation", error))?;
        Ok(response)
    }

    pub(crate) fn insert_workspace_system_notice_tx(
        tx: &Transaction<'_>,
        notice: &WorkspaceSystemNoticeRecord,
    ) -> Result<(), RuntimeError> {
        tx.execute(
            "INSERT INTO workspace_system_notices
             (notice_id,workspace_id,recipient_agent_id,kind,correlation_id,
              input_json,state,turn_id,attempt_count,last_error,created_at_ms,updated_at_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                notice.notice_id,
                notice.workspace_id,
                notice.recipient_agent_id,
                notice.kind,
                notice.correlation_id,
                json_to_string(&serde_json::json!(notice.input))?,
                notice.state,
                notice.turn_id,
                notice.attempt_count,
                notice.last_error,
                notice.created_at_ms,
                notice.updated_at_ms,
            ],
        )
        .map_err(|error| db_error("failed atomically admitting workspace notice", error))?;
        Ok(())
    }

    pub fn list_workspace_system_notices(
        &self,
        limit: usize,
    ) -> Result<Vec<WorkspaceSystemNoticeRecord>, RuntimeError> {
        self.list_workspace_system_notices_after(None, limit)
    }

    pub fn list_workspace_system_notices_after(
        &self,
        after: Option<&WorkspaceSystemNoticeRecord>,
        limit: usize,
    ) -> Result<Vec<WorkspaceSystemNoticeRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let mut statement = connection
            .prepare(&format!(
                "{SELECT_NOTICE} WHERE state IN ('pending','injecting')
             AND (?2 IS NULL OR
                 (created_at_ms, CASE WHEN kind = 'onboarding' THEN 0 ELSE 1 END, notice_id)
                 > (?2, ?3, ?4))
             ORDER BY created_at_ms, CASE WHEN kind = 'onboarding' THEN 0 ELSE 1 END, notice_id
             LIMIT ?1"
            ))
            .map_err(|error| db_error("failed preparing outstanding notices", error))?;
        let rows = statement
            .query_map(
                params![
                    i64::try_from(limit.min(256)).unwrap_or(256),
                    after.map(|notice| notice.created_at_ms),
                    after.map(|notice| i64::from(notice.kind != "onboarding")),
                    after.map(|notice| notice.notice_id.as_str()),
                ],
                notice_from_row,
            )
            .map_err(|error| db_error("failed querying outstanding notices", error))?;
        crate::db::collect_rows(rows)
    }

    pub fn is_workspace_system_notice_head(
        &self,
        notice: &WorkspaceSystemNoticeRecord,
    ) -> Result<bool, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let has_earlier: i64 = connection
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM workspace_system_notices
                   WHERE recipient_agent_id = ?1
                     AND state IN ('pending','injecting')
                     AND (created_at_ms, CASE WHEN kind = 'onboarding' THEN 0 ELSE 1 END, notice_id)
                         < (?2, ?3, ?4)
                 )",
                params![
                    notice.recipient_agent_id,
                    notice.created_at_ms,
                    i64::from(notice.kind != "onboarding"),
                    notice.notice_id,
                ],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking workspace notice recipient order", error))?;
        Ok(has_earlier == 0)
    }

    pub fn get_workspace_system_notice(
        &self,
        notice_id: &str,
    ) -> Result<Option<WorkspaceSystemNoticeRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                &format!("{SELECT_NOTICE} WHERE notice_id = ?1"),
                [notice_id],
                notice_from_row,
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace notice", error))
    }

    pub fn transition_workspace_system_notice(
        &self,
        notice_id: &str,
        from_state: &str,
        to_state: &str,
        turn_id: Option<&str>,
        error: Option<&str>,
        now: i64,
    ) -> Result<bool, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let changed = connection
            .execute(
                "UPDATE workspace_system_notices SET state = ?3,
              attempt_count = attempt_count + CASE WHEN ?3 = 'injecting' THEN 1 ELSE 0 END,
              turn_id = CASE WHEN ?3 = 'pending' THEN NULL ELSE COALESCE(?4,turn_id) END,
              last_error = ?5, updated_at_ms = ?6
             WHERE notice_id = ?1 AND state = ?2
               AND (?3 != 'cancelled' OR ?5 != 'recipient_archived' OR EXISTS (
                 SELECT 1 FROM workspace_agents
                 WHERE session_id = workspace_system_notices.recipient_agent_id
                   AND lifecycle_state = 'archived'
               ))",
                params![notice_id, from_state, to_state, turn_id, error, now],
            )
            .map_err(|error| db_error("failed transitioning workspace notice", error))?;
        Ok(changed == 1)
    }
}
