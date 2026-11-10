use crate::db::{db_error, open_connection};
use crate::repository_team_hooks::{active_workspace_hook_targets_tx, queue_team_hook_trigger_tx};
use crate::repository_workspace_agent::{update_session, workspace_agent_from_row, AGENT_SELECT};
use crate::repository_workspace_notice::queue_membership_lead_notice_tx;
use crate::SqliteRuntimeRepository;
use runtime_core::{
    RuntimeError, SessionRecord, WorkspaceAgentLifecycleState, WorkspaceAgentRecord,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

impl SqliteRuntimeRepository {
    pub fn set_workspace_agent_lifecycle(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        lifecycle: WorkspaceAgentLifecycleState,
        archive_reason: Option<&str>,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        self.set_workspace_agent_lifecycle_with_initiator(
            session,
            agent_id,
            lifecycle,
            archive_reason,
            changed_at,
            None,
        )
    }

    pub fn set_workspace_agent_lifecycle_with_initiator(
        &self,
        session: &SessionRecord,
        agent_id: &str,
        lifecycle: WorkspaceAgentLifecycleState,
        archive_reason: Option<&str>,
        changed_at: i64,
        initiator_agent_id: Option<&str>,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed starting workspace agent lifecycle transaction",
                    error,
                )
            })?;
        let updated = apply_workspace_agent_lifecycle_tx(
            &tx,
            session,
            agent_id,
            lifecycle,
            archive_reason,
            changed_at,
            initiator_agent_id,
            None,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing workspace agent lifecycle", error))?;
        Ok(updated)
    }
}

/// The same transaction-local lifecycle mutation is used by ordinary manager
/// calls and native operator receipts. A successful provider resume, updated
/// authoritative roster and exact operation receipt must commit atomically.
pub(crate) fn apply_workspace_agent_lifecycle_tx(
    tx: &Transaction<'_>,
    session: &SessionRecord,
    agent_id: &str,
    lifecycle: WorkspaceAgentLifecycleState,
    archive_reason: Option<&str>,
    changed_at: i64,
    initiator_agent_id: Option<&str>,
    allowed_lifecycle_operation: Option<&str>,
) -> Result<WorkspaceAgentRecord, RuntimeError> {
    if session.id != agent_id {
        return Err(RuntimeError::ProtocolViolation(
            "workspace agent lifecycle session mismatch".to_string(),
        ));
    }
    reject_unresolved_route_with_lifecycle_claim(tx, agent_id, allowed_lifecycle_operation)?;
    let (workspace_id, prior_lifecycle, prior_revision): (String, String, i64) = tx
            .query_row(
                "SELECT workspace_id, lifecycle_state, revision FROM workspace_agents WHERE session_id = ?1",
                params![agent_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading workspace agent lifecycle", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))?;
    update_session(&tx, session)?;
    let (archived_at, archive_reason) = match lifecycle {
        WorkspaceAgentLifecycleState::Active => (None, None),
        WorkspaceAgentLifecycleState::Archived => {
            tx.execute(
                "UPDATE workspaces
                     SET lead_agent_id = NULL, revision = revision + 1, updated_at = ?3
                     WHERE workspace_id = ?1 AND lead_agent_id = ?2",
                params![workspace_id, agent_id, changed_at],
            )
            .map_err(|error| db_error("failed clearing archived workspace lead", error))?;
            (Some(changed_at), archive_reason.map(str::to_string))
        }
    };
    tx.execute(
        "UPDATE workspace_agents
             SET lifecycle_state = ?2, archived_at = ?3, archive_reason = ?4,
                 revision = revision + 1, updated_at = ?5
             WHERE session_id = ?1",
        params![
            agent_id,
            lifecycle.as_str(),
            archived_at,
            archive_reason,
            changed_at
        ],
    )
    .map_err(|error| db_error("failed updating workspace agent lifecycle", error))?;
    if lifecycle == WorkspaceAgentLifecycleState::Active
        && prior_lifecycle == WorkspaceAgentLifecycleState::Archived.as_str()
    {
        // The notice worker cancels pending onboarding when it observes
        // an archived agent. On restore, requeue that *same* never-admitted
        // snapshot. A dispatched/ambiguous/manual-review notice must not
        // be retried or replaced with a new onboarding identity.
        tx.execute(
            "UPDATE workspace_system_notices
                 SET state = 'pending', last_error = NULL, updated_at_ms = ?2
                 WHERE notice_id = ?1 AND state = 'cancelled' AND turn_id IS NULL
                   AND last_error = 'recipient_archived'",
            params![format!("workspace-onboarding:{agent_id}"), changed_at],
        )
        .map_err(|error| db_error("failed requeueing safe onboarding on restore", error))?;
    }
    if prior_lifecycle != lifecycle.as_str() {
        let alias: String = tx
            .query_row(
                "SELECT identity_alias FROM workspace_agents WHERE session_id = ?1",
                [agent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed reading lifecycle notice alias", error))?;
        let action = match lifecycle {
            WorkspaceAgentLifecycleState::Active => "restored",
            WorkspaceAgentLifecycleState::Archived => "removed",
        };
        queue_membership_lead_notice_tx(
            &tx,
            &workspace_id,
            agent_id,
            &alias,
            initiator_agent_id,
            &format!(
                "workspace-agent-lifecycle:{agent_id}:revision-{}",
                prior_revision
                    .checked_add(1)
                    .ok_or_else(|| RuntimeError::InvalidState(
                        "workspace agent lifecycle revision overflow".into()
                    ))?
            ),
            action,
            changed_at,
        )?;
        if lifecycle == WorkspaceAgentLifecycleState::Archived {
            queue_team_hook_trigger_tx(
                &tx,
                &workspace_id,
                runtime_core::TeamHookEventName::PostTeammateRemove,
                &format!(
                    "workspace-hook:removed:{agent_id}:rev-{}",
                    prior_revision + 1
                ),
                initiator_agent_id,
                serde_json::json!({
                    "kind":"member", "action":"removed", "member_agent_id":agent_id,
                    "member_alias":alias,
                }),
                &active_workspace_hook_targets_tx(&tx, &workspace_id)?,
                changed_at,
            )?;
        }
    }
    tx.query_row(
        &format!("{AGENT_SELECT} WHERE a.session_id=?1 AND a.workspace_id=?2"),
        params![agent_id, workspace_id],
        workspace_agent_from_row,
    )
    .optional()
    .map_err(|error| db_error("failed reading transactional lifecycle result", error))?
    .ok_or_else(|| RuntimeError::NotFound(format!("workspace agent {agent_id}")))
}

pub(crate) fn reject_unresolved_route(
    connection: &Connection,
    agent_id: &str,
) -> Result<(), RuntimeError> {
    reject_unresolved_route_with_lifecycle_claim(connection, agent_id, None)
}

fn reject_unresolved_route_with_lifecycle_claim(
    connection: &Connection,
    agent_id: &str,
    allowed_lifecycle_operation: Option<&str>,
) -> Result<(), RuntimeError> {
    // Lifecycle transitions change the authoritative attachment state. A
    // stale manager or a direct store caller must not archive/restore around
    // an unresolved operator-native turn/policy intent, even if its in-memory
    // session snapshot looks quiescent. This fence shares the same SQLite
    // transaction as the lifecycle write.
    let claim: Option<(String, String)> = connection
        .query_row(
            "SELECT resource_kind,owner_operation_id FROM runtime_operation_resource_claims
             WHERE resource_id=?1 AND claim_mode='exclusive' AND resource_kind IN (?2,?3,?4,?5)
             AND (owner_operation_id!=?6 OR resource_kind NOT IN (?4,?5))
             LIMIT 1",
            params![
                agent_id,
                crate::repository_operator_native_policy::CLAIM_KIND,
                crate::repository_turn_authority::OPERATOR_TURN_CLAIM,
                crate::repository_operator_agent_restore::CLAIM_KIND,
                crate::repository_operator_agent_archive::CLAIM_KIND,
                allowed_lifecycle_operation.unwrap_or("")
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| db_error("failed checking lifecycle native effect claims", error))?;
    if let Some((kind, operation_id)) = claim {
        return Err(RuntimeError::operation_unresolved(
            operation_id,
            format!("agent lifecycle is fenced by unresolved native effect {kind}"),
        ));
    }
    let recovery_required: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1 AND status IN ('policy_recovery_required','restore_recovery_required','archive_recovery_required','create_recovery_required'))",
            [agent_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking lifecycle provider reattachment", error))?;
    if recovery_required {
        return Err(RuntimeError::Conflict(
            "agent lifecycle is frozen until independently reviewed native policy, archive or restore is reattached on startup".into(),
        ));
    }
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM workspace_agent_rebinds
         WHERE agent_id = ?1 AND phase IN ('intended', 'manual_review')",
            [agent_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking workspace route lifecycle fence", error))?;
    if count > 0 {
        return Err(RuntimeError::Conflict(
            "workspace agent provider worktree binding is unresolved".into(),
        ));
    }
    let edited: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM session_edit_reruns WHERE session_id=?1
         AND phase IN ('intended','native_started','fork_observed','manual_review')",
            [agent_id],
            |row| row.get(0),
        )
        .map_err(|e| db_error("failed checking native edit policy fence", e))?;
    if edited > 0 {
        return Err(RuntimeError::Conflict(
            "workspace agent has unresolved native edit-and-rerun".into(),
        ));
    }
    Ok(())
}
