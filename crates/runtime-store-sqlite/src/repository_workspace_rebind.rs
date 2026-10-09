use runtime_core::{
    normalized_json_hash, OperationActor, OperationPhase, OperationRecord,
    OperationResourceClaimRecord, OperationTransitionRecord, ProviderWorkspaceRebindEvidence,
    RuntimeError, SessionRecord, WorkspaceAgentRebindOperation, WorkspaceAgentRecreationPolicy,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::json;

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::SqliteRuntimeRepository;

const REBIND_CLAIM_KIND: &str = "workspace_agent_rebind";

fn encode(record: &WorkspaceAgentRebindOperation) -> Result<String, RuntimeError> {
    serde_json::to_string(record).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid workspace rebind record: {error}"))
    })
}

fn decode(raw: String) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
    serde_json::from_str(&raw).map_err(|error| {
        RuntimeError::Bootstrap(format!("corrupt workspace rebind record: {error}"))
    })
}

fn get(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<WorkspaceAgentRebindOperation>, RuntimeError> {
    let raw: Option<String> = connection
        .query_row(
            "SELECT record_json FROM workspace_agent_rebinds WHERE operation_id = ?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading worktree rebind operation", error))?;
    raw.map(decode).transpose()
}

fn release_route_claim(
    tx: &Transaction<'_>,
    operation_id: &str,
    agent_id: &str,
) -> Result<(), RuntimeError> {
    let (generation, acquired_at): (i64, i64) = tx
        .query_row(
            "SELECT fence_generation, acquired_at
         FROM runtime_operation_resource_claims
         WHERE owner_operation_id = ?1 AND resource_kind = ?2 AND resource_id = ?3",
            params![operation_id, REBIND_CLAIM_KIND, agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| db_error("failed loading exact workspace rebind claim", error))?;
    let generation = u64::try_from(generation)
        .map_err(|_| RuntimeError::Bootstrap("negative workspace rebind claim fence".into()))?;
    OperationAuthorityTransaction::new(tx).release_claim(&OperationResourceClaimRecord {
        resource_kind: REBIND_CLAIM_KIND.into(),
        resource_id: agent_id.into(),
        claim_mode: "exclusive".into(),
        owner_operation_id: operation_id.into(),
        fence_generation: generation,
        acquired_at,
    })
}

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
        if COMPLETED.contains(&observed_status) {
            operation.previous_cleanup_status = Some(observed_status.to_string());
            operation.previous_cleanup_diagnostic = None;
        } else {
            operation.previous_cleanup_diagnostic = Some(observed_status.to_string());
        }
        operation.updated_at = changed_at;
        tx.execute(
            "UPDATE workspace_agent_rebinds
             SET record_json = ?2, updated_at = ?3
             WHERE operation_id = ?1 AND phase = 'completed'",
            params![operation_id, encode(&operation)?, changed_at],
        )
        .map_err(|error| db_error("failed recording previous-worktree cleanup outcome", error))?;
        tx.commit().map_err(|error| {
            db_error("failed committing previous-worktree cleanup outcome", error)
        })?;
        Ok(operation)
    }

    pub fn get_workspace_agent_rebind_by_key(
        &self,
        workspace_id: &str,
        agent_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<WorkspaceAgentRebindOperation>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let existing: Option<String> = connection
            .query_row(
                "SELECT operation_id FROM workspace_agent_rebinds
             WHERE workspace_id = ?1 AND agent_id = ?2 AND idempotency_key = ?3",
                params![workspace_id, agent_id, idempotency_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed finding worktree rebind by key", error))?;
        existing
            .map(|id| get(&connection, &id))
            .transpose()
            .map(|item| item.flatten())
    }

    pub fn get_workspace_agent_rebind(
        &self,
        operation_id: &str,
    ) -> Result<Option<WorkspaceAgentRebindOperation>, RuntimeError> {
        get(&open_connection(&self.database_path)?, operation_id)
    }

    pub fn unresolved_workspace_agent_rebind(&self, agent_id: &str) -> Result<bool, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM workspace_agent_rebinds
                 WHERE agent_id = ?1 AND phase IN ('intended','manual_review')",
                [agent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed reading unresolved agent route", error))?;
        Ok(count > 0)
    }

    pub fn begin_workspace_agent_rebind(
        &self,
        operation: &WorkspaceAgentRebindOperation,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        if operation.phase != "intended" {
            return Err(RuntimeError::ProtocolViolation(
                "worktree rebind must begin as intended".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting worktree rebind intent", error))?;

        if let Some(key) = operation.idempotency_key.as_deref() {
            let existing: Option<String> = tx
                .query_row(
                    "SELECT operation_id FROM workspace_agent_rebinds
                 WHERE workspace_id = ?1 AND agent_id = ?2 AND idempotency_key = ?3",
                    params![operation.workspace_id, operation.agent_id, key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| db_error("failed reading worktree rebind key", error))?;
            if let Some(existing) = existing {
                let prior = get(&tx, &existing)?.ok_or_else(|| {
                    RuntimeError::Bootstrap("worktree rebind idempotency row disappeared".into())
                })?;
                if prior.destination_worktree_id != operation.destination_worktree_id
                    || prior.destination_cwd != operation.destination_cwd
                    || prior.expected_revision != operation.expected_revision
                    || prior.cleanup_previous_worktree != operation.cleanup_previous_worktree
                {
                    return Err(RuntimeError::Conflict(
                        "reused worktree rebind idempotency key with different input".into(),
                    ));
                }
                return Ok(prior);
            }
        }
        let (revision, lifecycle, session_cwd, session_worktree, active_turn, policy_json): (
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        ) = tx
            .query_row(
                "SELECT a.revision, a.lifecycle_state, s.cwd, s.worktree_id,
                    s.active_turn_id, a.recreation_policy_json
             FROM workspace_agents a JOIN sessions s ON s.id = a.session_id
             JOIN workspaces w ON w.workspace_id = a.workspace_id
             WHERE a.session_id = ?1 AND a.workspace_id = ?2 AND w.lifecycle_state = 'active'",
                params![operation.agent_id, operation.workspace_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| db_error("failed checking agent before rebind", error))?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("active workspace agent {}", operation.agent_id))
            })?;
        let current_policy: WorkspaceAgentRecreationPolicy = serde_json::from_str(&policy_json)
            .map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid agent recreation policy: {error}"))
            })?;
        if lifecycle != "active"
            || active_turn.is_some()
            || revision != i64::try_from(operation.expected_revision).unwrap_or(-1)
            || session_cwd.as_deref() != Some(operation.previous_cwd.as_str())
            || session_worktree != operation.previous_worktree_id
            || current_policy.authoritative_cwd != operation.previous_cwd
        {
            return Err(RuntimeError::Conflict(
                "workspace agent route, activity or revision changed before provider rebind".into(),
            ));
        }
        if let Some(target) = operation.destination_worktree_id.as_deref() {
            let expected: Option<(String, String)> = tx
                .query_row(
                    "SELECT repo_root, worktree_cwd FROM managed_worktrees WHERE id = ?1",
                    [target],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed reading destination worktree", error))?;
            if expected
                .as_ref()
                .is_none_or(|(_, cwd)| cwd != &operation.destination_cwd)
            {
                return Err(RuntimeError::Conflict(
                    "destination managed worktree changed".into(),
                ));
            }
        }
        if self.unresolved_workspace_agent_rebind_in_tx(&tx, &operation.agent_id)? {
            return Err(RuntimeError::Conflict(
                "another worktree rebind is unresolved; inspect its operation before retrying"
                    .into(),
            ));
        }
        let normalized_request = json!({
            "workspace_id": operation.workspace_id,
            "agent_id": operation.agent_id,
            "expected_revision": operation.expected_revision,
            "destination_worktree_id": operation.destination_worktree_id,
            "destination_cwd": operation.destination_cwd,
            "cleanup_previous_worktree": operation.cleanup_previous_worktree,
        });
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: operation.operation_id.clone(),
            workspace_id: Some(operation.workspace_id.clone()),
            kind: "workspace_agent_rebind".into(),
            actor: OperationActor::operator("runtime_operator"),
            idempotency_key: operation.idempotency_key.clone(),
            normalized_request_hash: normalized_json_hash(&normalized_request)?,
            normalized_request,
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: operation.created_at,
            updated_at: operation.updated_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: operation.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: Some(json!({
                "agent_id": operation.agent_id,
                "previous_cwd": operation.previous_cwd,
                "destination_cwd": operation.destination_cwd,
                "provider_effect": "not_yet_dispatched",
            })),
            created_at: operation.created_at,
        })?;
        authority.acquire_claim(
            &operation.operation_id,
            REBIND_CLAIM_KIND,
            &operation.agent_id,
            operation.created_at,
        )?;
        tx.execute(
            "INSERT INTO workspace_agent_rebinds
             (operation_id, workspace_id, agent_id, idempotency_key, phase, record_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'intended', ?5, ?6, ?7)",
            params![operation.operation_id, operation.workspace_id, operation.agent_id,
                    operation.idempotency_key, encode(operation)?, operation.created_at, operation.updated_at],
        ).map_err(|error| db_error("failed admitting worktree rebind intent", error))?;
        // Claim the destination before native provider work so cleanup sees
        // an active reservation during the in-flight/recovery-required window.
        if let Some(target) = operation.destination_worktree_id.as_deref() {
            let conflicting: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM managed_worktree_claims WHERE session_id = ?1
                 AND released_at IS NULL AND worktree_id IS NOT ?2",
                    params![operation.agent_id, operation.previous_worktree_id],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    db_error("failed checking existing worktree associations", error)
                })?;
            if conflicting != 0 {
                return Err(RuntimeError::Conflict(
                    "agent already has an unrecognized managed-worktree association".into(),
                ));
            }
            tx.execute(
                "INSERT INTO managed_worktree_claims(worktree_id, session_id, claim_role, created_at, released_at)
                 VALUES (?1, ?2, 'rebind_reservation', ?3, NULL)
                 ON CONFLICT(worktree_id, session_id) DO UPDATE
                 SET claim_role = 'rebind_reservation', created_at = excluded.created_at, released_at = NULL",
                params![target, operation.agent_id, operation.created_at],
            ).map_err(|error| db_error("failed reserving destination managed worktree", error))?;
        }
        tx.commit()
            .map_err(|error| db_error("failed committing worktree rebind intent", error))?;
        Ok(operation.clone())
    }

    fn unresolved_workspace_agent_rebind_in_tx(
        &self,
        tx: &Connection,
        agent_id: &str,
    ) -> Result<bool, RuntimeError> {
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM workspace_agent_rebinds WHERE agent_id = ?1
             AND phase IN ('intended','manual_review')",
                [agent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking rebind fences", error))?;
        Ok(count > 0)
    }

    pub fn classify_workspace_agent_rebind(
        &self,
        operation_id: &str,
        phase: &str,
        error_code: &str,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        if !matches!(phase, "rejected" | "manual_review") {
            return Err(RuntimeError::ProtocolViolation(
                "invalid rebind terminal classification".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting worktree rebind classification", error))?;
        let mut current = get(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("worktree rebind {operation_id}")))?;
        if current.phase != "intended" {
            return Err(RuntimeError::Conflict(
                "worktree rebind already classified; no implicit retry".into(),
            ));
        }
        current.phase = phase.to_string();
        current.error_code = Some(error_code.to_string());
        current.updated_at = changed_at;
        tx.execute(
            "UPDATE workspace_agent_rebinds SET phase = ?2, record_json = ?3, updated_at = ?4
             WHERE operation_id = ?1 AND phase = 'intended'",
            params![operation_id, phase, encode(&current)?, changed_at],
        )
        .map_err(|error| db_error("failed classifying worktree rebind", error))?;
        if phase == "rejected" {
            let authority = OperationAuthorityTransaction::new(&tx);
            authority.terminalize(
                operation_id,
                OperationPhase::Requested,
                OperationPhase::Failed,
                Some(&current.workspace_id),
                &serde_json::to_value(&current)
                    .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?,
                Some(error_code),
                Some("native workspace binding rejected before confirmed rebind"),
                changed_at,
            )?;
            if let Some(target) = current.destination_worktree_id.as_deref() {
                tx.execute(
                    "UPDATE managed_worktree_claims SET released_at = ?3
                     WHERE worktree_id = ?1 AND session_id = ?2
                       AND claim_role = 'rebind_reservation' AND released_at IS NULL",
                    params![target, current.agent_id, changed_at],
                )
                .map_err(|error| db_error("failed releasing rejected route reservation", error))?;
            }
            release_route_claim(&tx, operation_id, &current.agent_id)?;
        } else {
            let rows = tx
                .execute(
                    "UPDATE runtime_operations SET phase = 'manual_review',
                 error_code = ?2, error_message = 'provider workspace binding outcome unresolved',
                 updated_at = ?3 WHERE operation_id = ?1 AND phase = 'requested'",
                    params![operation_id, error_code, changed_at],
                )
                .map_err(|error| {
                    db_error("failed classifying canonical rebind operation", error)
                })?;
            if rows != 1 {
                return Err(RuntimeError::Conflict(
                    "canonical workspace rebind operation is no longer pending".into(),
                ));
            }
        }
        OperationAuthorityTransaction::new(&tx).append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: if phase == "rejected" {
                OperationPhase::Failed
            } else {
                OperationPhase::ManualReview
            },
            evidence: Some(json!({"classification": phase, "error_code": error_code})),
            created_at: changed_at,
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing worktree rebind classification", error))?;
        Ok(current)
    }

    pub fn finalize_workspace_agent_rebind(
        &self,
        operation_id: &str,
        updated_session: &SessionRecord,
        updated_policy: &WorkspaceAgentRecreationPolicy,
        evidence: &ProviderWorkspaceRebindEvidence,
        changed_at: i64,
    ) -> Result<WorkspaceAgentRebindOperation, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting verified rebind finalization", error))?;
        let mut operation = get(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("worktree rebind {operation_id}")))?;
        if operation.phase != "intended"
            || updated_session.id != operation.agent_id
            || updated_session.worktree_id != operation.destination_worktree_id
            || updated_session.cwd.as_deref() != Some(operation.destination_cwd.as_str())
            || updated_policy.authoritative_cwd != operation.destination_cwd
            || evidence.runtime_session_id != operation.agent_id
            || evidence.cwd != operation.destination_cwd
        {
            return Err(RuntimeError::ProtocolViolation(
                "provider evidence and durable route intent do not agree".into(),
            ));
        }
        let existing: Option<(i64, Option<String>, Option<String>, String)> = tx
            .query_row(
                "SELECT a.revision, s.cwd, s.active_turn_id, a.recreation_policy_json
             FROM workspace_agents a JOIN sessions s ON s.id = a.session_id
             WHERE a.session_id = ?1 AND a.workspace_id = ?2 AND a.lifecycle_state = 'active'",
                params![operation.agent_id, operation.workspace_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|error| db_error("failed checking final rebind revision", error))?;
        let Some((revision, cwd, active_turn, policy_json)) = existing else {
            return Err(RuntimeError::Conflict(
                "worktree rebind agent no longer active".into(),
            ));
        };
        let stored_policy: WorkspaceAgentRecreationPolicy = serde_json::from_str(&policy_json)
            .map_err(|error| RuntimeError::Bootstrap(format!("invalid stored policy: {error}")))?;
        if revision != i64::try_from(operation.expected_revision).unwrap_or(-1)
            || cwd.as_deref() != Some(operation.previous_cwd.as_str())
            || stored_policy.authoritative_cwd != operation.previous_cwd
            || active_turn.is_some()
        {
            return Err(RuntimeError::Conflict(
                "worktree rebind session or policy changed".into(),
            ));
        }
        if let Some(target) = operation.destination_worktree_id.as_deref() {
            let destination: Option<(String, String)> = tx
                .query_row(
                    "SELECT w.worktree_cwd, c.claim_role
                 FROM managed_worktrees w JOIN managed_worktree_claims c
                   ON c.worktree_id = w.id AND c.session_id = ?2 AND c.released_at IS NULL
                 WHERE w.id = ?1",
                    params![target, operation.agent_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed checking destination reservation", error))?;
            if destination.as_ref().is_none_or(|(cwd, role)| {
                cwd != &operation.destination_cwd || role != "rebind_reservation"
            }) {
                return Err(RuntimeError::Conflict(
                    "destination worktree reservation or path changed during provider rebind"
                        .into(),
                ));
            }
        }
        let other_claim: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM managed_worktree_claims
             WHERE session_id = ?1 AND released_at IS NULL
               AND worktree_id IS NOT ?2 AND worktree_id IS NOT ?3",
                params![
                    operation.agent_id,
                    operation.previous_worktree_id,
                    operation.destination_worktree_id
                ],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed validating previous worktree claims", error))?;
        if other_claim != 0 {
            return Err(RuntimeError::Conflict(
                "workspace agent has an unexpected active worktree claim".into(),
            ));
        }
        let updated = tx
            .execute(
                "UPDATE sessions SET cwd = ?2, worktree_id = ?3, updated_at = ?4,
                                 provider_session_ref = ?6, canonical_provider_session_ref = ?7
             WHERE id = ?1 AND cwd = ?5 AND active_turn_id IS NULL",
                params![
                    operation.agent_id,
                    operation.destination_cwd,
                    operation.destination_worktree_id,
                    changed_at,
                    operation.previous_cwd,
                    updated_session.provider_session_ref,
                    updated_session.canonical_provider_session_ref
                ],
            )
            .map_err(|error| db_error("failed committing verified session cwd", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "session cwd changed during rebind".into(),
            ));
        }
        tx.execute(
            "UPDATE workspace_agents
             SET recreation_policy_json = ?2, revision = revision + 1, updated_at = ?3
             WHERE session_id = ?1 AND workspace_id = ?4 AND revision = ?5",
            params![
                operation.agent_id,
                serde_json::to_string(updated_policy)
                    .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?,
                changed_at,
                operation.workspace_id,
                revision
            ],
        )
        .map_err(|error| db_error("failed committing verified recreation policy", error))?;
        if let Some(old) = operation.previous_worktree_id.as_deref() {
            tx.execute(
                "UPDATE managed_worktree_claims SET released_at = ?3
                 WHERE worktree_id = ?1 AND session_id = ?2 AND released_at IS NULL",
                params![old, operation.agent_id, changed_at],
            )
            .map_err(|error| db_error("failed releasing previous worktree association", error))?;
        }
        if let Some(target) = operation.destination_worktree_id.as_deref() {
            tx.execute(
                "INSERT INTO managed_worktree_claims(worktree_id,session_id,claim_role,created_at,released_at)
                 VALUES (?1, ?2, 'owner', ?3, NULL)
                 ON CONFLICT(worktree_id,session_id) DO UPDATE SET claim_role = 'owner',
                    created_at = excluded.created_at, released_at = NULL",
                params![target, operation.agent_id, changed_at],
            ).map_err(|error| db_error("failed claiming verified destination worktree", error))?;
        }
        operation.phase = "completed".into();
        operation.previous_cleanup_status = Some(
            if operation.cleanup_previous_worktree && operation.previous_worktree_id.is_some() {
                "pending".into()
            } else {
                "preserved".into()
            },
        );
        operation.updated_at = changed_at;
        operation.provider_evidence = Some(evidence.clone());
        tx.execute(
            "UPDATE workspace_agent_rebinds SET phase = 'completed', record_json = ?2, updated_at = ?3
             WHERE operation_id = ?1 AND phase = 'intended'",
            params![operation_id, encode(&operation)?, changed_at],
        ).map_err(|error| db_error("failed terminalizing verified worktree rebind", error))?;
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&operation.workspace_id),
            &serde_json::to_value(&operation)
                .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?,
            None,
            None,
            changed_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(json!({
                "provider_session_ref": evidence.provider_session_ref,
                "effective_cwd": evidence.cwd,
                "binding_generation": evidence.binding_generation,
            })),
            created_at: changed_at,
        })?;
        release_route_claim(&tx, operation_id, &operation.agent_id)?;
        tx.commit().map_err(|error| {
            db_error("failed committing verified workspace worktree route", error)
        })?;
        Ok(operation)
    }
}
