use runtime_core::{
    OperationPhase, OperationRecord, OperationTransitionRecord, RuntimeError,
    WorkspaceLifecycleState, WorkspaceRetirementCommand, WorkspaceRetirementResponse,
};
use rusqlite::{params, Connection, Transaction, TransactionBehavior};
use std::path::Path;

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::{operation_by_idempotency, workspace_by_id};
use crate::SqliteRuntimeRepository;

const RETIREMENT_KIND: &str = "workspace_retirement";

/// A retired identity can be reactivated only if its durable terminal
/// retirement receipt is intact. An incomplete/externally fabricated flag
/// never authorizes reuse of an unsafe workspace.
pub(crate) fn reactivate_retired_workspace(
    tx: &Transaction<'_>,
    retired: &runtime_core::WorkspaceRecord,
    changed_at: i64,
) -> Result<runtime_core::WorkspaceRecord, RuntimeError> {
    if retired.lifecycle_state != WorkspaceLifecycleState::Retired {
        return Err(RuntimeError::Conflict("workspace is not retired".into()));
    }
    let certified: bool = tx
        .query_row(
            "SELECT EXISTS (
                SELECT 1 FROM retired_workspaces r
                JOIN runtime_operations op ON op.operation_id=r.retired_by_operation_id
                WHERE r.workspace_id=?1 AND r.canonical_root=?2
                  AND op.workspace_id=r.workspace_id
                  AND op.kind='workspace_retirement' AND op.phase='completed'
                  AND json_extract(op.exact_terminal_result_json,'$.operation_id')=op.operation_id
                  AND json_extract(op.exact_terminal_result_json,'$.workspace.workspace_id')=?1
                  AND json_extract(op.exact_terminal_result_json,'$.workspace.canonical_root')=?2
                  AND json_extract(op.exact_terminal_result_json,'$.workspace.lifecycle_state')='retired')",
            params![retired.workspace_id, retired.canonical_root],
            |row| row.get(0),
        )
        .map_err(|e| db_error("failed verifying retired workspace receipt", e))?;
    if !certified {
        return Err(RuntimeError::Conflict(format!(
            "workspace {} has no proven terminal retirement; recovery is required",
            retired.workspace_id
        )));
    }
    let mut blockers = tx
        .prepare(
            "SELECT EXISTS(SELECT 1 FROM workspace_agents WHERE workspace_id=?1 AND lifecycle_state='active')
                  OR EXISTS(SELECT 1 FROM runtime_operations WHERE workspace_id=?1
                            AND phase IN ('requested','manual_review'))",
        )
        .map_err(|e| db_error("failed preparing workspace reactivation safety check", e))?;
    let unsafe_state: bool = blockers
        .query_row([retired.workspace_id.as_str()], |row| row.get(0))
        .map_err(|e| db_error("failed inspecting workspace reactivation state", e))?;
    drop(blockers);
    if unsafe_state || retired.lead_agent_id.is_some() {
        return Err(RuntimeError::Conflict(format!(
            "workspace {} cannot reactivate with active members, lead or unresolved operations",
            retired.workspace_id
        )));
    }
    tx.execute(
        "UPDATE workspaces SET lifecycle_state='active', lead_agent_id=NULL,
                revision=revision+1, updated_at=?2
         WHERE workspace_id=?1 AND lifecycle_state='retired'",
        params![retired.workspace_id, changed_at],
    )
    .map_err(|e| db_error("failed reactivating workspace identity", e))?;
    tx.execute(
        "DELETE FROM retired_workspaces WHERE workspace_id=?1",
        [retired.workspace_id.as_str()],
    )
    .map_err(|e| db_error("failed retiring workspace tombstone after reactivation", e))?;
    workspace_by_id(tx, &retired.workspace_id)?.ok_or_else(|| {
        RuntimeError::Bootstrap("reactivated workspace disappeared before commit".into())
    })
}

impl SqliteRuntimeRepository {
    /// The quiescent fast path has no native side effects. Every precondition,
    /// retired-root tombstone, operation receipt and workspace transition
    /// commits together. An occupied workspace must use a separately durable
    /// multi-resource settlement path; never mislabel it retired here.
    pub fn retire_empty_workspace(
        &self,
        command: &WorkspaceRetirementCommand,
    ) -> Result<WorkspaceRetirementResponse, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed beginning quiescent workspace retirement", e))?;

        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            command.idempotency_key.as_deref(),
        )? {
            if existing.kind != RETIREMENT_KIND
                || existing.normalized_request_hash != command.normalized_request_hash
            {
                return Err(RuntimeError::Conflict(format!(
                    "Idempotency-Key belongs to a different operation or input: {}",
                    existing.operation_id
                )));
            }
            let exact = existing.exact_terminal_result.ok_or_else(|| {
                RuntimeError::Conflict(format!(
                    "retirement {} is unresolved; inspect its durable operation",
                    existing.operation_id
                ))
            })?;
            return serde_json::from_value(exact).map_err(|e| {
                RuntimeError::ProtocolViolation(format!(
                    "invalid durable retirement result for {}: {e}",
                    existing.operation_id
                ))
            });
        }

        let workspace = workspace_by_id(&tx, &command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", command.workspace_id)))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::Conflict(format!(
                "workspace {} is not active",
                command.workspace_id
            )));
        }
        if workspace.revision != command.expected_revision {
            return Err(RuntimeError::Conflict(format!(
                "workspace {} revision changed: expected {}, actual {}",
                command.workspace_id, command.expected_revision, workspace.revision
            )));
        }
        if workspace.lead_agent_id.is_some() {
            return Err(RuntimeError::Conflict(
                "workspace lead must be cleared before quiescent retirement".into(),
            ));
        }
        // There are no provider-close assumptions here. Even archived agents
        // may have an unconfirmed native close; preserve them for the full
        // effect-evidenced retirement operation rather than guessing.
        ensure_quiescent_workspace(&tx, &command.workspace_id)?;
        ensure_unowned_native_resources_do_not_use_root(&tx, &workspace.canonical_root)?;

        let authority = OperationAuthorityTransaction::new(&tx);
        let requested_at = command.requested_at;
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: RETIREMENT_KIND.into(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: requested_at,
            updated_at: requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: Some(serde_json::json!({"settlement": "quiescent"})),
            created_at: requested_at,
        })?;
        let claim = authority.acquire_claim(
            &command.operation_id,
            "workspace",
            &command.workspace_id,
            requested_at,
        )?;
        let affected = tx
            .execute(
                "UPDATE workspaces SET lifecycle_state='retired', lead_agent_id=NULL,
                     revision=revision+1, updated_at=?3
                 WHERE workspace_id=?1 AND revision=?2 AND lifecycle_state='active'",
                params![
                    command.workspace_id,
                    i64::try_from(command.expected_revision).map_err(|_| {
                        RuntimeError::InvalidState("workspace revision overflow".into())
                    })?,
                    requested_at
                ],
            )
            .map_err(|e| db_error("failed retiring quiescent workspace", e))?;
        if affected != 1 {
            return Err(RuntimeError::Conflict(
                "workspace changed while retiring".into(),
            ));
        }
        tx.execute(
            "INSERT INTO retired_workspaces
             (workspace_id, canonical_root, retired_by_operation_id, retired_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                command.workspace_id,
                workspace.canonical_root,
                command.operation_id,
                requested_at
            ],
        )
        .map_err(|e| db_error("failed preserving retired workspace identity", e))?;
        let retired = workspace_by_id(&tx, &command.workspace_id)?.ok_or_else(|| {
            RuntimeError::Bootstrap("workspace disappeared before retirement commit".into())
        })?;
        let response = WorkspaceRetirementResponse {
            operation_id: command.operation_id.clone(),
            workspace: retired,
        };
        let exact = serde_json::to_value(&response).map_err(|e| {
            RuntimeError::Bootstrap(format!("failed encoding retirement result: {e}"))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&command.workspace_id),
            &exact,
            None,
            None,
            requested_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({
                "lifecycle_state": "retired",
                "resource_fence_generation": claim.fence_generation,
                "external_effects": 0,
            })),
            created_at: requested_at,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|e| db_error("failed committing quiescent retirement", e))?;
        Ok(response)
    }
}

/// These guards are deliberately stronger than an empty roster: persisted
/// provider attachments, pending process results, transport and hook effects
/// must not be silently abandoned by a lifecycle flag transition.
fn ensure_quiescent_workspace(db: &Connection, workspace_id: &str) -> Result<(), RuntimeError> {
    let blockers = [
        (
            "workspace session ownership (including archived provider attachments)",
            "SELECT EXISTS(SELECT 1 FROM workspace_session_ownership WHERE workspace_id=?1)",
        ),
        (
            "workspace agents",
            "SELECT EXISTS(SELECT 1 FROM workspace_agents WHERE workspace_id=?1)",
        ),
        (
            "unsettled managed processes",
            "SELECT EXISTS(SELECT 1 FROM managed_processes WHERE workspace_id=?1
               AND (status IN ('queued','launch_reserved','running')
               OR completion_state IN ('pending','injecting')))",
        ),
        (
            "unfinished message deliveries",
            "SELECT EXISTS(SELECT 1 FROM agent_deliveries d
             JOIN agent_messages m ON m.id=d.message_id
             WHERE m.workspace_id=?1 AND d.status NOT IN ('delivered','failed','cancelled'))",
        ),
        (
            "unsettled lifecycle notices",
            "SELECT EXISTS(SELECT 1 FROM workspace_system_notices WHERE workspace_id=?1
             AND state IN ('pending','injecting','manual_review'))",
        ),
        (
            "unsettled hook effects",
            "SELECT EXISTS(SELECT 1 FROM workspace_hook_effects WHERE workspace_id=?1
             AND (phase IN ('intended','started','manual_review')
             OR application_state='pending'))",
        ),
        (
            "unresolved workspace operations",
            "SELECT EXISTS(SELECT 1 FROM runtime_operations WHERE workspace_id=?1
             AND phase IN ('requested','manual_review'))",
        ),
        (
            "unresolved legacy migration subjects",
            "SELECT EXISTS(SELECT 1 FROM legacy_workspace_migration_subjects WHERE workspace_id=?1
             AND classification='unresolved')",
        ),
    ];
    for (description, query) in blockers {
        let exists = db
            .query_row(query, [workspace_id], |row| row.get::<_, bool>(0))
            .map_err(|e| db_error("failed checking workspace retirement obligations", e))?;
        if exists {
            return Err(RuntimeError::Conflict(format!(
                "workspace {workspace_id} has {description}; recoverable resource settlement is required before retirement"
            )));
        }
    }
    Ok(())
}

/// The legacy `/v1` authority can still contain a provider session or process
/// working in the repository without an explicit workspace ownership row.
/// Absence of the v2 roster is not proof that the root is truly quiescent.
fn ensure_unowned_native_resources_do_not_use_root(
    db: &Connection,
    canonical_root: &str,
) -> Result<(), RuntimeError> {
    let root = Path::new(canonical_root);
    for (kind, sql) in [
        (
            "unowned provider session",
            "SELECT s.id,s.cwd FROM sessions s
             WHERE s.cwd IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM workspace_session_ownership o WHERE o.session_id=s.id)",
        ),
        (
            "unowned managed process",
            "SELECT process_id,cwd FROM managed_processes
             WHERE workspace_id IS NULL AND cwd IS NOT NULL
               AND (status IN ('queued','launch_reserved','running')
                 OR completion_state IN ('pending','injecting'))",
        ),
        (
            "unowned legacy process",
            "SELECT id,cwd FROM processes WHERE cwd IS NOT NULL
             AND status NOT IN ('completed','failed','killed','canceled','timed_out','interrupted')",
        ),
    ] {
        let mut statement = db.prepare(sql).map_err(|e| {
            db_error("failed preparing legacy retirement obligation scan", e)
        })?;
        let rows = statement
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
            .map_err(|e| db_error("failed scanning legacy retirement obligations", e))?;
        for row in rows {
            let (id, cwd) = row.map_err(|e| {
                db_error("failed reading legacy retirement obligation", e)
            })?;
            let cwd = Path::new(&cwd);
            if cwd.starts_with(root)
                || cwd
                    .canonicalize()
                    .is_ok_and(|canonical| canonical.starts_with(root))
            {
                return Err(RuntimeError::Conflict(format!(
                    "workspace root {canonical_root} has {kind} {id}; its native state must be reconciled before retirement"
                )));
            }
        }
    }
    Ok(())
}
