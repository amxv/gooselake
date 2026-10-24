use runtime_core::{
    OperationPhase, OperationRecord, OperationTransitionRecord, RuntimeError,
    WorkspaceLifecycleState, WorkspaceRecord, WorkspaceRetirementBlocker,
    WorkspaceRetirementCommand, WorkspaceRetirementPreflight, WorkspaceRetirementResponse,
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
    /// A consistent, read-only snapshot of the same persisted obligations the
    /// quiescent retirement transaction later rechecks under its write lock.
    pub fn workspace_retirement_preflight(
        &self,
        workspace_id: &str,
    ) -> Result<WorkspaceRetirementPreflight, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|e| db_error("failed beginning retirement preflight snapshot", e))?;
        let workspace = workspace_by_id(&tx, workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        let blockers = retirement_blockers(&tx, &workspace)?;
        let eligible_for_quiescent_retirement =
            workspace.lifecycle_state == WorkspaceLifecycleState::Active && blockers.is_empty();
        tx.commit()
            .map_err(|e| db_error("failed finalizing retirement preflight snapshot", e))?;
        Ok(WorkspaceRetirementPreflight {
            workspace,
            eligible_for_quiescent_retirement,
            blockers,
        })
    }

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
        if let Some(blocker) = retirement_blockers(&tx, &workspace)?.first() {
            return Err(RuntimeError::Conflict(format!(
                "workspace {} has {}; recoverable resource settlement is required before retirement",
                workspace.workspace_id, blocker.description
            )));
        }

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

/// These checks are stronger than an empty roster. A preview never clears a
/// blocker or proves native provider-close success, and the write transaction
/// runs this exact classifier again before committing a quiescent retirement.
fn retirement_blockers(
    db: &Connection,
    workspace: &WorkspaceRecord,
) -> Result<Vec<WorkspaceRetirementBlocker>, RuntimeError> {
    let mut blockers = Vec::new();
    if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
        add_blocker(
            &mut blockers,
            "workspace_not_active",
            "workspace is not active",
            1,
        );
    }
    if workspace.lead_agent_id.is_some() {
        add_blocker(
            &mut blockers,
            "lead_not_cleared",
            "workspace lead remains assigned",
            1,
        );
    }
    let queries = [
        ("session_ownership", "workspace session ownership (including archived provider attachments)",
         "SELECT COUNT(*) FROM workspace_session_ownership WHERE workspace_id=?1"),
        ("agent_records", "workspace agents (including archived members)",
         "SELECT COUNT(*) FROM workspace_agents WHERE workspace_id=?1"),
        ("managed_processes_unsettled", "unsettled managed processes",
         "SELECT COUNT(*) FROM managed_processes WHERE workspace_id=?1 AND (status IN ('queued','launch_reserved','running') OR completion_state IN ('pending','injecting'))"),
        ("message_deliveries_unfinished", "unfinished message deliveries",
         "SELECT COUNT(*) FROM agent_deliveries d JOIN agent_messages m ON m.id=d.message_id WHERE m.workspace_id=?1 AND d.status NOT IN ('delivered','failed','cancelled')"),
        ("lifecycle_notices_unsettled", "unsettled lifecycle notices",
         "SELECT COUNT(*) FROM workspace_system_notices WHERE workspace_id=?1 AND state IN ('pending','injecting','manual_review')"),
        ("hook_effects_unsettled", "unsettled hook effects",
         "SELECT COUNT(*) FROM workspace_hook_effects WHERE workspace_id=?1 AND (phase IN ('intended','started','manual_review') OR application_state='pending')"),
        ("operations_unresolved", "unresolved workspace operations",
         "SELECT COUNT(*) FROM runtime_operations WHERE workspace_id=?1 AND phase IN ('requested','manual_review')"),
        ("migration_subjects_unresolved", "unresolved legacy migration subjects",
         "SELECT COUNT(*) FROM legacy_workspace_migration_subjects WHERE workspace_id=?1 AND classification='unresolved'"),
    ];
    for (code, description, sql) in queries {
        let count = db
            .query_row(sql, [workspace.workspace_id.as_str()], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|e| db_error("failed checking workspace retirement obligations", e))?;
        add_blocker(
            &mut blockers,
            code,
            description,
            u64::try_from(count).map_err(|_| {
                RuntimeError::ProtocolViolation("negative retirement obligation count".into())
            })?,
        );
    }

    // The legacy `/v1` authority may still use this root without a v2
    // ownership row. An empty workspace roster is not native quiescence.
    let root = Path::new(&workspace.canonical_root);
    for (code, description, sql) in [
        ("unowned_provider_sessions", "unowned provider sessions in workspace root",
         "SELECT s.id,s.cwd FROM sessions s WHERE s.cwd IS NOT NULL AND NOT EXISTS(SELECT 1 FROM workspace_session_ownership o WHERE o.session_id=s.id)"),
        ("unowned_managed_processes", "unowned managed processes in workspace root",
         "SELECT process_id,cwd FROM managed_processes WHERE workspace_id IS NULL AND cwd IS NOT NULL AND (status IN ('queued','launch_reserved','running') OR completion_state IN ('pending','injecting'))"),
        ("unowned_legacy_processes", "unowned legacy processes in workspace root",
         "SELECT id,cwd FROM processes WHERE cwd IS NOT NULL AND status NOT IN ('completed','failed','killed','canceled','timed_out','interrupted')"),
    ] {
        let mut statement = db.prepare(sql)
            .map_err(|e| db_error("failed preparing legacy retirement obligation scan", e))?;
        let rows = statement.query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| db_error("failed scanning legacy retirement obligations", e))?;
        let mut count = 0_u64;
        for row in rows {
            let cwd = row.map_err(|e| db_error("failed reading legacy retirement obligation", e))?;
            let cwd = Path::new(&cwd);
            if cwd.starts_with(root) || cwd.canonicalize().is_ok_and(|p| p.starts_with(root)) {
                count += 1;
            }
        }
        add_blocker(&mut blockers, code, description, count);
    }
    Ok(blockers)
}

fn add_blocker(
    blockers: &mut Vec<WorkspaceRetirementBlocker>,
    code: &str,
    description: &str,
    count: u64,
) {
    if count > 0 {
        blockers.push(WorkspaceRetirementBlocker {
            code: code.to_string(),
            count,
            description: description.to_string(),
        });
    }
}
