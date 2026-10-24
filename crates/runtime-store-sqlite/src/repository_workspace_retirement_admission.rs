use runtime_core::{
    OperationEffectRecord, OperationPhase, OperationRecord, OperationTransitionRecord,
    RuntimeError, WorkspaceLifecycleState, WorkspaceRetirementAdmission,
    WorkspaceRetirementCommand, WorkspaceRetirementPlan, WorkspaceRetirementResponse,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::db::{collect_rows, db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::{operation_by_idempotency, workspace_by_id};
use crate::repository_workspace_retirement::retirement_blockers;
use crate::SqliteRuntimeRepository;

const RETIREMENT_KIND: &str = "workspace_retirement";

/// Snapshot every target under the same write lock that admits the retirement
/// operation. The transaction records one native-effect intent per target and
/// freezes all new relevant SQLite ingress before any external effect occurs.
pub(crate) fn retirement_plan(
    db: &Connection,
    workspace_id: &str,
    root: &str,
    operation_id: &str,
) -> Result<WorkspaceRetirementPlan, RuntimeError> {
    Ok(WorkspaceRetirementPlan {
        operation_id: operation_id.to_string(),
        workspace_id: workspace_id.to_string(),
        agent_ids: ids(db, "SELECT session_id FROM workspace_session_ownership WHERE workspace_id=?1 ORDER BY session_id", workspace_id)?,
        turn_ids: ids(db, "SELECT s.active_turn_id FROM sessions s JOIN workspace_session_ownership o ON o.session_id=s.id WHERE o.workspace_id=?1 AND s.active_turn_id IS NOT NULL ORDER BY s.id", workspace_id)?,
        managed_process_ids: ids(db, "SELECT p.process_id FROM managed_processes p WHERE (p.workspace_id=?1 OR EXISTS(SELECT 1 FROM workspace_session_ownership o WHERE o.workspace_id=?1 AND o.session_id=p.owner_session_id)) AND (p.status IN ('queued','launch_reserved','running') OR p.completion_state IN ('pending','injecting')) ORDER BY p.process_id", workspace_id)?,
        delivery_ids: ids(db, "SELECT d.id FROM agent_deliveries d JOIN agent_messages m ON m.id=d.message_id WHERE d.status NOT IN ('injected','failed','cancelled') AND (m.workspace_id=?1 OR EXISTS(SELECT 1 FROM workspace_session_ownership o WHERE o.workspace_id=?1 AND o.session_id=d.recipient_agent_id) OR EXISTS(SELECT 1 FROM workspace_session_ownership o WHERE o.workspace_id=?1 AND o.session_id=m.sender_agent_id)) ORDER BY d.id", workspace_id)?,
        notice_ids: ids(db, "SELECT notice_id FROM workspace_system_notices WHERE workspace_id=?1 AND state IN ('pending','injecting','manual_review') ORDER BY notice_id", workspace_id)?,
        hook_effect_ids: ids(db, "SELECT json_array(parent_event_id,event_name,hook_name) FROM workspace_hook_effects WHERE workspace_id=?1 AND (phase IN ('intended','started','manual_review') OR application_state='pending') ORDER BY parent_event_id,event_name,hook_name", workspace_id)?,
        preserved_worktree_ids: ids(db, "SELECT id FROM managed_worktrees WHERE repo_root=?1 ORDER BY id", root)?,
    })
}

fn ids(db: &Connection, sql: &str, key: &str) -> Result<Vec<String>, RuntimeError> {
    let mut statement = db.prepare(sql).map_err(|error| {
        db_error(
            "failed preparing workspace retirement target snapshot",
            error,
        )
    })?;
    let rows = statement
        .query_map([key], |row| row.get::<_, String>(0))
        .map_err(|error| db_error("failed enumerating workspace retirement targets", error))?;
    collect_rows(rows)
}

fn saved_plan(
    db: &Connection,
    workspace_id: &str,
    operation_id: &str,
) -> Result<WorkspaceRetirementPlan, RuntimeError> {
    let value: Option<String> = db.query_row(
        "SELECT snapshot_json FROM workspace_retirement_fences WHERE workspace_id=?1 AND operation_id=?2",
        params![workspace_id, operation_id], |row| row.get(0),
    ).optional().map_err(|error| db_error("failed reading durable workspace retirement snapshot", error))?;
    let value = value.ok_or_else(|| RuntimeError::Conflict(format!(
        "retirement operation {operation_id} has no resumable resource fence; manual recovery required"
    )))?;
    serde_json::from_str(&value).map_err(|error| {
        RuntimeError::Bootstrap(format!(
            "invalid durable workspace retirement snapshot: {error}"
        ))
    })
}

impl SqliteRuntimeRepository {
    /// A process crash makes every previously sent or potentially sent
    /// external retirement request uncertain. No native replay is legal until
    /// an operator reconciles the exact target/provider identity.
    pub fn quarantine_ambiguous_retirement_effects_after_restart(
        &self,
        at_ms: i64,
    ) -> Result<usize, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error("failed beginning retirement restart reconciliation", error)
            })?;
        let changed = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='manual_review',
             evidence_json=json_object('outcome','native_effect_unverified_after_restart',
               'prior_evidence',json(evidence_json)),updated_at=?1
             WHERE operation_id IN (SELECT operation_id FROM workspace_retirement_fences
                 WHERE phase='frozen')
               AND effect_kind IN ('workspace_provider_close','workspace_turn_interrupt')
               AND phase IN ('started','observed')",
                [at_ms],
            )
            .map_err(|error| {
                db_error(
                    "failed quarantining unknown external retirement effects",
                    error,
                )
            })?;
        tx.execute(
            "UPDATE workspace_retirement_fences SET phase='manual_review',updated_at=?1
            WHERE phase='frozen' AND EXISTS(
              SELECT 1 FROM runtime_operation_effects e
              WHERE e.operation_id=workspace_retirement_fences.operation_id
                AND e.effect_kind IN ('workspace_provider_close','workspace_turn_interrupt')
                AND e.phase='manual_review')",
            [at_ms],
        )
        .map_err(|error| db_error("failed freezing restarted uncertain retirement", error))?;
        tx.execute(
            "UPDATE runtime_operations SET phase='manual_review',updated_at=?1
            WHERE phase='requested' AND EXISTS(
                SELECT 1 FROM workspace_retirement_fences f
                WHERE f.operation_id=runtime_operations.operation_id AND f.phase='manual_review')",
            [at_ms],
        )
        .map_err(|error| db_error("failed persisting retirement manual review", error))?;
        tx.commit()
            .map_err(|error| db_error("failed committing retirement restart quarantine", error))?;
        Ok(changed)
    }

    pub fn list_recoverable_workspace_retirements(
        &self,
    ) -> Result<Vec<WorkspaceRetirementCommand>, RuntimeError> {
        let conn = open_connection(&self.database_path)?;
        let mut statement = conn
            .prepare(
                "SELECT op.operation_id,op.actor_kind,op.actor_id,op.idempotency_key,
              op.normalized_request_hash,op.normalized_request_json,
              op.workspace_id,op.created_at
             FROM runtime_operations op JOIN workspace_retirement_fences f
               ON f.operation_id=op.operation_id
             WHERE op.kind='workspace_retirement' AND op.phase='requested'
               AND f.phase='frozen' AND op.idempotency_key IS NOT NULL
             ORDER BY op.created_at,op.operation_id",
            )
            .map_err(|error| db_error("failed preparing recoverable retirement scan", error))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            })
            .map_err(|error| db_error("failed scanning unfinished retirement operations", error))?;
        let mut commands = Vec::new();
        for row in rows {
            let (
                operation_id,
                actor_kind,
                actor_id,
                idempotency_key,
                hash,
                raw,
                workspace_id,
                requested_at,
            ) = row.map_err(|error| {
                db_error("failed reading recoverable retirement operation", error)
            })?;
            let actor_kind =
                runtime_core::OperationActorKind::from_str(&actor_kind).ok_or_else(|| {
                    RuntimeError::ProtocolViolation("invalid durable retirement actor".into())
                })?;
            let normalized_request: serde_json::Value =
                serde_json::from_str(&raw).map_err(|error| {
                    RuntimeError::Bootstrap(format!("invalid stored retirement command: {error}"))
                })?;
            let expected_revision = normalized_request
                .get("expected_revision")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    RuntimeError::ProtocolViolation(
                        "retirement command lacks expected revision".into(),
                    )
                })?;
            commands.push(WorkspaceRetirementCommand {
                operation_id,
                actor: runtime_core::OperationActor {
                    kind: actor_kind,
                    identifier: actor_id,
                },
                idempotency_key: Some(idempotency_key),
                normalized_request_hash: hash,
                normalized_request,
                workspace_id,
                expected_revision,
                requested_at,
            });
        }
        Ok(commands)
    }

    pub fn retiring_operation_for_session(
        &self,
        session_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let db = open_connection(&self.database_path)?;
        db.query_row(
            "SELECT f.operation_id FROM workspace_retirement_fences f
             JOIN workspace_session_ownership o ON o.workspace_id=f.workspace_id
             WHERE o.session_id=?1 AND f.phase IN ('frozen','manual_review','retired')
             UNION ALL
             SELECT r.operation_id FROM workspace_retirement_routes r
             JOIN sessions s ON (s.cwd=r.route
               OR substr(s.cwd,1,length(r.route)+1)=r.route||'/')
             WHERE s.id=?1 AND NOT EXISTS(
               SELECT 1 FROM workspace_session_ownership o WHERE o.session_id=s.id)
             LIMIT 1",
            [session_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed inspecting retirement-aware session recovery", error))
    }

    pub fn admit_workspace_retirement(
        &self,
        command: &WorkspaceRetirementCommand,
    ) -> Result<WorkspaceRetirementAdmission, RuntimeError> {
        if command.idempotency_key.is_none() {
            return Err(RuntimeError::InvalidState(
                "populated-workspace retirement requires Idempotency-Key for durable resume and recovery".into()
            ));
        }
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting workspace retirement admission", error))?;

        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            command.idempotency_key.as_deref(),
        )? {
            if existing.kind != RETIREMENT_KIND
                || existing.normalized_request_hash != command.normalized_request_hash
                || existing.workspace_id.as_deref() != Some(command.workspace_id.as_str())
            {
                return Err(RuntimeError::Conflict(format!(
                    "Idempotency-Key belongs to a different operation or input: {}",
                    existing.operation_id
                )));
            }
            if let Some(exact) = existing.exact_terminal_result {
                let response = serde_json::from_value::<WorkspaceRetirementResponse>(exact)
                    .map_err(|error| {
                        RuntimeError::Bootstrap(format!(
                            "invalid retirement replay receipt: {error}"
                        ))
                    })?;
                return Ok(WorkspaceRetirementAdmission::Replay(response));
            }
            let plan = saved_plan(&tx, &command.workspace_id, &existing.operation_id)?;
            return Ok(WorkspaceRetirementAdmission::Resume(plan));
        }

        let workspace = workspace_by_id(&tx, &command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", command.workspace_id)))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::Conflict("workspace is not active".into()));
        }
        if workspace.revision != command.expected_revision {
            return Err(RuntimeError::Conflict(format!(
                "workspace revision changed: expected {}, actual {}",
                command.expected_revision, workspace.revision
            )));
        }
        let existing_fence: Option<String> = tx
            .query_row(
                "SELECT operation_id FROM workspace_retirement_fences WHERE workspace_id=?1",
                [&workspace.workspace_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking active retirement fence", error))?;
        if let Some(operation_id) = existing_fence {
            return Err(RuntimeError::Conflict(format!(
                "workspace {} has unresolved retirement operation {operation_id}; use the original idempotency key or reconcile it",
                workspace.workspace_id
            )));
        }
        // Other in-flight operator/model operations might already be outside
        // SQLite performing native work. Require reconciliation before freeze.
        let unresolved: i64 = tx.query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE workspace_id=?1 AND phase IN ('requested','manual_review')",
            [&workspace.workspace_id], |row| row.get(0),
        ).map_err(|error| db_error("failed scanning unsettled workspace operations", error))?;
        if unresolved != 0 {
            return Err(RuntimeError::Conflict(format!(
                "workspace has {unresolved} unresolved operations that must be reconciled before retirement"
            )));
        }

        for blocker in retirement_blockers(&tx, &workspace)? {
            if matches!(
                blocker.code.as_str(),
                "unowned_provider_sessions"
                    | "unowned_managed_processes"
                    | "unowned_legacy_processes"
                    | "legacy_deliveries_unfinished"
                    | "migration_subjects_unresolved"
                    | "native_rebinds_unresolved"
                    | "native_compactions_unresolved"
                    | "native_edits_unresolved"
                    | "native_turns_unresolved"
                    | "native_attachment_unresolved"
            ) {
                return Err(RuntimeError::Conflict(format!(
                    "workspace has {} {}; resolve legacy authority before retirement",
                    blocker.count, blocker.description
                )));
            }
        }

        let plan = retirement_plan(
            &tx,
            &workspace.workspace_id,
            &workspace.canonical_root,
            &command.operation_id,
        )?;
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: RETIREMENT_KIND.to_string(),
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
            evidence: Some(serde_json::json!({"settlement": "frozen", "snapshot": plan})),
            created_at: command.requested_at,
        })?;
        authority.acquire_claim(
            &command.operation_id,
            "workspace",
            &command.workspace_id,
            command.requested_at,
        )?;

        for (effect_kind, target_kind, ids) in [
            ("workspace_provider_close", "agent", &plan.agent_ids),
            ("workspace_turn_interrupt", "turn", &plan.turn_ids),
            (
                "workspace_process_settle",
                "process",
                &plan.managed_process_ids,
            ),
            ("workspace_delivery_settle", "delivery", &plan.delivery_ids),
            ("workspace_notice_settle", "notice", &plan.notice_ids),
            ("workspace_hook_settle", "hook", &plan.hook_effect_ids),
            (
                "workspace_worktree_preserve",
                "worktree",
                &plan.preserved_worktree_ids,
            ),
        ] {
            for target_id in ids {
                let prior_close = if effect_kind == "workspace_provider_close" {
                    crate::repository_workspace_retirement_native_close::verified_prior_close(
                        &tx,
                        &command.workspace_id,
                        target_id,
                    )?
                } else {
                    None
                };
                authority.insert_effect(&OperationEffectRecord {
                    effect_id: format!("{}:{effect_kind}:{target_id}", command.operation_id),
                    operation_id: command.operation_id.clone(),
                    effect_kind: effect_kind.into(),
                    target_kind: target_kind.into(),
                    target_id: target_id.clone(),
                    phase: if prior_close.is_some() {
                        "finalized"
                    } else {
                        "intended"
                    }
                    .into(),
                    idempotency_key: format!("{effect_kind}:{target_id}"),
                    evidence: Some(
                        prior_close.unwrap_or_else(|| serde_json::json!({"snapshot": "admission"})),
                    ),
                    attempt_count: 0,
                    created_at: command.requested_at,
                    updated_at: command.requested_at,
                })?;
            }
        }

        tx.execute(
            "INSERT INTO workspace_retirement_fences
            (workspace_id,operation_id,phase,snapshot_json,created_at,updated_at)
            VALUES (?1,?2,'frozen',?3,?4,?4)",
            params![
                command.workspace_id,
                command.operation_id,
                json_to_string(&serde_json::to_value(&plan).map_err(|error| {
                    RuntimeError::Bootstrap(format!("failed encoding retirement plan: {error}"))
                })?)?,
                command.requested_at
            ],
        )
        .map_err(|error| db_error("failed committing workspace retirement freeze", error))?;
        crate::repository_workspace_retirement_events::retirement_started(
            &tx,
            &plan,
            command.requested_at,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing workspace retirement admission", error))?;
        Ok(WorkspaceRetirementAdmission::New(plan))
    }
}
