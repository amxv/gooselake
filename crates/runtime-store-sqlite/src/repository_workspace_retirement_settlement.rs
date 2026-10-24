use runtime_core::{
    OperationPhase, OperationResourceClaimRecord, OperationTransitionRecord, RuntimeError,
    WorkspaceRetirementPlan, WorkspaceRetirementResponse,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::workspace_by_id;
use crate::repository_workspace_retirement::retirement_blockers;
use crate::SqliteRuntimeRepository;

fn effect_phase(
    db: &Transaction<'_>,
    operation_id: &str,
    kind: &str,
    target_id: &str,
) -> Result<String, RuntimeError> {
    db.query_row(
        "SELECT phase FROM runtime_operation_effects
         WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3",
        params![operation_id, kind, target_id],
        |row| row.get(0),
    )
    .map_err(|error| db_error("missing or invalid retirement effect intent", error))
}

fn settle_effect(
    db: &Transaction<'_>,
    operation_id: &str,
    kind: &str,
    target_id: &str,
    phase: &str,
    evidence: &str,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    let updated = db
        .execute(
            "UPDATE runtime_operation_effects SET phase=?4, evidence_json=?5, updated_at=?6
         WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3 AND phase='intended'",
            params![
                operation_id,
                kind,
                target_id,
                phase,
                json_to_string(&serde_json::json!({
                    "outcome":evidence,"native_effect":false,"at_ms":at_ms
                }))?,
                at_ms
            ],
        )
        .map_err(|error| db_error("failed settling retirement resource effect", error))?;
    if updated != 1 {
        return Err(RuntimeError::Conflict(format!(
            "retirement effect {kind}/{target_id} changed while settling"
        )));
    }
    Ok(())
}

fn current_status(
    db: &Transaction<'_>,
    sql: &str,
    id: &str,
) -> Result<Option<String>, RuntimeError> {
    db.query_row(sql, [id], |row| row.get(0))
        .optional()
        .map_err(|error| db_error("failed reading retirement resource status", error))
}

pub(crate) fn settle_process_completion(
    db: &Transaction<'_>,
    process_id: &str,
    at_ms: i64,
) -> Result<(&'static str, &'static str), RuntimeError> {
    let admission: Option<(String,String)> = db.query_row(
        "SELECT a.dispatch_state,a.turn_id FROM turn_admissions a JOIN managed_processes p
         ON p.owner_session_id=a.session_id WHERE p.process_id=?1
           AND json_extract(a.correlation_json,'$.correlation_id')='process_completion:'||p.process_id
         ORDER BY a.admitted_at DESC,a.turn_id DESC LIMIT 1",
        [process_id], |row| Ok((row.get(0)?,row.get(1)?)),
    ).optional().map_err(|error| db_error("failed reconciling retiring process completion input",error))?;
    if let Some((state, turn_id)) = admission.as_ref() {
        if state == "dispatched" {
            db.execute(
                "UPDATE managed_processes SET completion_state='delivered',
                completion_turn_id=?2,completion_last_error=NULL,completion_updated_at=?3,
                updated_at=MAX(updated_at,?3) WHERE process_id=?1",
                params![process_id, turn_id, at_ms],
            )
            .map_err(|error| {
                db_error("failed reconciling delivered retirement completion", error)
            })?;
            return Ok(("finalized", "completion_dispatch_verified"));
        }
        if !matches!(state.as_str(), "pending" | "not_dispatched") {
            return Ok(("manual_review", "process_completion_outcome_unresolved"));
        }
    }
    // The freeze prevents new dispatch. Keep a never-delivered terminal result
    // durable and pending for an explicit owner restore instead of dropping it.
    db.execute(
        "UPDATE managed_processes SET completion_state='pending',completion_turn_id=NULL,
        completion_last_error='workspace_retirement_deferred_completion',
        completion_updated_at=?2,updated_at=MAX(updated_at,?2) WHERE process_id=?1",
        params![process_id, at_ms],
    )
    .map_err(|error| {
        db_error(
            "failed deferring retired process completion until restore",
            error,
        )
    })?;
    Ok(("finalized", "completion_deferred_until_agent_restore"))
}

impl SqliteRuntimeRepository {
    /// Settle only purely local, reversible/cancellable effects. This method
    /// never claims provider closure, interrupts an active native turn, kills a
    /// process, delivers a notice, or deletes a worktree. Any effect already
    /// potentially dispatched is quarantined for explicit reconciliation.
    pub fn settle_passive_workspace_retirement(
        &self,
        operation_id: &str,
        at_ms: i64,
    ) -> Result<WorkspaceRetirementPlan, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting passive retirement settlement", error))?;
        let snapshot: Option<String> = tx
            .query_row(
                "SELECT f.snapshot_json FROM workspace_retirement_fences f
             JOIN runtime_operations op ON op.operation_id=f.operation_id
             WHERE f.operation_id=?1 AND op.phase IN ('requested','manual_review')",
                [operation_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading frozen retirement operation", error))?;
        let snapshot = snapshot.ok_or_else(|| {
            RuntimeError::Conflict(format!(
                "retirement operation {operation_id} is not frozen and resumable"
            ))
        })?;
        let plan: WorkspaceRetirementPlan = serde_json::from_str(&snapshot).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid durable retirement plan: {error}"))
        })?;

        let mut quarantine = false;
        for id in &plan.managed_process_ids {
            if effect_phase(&tx, operation_id, "workspace_process_settle", id)? != "intended" {
                continue;
            }
            let status: Option<(String, String)> = tx
                .query_row(
                    "SELECT status,completion_state FROM managed_processes WHERE process_id=?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed inspecting retired managed process", error))?;
            let outcome = match status.as_ref().map(|(status, _)| status.as_str()) {
                Some("queued") => {
                    tx.execute(
                        "UPDATE managed_processes SET status='canceled',
                        terminal_reason='workspace_retirement_before_launch',
                        ended_at=?2,terminal_recorded_at=?2,completion_state='not_required',
                        completion_updated_at=?2,updated_at=?2
                        WHERE process_id=?1 AND status='queued'",
                        params![id, at_ms],
                    )
                    .map_err(|error| db_error("failed cancelling retired queued process", error))?;
                    Some(("finalized", "queued_process_never_launched"))
                }
                Some(
                    "completed" | "failed" | "timed_out" | "killed" | "canceled" | "interrupted",
                ) => match status.as_ref().map(|(_, completion)| completion.as_str()) {
                    Some("pending" | "injecting") => {
                        let completion = settle_process_completion(&tx, id, at_ms)?;
                        quarantine |= completion.0 == "manual_review";
                        Some(completion)
                    }
                    _ => Some(("finalized", "process_already_terminal")),
                },
                Some("launch_reserved" | "running") => None,
                _ => {
                    quarantine = true;
                    Some(("manual_review", "process_identity_missing"))
                }
            };
            if let Some((phase, reason)) = outcome {
                settle_effect(
                    &tx,
                    operation_id,
                    "workspace_process_settle",
                    id,
                    phase,
                    reason,
                    at_ms,
                )?;
            }
        }
        for id in &plan.delivery_ids {
            if effect_phase(&tx, operation_id, "workspace_delivery_settle", id)? != "intended" {
                continue;
            }
            let status =
                current_status(&tx, "SELECT status FROM agent_deliveries WHERE id=?1", id)?;
            let outcome = match status.as_deref() {
                Some("pending" | "deferred") => {
                    tx.execute(
                        "UPDATE agent_deliveries SET status='cancelled',
                        last_error_code='workspace_retirement',updated_at=?2
                        WHERE id=?1 AND status IN ('pending','deferred')",
                        params![id, at_ms],
                    )
                    .map_err(|error| db_error("failed cancelling undelivered message", error))?;
                    tx.execute(
                        "UPDATE team_deliveries SET status='cancelled',
                         last_error_code='workspace_retirement',updated_at=?2
                         WHERE id=?1 AND status IN ('pending','deferred')",
                        params![id, at_ms],
                    )
                    .map_err(|error| db_error("failed mirroring retired legacy delivery", error))?;
                    ("finalized", "undelivered_cancelled")
                }
                Some("injected" | "failed" | "cancelled") => ("finalized", "already_terminal"),
                _ => {
                    quarantine = true;
                    ("manual_review", "delivery_outcome_unresolved")
                }
            };
            settle_effect(
                &tx,
                operation_id,
                "workspace_delivery_settle",
                id,
                outcome.0,
                outcome.1,
                at_ms,
            )?;
        }
        for id in &plan.notice_ids {
            if effect_phase(&tx, operation_id, "workspace_notice_settle", id)? != "intended" {
                continue;
            }
            let status = current_status(
                &tx,
                "SELECT state FROM workspace_system_notices WHERE notice_id=?1",
                id,
            )?;
            let outcome = match status.as_deref() {
                Some("pending") => {
                    tx.execute(
                        "UPDATE workspace_system_notices SET state='cancelled',
                        last_error='workspace_retirement',updated_at_ms=?2
                        WHERE notice_id=?1 AND state='pending'",
                        params![id, at_ms],
                    )
                    .map_err(|error| {
                        db_error("failed cancelling queued lifecycle notice", error)
                    })?;
                    ("finalized", "undelivered_cancelled")
                }
                Some("delivered" | "cancelled") => ("finalized", "already_terminal"),
                _ => {
                    quarantine = true;
                    ("manual_review", "notice_outcome_unresolved")
                }
            };
            settle_effect(
                &tx,
                operation_id,
                "workspace_notice_settle",
                id,
                outcome.0,
                outcome.1,
                at_ms,
            )?;
        }
        for id in &plan.hook_effect_ids {
            if effect_phase(&tx, operation_id, "workspace_hook_settle", id)? != "intended" {
                continue;
            }
            let keys: Vec<String> = serde_json::from_str(id).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid hook retirement identity: {error}"))
            })?;
            if keys.len() != 3 {
                return Err(RuntimeError::Bootstrap(
                    "invalid hook retirement key width".into(),
                ));
            }
            let state: Option<(String, String)> = tx
                .query_row(
                    "SELECT phase,application_state FROM workspace_hook_effects
                 WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name=?3 AND hook_name=?4",
                    params![plan.workspace_id, keys[0], keys[1], keys[2]],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed reading pending hook retirement", error))?;
            let outcome = match state.as_ref().map(|(phase, _)| phase.as_str()) {
                Some("intended") | Some("succeeded") => {
                    tx.execute("UPDATE workspace_hook_effects SET
                        phase=CASE WHEN phase='intended' THEN 'failed' ELSE phase END,
                        application_state='rejected',application_error_code='workspace_retirement',
                        updated_at_ms=?5
                        WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name=?3 AND hook_name=?4",
                        params![plan.workspace_id,keys[0],keys[1],keys[2],at_ms])
                        .map_err(|error|db_error("failed preventing retired hook output",error))?;
                    ("finalized", "hook_context_rejected")
                }
                Some("failed" | "timed_out") => ("finalized", "hook_already_terminal"),
                _ => {
                    quarantine = true;
                    ("manual_review", "hook_dispatch_outcome_unresolved")
                }
            };
            settle_effect(
                &tx,
                operation_id,
                "workspace_hook_settle",
                id,
                outcome.0,
                outcome.1,
                at_ms,
            )?;
        }
        for id in &plan.preserved_worktree_ids {
            if effect_phase(&tx, operation_id, "workspace_worktree_preserve", id)? != "intended" {
                continue;
            }
            // Retaining the underlying files is unconditional, but detaching
            // live worktree claims is only safe after all affected provider
            // attachments and managed processes have terminal proof.
            let outstanding: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM runtime_operation_effects e WHERE e.operation_id=?1
                 AND e.effect_kind IN ('workspace_provider_close','workspace_process_settle')
                 AND e.phase<>'finalized'",
                    [operation_id],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    db_error("failed fencing retirement worktree claim release", error)
                })?;
            if outstanding != 0 {
                continue;
            }
            let changed=tx.execute(
                "UPDATE managed_worktrees SET deletion_policy='retain_on_last_claim',
                    updated_at=?3,revision=revision+1
                 WHERE id=?1 AND repo_root=(SELECT canonical_root FROM workspaces WHERE workspace_id=?2)",
                params![id,plan.workspace_id,at_ms],
            ).map_err(|error|db_error("failed preserving workspace worktree",error))?;
            if changed == 1 {
                tx.execute(
                    "UPDATE managed_worktree_claims SET released_at=?3
                     WHERE worktree_id=?1 AND released_at IS NULL
                       AND session_id IN (SELECT session_id FROM workspace_session_ownership
                           WHERE workspace_id=?2)",
                    params![id, plan.workspace_id, at_ms],
                )
                .map_err(|error| {
                    db_error("failed releasing verified retired worktree claims", error)
                })?;
            }
            if changed == 0 {
                quarantine = true;
            }
            settle_effect(
                &tx,
                operation_id,
                "workspace_worktree_preserve",
                id,
                if changed == 1 {
                    "finalized"
                } else {
                    "manual_review"
                },
                if changed == 1 {
                    "worktree_preserved"
                } else {
                    "worktree_identity_unresolved"
                },
                at_ms,
            )?;
        }
        if quarantine {
            tx.execute(
                "UPDATE workspace_retirement_fences SET phase='manual_review',updated_at=?2
                WHERE operation_id=?1",
                params![operation_id, at_ms],
            )
            .map_err(|error| db_error("failed quarantining retirement fence", error))?;
            tx.execute(
                "UPDATE runtime_operations SET phase='manual_review',updated_at=?2
                WHERE operation_id=?1 AND phase='requested'",
                params![operation_id, at_ms],
            )
            .map_err(|error| db_error("failed quarantining retirement operation", error))?;
        }
        tx.commit()
            .map_err(|error| db_error("failed committing passive retirement settlement", error))?;
        Ok(plan)
    }

    /// Finalization has no provider/native side effects. It requires every
    /// snapshotted effect to be independently receipted and all current safety
    /// invariants to be rechecked within this very write transaction.
    /// `None` means work remains; it never means retired.
    pub fn finalize_settled_workspace_retirement(
        &self,
        operation_id: &str,
        at_ms: i64,
    ) -> Result<Option<WorkspaceRetirementResponse>, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting retirement finalization", error))?;
        let frozen: Option<(String, String, String)> = tx.query_row(
            "SELECT workspace_id,snapshot_json,phase FROM workspace_retirement_fences WHERE operation_id=?1",
            [operation_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).optional().map_err(|error| db_error("failed finding retirement finalization fence", error))?;
        let (workspace_id, snapshot, phase) = frozen.ok_or_else(|| {
            RuntimeError::Conflict("retirement has no durable resource fence".into())
        })?;
        if phase == "manual_review" {
            return Err(RuntimeError::Conflict(
                "retirement contains an unresolved effect requiring manual review".into(),
            ));
        }
        if phase != "frozen" {
            return Err(RuntimeError::Conflict(
                "retirement is no longer in the frozen settlement phase".into(),
            ));
        }
        let plan: WorkspaceRetirementPlan = serde_json::from_str(&snapshot).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid retirement plan: {error}"))
        })?;
        let expected_effect_count = plan.agent_ids.len()
            + plan.turn_ids.len()
            + plan.managed_process_ids.len()
            + plan.delivery_ids.len()
            + plan.notice_ids.len()
            + plan.hook_effect_ids.len()
            + plan.preserved_worktree_ids.len();
        let (total, outstanding, verified_closures): (i64, i64, i64) = tx
            .query_row(
                "SELECT COUNT(*),
                    COALESCE(SUM(CASE WHEN phase<>'finalized' THEN 1 ELSE 0 END),0),
                    COALESCE(SUM(CASE WHEN effect_kind='workspace_provider_close'
                        AND phase='finalized'
                        AND json_extract(evidence_json,'$.verified')=1
                        THEN 1 ELSE 0 END),0)
             FROM runtime_operation_effects WHERE operation_id=?1",
                [operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|error| db_error("failed verifying retirement effect closure", error))?;
        if usize::try_from(total).ok() != Some(expected_effect_count)
            || usize::try_from(verified_closures).ok() != Some(plan.agent_ids.len())
            || outstanding != 0
        {
            return Ok(None);
        }
        let workspace = workspace_by_id(&tx, &workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {workspace_id}")))?;
        if workspace.lifecycle_state != runtime_core::WorkspaceLifecycleState::Active {
            return Err(RuntimeError::Conflict(
                "retirement workspace lifecycle changed unexpectedly".into(),
            ));
        }
        if workspace.lead_agent_id.is_some() {
            return Ok(None);
        }
        let active_agents: i64 = tx.query_row(
            "SELECT COUNT(*) FROM workspace_agents WHERE workspace_id=?1 AND lifecycle_state<>'archived'",
            [&workspace_id],|row|row.get(0),
        ).map_err(|error| db_error("failed checking archived workspace members",error))?;
        let open_sessions: i64 = tx.query_row(
            "SELECT COUNT(*) FROM workspace_session_ownership o JOIN sessions s ON s.id=o.session_id
             WHERE o.workspace_id=?1 AND (s.status<>'closed' OR s.active_turn_id IS NOT NULL)",
            [&workspace_id],|row|row.get(0),
        ).map_err(|error| db_error("failed checking closed provider attachments",error))?;
        if active_agents != 0 || open_sessions != 0 {
            return Ok(None);
        }
        // Recheck newly appeared obligations and races after the snapshot,
        // except immutable historical ownership and the in-flight retirement
        // operation/fence themselves. Anything else must block finalization.
        for blocker in retirement_blockers(&tx, &workspace)? {
            if !matches!(
                blocker.code.as_str(),
                "session_ownership"
                    | "agent_records"
                    | "retirement_in_progress"
                    | "operations_unresolved"
            ) {
                return Ok(None);
            }
            if blocker.code == "operations_unresolved" && blocker.count != 1 {
                return Ok(None);
            }
        }
        let claim: (i64, i64) = tx
            .query_row(
                "SELECT fence_generation,acquired_at FROM runtime_operation_resource_claims
             WHERE resource_kind='workspace' AND resource_id=?1 AND owner_operation_id=?2",
                params![workspace_id, operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| db_error("retirement lost exact workspace claim", error))?;
        let changed = tx
            .execute(
                "UPDATE workspaces SET lifecycle_state='retired',lead_agent_id=NULL,
                revision=revision+1,updated_at=?2
             WHERE workspace_id=?1 AND lifecycle_state='active' AND lead_agent_id IS NULL",
                params![workspace_id, at_ms],
            )
            .map_err(|error| db_error("failed finalizing retired workspace identity", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "workspace changed during retirement finalization".into(),
            ));
        }
        tx.execute(
            "INSERT INTO retired_workspaces
            (workspace_id,canonical_root,retired_by_operation_id,retired_at)
            VALUES (?1,?2,?3,?4)",
            params![workspace_id, workspace.canonical_root, operation_id, at_ms],
        )
        .map_err(|error| db_error("failed recording completed retirement tombstone", error))?;
        let retired = workspace_by_id(&tx, &workspace_id)?.ok_or_else(|| {
            RuntimeError::Bootstrap("retired workspace disappeared before finalization".into())
        })?;
        let response = WorkspaceRetirementResponse {
            operation_id: operation_id.into(),
            workspace: retired,
        };
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&workspace_id),
            &serde_json::to_value(&response).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid retirement result: {error}"))
            })?,
            None,
            None,
            at_ms,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id:operation_id.into(),sequence:tx.query_row(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_operation_transitions WHERE operation_id=?1",
                [operation_id],|row|row.get::<_,u64>(0),
            ).map_err(|error|db_error("failed allocating retirement completion transition",error))?,
            from_phase:Some(OperationPhase::Requested),to_phase:OperationPhase::Completed,
            evidence:Some(serde_json::json!({"snapshot_effects":total,
                "verified_provider_closures":verified_closures,"worktrees_preserved":plan.preserved_worktree_ids.len()})),
            created_at:at_ms,
        })?;
        authority.release_claim(&OperationResourceClaimRecord {
            resource_kind: "workspace".into(),
            resource_id: workspace_id.clone(),
            claim_mode: "exclusive".into(),
            owner_operation_id: operation_id.into(),
            fence_generation: u64::try_from(claim.0).map_err(|_| {
                RuntimeError::Bootstrap("invalid retirement fence generation".into())
            })?,
            acquired_at: claim.1,
        })?;
        tx.execute(
            "UPDATE workspace_retirement_fences SET phase='retired',updated_at=?2
            WHERE operation_id=?1 AND phase='frozen'",
            params![operation_id, at_ms],
        )
        .map_err(|error| db_error("failed terminalizing retirement ingress fence", error))?;
        crate::repository_workspace_retirement_events::retirement_completed(
            &tx,
            operation_id,
            &workspace_id,
            at_ms,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing complete workspace retirement", error))?;
        Ok(Some(response))
    }
}
