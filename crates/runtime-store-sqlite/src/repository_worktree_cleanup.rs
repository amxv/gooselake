//! Native worktree removal is an irreversible two-step Git effect. Persist a
//! canonical operation and both Git identity reservations before dispatch,
//! then receipt native evidence and its lifecycle event in one transaction.
use runtime_core::{
    normalized_json_hash, ManagedWorktreeCleanupAdmission, ManagedWorktreeCleanupCommand,
    ManagedWorktreeRecord, NewRuntimeEvent, OperationActor, OperationActorKind,
    OperationEffectRecord, OperationPhase, OperationRecord, OperationResourceClaimRecord,
    OperationTransitionRecord, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
    WorktreeCleanupResponse,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_workspace::operation_by_id;
use crate::repository_workspace_worktree_creation::{
    branch_resource_id, BRANCH_CLAIM_KIND, CLAIM_KIND,
};
use crate::SqliteRuntimeRepository;

const CLEANUP_KIND: &str = "managed_worktree_cleanup";
const EFFECT_KIND: &str = "git_worktree_cleanup";

#[derive(Debug)]
struct CleanupReceipt {
    operation_id: String,
    repo_root: String,
    checkout_path: String,
    branch_name: String,
    phase: String,
    response_json: Option<String>,
}

fn receipt_for(
    tx: &Connection,
    worktree: &ManagedWorktreeRecord,
) -> Result<Option<CleanupReceipt>, RuntimeError> {
    tx.query_row(
        "SELECT operation_id, repo_root, checkout_path, branch_name, phase, response_json
         FROM managed_worktree_cleanup_receipts
         WHERE worktree_id=?1 AND worktree_created_at=?2",
        params![worktree.id, worktree.created_at],
        |row| {
            Ok(CleanupReceipt {
                operation_id: row.get(0)?,
                repo_root: row.get(1)?,
                checkout_path: row.get(2)?,
                branch_name: row.get(3)?,
                phase: row.get(4)?,
                response_json: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading managed worktree cleanup receipt", error))
}

fn classify(
    existing: CleanupReceipt,
    worktree: &ManagedWorktreeRecord,
) -> Result<ManagedWorktreeCleanupAdmission, RuntimeError> {
    if existing.repo_root != worktree.repo_root
        || existing.checkout_path != worktree.worktree_cwd
        || existing.branch_name != worktree.branch_name
    {
        return Err(RuntimeError::Conflict(
            "managed worktree identity changed since native cleanup admission".into(),
        ));
    }
    if existing.phase == "completed" {
        let response = existing.response_json.ok_or_else(|| {
            RuntimeError::Bootstrap("completed native cleanup has no exact receipt".into())
        })?;
        return Ok(ManagedWorktreeCleanupAdmission::Replay {
            operation_id: existing.operation_id,
            response: serde_json::from_str(&response).map_err(|error| {
                RuntimeError::Bootstrap(format!("corrupt worktree cleanup receipt: {error}"))
            })?,
        });
    }
    Ok(ManagedWorktreeCleanupAdmission::Unresolved {
        operation_id: existing.operation_id,
    })
}

fn worktree_event(
    tx: &Transaction<'_>,
    worktree_id: &str,
    operation_id: &str,
    suffix: &str,
    kind: &str,
    payload: serde_json::Value,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    insert_runtime_event_in_transaction(
        tx,
        &NewRuntimeEvent {
            event_id: format!("{operation_id}:event:worktree:{suffix}"),
            scope: RuntimeEventScope::Worktree,
            scope_id: worktree_id.to_string(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: kind.into(),
            criticality: RuntimeEventCriticality::Critical,
            payload,
            provider: None,
            provider_seq: None,
            created_at: at_ms,
        },
    )?;
    Ok(())
}

fn existing_claim(
    tx: &Transaction<'_>,
    operation_id: &str,
    kind: &str,
    resource_id: &str,
) -> Result<OperationResourceClaimRecord, RuntimeError> {
    let (generation, acquired_at): (i64, i64) = tx
        .query_row(
            "SELECT fence_generation, acquired_at FROM runtime_operation_resource_claims
             WHERE owner_operation_id=?1 AND resource_kind=?2 AND resource_id=?3
               AND claim_mode='exclusive'",
            params![operation_id, kind, resource_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| db_error("failed reading native cleanup reservation", error))?
        .ok_or_else(|| {
            RuntimeError::Conflict("native cleanup lost its identity reservation".into())
        })?;
    Ok(OperationResourceClaimRecord {
        resource_kind: kind.into(),
        resource_id: resource_id.into(),
        claim_mode: "exclusive".into(),
        owner_operation_id: operation_id.into(),
        fence_generation: u64::try_from(generation)
            .map_err(|_| RuntimeError::Bootstrap("negative cleanup fence generation".into()))?,
        acquired_at,
    })
}

impl SqliteRuntimeRepository {
    pub fn replay_managed_worktree_cleanup(
        &self,
        worktree: &ManagedWorktreeRecord,
    ) -> Result<Option<ManagedWorktreeCleanupAdmission>, RuntimeError> {
        let db = open_connection(&self.database_path)?;
        receipt_for(&*db, worktree)?
            .map(|row| classify(row, worktree))
            .transpose()
    }

    pub fn native_worktree_cleanup_owner(
        &self,
        repo_root: &str,
        checkout_path: &str,
        branch_name: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let db = open_connection(&self.database_path)?;
        db.query_row(
            "SELECT operation_id FROM managed_worktree_cleanup_receipts
             WHERE phase != 'completed' AND
               (checkout_path=?1 OR (repo_root=?2 AND branch_name=?3)) LIMIT 1",
            params![checkout_path, repo_root, branch_name],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed checking unresolved native cleanup", error))
    }

    pub fn admit_managed_worktree_cleanup(
        &self,
        command: &ManagedWorktreeCleanupCommand,
    ) -> Result<ManagedWorktreeCleanupAdmission, RuntimeError> {
        let worktree = &command.worktree;
        if command.operation_id.is_empty() || worktree.id.is_empty() {
            return Err(RuntimeError::InvalidState(
                "invalid native cleanup identity".into(),
            ));
        }
        let mut db = open_connection(&self.database_path)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native cleanup admission", error))?;
        if let Some(existing) = receipt_for(&tx, worktree)? {
            return classify(existing, worktree);
        }

        // Re-evaluate the original observed generation and all live SQLite
        // bindings inside the immediate writer lock, not just a prior process
        // local mutex. A changed native identity needs a fresh preflight.
        let persisted: Option<(String, String, String, String, i64)> = tx
            .query_row(
                "SELECT repo_root, worktree_cwd, branch_name, deletion_policy, created_at
             FROM managed_worktrees WHERE id=?1",
                [&worktree.id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| db_error("failed confirming cleanup native identity", error))?;
        if persisted
            != Some((
                worktree.repo_root.clone(),
                worktree.worktree_cwd.clone(),
                worktree.branch_name.clone(),
                worktree.deletion_policy.clone(),
                worktree.created_at,
            ))
        {
            return Err(RuntimeError::Conflict(
                "managed worktree changed before native cleanup".into(),
            ));
        }
        if worktree.deletion_policy != "delete_on_last_claim" {
            return Err(RuntimeError::Conflict(
                "managed worktree retention policy prohibits native cleanup".into(),
            ));
        }
        let active: i64 = tx
            .query_row(
                "SELECT (SELECT COUNT(*) FROM managed_worktree_claims
                     WHERE worktree_id=?1 AND released_at IS NULL)
                  + (SELECT COUNT(*) FROM sessions
                     WHERE status!='closed' AND (worktree_id=?1 OR cwd=?2))",
                params![worktree.id, worktree.worktree_cwd],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed confirming no live cleanup claims or sessions",
                    error,
                )
            })?;
        if active > 0 {
            return Err(RuntimeError::Conflict(
                "worktree has live claims or session bindings".into(),
            ));
        }

        let input = serde_json::json!({
            "worktree_id": worktree.id,
            "worktree_created_at": worktree.created_at,
            "repo_root": worktree.repo_root,
            "checkout_path": worktree.worktree_cwd,
            "branch_name": worktree.branch_name,
            "deletion_policy": worktree.deletion_policy,
            "reason": command.reason,
        });
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: None,
            kind: CLEANUP_KIND.into(),
            actor: OperationActor {
                kind: OperationActorKind::System,
                identifier: "managed_worktree_service".into(),
            },
            idempotency_key: Some(format!("{}:{}", worktree.id, worktree.created_at)),
            normalized_request_hash: normalized_json_hash(&input)?,
            normalized_request: input,
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
            evidence: Some(serde_json::json!({"native_effect":EFFECT_KIND})),
            created_at: command.requested_at,
        })?;
        authority.acquire_claim(
            &command.operation_id,
            CLAIM_KIND,
            &worktree.worktree_cwd,
            command.requested_at,
        )?;
        authority.acquire_claim(
            &command.operation_id,
            BRANCH_CLAIM_KIND,
            &branch_resource_id(&worktree.repo_root, &worktree.branch_name),
            command.requested_at,
        )?;
        authority.insert_effect(&OperationEffectRecord {
            effect_id: format!("fx_{}", command.operation_id),
            operation_id: command.operation_id.clone(),
            effect_kind: EFFECT_KIND.into(),
            target_kind: "git_checkout".into(),
            target_id: worktree.worktree_cwd.clone(),
            phase: "intended".into(),
            idempotency_key: "native_git_remove_and_branch_delete".into(),
            evidence: Some(serde_json::json!({
                "repo_root": worktree.repo_root,
                "checkout_path": worktree.worktree_cwd,
                "branch_name": worktree.branch_name,
                "native_dispatched": "unknown_after_admission",
            })),
            attempt_count: 0,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        tx.execute(
            "INSERT INTO managed_worktree_cleanup_receipts
             (operation_id, worktree_id, worktree_created_at, repo_root, checkout_path,
              branch_name, deletion_policy, reason, phase, requested_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'pending',?9)",
            params![
                command.operation_id,
                worktree.id,
                worktree.created_at,
                worktree.repo_root,
                worktree.worktree_cwd,
                worktree.branch_name,
                worktree.deletion_policy,
                command.reason,
                command.requested_at
            ],
        )
        .map_err(|error| db_error("failed persisting native cleanup receipt intent", error))?;
        worktree_event(
            &tx,
            &worktree.id,
            &command.operation_id,
            "admitted",
            "worktree.cleanup_admitted",
            serde_json::json!({"operation_id": command.operation_id,
                "checkout_path": worktree.worktree_cwd, "branch_name": worktree.branch_name}),
            command.requested_at,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing native cleanup admission", error))?;
        Ok(ManagedWorktreeCleanupAdmission::Execute {
            operation_id: command.operation_id.clone(),
        })
    }

    /// Called only after native Git, or after independent read-only evidence
    /// proves both the exact checkout and branch are already absent. A failed
    /// terminal DB commit leaves the original reservations and intent intact.
    pub fn complete_managed_worktree_cleanup(
        &self,
        operation_id: &str,
        response: &WorktreeCleanupResponse,
        completed_at: i64,
    ) -> Result<(), RuntimeError> {
        let mut db = open_connection(&self.database_path)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native cleanup settlement", error))?;
        let (worktree_id, repo_root, checkout_path, branch_name, phase): (
            String,
            String,
            String,
            String,
            String,
        ) = tx
            .query_row(
                "SELECT worktree_id, repo_root, checkout_path, branch_name, phase
                 FROM managed_worktree_cleanup_receipts WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| db_error("failed reading cleanup intent for settlement", error))?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("native cleanup operation {operation_id}"))
            })?;
        if worktree_id != response.worktree_id || phase == "completed" {
            return Err(RuntimeError::Conflict(
                "native cleanup receipt already settled or has mismatched identity".into(),
            ));
        }
        let operation = operation_by_id(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("operation {operation_id}")))?;
        if operation.kind != CLEANUP_KIND
            || !matches!(
                operation.phase,
                OperationPhase::Requested | OperationPhase::ManualReview
            )
        {
            return Err(RuntimeError::Conflict(
                "native cleanup operation has unexpected authority".into(),
            ));
        }
        let was_review = operation.phase == OperationPhase::ManualReview;
        let exact = serde_json::to_value(response).map_err(|error| {
            RuntimeError::Bootstrap(format!("failed encoding cleanup receipt: {error}"))
        })?;
        let succeeded = response.status == "deleted"
            && response.worktree_path_deleted
            && response.branch_deleted
            && response.diagnostics.is_empty();
        if was_review && !succeeded {
            return Err(RuntimeError::Conflict(
                "native cleanup in review cannot be retried without independent evidence".into(),
            ));
        }
        let next_phase = if succeeded {
            "completed"
        } else {
            "manual_review"
        };
        tx.execute(
            "UPDATE managed_worktree_cleanup_receipts SET phase=?2, response_json=?3, completed_at=?4
             WHERE operation_id=?1 AND phase!='completed'",
            params![operation_id, next_phase, json_to_string(&exact)?, completed_at],
        ).map_err(|error| db_error("failed persisting native cleanup outcome", error))?;
        tx.execute(
            "UPDATE runtime_operation_effects SET phase=?2, attempt_count=1,
             evidence_json=?3, updated_at=?4 WHERE operation_id=?1 AND effect_kind=?5",
            params![
                operation_id,
                if succeeded { "finalized" } else { "uncertain" },
                json_to_string(&serde_json::json!({"receipt":exact,"native_evidence":"observed"}))?,
                completed_at,
                EFFECT_KIND
            ],
        )
        .map_err(|error| db_error("failed persisting native cleanup effect evidence", error))?;
        let authority = OperationAuthorityTransaction::new(&tx);
        if succeeded {
            authority.terminalize(
                operation_id,
                operation.phase,
                OperationPhase::Completed,
                None,
                &exact,
                None,
                None,
                completed_at,
            )?;
        } else {
            tx.execute(
                "UPDATE runtime_operations SET phase='manual_review',
                 error_code='native_cleanup_incomplete', error_message='independent native effect review required',
                 updated_at=?2 WHERE operation_id=?1 AND phase='requested'",
                params![operation_id, completed_at],
            ).map_err(|error| db_error("failed recording native cleanup review requirement", error))?;
        }
        authority.append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: if was_review { 3 } else { 2 },
            from_phase: Some(operation.phase),
            to_phase: if succeeded {
                OperationPhase::Completed
            } else {
                OperationPhase::ManualReview
            },
            evidence: Some(serde_json::json!({"native_cleanup":next_phase})),
            created_at: completed_at,
        })?;
        if succeeded {
            let branch_key = branch_resource_id(&repo_root, &branch_name);
            for (kind, key) in [
                (CLAIM_KIND, checkout_path.as_str()),
                (BRANCH_CLAIM_KIND, branch_key.as_str()),
            ] {
                let claim = existing_claim(&tx, operation_id, kind, key)?;
                authority.release_claim(&claim)?;
            }
        }
        worktree_event(
            &tx,
            &worktree_id,
            operation_id,
            if was_review { "reconciled" } else { "settled" },
            if succeeded {
                "worktree.cleaned_up"
            } else {
                "worktree.cleanup_failed"
            },
            serde_json::json!({"operation_id":operation_id, "response":exact,
                "after_manual_review":was_review}),
            completed_at,
        )?;
        tx.commit().map_err(|error| {
            db_error(
                "failed committing native cleanup receipt and journal",
                error,
            )
        })
    }
}
