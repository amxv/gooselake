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
mod legacy;
mod protocol;
mod receipts;

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
    tx: &Connection,
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
        let response = protocol::verify_replay(tx, &existing.operation_id)?;
        let stored = existing.response_json.ok_or_else(|| {
            RuntimeError::Bootstrap("completed native cleanup has no exact receipt".into())
        })?;
        let stored: serde_json::Value = serde_json::from_str(&stored)
            .map_err(|_| protocol::conflict("invalid completed cleanup receipt"))?;
        if stored != protocol::value(&response)? {
            return Err(protocol::conflict(
                "completed cleanup replay result disagrees",
            ));
        }
        return Ok(ManagedWorktreeCleanupAdmission::Replay {
            operation_id: existing.operation_id,
            response,
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

fn require_eligible(
    tx: &Transaction<'_>,
    target: &runtime_core::ManagedCleanupTarget,
) -> Result<(), RuntimeError> {
    // Re-evaluate the original observed generation and all live SQLite
    // bindings inside the immediate writer lock, not just a prior process
    // local mutex. A changed native identity needs a fresh preflight.
    let persisted: Option<(String, String, String, String, i64)> = tx
        .query_row(
            "SELECT repo_root, worktree_cwd, branch_name, deletion_policy, created_at
             FROM managed_worktrees WHERE id=?1",
            [&target.worktree_id],
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
            target.repo_root.clone(),
            target.checkout_path.clone(),
            target.branch_name.clone(),
            target.deletion_policy.clone(),
            target.worktree_created_at,
        ))
    {
        return Err(RuntimeError::Conflict(
            "managed worktree changed before native cleanup".into(),
        ));
    }
    if target.deletion_policy != "delete_on_last_claim" {
        return Err(RuntimeError::Conflict(
            "managed worktree retention policy prohibits native cleanup".into(),
        ));
    }
    let active: i64 = tx
        .query_row(
            "SELECT (SELECT COUNT(*) FROM managed_worktree_claims
                     WHERE worktree_id=?1 AND released_at IS NULL)
                  + (SELECT COUNT(*) FROM sessions
                     WHERE status!='closed' AND (worktree_id=?1 OR cwd=?2
                       OR substr(cwd, 1, length(?2) + 1) = ?2 || '/'))",
            params![target.worktree_id, target.checkout_path],
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

    let mut legacy_targets =
        crate::repository_legacy_native_targets::LegacyNativeTargetBatch::default();
    legacy_targets.checkout(
        Some(&target.worktree_id),
        Some(&target.checkout_path),
        Some(&target.repo_root),
        Some(&target.branch_name),
    )?;
    legacy_targets.guard(tx)?;

    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn replay_managed_worktree_cleanup(
        &self,
        worktree: &ManagedWorktreeRecord,
    ) -> Result<Option<ManagedWorktreeCleanupAdmission>, RuntimeError> {
        let mut db = open_connection(&self.database_path)?;
        // All carriers used for read-only replay come from one SQLite snapshot.
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting cleanup replay snapshot", error))?;
        let result = receipt_for(&tx, worktree)?
            .map(|row| classify(&tx, row, worktree))
            .transpose()?;
        tx.commit()
            .map_err(|error| db_error("failed ending cleanup replay snapshot", error))?;
        Ok(result)
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
            return classify(&tx, existing, worktree);
        }

        let target = protocol::target(worktree);
        require_eligible(&tx, &target)?;

        let input = protocol::input(&target, &command.reason);
        let authority = OperationAuthorityTransaction::new(&tx);
        // Capture the exact returned reservations. A later lookup cannot
        // replace these generations when recording an original action.
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
            normalized_request: input.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        let checkout_claim = authority.acquire_claim(
            &command.operation_id,
            CLAIM_KIND,
            &worktree.worktree_cwd,
            command.requested_at,
        )?;
        let branch_claim = authority.acquire_claim(
            &command.operation_id,
            BRANCH_CLAIM_KIND,
            &branch_resource_id(&worktree.repo_root, &worktree.branch_name),
            command.requested_at,
        )?;
        let binding = runtime_core::ManagedCleanupBinding {
            version: 1,
            operation_id: command.operation_id.clone(),
            target,
            reason: command.reason.clone(),
            requested_at: command.requested_at,
            normalized_request_hash: normalized_json_hash(&input)?,
            checkout_claim,
            branch_claim,
        };
        authority.append_transition(&protocol::admission_transition(&binding))?;
        let grant = runtime_core::CheckoutRemovalGrant { binding };
        let checkout = protocol::checkout_started(&grant);
        let branch = protocol::branch_intended(&grant.binding);
        authority.insert_effect(&checkout)?;
        authority.insert_effect(&branch)?;
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
        protocol::pending(&tx, &grant.binding)?;
        protocol::verify_effects(&tx, &command.operation_id, &[checkout, branch])?;
        tx.commit()
            .map_err(|error| db_error("failed committing native cleanup admission", error))?;
        Ok(ManagedWorktreeCleanupAdmission::Execute { grant })
    }

    /// Historical raw responses are not original action acknowledgements.
    /// This compatibility entry point grants no settlement authority.
    pub fn complete_managed_worktree_cleanup(
        &self,
        _operation_id: &str,
        _response: &WorktreeCleanupResponse,
        _completed_at: i64,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported("raw cleanup responses cannot settle native effects; original action acknowledgements required".into()))
    }
}
