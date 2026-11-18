use runtime_core::{ManagedWorktreeClaimRecord, ManagedWorktreeRecord, RuntimeError};
use rusqlite::{OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::repository_worktree_claim_events::repair_historical_claims_in_transaction;
use crate::repository_worktree_record_events::repair_historical_records_in_transaction;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// Startup legacy worktree record merges/tombstones and the corresponding
    /// claim normalization derive from the SAME hydrated snapshot. Do not
    /// allow a later claim conflict to leave earlier record repairs committed.
    /// Native Git/provider side effects are not part of this transaction.
    pub fn repair_historical_worktree_startup_atomically(
        &self,
        record_repairs: &[(&ManagedWorktreeRecord, ManagedWorktreeRecord)],
        claim_originals: &[ManagedWorktreeClaimRecord],
        claim_repairs: &[ManagedWorktreeClaimRecord],
    ) -> Result<(), RuntimeError> {
        if record_repairs.is_empty() && claim_repairs.is_empty() {
            return Ok(());
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error("failed beginning historical worktree startup repair", error)
            })?;
        // This API receives the complete raw hydration. Check new keys too,
        // including record-only repairs with no planned claim changes.
        crate::repository_worktree_claim_generation::verify_full_snapshot(&tx, claim_originals)?;
        crate::repository_legacy_native_targets::guard_legacy_native_repairs(
            &tx,
            record_repairs,
            claim_repairs,
        )?;
        // Both helpers compare their source rows while this writer lock is
        // held. A later claim/source/destination/SQL failure rolls back ALL
        // earlier worktree record writes and every claim write.
        if !record_repairs.is_empty() {
            repair_historical_records_in_transaction(&tx, record_repairs)?;
        }
        if !claim_repairs.is_empty() {
            repair_historical_claims_in_transaction(&tx, Some(claim_originals), claim_repairs)?;
        }
        // A later carrier/trigger fault must not invalidate an earlier plan.
        // Verify the final complete claim keyset and every participating record.
        let mut expected = claim_originals
            .iter()
            .cloned()
            .map(|claim| ((claim.worktree_id.clone(), claim.session_id.clone()), claim))
            .collect::<std::collections::BTreeMap<_, _>>();
        for proposed in claim_repairs {
            let mut claim = proposed.clone();
            if claim.claim_generation.is_none() {
                claim.claim_generation = Some(1); // The repair helper admitted exact absence.
            }
            expected.insert((claim.worktree_id.clone(), claim.session_id.clone()), claim);
        }
        crate::repository_worktree_claim_generation::verify_full_snapshot(
            &tx,
            &expected.into_values().collect::<Vec<_>>(),
        )?;
        for (_, repaired) in record_repairs {
            let current = tx
                .query_row(
                    "SELECT id,repo_root,worktree_root,worktree_cwd,branch_name,worktree_name,
                 unified_workspace_path,deletion_policy,created_by_session_id,
                 created_by_operation_id,created_at,updated_at FROM managed_worktrees WHERE id=?1",
                    [&repaired.id],
                    crate::repository_worktree_status::worktree_from_row,
                )
                .optional()
                .map_err(|e| db_error("failed verifying final startup record carrier", e))?;
            if current.as_ref() != Some(repaired) {
                return Err(RuntimeError::Conflict(
                    "startup record carrier changed during combined repair".into(),
                ));
            }
        }
        tx.commit().map_err(|error| {
            db_error(
                "failed committing historical worktree startup repair",
                error,
            )
        })?;
        Ok(())
    }
}
