use runtime_core::{ManagedWorktreeClaimRecord, ManagedWorktreeRecord, RuntimeError};
use rusqlite::TransactionBehavior;

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
        tx.commit().map_err(|error| {
            db_error(
                "failed committing historical worktree startup repair",
                error,
            )
        })?;
        Ok(())
    }
}
