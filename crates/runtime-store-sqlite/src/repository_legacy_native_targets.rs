//! Target construction for fresh native effects and changed maintenance rows.
//! These guards use the caller's transaction; they never hydrate legacy history.
use runtime_core::legacy_spawn_obligations::{
    LegacyNativeTarget, LegacyScanIncomplete, LegacyScanLimit, LegacySpawnScanStats,
};
use runtime_core::{ManagedWorktreeClaimRecord, ManagedWorktreeRecord, RuntimeError};
use rusqlite::{params, Connection, OptionalExtension};

use crate::db::db_error;
use crate::repository_legacy_spawn_obligations::{
    guard_legacy_native_targets, targets_within_budget,
};

#[derive(Default)]
pub(crate) struct LegacyNativeTargetBatch {
    targets: Vec<LegacyNativeTarget>,
}

impl LegacyNativeTargetBatch {
    fn push(&mut self, target: LegacyNativeTarget) -> Result<(), RuntimeError> {
        if self.targets.contains(&target) {
            return Ok(());
        }
        self.targets.push(target);
        if !targets_within_budget(&self.targets) {
            return Err(RuntimeError::LegacyNativeScanIncomplete {
                incomplete: LegacyScanIncomplete {
                    stream: None,
                    after_operation_id: None,
                    reason: LegacyScanLimit::TargetBudget,
                    stats: LegacySpawnScanStats::default(),
                },
            });
        }
        Ok(())
    }

    pub(crate) fn session(&mut self, session_id: &str) -> Result<(), RuntimeError> {
        self.push(LegacyNativeTarget::Session(session_id.to_owned()))
    }

    pub(crate) fn checkout(
        &mut self,
        worktree_id: Option<&str>,
        checkout_path: Option<&str>,
        repository_root: Option<&str>,
        branch_name: Option<&str>,
    ) -> Result<(), RuntimeError> {
        // Empty historical fields are unknown coordinates, not evidence of
        // absence. A wholly unusable target is explicit ScanIncomplete.
        let part = |value: Option<&str>| {
            value
                .filter(|value| !value.is_empty() && !value.contains('\0'))
                .map(str::to_owned)
        };
        self.push(LegacyNativeTarget::Checkout {
            worktree_id: part(worktree_id),
            checkout_path: part(checkout_path),
            repository_root: part(repository_root),
            branch_name: part(branch_name),
        })
    }

    pub(crate) fn worktree(
        &mut self,
        worktree: &ManagedWorktreeRecord,
    ) -> Result<(), RuntimeError> {
        self.checkout(
            Some(&worktree.id),
            Some(&worktree.worktree_cwd),
            Some(&worktree.repo_root),
            Some(&worktree.branch_name),
        )
    }

    /// An explicit cwd is the immutable route being protected. Catalog data
    /// contributes current positive ID/repository/branch matches only; it
    /// never replaces that frozen spelling or proves historical disjointness.
    pub(crate) fn route(
        &mut self,
        connection: &Connection,
        worktree_id: Option<&str>,
        frozen_cwd: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let saved: Option<(String, String, String)> = match worktree_id {
            Some(id) => connection
                .query_row(
                    "SELECT worktree_cwd,repo_root,branch_name FROM managed_worktrees WHERE id=?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(|error| db_error("failed reading guarded checkout target", error))?,
            None => None,
        };
        self.checkout(
            worktree_id,
            frozen_cwd.or_else(|| saved.as_ref().map(|row| row.0.as_str())),
            saved.as_ref().map(|row| row.1.as_str()),
            saved.as_ref().map(|row| row.2.as_str()),
        )
    }

    pub(crate) fn claim(
        &mut self,
        connection: &Connection,
        claim: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        self.session(&claim.session_id)?;
        self.route(connection, Some(&claim.worktree_id), None)
    }

    pub(crate) fn guard(self, connection: &Connection) -> Result<(), RuntimeError> {
        guard_legacy_native_targets(connection, &self.targets)
    }
}

/// Exact unchanged repairs have no new authority to grant. Changed record
/// paths include BOTH old and new identities. Changed claim keys include
/// their affected Session and current checkout; orphan IDs remain unknown.
/// The lower repair helpers retain all source/duplicate/generation checks.
pub(crate) fn guard_legacy_native_repairs(
    connection: &Connection,
    records: &[(&ManagedWorktreeRecord, ManagedWorktreeRecord)],
    claims: &[ManagedWorktreeClaimRecord],
) -> Result<(), RuntimeError> {
    let mut targets = LegacyNativeTargetBatch::default();
    for (original, normalized) in records {
        if *original != normalized {
            targets.worktree(original)?;
            targets.worktree(normalized)?;
        }
    }
    for claim in claims {
        let current: Option<(String, i64, Option<i64>)> = connection
            .query_row(
                "SELECT claim_role,created_at,released_at FROM managed_worktree_claims
             WHERE worktree_id=?1 AND session_id=?2",
                params![claim.worktree_id, claim.session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| db_error("failed comparing guarded historical claim", error))?;
        if current
            .as_ref()
            .is_some_and(|(role, created_at, released_at)| {
                role == &claim.claim_role
                    && *created_at == claim.created_at
                    && *released_at == claim.released_at
            })
        {
            continue;
        }
        targets.claim(connection, claim)?;
    }
    targets.guard(connection)
}
