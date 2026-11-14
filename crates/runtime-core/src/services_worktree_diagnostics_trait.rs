// Optional exact aggregate for historical worktrees and live/orphan claims.
// Unsupported stores preserve the existing hydrated diagnostics contract.
macro_rules! default_worktree_diagnostic_reads {
    () => {
        fn managed_worktree_diagnostic_counts(
            &self,
        ) -> Result<Option<crate::ManagedWorktreeDiagnosticCounts>, crate::RuntimeError> {
            Ok(None)
        }
    };
}

pub(super) use default_worktree_diagnostic_reads;
