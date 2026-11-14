macro_rules! delegate_worktree_diagnostic_reads {
    () => {
        fn managed_worktree_diagnostic_counts(
            &self,
        ) -> Result<Option<runtime_core::ManagedWorktreeDiagnosticCounts>, RuntimeError> {
            self.repository
                .managed_worktree_diagnostic_counts()
                .map(Some)
        }
    };
}

pub(crate) use delegate_worktree_diagnostic_reads;
