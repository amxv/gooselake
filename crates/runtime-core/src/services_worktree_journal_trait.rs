//! Default fail-closed journal methods for non-SQLite RuntimeStore backends.
//! Native Git cannot be made atomic with a database transaction, so callers
//! must never silently fall back to a best-effort post-native event append.
macro_rules! default_worktree_record_journal {
    () => {
        /// Historical startup normalization must compare the hydrated source
        /// to the current durable row inside the same writer transaction.
        /// Non-SQLite stores cannot silently substitute a blind UPSERT.
        fn repair_managed_worktree_if_unchanged(
            &self,
            _original: &crate::ManagedWorktreeRecord,
            _normalized: &crate::ManagedWorktreeRecord,
        ) -> Result<(), crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic managed-worktree startup snapshot repair unavailable".into(),
            ))
        }

        /// A historical duplicate-identity merge may change a winner and
        /// tombstone several losers. No row may commit if any original source
        /// is stale. SQLite applies this batch in one writer transaction.
        fn repair_managed_worktrees_if_unchanged(
            &self,
            _repairs: &[(&crate::ManagedWorktreeRecord, crate::ManagedWorktreeRecord)],
        ) -> Result<(), crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic managed-worktree historical repair batch unavailable".into(),
            ))
        }

        /// Historical worktree records and claims derive from one hydration.
        /// Never commit a changed record plan before a later changed claim
        /// plan can fail its raw-source or native-rebind authority checks.
        fn repair_historical_worktree_startup_atomically(
            &self,
            _record_repairs: &[(&crate::ManagedWorktreeRecord, crate::ManagedWorktreeRecord)],
            _claim_originals: &[crate::ManagedWorktreeClaimRecord],
            _claim_repairs: &[crate::ManagedWorktreeClaimRecord],
        ) -> Result<(), crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic historical worktree record+claim startup repair unavailable".into(),
            ))
        }

        /// Commit native Git worktree record and critical lifecycle evidence
        /// in one durable store transaction after independently verified Git.
        fn record_managed_worktree_event(
            &self,
            _record: &crate::ManagedWorktreeRecord,
            _init_script_status: &str,
            _recovered: bool,
            _session_id: Option<&str>,
            _team_id: Option<&str>,
        ) -> Result<crate::ManagedWorktreeRecord, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic managed-worktree creation journaling unavailable".into(),
            ))
        }
    };
}

pub(crate) use default_worktree_record_journal;

macro_rules! default_team_spawn_admission {
    () => {
        /// One bounded snapshot of legacy spawn history. Read-only preflight
        /// never substitutes for a transaction-owned new-effect admission.
        fn classify_legacy_spawn_obligations(
            &self,
            _targets: &[crate::legacy_spawn_obligations::LegacyNativeTarget],
            _budget: crate::legacy_spawn_obligations::LegacySpawnScanBudget,
        ) -> Result<crate::legacy_spawn_obligations::LegacySpawnScan, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "bounded legacy native obligation classification unavailable".into(),
            ))
        }

        /// Tolerant bounded historical inspection must not replace strict
        /// authority reads or grant permission to repeat native work.
        fn inspect_legacy_team_operation(
            &self,
            _operation_id: &str,
            _after_diagnostic_id: Option<i64>,
            _limit: usize,
        ) -> Result<
            Option<crate::legacy_operation_inspection::LegacyTeamOperationInspection>,
            crate::RuntimeError,
        > {
            Err(crate::RuntimeError::Unsupported(
                "bounded historical team operation inspection unavailable".into(),
            ))
        }

        /// A coordinate is inspection evidence only, never success or native
        /// execution authority when an old preset expansion cannot be proved.
        fn find_team_member_spawn_operation(
            &self,
            _team_id: &str,
            _source_session_id: &str,
            _idempotency_key: &str,
        ) -> Result<Option<String>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable caller-bound team spawn coordinate lookup unavailable".into(),
            ))
        }

        /// Replayed callers must find the exact original durable record before
        /// inspecting mutable roster/session state, otherwise fail closed.
        fn replay_team_member_spawn(
            &self,
            _team_id: &str,
            _source_session_id: &str,
            _idempotency_key: &str,
            _request_hash: &str,
        ) -> Result<Option<crate::TeamMemberSpawnAdmission>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable caller-bound team spawn replay unavailable".into(),
            ))
        }

        /// Reserve a keyed spawn and planned recovery journal atomically.
        fn admit_team_member_spawn(
            &self,
            _operation_id: &str,
            _request: &crate::TeamMemberSpawnRequest,
            _request_hash: &str,
            _requested_at: i64,
        ) -> Result<crate::TeamMemberSpawnAdmission, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable caller-bound team spawn admission unavailable".into(),
            ))
        }

        /// Terminal response must share a transaction with completed stage.
        fn complete_team_member_spawn(
            &self,
            _operation_id: &str,
            _response: &crate::TeamMemberSpawnResponse,
            _completed_at: i64,
        ) -> Result<(), crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable team spawn completion receipt unavailable".into(),
            ))
        }
    };
}

pub(crate) use default_team_spawn_admission;

macro_rules! default_worktree_claim_cleanup_journal {
    () => {
        /// An unjournaled historical startup repair may normalize multiple
        /// claims. The batch must commit all or none under one writer lock;
        /// stores without this guarantee must not fall back to serial upserts.
        fn repair_managed_worktree_claims_atomically(
            &self,
            _repairs: &[crate::ManagedWorktreeClaimRecord],
        ) -> Result<(), crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic historical managed-worktree claim repair unavailable".into(),
            ))
        }

        /// Unlike legacy unscoped upserts, startup repairs compare every
        /// hydrated raw claim source and require absent new target keys
        /// inside the writer transaction. Never silently fall back to UPSERT.
        fn repair_managed_worktree_claims_if_unchanged_atomically(
            &self,
            _originals: &[crate::ManagedWorktreeClaimRecord],
            _repairs: &[crate::ManagedWorktreeClaimRecord],
        ) -> Result<(), crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "historical managed-worktree claim source snapshot repair unavailable".into(),
            ))
        }

        /// A claim/release and its critical worktree event are one authority.
        fn transition_managed_worktree_claim(
            &self,
            _record: &crate::ManagedWorktreeClaimRecord,
            _transition: crate::ManagedWorktreeClaimTransition,
        ) -> Result<crate::ManagedWorktreeClaimRecord, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic managed-worktree claim journaling unavailable".into(),
            ))
        }

        /// Explicit release must recheck native session binding inside the
        /// same writer transaction as the claim release and Critical event.
        /// Unrelated generic releases (such as roster member removal) retain
        /// their own semantics; stores lacking this guard must fail closed.
        fn release_managed_worktree_claim_if_unbound(
            &self,
            _record: &crate::ManagedWorktreeClaimRecord,
            _observed: &crate::ManagedWorktreeClaimRecord,
            _expected_worktree_cwd: &str,
        ) -> Result<crate::ManagedWorktreeClaimRecord, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "atomic explicit worktree claim release binding fence unavailable".into(),
            ))
        }

        fn replay_managed_worktree_cleanup(
            &self,
            _worktree: &crate::ManagedWorktreeRecord,
        ) -> Result<Option<crate::ManagedWorktreeCleanupAdmission>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "managed-worktree native cleanup receipt lookup unavailable".into(),
            ))
        }

        fn admit_managed_worktree_cleanup(
            &self,
            _command: &crate::ManagedWorktreeCleanupCommand,
        ) -> Result<crate::ManagedWorktreeCleanupAdmission, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "managed-worktree native cleanup admission unavailable".into(),
            ))
        }

        fn complete_managed_worktree_cleanup(
            &self,
            _operation_id: &str,
            _response: &crate::WorktreeCleanupResponse,
            _completed_at: i64,
        ) -> Result<(), crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "managed-worktree native cleanup settlement unavailable".into(),
            ))
        }

        fn native_worktree_cleanup_owner(
            &self,
            _repo_root: &str,
            _checkout_path: &str,
            _branch_name: &str,
        ) -> Result<Option<String>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "managed-worktree native cleanup reservation lookup unavailable".into(),
            ))
        }
    };
}

pub(crate) use default_worktree_claim_cleanup_journal;
