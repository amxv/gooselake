//! Default fail-closed journal methods for non-SQLite RuntimeStore backends.
//! Native Git cannot be made atomic with a database transaction, so callers
//! must never silently fall back to a best-effort post-native event append.
macro_rules! default_worktree_record_journal {
    () => {
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
