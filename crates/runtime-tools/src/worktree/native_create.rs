use super::{PlannedWorktreePaths, RuntimeWorktreeService};
use crate::now_ms;
use runtime_core::{
    resolve_repository_identity, ManagedWorktreeRecord, RuntimeError, WorktreeCreateRequest,
    WorktreeCreateResponse,
};
use std::path::Path;

impl RuntimeWorktreeService {
    /// Recover the exact deterministic checkout left behind if native Git
    /// completed but a process died before the managed record committed.
    /// Never adopt a foreign repository, different branch, or symlink escape.
    pub(super) fn recover_native_checkout(
        &self,
        planned: &PlannedWorktreePaths,
        request: &WorktreeCreateRequest,
        event_source_session_id: Option<&str>,
    ) -> Result<Option<ManagedWorktreeRecord>, RuntimeError> {
        let path = Path::new(&planned.worktree_cwd);
        if !path.is_dir() {
            return Ok(None);
        }
        let managed_root = std::fs::canonicalize(&planned.worktree_root)
            .map_err(|error| RuntimeError::Io(format!("cannot resolve managed root: {error}")))?;
        let canonical = std::fs::canonicalize(path)
            .map_err(|error| RuntimeError::Io(format!("cannot resolve worktree path: {error}")))?;
        if canonical == managed_root || !canonical.starts_with(&managed_root) {
            return Ok(None);
        }
        let repo = resolve_repository_identity(Path::new(&planned.repo_root))?;
        let checkout = match resolve_repository_identity(path) {
            Ok(identity) => identity,
            Err(_) => return Ok(None),
        };
        if checkout.fingerprint != repo.fingerprint
            || checkout.git_common_dir != repo.git_common_dir
        {
            return Ok(None);
        }
        let (branch, _, _) = match Self::run_git_for_repo(
            &planned.worktree_cwd,
            &["symbolic-ref", "--quiet", "--short", "HEAD"],
            &[],
        ) {
            Ok(output) => output,
            Err(_) => return Ok(None),
        };
        if branch.trim() != planned.branch_name {
            return Ok(None);
        }
        self.upsert_worktree_record(
            self.allocate_worktree_id(),
            planned,
            self.normalize_deletion_policy(request.deletion_policy.as_deref()),
            request.created_by_session_id.clone(),
            request.operation_id.clone(),
            "recovered_existing_checkout",
            true,
            event_source_session_id,
            request.team_id.as_deref(),
        )
        .map(Some)
    }

    pub(super) async fn create_worktree_for_repository(
        &self,
        repo_root: String,
        request: WorktreeCreateRequest,
        event_source_session_id: Option<String>,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        // Both legacy and workspace-native creation perform Git and optional
        // init-script work before the managed row is persisted. Serialize the
        // whole native window with retirement, and check its durable fence
        // before creating anything. An INSERT trigger alone would be too late.
        let repo_root = resolve_repository_identity(Path::new(&repo_root))?.canonical_root;
        let lifecycle_lock = runtime_core::repository_lifecycle_lock(&repo_root).await;
        let _admission_guard = lifecycle_lock.read().await;
        self.ensure_repository_native_mutation_allowed(&repo_root)?;
        Self::validate_managed_worktree_name(&request.worktree_name)?;
        if let Some(prefix) = request.branch_prefix.as_deref() {
            for segment in prefix.split('/') {
                Self::validate_managed_worktree_name(segment)?;
            }
        }
        let planned = self.plan_worktree_paths(
            repo_root.as_str(),
            request.worktree_name.as_str(),
            request.branch_prefix.as_deref(),
        );

        let repo_lock = self.lock_for_repo(planned.repo_root.as_str()).await;
        let _repo_guard = repo_lock.lock().await;

        if let Some(operation_id) = self.store.native_worktree_cleanup_owner(
            &planned.repo_root,
            &planned.worktree_cwd,
            &planned.branch_name,
        )? {
            return Err(RuntimeError::operation_unresolved(operation_id.clone(),
                format!("native cleanup may already have removed this checkout/branch; inspect /v2/operations/{operation_id} before creating it again")));
        }

        if let Some(owner) = self
            .store
            .native_worktree_creation_claim_owner(&planned.worktree_cwd)?
        {
            if request.operation_id.as_deref() != Some(&owner) {
                return Err(RuntimeError::Conflict(format!(
                    "checkout is reserved by unresolved native worktree operation {owner}"
                )));
            }
        }
        if let Some(owner) = self
            .store
            .native_worktree_creation_branch_owner(&planned.repo_root, &planned.branch_name)?
        {
            if request.operation_id.as_deref() != Some(&owner) {
                return Err(RuntimeError::Conflict(format!(
                    "Git branch is reserved by unresolved native worktree operation {owner}"
                )));
            }
        }

        let existing_snapshot = self.store.managed_worktree_creation_snapshot(
            &planned.repo_root,
            &planned.worktree_cwd,
            &planned.branch_name,
        )?;
        if let Some((existing, active_claim_count)) = existing_snapshot {
            if let Some(requested) = request.deletion_policy.as_deref() {
                if self.normalize_deletion_policy(Some(requested)) != existing.deletion_policy {
                    return Err(RuntimeError::Conflict(
                        "existing managed worktree has a different retention policy".into(),
                    ));
                }
            }
            let live_artifacts = Self::has_live_artifacts_for_record(&existing);
            let stale_cleaned = active_claim_count == 0
                && !live_artifacts
                && existing.deletion_policy == "delete_on_last_claim";
            if !stale_cleaned {
                return Ok(WorktreeCreateResponse {
                    worktree: existing,
                    created: false,
                    init_script_status: "skipped_existing".to_string(),
                });
            }
        }

        let branch_ref = format!("refs/heads/{}", planned.branch_name);
        let (_, _, branch_exit_code) = Self::run_git_for_repo(
            planned.repo_root.as_str(),
            &["show-ref", "--verify", "--quiet", branch_ref.as_str()],
            &[1],
        )?;
        if branch_exit_code == 0 || Path::new(planned.worktree_cwd.as_str()).exists() {
            if let Some(worktree) = self.recover_native_checkout(
                &planned,
                &request,
                event_source_session_id.as_deref(),
            )? {
                return Ok(WorktreeCreateResponse {
                    worktree,
                    created: false,
                    init_script_status: "recovered_existing_checkout".into(),
                });
            }
            return Err(RuntimeError::InvalidState(format!(
                "worktree name '{}' already exists",
                planned.worktree_name
            )));
        }

        std::fs::create_dir_all(planned.worktree_root.as_str()).map_err(|error| {
            RuntimeError::Io(format!(
                "failed to create worktree root {}: {error}",
                planned.worktree_root
            ))
        })?;

        let mut git_args = vec![
            "worktree",
            "add",
            "-b",
            planned.branch_name.as_str(),
            planned.worktree_cwd.as_str(),
        ];
        let trimmed_base = request
            .base_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(base_ref) = trimmed_base {
            git_args.push(base_ref);
        }
        if let Err(error) =
            Self::run_git_for_repo(planned.repo_root.as_str(), git_args.as_slice(), &[])
        {
            if let Some(worktree) = self.recover_native_checkout(
                &planned,
                &request,
                event_source_session_id.as_deref(),
            )? {
                return Ok(WorktreeCreateResponse {
                    worktree,
                    created: false,
                    init_script_status: "recovered_existing_checkout".into(),
                });
            }
            return Err(error);
        }

        let init_script_status = if request.run_init_script.unwrap_or(true) {
            match self.run_worktree_init_script(planned.worktree_cwd.as_str()) {
                Ok(status) => status,
                Err(error) => {
                    let _ = self.store.append_team_operation_diagnostic(
                        request.operation_id.as_deref(),
                        request.team_id.as_deref(),
                        "worktree_init_failed",
                        error.to_string().as_str(),
                        &serde_json::json!({
                            "worktree_cwd": planned.worktree_cwd,
                            "branch_name": planned.branch_name
                        }),
                        now_ms(),
                    );
                    let _ = Self::run_git_for_repo(
                        planned.repo_root.as_str(),
                        &["worktree", "remove", planned.worktree_cwd.as_str()],
                        &[128, 255],
                    );
                    let _ = Self::run_git_for_repo(
                        planned.repo_root.as_str(),
                        &["branch", "-d", planned.branch_name.as_str()],
                        &[1],
                    );
                    return Err(error);
                }
            }
        } else {
            "skipped_disabled".to_string()
        };

        let worktree = self.upsert_worktree_record(
            self.allocate_worktree_id(),
            &planned,
            self.normalize_deletion_policy(request.deletion_policy.as_deref()),
            request.created_by_session_id,
            request.operation_id,
            &init_script_status,
            false,
            event_source_session_id.as_deref(),
            request.team_id.as_deref(),
        )?;
        Ok(WorktreeCreateResponse {
            worktree,
            created: true,
            init_script_status,
        })
    }
}
