use std::path::Path;

use async_trait::async_trait;
use runtime_core::{
    resolve_repository_identity, ManagedWorktreeClaimRecord, ManagedWorktreeRecord, RuntimeError,
    WorkspaceLifecycleState, WorkspaceWorktreeCreateRequest, WorktreeClaimRequest,
    WorktreeClaimResponse, WorktreeCleanupRequest, WorktreeCleanupResponse, WorktreeCreateRequest,
    WorktreeCreateResponse, WorktreeReleaseRequest, WorktreeReleaseResponse, WorktreeService,
};

use crate::now_ms;

use super::{PlannedWorktreePaths, RuntimeWorktreeService};

#[async_trait]
impl WorktreeService for RuntimeWorktreeService {
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let mut rows = hydrated
            .managed_worktrees
            .into_iter()
            .filter(|row| !Self::is_record_tombstoned(row))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.created_at.cmp(&right.created_at));
        Ok(rows)
    }

    async fn get_worktree(&self, worktree_id: &str) -> Result<ManagedWorktreeRecord, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        self.get_worktree_from_hydrated(worktree_id, &hydrated)
    }

    async fn create_worktree(
        &self,
        request: WorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        self.ensure_enabled()?;
        let source_session = self
            .runtime
            .get_session(request.source_session_id.as_str())
            .await?;
        let source_cwd = source_session.cwd.clone().ok_or_else(|| {
            RuntimeError::InvalidState(
                "source session has no cwd for worktree planning".to_string(),
            )
        })?;
        let repo_root = match request.repo_root.as_deref() {
            Some(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => Self::resolve_repo_root_from_source_cwd(source_cwd.as_str())?,
        };
        self.create_worktree_for_repository(repo_root, request, Some(source_session.id))
            .await
    }

    async fn create_workspace_worktree(
        &self,
        request: WorkspaceWorktreeCreateRequest,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        self.ensure_enabled()?;
        let deletion_policy = match request.deletion_policy.as_deref() {
            None => "retain_on_last_claim",
            Some("retain_on_last_claim") => "retain_on_last_claim",
            Some("delete_on_last_claim") => "delete_on_last_claim",
            _ => {
                return Err(RuntimeError::InvalidState(
                    "worktree deletion_policy must be retain_on_last_claim or delete_on_last_claim"
                        .into(),
                ));
            }
        };
        let workspace = self
            .store
            .get_workspace(&request.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", request.workspace_id)))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active {
            return Err(RuntimeError::Conflict("workspace is not active".into()));
        }
        let repository = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        self.create_worktree_for_repository(
            repository.canonical_root,
            WorktreeCreateRequest {
                team_id: None,
                source_session_id: String::new(),
                repo_root: None,
                worktree_name: request.worktree_name,
                branch_prefix: request.branch_prefix,
                base_ref: request.base_ref,
                deletion_policy: Some(deletion_policy.to_string()),
                run_init_script: Some(request.run_init_script.unwrap_or(false)),
                created_by_session_id: None,
                operation_id: None,
            },
            None,
        )
        .await
    }

    async fn claim_worktree(
        &self,
        request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError> {
        self.ensure_enabled()?;
        if self
            .store
            .unresolved_workspace_agent_rebind(&request.session_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has an unresolved provider rebind; worktree claims cannot change".into(),
            ));
        }
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        let repository_lock = self.lock_for_repo(&worktree.repo_root).await;
        let _repository_guard = repository_lock.lock().await;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        if self
            .store
            .unresolved_workspace_agent_rebind(&request.session_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has an unresolved provider rebind; worktree claims cannot change".into(),
            ));
        }

        let conflicting_claim = hydrated.managed_worktree_claims.iter().find(|row| {
            row.session_id == request.session_id
                && row.released_at.is_none()
                && row.worktree_id != request.worktree_id
        });
        if let Some(conflict) = conflicting_claim {
            return Err(RuntimeError::InvalidState(format!(
                "session {} already has an active claim on worktree {}",
                request.session_id, conflict.worktree_id
            )));
        }

        let claim = ManagedWorktreeClaimRecord {
            worktree_id: request.worktree_id.clone(),
            session_id: request.session_id.clone(),
            claim_role: request.claim_role.trim().to_ascii_lowercase(),
            created_at: now_ms(),
            released_at: None,
        };
        self.store.upsert_managed_worktree_claim(&claim)?;
        self.append_worktree_event(
            worktree.id.as_str(),
            "worktree.claimed",
            serde_json::json!({ "claim": claim }),
            Some(request.session_id),
            None,
        )
        .await;
        Ok(WorktreeClaimResponse { worktree, claim })
    }

    async fn release_worktree(
        &self,
        request: WorktreeReleaseRequest,
    ) -> Result<WorktreeReleaseResponse, RuntimeError> {
        self.ensure_enabled()?;
        if self
            .store
            .unresolved_workspace_agent_rebind(&request.session_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has an unresolved provider rebind; worktree claims cannot change".into(),
            ));
        }
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        let repository_lock = self.lock_for_repo(&worktree.repo_root).await;
        let repository_guard = repository_lock.lock().await;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        if self
            .store
            .unresolved_workspace_agent_rebind(&request.session_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has an unresolved provider rebind; worktree claims cannot change".into(),
            ));
        }
        if hydrated.sessions.iter().any(|session| {
            session.id == request.session_id
                && session.status != "closed"
                && (session.worktree_id.as_deref() == Some(worktree.id.as_str())
                    || session.cwd.as_deref() == Some(worktree.worktree_cwd.as_str()))
        }) {
            return Err(RuntimeError::Conflict(
                "cannot release a worktree still bound to a live provider session; rebind or close the agent first".into(),
            ));
        }
        let existing_claim = hydrated
            .managed_worktree_claims
            .iter()
            .find(|row| {
                row.worktree_id == request.worktree_id && row.session_id == request.session_id
            })
            .cloned()
            .ok_or_else(|| {
                RuntimeError::NotFound(format!(
                    "worktree claim {}:{}",
                    request.worktree_id, request.session_id
                ))
            })?;
        if existing_claim.claim_role == "rebind_reservation" && existing_claim.released_at.is_none()
        {
            return Err(RuntimeError::Conflict(
                "destination worktree is reserved for a provider rebind; inspect the operation before releasing it"
                    .into(),
            ));
        }
        let released_claim = ManagedWorktreeClaimRecord {
            released_at: Some(now_ms()),
            ..existing_claim
        };
        self.store.upsert_managed_worktree_claim(&released_claim)?;
        self.append_worktree_event(
            worktree.id.as_str(),
            "worktree.released",
            serde_json::json!({ "claim": released_claim }),
            Some(request.session_id),
            None,
        )
        .await;

        drop(repository_guard);
        let hydrated_after = self.store.hydrate_runtime_state()?;
        let active_claim_count = self
            .active_claims_for(&hydrated_after, worktree.id.as_str())
            .len();
        let cleanup = if request.cleanup_if_last_claim.unwrap_or(true) && active_claim_count == 0 {
            Some(
                self.cleanup_worktree(WorktreeCleanupRequest {
                    worktree_id: worktree.id.clone(),
                    reason: Some("release_last_claim".to_string()),
                })
                .await?,
            )
        } else {
            None
        };

        Ok(WorktreeReleaseResponse {
            worktree,
            released_claim,
            active_claim_count,
            cleanup,
        })
    }

    async fn cleanup_worktree(
        &self,
        request: WorktreeCleanupRequest,
    ) -> Result<WorktreeCleanupResponse, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        let active_claim_count = self
            .active_claims_for(&hydrated, worktree.id.as_str())
            .len();
        if active_claim_count > 0 {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: "skipped_live_claims".to_string(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        if Self::has_live_session_binding(&hydrated, &worktree) {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: "skipped_live_binding".into(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        if worktree.deletion_policy != "delete_on_last_claim" {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: "retained_by_policy".to_string(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        let repo_lock = self.lock_for_repo(worktree.repo_root.as_str()).await;
        let _repo_guard = repo_lock.lock().await;
        // A provider rebind reserves its destination in durable SQLite
        // before invoking the provider. Recheck claims immediately before
        // native deletion, not just before waiting for the repository lock.
        let hydrated_current = self.store.hydrate_runtime_state()?;
        let active_now = self
            .active_claims_for(&hydrated_current, worktree.id.as_str())
            .len();
        if active_now > 0 {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: "skipped_live_claims".to_string(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count: active_now,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        if Self::has_live_session_binding(&hydrated_current, &worktree) {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: "skipped_live_binding".into(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count: active_now,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        // Cleanup is allowed only for positively identified managed checkouts
        // with no local edits or unmerged branch history. A retained/external
        // or changed checkout must never be force-deleted by a stale claim.
        if let Some(blocker) = self.native_cleanup_blocker(&worktree) {
            return Ok(WorktreeCleanupResponse {
                worktree_id: worktree.id,
                status: blocker.to_string(),
                deletion_policy: worktree.deletion_policy,
                active_claim_count: active_now,
                worktree_path_deleted: false,
                branch_deleted: false,
                diagnostics: Vec::new(),
            });
        }

        let mut diagnostics = Vec::new();
        let mut worktree_path_deleted = false;
        let mut branch_deleted = false;
        if Path::new(worktree.worktree_cwd.as_str()).exists() {
            match Self::run_git_for_repo(
                worktree.repo_root.as_str(),
                &["worktree", "remove", worktree.worktree_cwd.as_str()],
                &[],
            ) {
                Ok(_) => {
                    worktree_path_deleted = !Path::new(worktree.worktree_cwd.as_str()).exists();
                }
                Err(error) => diagnostics.push(error.to_string()),
            }
        } else {
            worktree_path_deleted = true;
        }
        if worktree_path_deleted {
            match Self::run_git_for_repo(
                worktree.repo_root.as_str(),
                &[
                    "show-ref",
                    "--verify",
                    "--quiet",
                    &format!("refs/heads/{}", worktree.branch_name),
                ],
                &[1],
            ) {
                Ok((_, _, exit_code)) if exit_code == 1 => {
                    branch_deleted = true;
                }
                Ok(_) => match Self::run_git_for_repo(
                    worktree.repo_root.as_str(),
                    &["branch", "-d", worktree.branch_name.as_str()],
                    &[],
                ) {
                    Ok((_, _, 0)) => branch_deleted = true,
                    Ok(_) => diagnostics.push("branch deletion did not complete".into()),
                    Err(error) => diagnostics.push(error.to_string()),
                },
                Err(error) => diagnostics.push(error.to_string()),
            }
        } else {
            diagnostics.push("native worktree removal did not complete; branch retained".into());
        }

        let status = if diagnostics.is_empty() && worktree_path_deleted && branch_deleted {
            "deleted".to_string()
        } else {
            "cleanup_failed".to_string()
        };
        if diagnostics.is_empty() {
            self.append_worktree_event(
                worktree.id.as_str(),
                "worktree.cleaned_up",
                serde_json::json!({
                    "worktree_id": worktree.id,
                    "reason": request.reason,
                    "worktree_path_deleted": worktree_path_deleted,
                    "branch_deleted": branch_deleted,
                }),
                None,
                None,
            )
            .await;
        } else {
            let _ = self.store.append_team_operation_diagnostic(
                None,
                None,
                "worktree_cleanup_failed",
                "managed worktree cleanup failed",
                &serde_json::json!({
                    "worktree_id": worktree.id,
                    "diagnostics": diagnostics,
                }),
                now_ms(),
            );
            self.append_worktree_event(
                worktree.id.as_str(),
                "worktree.cleanup_failed",
                serde_json::json!({
                    "worktree_id": worktree.id,
                    "reason": request.reason,
                    "diagnostics": diagnostics,
                }),
                None,
                None,
            )
            .await;
        }

        Ok(WorktreeCleanupResponse {
            worktree_id: worktree.id,
            status,
            deletion_policy: worktree.deletion_policy,
            active_claim_count,
            worktree_path_deleted,
            branch_deleted,
            diagnostics,
        })
    }

    async fn spawn_team_member(
        &self,
        request: runtime_core::TeamMemberSpawnRequest,
    ) -> Result<runtime_core::TeamMemberSpawnResponse, RuntimeError> {
        self.spawn_team_member_impl(request).await
    }

    async fn on_member_removed(
        &self,
        request: runtime_core::WorktreeMemberRemovedRequest,
    ) -> Result<runtime_core::WorktreeMemberRemovedResponse, RuntimeError> {
        self.on_member_removed_impl(request).await
    }
}

impl RuntimeWorktreeService {
    fn has_live_session_binding(
        state: &runtime_core::RuntimeHydratedState,
        worktree: &ManagedWorktreeRecord,
    ) -> bool {
        state.sessions.iter().any(|session| {
            session.status != "closed"
                && (session.worktree_id.as_deref() == Some(worktree.id.as_str())
                    || session.cwd.as_deref() == Some(worktree.worktree_cwd.as_str()))
        })
    }

    fn native_cleanup_blocker(&self, worktree: &ManagedWorktreeRecord) -> Option<&'static str> {
        let worktree_path = Path::new(&worktree.worktree_cwd);
        let trusted_root = match std::fs::canonicalize(&self.config.root_dir) {
            Ok(root) => root,
            Err(_) => return Some("skipped_unverified_native"),
        };
        let parent = match worktree_path
            .parent()
            .and_then(|path| path.canonicalize().ok())
        {
            Some(parent) => parent,
            None => return Some("skipped_unverified_native"),
        };
        if !parent.starts_with(&trusted_root) {
            return Some("skipped_external_worktree");
        }
        let repo = match resolve_repository_identity(Path::new(&worktree.repo_root)) {
            Ok(identity) => identity,
            Err(_) => return Some("skipped_unverified_native"),
        };
        if worktree_path.exists() {
            let checkout = match resolve_repository_identity(worktree_path) {
                Ok(identity) => identity,
                Err(_) => return Some("skipped_unverified_native"),
            };
            if checkout.fingerprint != repo.fingerprint
                || checkout.git_common_dir != repo.git_common_dir
                || std::fs::canonicalize(worktree_path)
                    .ok()
                    .is_none_or(|path| !path.starts_with(&trusted_root))
            {
                return Some("skipped_external_worktree");
            }
            let Ok((branch, _, _)) = Self::run_git_for_repo(
                &worktree.worktree_cwd,
                &["symbolic-ref", "--quiet", "--short", "HEAD"],
                &[],
            ) else {
                return Some("skipped_unverified_native");
            };
            if branch.trim() != worktree.branch_name {
                return Some("skipped_unverified_native");
            }
            let Ok((changes, _, _)) = Self::run_git_for_repo(
                &worktree.worktree_cwd,
                &[
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    "--ignored",
                ],
                &[],
            ) else {
                return Some("skipped_unverified_native");
            };
            if !changes.trim().is_empty() {
                return Some("skipped_dirty_worktree");
            }
        }
        let branch_ref = format!("refs/heads/{}", worktree.branch_name);
        let Ok((_, _, exists)) = Self::run_git_for_repo(
            &worktree.repo_root,
            &["show-ref", "--verify", "--quiet", &branch_ref],
            &[1],
        ) else {
            return Some("skipped_unverified_native");
        };
        if exists == 0 {
            let Ok((_, _, ancestor)) = Self::run_git_for_repo(
                &worktree.repo_root,
                &["merge-base", "--is-ancestor", &branch_ref, "HEAD"],
                &[1],
            ) else {
                return Some("skipped_unverified_native");
            };
            if ancestor != 0 {
                return Some("skipped_unmerged_branch");
            }
        }
        None
    }

    /// Recover the exact deterministic checkout left behind if native Git
    /// completed but a process died before the managed record committed.
    /// Never adopt a foreign repository, different branch, or symlink escape.
    fn recover_native_checkout(
        &self,
        planned: &PlannedWorktreePaths,
        request: &WorktreeCreateRequest,
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
        )
        .map(Some)
    }

    async fn create_worktree_for_repository(
        &self,
        repo_root: String,
        request: WorktreeCreateRequest,
        event_source_session_id: Option<String>,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
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

        let hydrated_before = self.store.hydrate_runtime_state()?;
        if let Some(existing) = self.worktree_by_identity(&hydrated_before, &planned) {
            if let Some(requested) = request.deletion_policy.as_deref() {
                if self.normalize_deletion_policy(Some(requested)) != existing.deletion_policy {
                    return Err(RuntimeError::Conflict(
                        "existing managed worktree has a different retention policy".into(),
                    ));
                }
            }
            let active_claim_count = self
                .active_claims_for(&hydrated_before, existing.id.as_str())
                .len();
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
            if let Some(worktree) = self.recover_native_checkout(&planned, &request)? {
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
            if let Some(worktree) = self.recover_native_checkout(&planned, &request)? {
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
        )?;
        self.append_worktree_event(
            worktree.id.as_str(),
            "worktree.created",
            serde_json::json!({
                "worktree": worktree,
                "init_script_status": init_script_status,
            }),
            event_source_session_id,
            request.team_id,
        )
        .await;
        Ok(WorktreeCreateResponse {
            worktree,
            created: true,
            init_script_status,
        })
    }
}
