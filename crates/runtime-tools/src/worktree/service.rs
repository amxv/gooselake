use std::path::Path;

use async_trait::async_trait;
use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeRecord, RuntimeError, WorktreeClaimRequest,
    WorktreeClaimResponse, WorktreeCleanupRequest, WorktreeCleanupResponse, WorktreeCreateRequest,
    WorktreeCreateResponse, WorktreeReleaseRequest, WorktreeReleaseResponse, WorktreeService,
};

use crate::now_ms;

use super::RuntimeWorktreeService;

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
        let planned = self.plan_worktree_paths(
            repo_root.as_str(),
            request.worktree_name.as_str(),
            request.branch_prefix.as_deref(),
        );

        let repo_lock = self.lock_for_repo(planned.repo_root.as_str()).await;
        let _repo_guard = repo_lock.lock().await;

        let hydrated_before = self.store.hydrate_runtime_state()?;
        if let Some(existing) = self.worktree_by_identity(&hydrated_before, &planned) {
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
        Self::run_git_for_repo(planned.repo_root.as_str(), git_args.as_slice(), &[])?;

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
                        &[
                            "worktree",
                            "remove",
                            "--force",
                            planned.worktree_cwd.as_str(),
                        ],
                        &[128, 255],
                    );
                    let _ = Self::run_git_for_repo(
                        planned.repo_root.as_str(),
                        &["branch", "-D", planned.branch_name.as_str()],
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
            Some(source_session.id.clone()),
            request.team_id,
        )
        .await;
        Ok(WorktreeCreateResponse {
            worktree,
            created: true,
            init_script_status,
        })
    }

    async fn claim_worktree(
        &self,
        request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError> {
        self.ensure_enabled()?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;

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
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
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

        let mut diagnostics = Vec::new();
        let mut worktree_path_deleted = false;
        let mut branch_deleted = false;
        if Path::new(worktree.worktree_cwd.as_str()).exists() {
            match Self::run_git_for_repo(
                worktree.repo_root.as_str(),
                &[
                    "worktree",
                    "remove",
                    "--force",
                    worktree.worktree_cwd.as_str(),
                ],
                &[128, 255],
            ) {
                Ok(_) => {
                    worktree_path_deleted = !Path::new(worktree.worktree_cwd.as_str()).exists();
                }
                Err(error) => diagnostics.push(error.to_string()),
            }
        } else {
            worktree_path_deleted = true;
        }
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
                &["branch", "-D", worktree.branch_name.as_str()],
                &[1],
            ) {
                Ok(_) => branch_deleted = true,
                Err(error) => diagnostics.push(error.to_string()),
            },
            Err(error) => diagnostics.push(error.to_string()),
        }

        let status = if diagnostics.is_empty() {
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
