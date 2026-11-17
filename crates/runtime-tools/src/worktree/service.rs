use std::path::Path;

use async_trait::async_trait;
use runtime_core::{
    resolve_repository_identity, ManagedWorktreeClaimRecord, ManagedWorktreeClaimTransition,
    ManagedWorktreeCleanupAdmission, ManagedWorktreeCleanupCommand, ManagedWorktreeRecord,
    OperationActor, RuntimeError, WorkspaceLifecycleState, WorkspaceWorktreeCreateRequest,
    WorkspaceWorktreeCreationResult, WorktreeClaimRequest, WorktreeClaimResponse,
    WorktreeCleanupRequest, WorktreeCleanupResponse, WorktreeCreateRequest, WorktreeCreateResponse,
    WorktreeReleaseRequest, WorktreeReleaseResponse, WorktreeService,
};

use crate::now_ms;

use super::RuntimeWorktreeService;

#[async_trait]
impl WorktreeService for RuntimeWorktreeService {
    fn plan_member_agent_create_route(
        &self,
        workspace_id: &str,
        worktree_name: &str,
    ) -> Result<runtime_core::MemberAgentCreateRoute, RuntimeError> {
        self.plan_member_create_route(workspace_id, worktree_name)
    }

    async fn execute_member_agent_create_worktree(
        &self,
        command: &runtime_core::MemberAgentCreateCommand,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        self.execute_member_create_worktree(command).await
    }

    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }

    async fn list_worktrees(&self) -> Result<Vec<ManagedWorktreeRecord>, RuntimeError> {
        self.ensure_enabled()?;
        let mut rows = self
            .store
            .list_managed_worktrees()?
            .into_iter()
            .filter(|row| !Self::is_record_tombstoned(row))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.created_at.cmp(&right.created_at));
        Ok(rows)
    }

    async fn get_worktree(&self, worktree_id: &str) -> Result<ManagedWorktreeRecord, RuntimeError> {
        self.ensure_enabled()?;
        // The store already has a primary-key point lookup. A single worktree
        // read must not hydrate every unrelated session, message, event or
        // process, nor fail because one of those projections is unreadable.
        self.store
            .get_managed_worktree(worktree_id)?
            .filter(|record| !Self::is_record_tombstoned(record))
            .ok_or_else(|| RuntimeError::NotFound(format!("worktree {worktree_id}")))
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

    async fn create_workspace_worktree_idempotent(
        &self,
        request: WorkspaceWorktreeCreateRequest,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<WorkspaceWorktreeCreationResult, RuntimeError> {
        self.create_workspace_worktree_with_receipt(request, actor, idempotency_key)
            .await
    }

    async fn claim_worktree(
        &self,
        request: WorktreeClaimRequest,
    ) -> Result<WorktreeClaimResponse, RuntimeError> {
        self.ensure_enabled()?;
        let claim_role = Self::validate_public_claim_role(&request.claim_role)?;
        if self
            .store
            .unresolved_workspace_agent_rebind(&request.session_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has an unresolved provider rebind; worktree claims cannot change".into(),
            ));
        }
        let worktree = self.get_worktree(request.worktree_id.as_str()).await?;
        if let Some(operation_id) = self.store.native_worktree_cleanup_owner(
            &worktree.repo_root,
            &worktree.worktree_cwd,
            &worktree.branch_name,
        )? {
            return Err(RuntimeError::operation_unresolved(
                operation_id.clone(),
                format!(
                    "native worktree cleanup is unresolved; inspect /v2/operations/{operation_id}"
                ),
            ));
        }
        if let Some(ManagedWorktreeCleanupAdmission::Replay { operation_id, .. }) =
            self.store.replay_managed_worktree_cleanup(&worktree)?
        {
            return Err(RuntimeError::Conflict(format!(
                "worktree was already cleaned up by operation {operation_id}; create a new checkout before claiming it"
            )));
        }
        let repository_lock = self.lock_for_repo(&worktree.repo_root).await;
        let _repository_guard = repository_lock.lock().await;
        let worktree = self.get_worktree(request.worktree_id.as_str()).await?;
        if let Some(operation_id) = self.store.native_worktree_cleanup_owner(
            &worktree.repo_root,
            &worktree.worktree_cwd,
            &worktree.branch_name,
        )? {
            return Err(RuntimeError::operation_unresolved(
                operation_id.clone(),
                format!(
                    "native worktree cleanup is unresolved; inspect /v2/operations/{operation_id}"
                ),
            ));
        }
        if let Some(ManagedWorktreeCleanupAdmission::Replay { operation_id, .. }) =
            self.store.replay_managed_worktree_cleanup(&worktree)?
        {
            return Err(RuntimeError::Conflict(format!(
                "worktree was already cleaned up by operation {operation_id}; create a new checkout before claiming it"
            )));
        }
        if self
            .store
            .unresolved_workspace_agent_rebind(&request.session_id)?
        {
            return Err(RuntimeError::Conflict(
                "agent has an unresolved provider rebind; worktree claims cannot change".into(),
            ));
        }

        // SQLite serializes claim exclusion and its Critical event; this
        // preflight preserves original conflict order without global hydration.
        let conflicting_claim = self.store.first_active_foreign_worktree_claim_for_session(
            request.session_id.as_str(),
            request.worktree_id.as_str(),
        )?;
        if let Some(conflict) = conflicting_claim {
            return Err(RuntimeError::InvalidState(format!(
                "session {} already has an active claim on worktree {}",
                request.session_id, conflict
            )));
        }

        let claim = ManagedWorktreeClaimRecord {
            worktree_id: request.worktree_id.clone(),
            session_id: request.session_id.clone(),
            claim_role,
            created_at: now_ms(),
            released_at: None,
        };
        let claim = self
            .store
            .transition_managed_worktree_claim(&claim, ManagedWorktreeClaimTransition::Claim)?;
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
        self.get_worktree(request.worktree_id.as_str()).await?;
        self.store
            .get_managed_worktree_claim(&request.worktree_id, &request.session_id)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!(
                    "worktree claim {}:{}",
                    request.worktree_id, request.session_id
                ))
            })?;
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
            session.id == request.session_id && Self::session_binds_worktree(session, &worktree)
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
            ..existing_claim.clone()
        };
        let released_claim = self.store.release_managed_worktree_claim_if_unbound(
            &released_claim,
            &existing_claim,
            &worktree.worktree_cwd,
        )?;

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
        // Reject only missing/tombstoned IDs without decoding unrelated
        // sessions; existing IDs still require the *original* full safety
        // snapshot before taking any native Git cleanup locks or decisions.
        self.get_worktree(request.worktree_id.as_str()).await?;
        let hydrated = self.store.hydrate_runtime_state()?;
        let worktree = self.get_worktree_from_hydrated(request.worktree_id.as_str(), &hydrated)?;
        let lifecycle_lock = runtime_core::repository_lifecycle_lock(&worktree.repo_root).await;
        let _lifecycle_guard = lifecycle_lock.read().await;
        // All native cleanup and its durable SQLite intent serialize with
        // normal checkout creation, including replay after process restart.
        let repo_lock = self.lock_for_repo(&worktree.repo_root).await;
        let _repo_guard = repo_lock.lock().await;
        if let Some(receipt) = self.store.replay_managed_worktree_cleanup(&worktree)? {
            return self.replay_or_reconcile_managed_cleanup(&worktree, receipt);
        }
        self.ensure_repository_native_mutation_allowed(&worktree.repo_root)?;
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

        // An admitted creation may have completed Git and persisted its row
        // without yet committing the exact terminal receipt. Deletion must
        // not race this unknown native effect, even on legacy cleanup routes.
        if let Some(operation_id) = self
            .store
            .native_worktree_creation_claim_owner(&worktree.worktree_cwd)?
        {
            return Err(RuntimeError::Conflict(format!(
                "cannot delete checkout while native worktree operation {operation_id} is unresolved"
            )));
        }
        if let Some(operation_id) = self
            .store
            .native_worktree_creation_branch_owner(&worktree.repo_root, &worktree.branch_name)?
        {
            return Err(RuntimeError::Conflict(format!(
                "cannot delete a branch reserved by unresolved native worktree operation {operation_id}"
            )));
        }
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

        // This committed native intent plus exclusive path/branch claims is
        // the last step before Git. Losing a response can never permit a
        // second automatic remove or branch delete for the same generation.
        let admitted =
            self.store
                .admit_managed_worktree_cleanup(&ManagedWorktreeCleanupCommand {
                    operation_id: format!("op_wt_cleanup_{:032x}", rand::random::<u128>()),
                    worktree: worktree.clone(),
                    reason: request.reason.clone(),
                    requested_at: now_ms(),
                })?;
        let operation_id = match admitted {
            ManagedWorktreeCleanupAdmission::Replay { response, .. } => return Ok(response),
            pending @ ManagedWorktreeCleanupAdmission::Unresolved { .. } => {
                return self.replay_or_reconcile_managed_cleanup(&worktree, pending);
            }
            ManagedWorktreeCleanupAdmission::Execute { operation_id } => operation_id,
        };

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
        if !diagnostics.is_empty() {
            let _ = self.store.append_team_operation_diagnostic(
                Some(&operation_id),
                None,
                "worktree_cleanup_failed",
                "managed worktree cleanup failed",
                &serde_json::json!({
                    "worktree_id": worktree.id,
                    "diagnostics": diagnostics,
                }),
                now_ms(),
            );
        }
        let response = WorktreeCleanupResponse {
            worktree_id: worktree.id,
            status,
            deletion_policy: worktree.deletion_policy,
            active_claim_count,
            worktree_path_deleted,
            branch_deleted,
            diagnostics,
        };
        self.store.complete_managed_worktree_cleanup(&operation_id, &response, now_ms())
            .map_err(|_| RuntimeError::operation_unresolved(operation_id.clone(),
                format!("native Git result may be committed, but journal settlement is unknown; inspect /v2/operations/{operation_id} before retrying")))?;
        Ok(response)
    }

    async fn spawn_team_member(
        &self,
        request: runtime_core::TeamMemberSpawnRequest,
    ) -> Result<runtime_core::TeamMemberSpawnResponse, RuntimeError> {
        self.spawn_team_member_guarded(request).await
    }

    async fn on_member_removed(
        &self,
        request: runtime_core::WorktreeMemberRemovedRequest,
    ) -> Result<runtime_core::WorktreeMemberRemovedResponse, RuntimeError> {
        self.on_member_removed_impl(request).await
    }
}

impl RuntimeWorktreeService {
    /// An unresolved native cleanup is never dispatched a second time. The
    /// only automatic settlement allowed after a lost response/restart is an
    /// evidence-only read proving BOTH the checkout and exact Git branch are
    /// absent. Partial/dirty/foreign/unknown outcomes stay fenced for review.
    fn replay_or_reconcile_managed_cleanup(
        &self,
        worktree: &ManagedWorktreeRecord,
        admission: ManagedWorktreeCleanupAdmission,
    ) -> Result<WorktreeCleanupResponse, RuntimeError> {
        let operation_id = match admission {
            ManagedWorktreeCleanupAdmission::Replay { response, .. } => return Ok(response),
            ManagedWorktreeCleanupAdmission::Unresolved { operation_id } => operation_id,
            ManagedWorktreeCleanupAdmission::Execute { operation_id } => {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "native cleanup {operation_id} cannot be dispatched by a replay lookup"
                )));
            }
        };
        let absent_checkout = std::fs::symlink_metadata(&worktree.worktree_cwd)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound);
        let absent_branch = matches!(
            Self::run_git_for_repo(
                &worktree.repo_root,
                &[
                    "show-ref",
                    "--verify",
                    "--quiet",
                    &format!("refs/heads/{}", worktree.branch_name)
                ],
                &[1],
            ),
            Ok((_, _, 1))
        );
        if absent_checkout && absent_branch && self.native_cleanup_blocker(worktree).is_none() {
            let response = WorktreeCleanupResponse {
                worktree_id: worktree.id.clone(),
                status: "deleted".into(),
                deletion_policy: worktree.deletion_policy.clone(),
                active_claim_count: 0,
                worktree_path_deleted: true,
                branch_deleted: true,
                diagnostics: Vec::new(),
            };
            self.store.complete_managed_worktree_cleanup(&operation_id, &response, now_ms())
                .map_err(|_| RuntimeError::operation_unresolved(operation_id.clone(),
                    format!("verified absent native effects, but durable receipt settlement remains unknown; inspect /v2/operations/{operation_id}")))?;
            return Ok(response);
        }
        Err(RuntimeError::operation_unresolved(operation_id.clone(),
            format!("native cleanup may have executed; inspect /v2/operations/{operation_id}; do not retry Git without independent effect evidence")))
    }

    // Call while holding the repository lifecycle read lock through the
    // corresponding native Git action and its durable record update.
    pub(super) fn ensure_repository_native_mutation_allowed(
        &self,
        repo_root: &str,
    ) -> Result<(), RuntimeError> {
        let workspaces = match self.store.list_workspaces() {
            Ok(workspaces) => workspaces,
            Err(RuntimeError::Unsupported(_)) => Vec::new(),
            Err(error) => return Err(error),
        };
        if workspaces.is_empty() {
            return Ok(());
        }
        let checkout_roots = runtime_core::repository_checkout_roots(Path::new(repo_root))?;
        if checkout_roots.is_empty() {
            return Err(RuntimeError::InvalidState(
                "native worktree mutation cannot prove a checkout identity for this bare repository".into(),
            ));
        }
        for workspace in &workspaces {
            // A workspace registered at a Git subdirectory or another linked
            // checkout still owns native obligations in this *same* primary
            // repository. Checking only an equal filesystem path can let an
            // old /v1 worktree creation bypass its retirement freeze.
            let same_repository = checkout_roots.iter().any(|checkout| {
                runtime_core::checkout_contains_workspace(
                    checkout,
                    Path::new(&workspace.canonical_root),
                )
            });
            if !same_repository {
                continue;
            }
            if workspace.lifecycle_state != WorkspaceLifecycleState::Active
                || self
                    .store
                    .workspace_retirement_preflight(&workspace.workspace_id)?
                    .blockers
                    .iter()
                    .any(|blocker| blocker.code == "retirement_in_progress")
            {
                return Err(RuntimeError::Conflict(format!(
                    "workspace {} is frozen for retirement; native worktree mutation is unavailable",
                    workspace.workspace_id
                )));
            }
        }
        Ok(())
    }

    fn has_live_session_binding(
        state: &runtime_core::RuntimeHydratedState,
        worktree: &ManagedWorktreeRecord,
    ) -> bool {
        state
            .sessions
            .iter()
            .any(|session| Self::session_binds_worktree(session, worktree))
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
}
