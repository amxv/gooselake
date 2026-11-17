use std::path::{Component, Path, PathBuf};

use runtime_core::{
    resolve_repository_identity, ManagedWorktreeRecord, MemberAgentCreateCommand,
    MemberAgentCreateRoute, RuntimeError, WorkspaceWorktreeCreationAdmission,
    WorktreeCreateResponse,
};

use super::RuntimeWorktreeService;
use crate::now_ms;

/// Resolve existing ancestors without creating anything. A missing future
/// managed root must not let a symlink or relative spelling change the plan.
fn canonical_future_path(path: &Path) -> Result<PathBuf, RuntimeError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| RuntimeError::Io(e.to_string()))?
            .join(path)
    };
    if absolute
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(RuntimeError::InvalidState(
            "managed root must not contain unresolved parent components".into(),
        ));
    }
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();
    loop {
        match std::fs::symlink_metadata(ancestor) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    ancestor
                        .file_name()
                        .ok_or_else(|| {
                            RuntimeError::InvalidState(
                                "managed root has no existing ancestor".into(),
                            )
                        })?
                        .to_owned(),
                );
                ancestor = ancestor.parent().ok_or_else(|| {
                    RuntimeError::InvalidState("managed root has no parent".into())
                })?;
            }
            Err(error) => return Err(RuntimeError::Io(error.to_string())),
        }
    }
    let mut result =
        std::fs::canonicalize(ancestor).map_err(|e| RuntimeError::Io(e.to_string()))?;
    if !result.is_dir() {
        return Err(RuntimeError::InvalidState(
            "managed root ancestor is not a directory".into(),
        ));
    }
    for part in suffix.into_iter().rev() {
        result.push(part);
    }
    Ok(result)
}

impl RuntimeWorktreeService {
    pub(super) fn plan_member_create_route(
        &self,
        workspace_id: &str,
        name: &str,
    ) -> Result<MemberAgentCreateRoute, RuntimeError> {
        let name = name.trim();
        let (repository, existing) = self
            .runtime
            .plan_existing_member_create_route(workspace_id, name)?;
        if let Some(existing) = existing {
            return Ok(existing);
        }
        self.ensure_enabled()?;
        Self::validate_managed_worktree_name(name)?;
        let planned = self.plan_worktree_paths(&repository.canonical_root, name, Some("gg"));
        let configured_root = canonical_future_path(Path::new(&self.config.root_dir))?;
        let root = configured_root.join(&planned.unified_workspace_path);
        let cwd = root.join(planned.branch_name.replace('/', "--"));
        let (base, _, _) = Self::run_git_for_repo(
            &repository.canonical_root,
            &["rev-parse", "--verify", "HEAD^{commit}"],
            &[],
        )?;
        let base = base.trim().to_owned();
        if !matches!(base.len(), 40 | 64) || !base.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(RuntimeError::Conflict(
                "member worktree base is not an exact Git commit".into(),
            ));
        }
        let at = now_ms();
        Ok(MemberAgentCreateRoute::New {
            repository: repository.clone(),
            configured_root: configured_root.to_string_lossy().into_owned(),
            base_commit: base,
            worktree: ManagedWorktreeRecord {
                id: self.allocate_worktree_id(),
                repo_root: repository.canonical_root,
                worktree_root: root.to_string_lossy().into_owned(),
                worktree_cwd: cwd.to_string_lossy().into_owned(),
                branch_name: planned.branch_name,
                worktree_name: planned.worktree_name,
                unified_workspace_path: planned.unified_workspace_path,
                deletion_policy: "retain_on_last_claim".into(),
                created_by_session_id: None,
                created_by_operation_id: Some(format!(
                    "op_member_git_{:032x}",
                    rand::random::<u128>()
                )),
                created_at: at,
                updated_at: at,
            },
        })
    }

    pub(super) async fn execute_member_create_worktree(
        &self,
        command: &MemberAgentCreateCommand,
    ) -> Result<WorktreeCreateResponse, RuntimeError> {
        let git = command.worktree_command()?;
        let MemberAgentCreateRoute::New {
            repository,
            worktree,
            configured_root,
            base_commit,
        } = &command.intent.route
        else {
            return Err(RuntimeError::Conflict(
                "member create does not select a new checkout".into(),
            ));
        };
        // Historical lookup precedes mutable filesystem/configuration checks.
        if let Some(prior) = self.store.replay_workspace_worktree_creation(
            &git.actor,
            &git.idempotency_key,
            &git.workspace_id,
            &git.normalized_request_hash,
        )? {
            return match prior {
                WorkspaceWorktreeCreationAdmission::Replay { response, .. } => Ok(response),
                WorkspaceWorktreeCreationAdmission::Unresolved { operation_id }
                | WorkspaceWorktreeCreationAdmission::Execute { operation_id } => {
                    Err(RuntimeError::operation_unresolved(
                        operation_id,
                        "member Git effect already has authority; never repeat native work",
                    ))
                }
            };
        }
        self.ensure_enabled()?;
        let lifecycle = runtime_core::repository_lifecycle_lock(&repository.canonical_root).await;
        let _lifecycle = lifecycle.read().await;
        let lock = self.lock_for_repo(&repository.canonical_root).await;
        let _guard = lock.lock().await;
        if resolve_repository_identity(Path::new(&command.intent.canonical_root))? != *repository
            || canonical_future_path(Path::new(&self.config.root_dir))?.to_string_lossy()
                != configured_root.as_str()
            || canonical_future_path(Path::new(&worktree.worktree_root))?.to_string_lossy()
                != worktree.worktree_root
        {
            return Err(RuntimeError::Conflict(
                "member Git repository/configured route changed before admission".into(),
            ));
        }
        Self::validate_managed_worktree_name(&worktree.worktree_name)?;
        match std::fs::symlink_metadata(&worktree.worktree_cwd) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(RuntimeError::Io(e.to_string())),
            Ok(_) => {
                return Err(RuntimeError::Conflict(
                    "member Git route already exists; no adoption is authorized".into(),
                ))
            }
        }
        let branch_ref = format!("refs/heads/{}", worktree.branch_name);
        let (_, _, branch_exists) = Self::run_git_for_repo(
            &repository.canonical_root,
            &["show-ref", "--verify", "--quiet", &branch_ref],
            &[1],
        )?;
        if branch_exists == 0
            || runtime_core::repository_checkout_roots(Path::new(&repository.canonical_root))?
                .iter()
                .any(|p| p == Path::new(&worktree.worktree_cwd))
        {
            return Err(RuntimeError::Conflict(
                "member Git branch/checkout already exists; no adoption is authorized".into(),
            ));
        }
        match self.store.admit_member_worktree_creation(command, &git)? {
            WorkspaceWorktreeCreationAdmission::Execute { .. } => {}
            WorkspaceWorktreeCreationAdmission::Replay { response, .. } => return Ok(response),
            WorkspaceWorktreeCreationAdmission::Unresolved { operation_id } => {
                return Err(RuntimeError::operation_unresolved(
                    operation_id,
                    "member Git execution was already admitted",
                ))
            }
        }
        let result = (|| {
            std::fs::create_dir_all(&worktree.worktree_root)
                .map_err(|e| RuntimeError::Io(e.to_string()))?;
            if std::fs::canonicalize(&worktree.worktree_root)
                .map_err(|e| RuntimeError::Io(e.to_string()))?
                .to_string_lossy()
                != worktree.worktree_root
            {
                return Err(RuntimeError::Conflict(
                    "member Git root changed after admission".into(),
                ));
            }
            // The sole new Git dispatch. No native reuse, init script,
            // compensation, or retry follows an error/lost receipt.
            Self::run_git_for_repo(
                &repository.canonical_root,
                &[
                    "-c",
                    "core.hooksPath=/dev/null",
                    "worktree",
                    "add",
                    "-b",
                    &worktree.branch_name,
                    &worktree.worktree_cwd,
                    base_commit,
                ],
                &[],
            )?;
            if resolve_repository_identity(Path::new(&worktree.worktree_cwd))? != *repository
                || std::fs::canonicalize(&worktree.worktree_cwd)
                    .map_err(|e| RuntimeError::Io(e.to_string()))?
                    .to_string_lossy()
                    != worktree.worktree_cwd
            {
                return Err(RuntimeError::Conflict(
                    "member native checkout returned a different repository/path".into(),
                ));
            }
            let (branch, _, _) = Self::run_git_for_repo(
                &worktree.worktree_cwd,
                &["symbolic-ref", "--quiet", "--short", "HEAD"],
                &[],
            )?;
            let (head, _, _) = Self::run_git_for_repo(
                &worktree.worktree_cwd,
                &["rev-parse", "--verify", "HEAD"],
                &[],
            )?;
            if branch.trim() != worktree.branch_name || head.trim() != base_commit {
                return Err(RuntimeError::Conflict(
                    "member native checkout differs from frozen branch/base".into(),
                ));
            }
            let response = WorktreeCreateResponse {
                worktree: worktree.clone(),
                created: true,
                init_script_status: "skipped_disabled".into(),
            };
            // Row, Critical record event, exact Git receipt and route-pin
            // transfer commit together in the shared store transaction.
            self.store.complete_workspace_worktree_creation(
                &git.operation_id,
                &response,
                now_ms(),
            )?;
            Ok(response)
        })();
        match result {
            Ok(response) => Ok(response),
            Err(error) => {
                if let Some(WorkspaceWorktreeCreationAdmission::Replay { response, .. }) =
                    self.store.replay_workspace_worktree_creation(
                        &git.actor,
                        &git.idempotency_key,
                        &git.workspace_id,
                        &git.normalized_request_hash,
                    )?
                {
                    return Ok(response);
                }
                let persisted = self.store.mark_workspace_worktree_creation_uncertain(
                    &git.operation_id,
                    &error.to_string(),
                    now_ms(),
                );
                Err(RuntimeError::operation_unresolved(&git.operation_id,format!("member Git outcome requires original-operation inspection: {error}; quarantine={}",persisted.is_ok())))
            }
        }
    }
}
