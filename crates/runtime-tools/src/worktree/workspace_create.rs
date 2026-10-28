use std::path::Path;

use runtime_core::{
    normalized_json_hash, resolve_repository_identity, OperationActor, RuntimeError,
    WorkspaceWorktreeCreateRequest, WorkspaceWorktreeCreationAdmission,
    WorkspaceWorktreeCreationCommand, WorkspaceWorktreeCreationResult, WorktreeCreateRequest,
};

use super::RuntimeWorktreeService;
use crate::now_ms;

impl RuntimeWorktreeService {
    pub(super) async fn create_workspace_worktree_with_receipt(
        &self,
        request: WorkspaceWorktreeCreateRequest,
        actor: OperationActor,
        idempotency_key: String,
    ) -> Result<WorkspaceWorktreeCreationResult, RuntimeError> {
        Self::validate_managed_worktree_name(&request.worktree_name)?;
        if let Some(prefix) = request.branch_prefix.as_deref() {
            for segment in prefix.split('/') {
                Self::validate_managed_worktree_name(segment)?;
            }
        }
        let requested_deletion_policy = match request.deletion_policy.as_deref() {
            None => None,
            Some("retain_on_last_claim") => Some("retain_on_last_claim"),
            Some("delete_on_last_claim") => Some("delete_on_last_claim"),
            _ => {
                return Err(RuntimeError::InvalidState(
                    "worktree deletion_policy must be retain_on_last_claim or delete_on_last_claim"
                        .into(),
                ));
            }
        };
        let normalized_request = serde_json::json!({
            "workspace_id": request.workspace_id,
            "worktree_name": request.worktree_name.trim(),
            "branch_prefix": request.branch_prefix.as_deref().map(str::trim)
                .filter(|value| !value.is_empty()).unwrap_or("gg"),
            "base_ref": request.base_ref.as_deref().map(str::trim)
                .filter(|value| !value.is_empty()).unwrap_or("HEAD"),
            // Preserve the distinction between caller intent and the mutable
            // configured default. An exact retry after a config change must
            // still find the original operation and its effective policy.
            "deletion_policy": requested_deletion_policy,
            "run_init_script": request.run_init_script.unwrap_or(false),
        });
        let normalized_request_hash = normalized_json_hash(&normalized_request)?;
        // A historical exact receipt must be available even after the Git
        // root disappears or the operator changes the managed-root config.
        if let Some(existing) = self.store.replay_workspace_worktree_creation(
            &actor,
            &idempotency_key,
            &request.workspace_id,
            &normalized_request_hash,
        )? {
            match existing {
                WorkspaceWorktreeCreationAdmission::Replay {
                    operation_id,
                    response,
                } => {
                    return Ok(WorkspaceWorktreeCreationResult {
                        operation_id,
                        response,
                    });
                }
                WorkspaceWorktreeCreationAdmission::Unresolved { operation_id } => {
                    return Err(RuntimeError::Conflict(format!(
                        "worktree create operation {operation_id} is unresolved; inspect /v2/operations/{operation_id} without retrying Git"
                    )));
                }
                WorkspaceWorktreeCreationAdmission::Execute { .. } => {
                    return Err(RuntimeError::ProtocolViolation(
                        "read-only worktree replay unexpectedly admitted native work".into(),
                    ));
                }
            }
        }
        // The feature may have been disabled since the original success, but
        // a matching historical receipt is still immutable and replayable.
        self.ensure_enabled()?;
        let deletion_policy = self.normalize_deletion_policy(requested_deletion_policy);
        let workspace = self
            .store
            .get_workspace(&request.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", request.workspace_id)))?;
        let repository = resolve_repository_identity(Path::new(&workspace.canonical_root))?;
        let planned = self.plan_worktree_paths(
            &repository.canonical_root,
            &request.worktree_name,
            request.branch_prefix.as_deref(),
        );
        // The admission and legacy Git path synchronize on the same repository
        // mutex. Releasing it before invoking the native executor is safe:
        // that executor checks the durable reservation under the mutex.
        let lifecycle_lock =
            runtime_core::repository_lifecycle_lock(&repository.canonical_root).await;
        let lifecycle_guard = lifecycle_lock.read().await;
        let repo_lock = self.lock_for_repo(&repository.canonical_root).await;
        let repo_guard = repo_lock.lock().await;
        let admitted =
            self.store
                .admit_workspace_worktree_creation(&WorkspaceWorktreeCreationCommand {
                    operation_id: format!("op_wt_{:032x}", rand::random::<u128>()),
                    workspace_id: request.workspace_id,
                    actor,
                    idempotency_key,
                    normalized_request_hash,
                    normalized_request,
                    repository_root: repository.canonical_root.clone(),
                    branch_name: planned.branch_name,
                    checkout_path: planned.worktree_cwd,
                    requested_at: now_ms(),
                })?;
        drop(repo_guard);
        drop(lifecycle_guard);
        let operation_id = match admitted {
            WorkspaceWorktreeCreationAdmission::Replay {
                operation_id,
                response,
            } => {
                return Ok(WorkspaceWorktreeCreationResult {
                    operation_id,
                    response,
                });
            }
            WorkspaceWorktreeCreationAdmission::Unresolved { operation_id } => {
                return Err(RuntimeError::Conflict(format!(
                    "worktree create operation {operation_id} is unresolved; inspect /v2/operations/{operation_id} and do not retry the native effect"
                )));
            }
            WorkspaceWorktreeCreationAdmission::Execute { operation_id } => operation_id,
        };

        let result = self
            .create_worktree_for_repository(
                repository.canonical_root,
                WorktreeCreateRequest {
                    team_id: None,
                    source_session_id: String::new(),
                    repo_root: None,
                    worktree_name: request.worktree_name,
                    branch_prefix: request.branch_prefix,
                    base_ref: request.base_ref,
                    deletion_policy: Some(deletion_policy),
                    run_init_script: Some(request.run_init_script.unwrap_or(false)),
                    created_by_session_id: None,
                    operation_id: Some(operation_id.clone()),
                },
                None,
            )
            .await;
        match result {
            Ok(response) => {
                self.store
                    .complete_workspace_worktree_creation(&operation_id, &response, now_ms())
                    .map_err(|_| RuntimeError::Conflict(format!(
                        "worktree create operation {operation_id} completed native work but its exact receipt remains unresolved; inspect /v2/operations/{operation_id} before retrying"
                    )))?;
                Ok(WorkspaceWorktreeCreationResult {
                    operation_id,
                    response,
                })
            }
            Err(error) => {
                self.store
                    .mark_workspace_worktree_creation_uncertain(
                        &operation_id,
                        &error.to_string(),
                        now_ms(),
                    )
                    .map_err(|_| RuntimeError::Conflict(format!(
                        "worktree create operation {operation_id} has an unresolved native result and could not record review status; inspect /v2/operations/{operation_id}"
                    )))?;
                Err(RuntimeError::Conflict(format!(
                    "worktree create operation {operation_id} has an unresolved native outcome; inspect the operation before taking recovery action: {error}"
                )))
            }
        }
    }
}
