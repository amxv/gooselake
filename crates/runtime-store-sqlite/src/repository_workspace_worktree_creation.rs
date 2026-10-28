use runtime_core::{
    OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord,
    OperationResourceClaimRecord, OperationTransitionRecord, RuntimeError, WorkspaceLifecycleState,
    WorkspaceWorktreeCreationAdmission, WorkspaceWorktreeCreationCommand, WorktreeCreateResponse,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::{operation_by_id, operation_by_idempotency, workspace_by_id};
use crate::SqliteRuntimeRepository;

const CREATE_KIND: &str = "workspace_worktree_create";
const CLAIM_KIND: &str = "managed_worktree_native_checkout";
const BRANCH_CLAIM_KIND: &str = "managed_worktree_native_branch";
const EFFECT_KIND: &str = "git_worktree_create";

fn branch_resource_id(repository_root: &str, branch_name: &str) -> String {
    serde_json::json!([repository_root, branch_name]).to_string()
}

impl SqliteRuntimeRepository {
    pub fn replay_workspace_worktree_creation(
        &self,
        actor: &OperationActor,
        idempotency_key: &str,
        workspace_id: &str,
        normalized_request_hash: &str,
    ) -> Result<Option<WorkspaceWorktreeCreationAdmission>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        operation_by_idempotency(
            &connection,
            actor.kind,
            &actor.identifier,
            Some(idempotency_key),
        )?
        .map(|existing| classify_existing_creation(existing, workspace_id, normalized_request_hash))
        .transpose()
    }

    pub fn native_worktree_creation_claim_owner(
        &self,
        checkout_path: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",
                params![CLAIM_KIND, checkout_path],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed loading native worktree reservation", error))
    }

    pub fn native_worktree_creation_branch_owner(
        &self,
        repository_root: &str,
        branch_name: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",
                params![
                    BRANCH_CLAIM_KIND,
                    branch_resource_id(repository_root, branch_name)
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed loading native Git branch reservation", error))
    }

    /// A committed intent is the sole permission to initiate the native Git
    /// effect. Even an in-flight request replay returns Unresolved: neither a
    /// Git checkout nor a shell init script can be safely assumed unexecuted.
    pub fn admit_workspace_worktree_creation(
        &self,
        command: &WorkspaceWorktreeCreationCommand,
    ) -> Result<WorkspaceWorktreeCreationAdmission, RuntimeError> {
        if command.actor.kind != OperationActorKind::Operator
            || command.actor.identifier.is_empty()
            || command.workspace_id.is_empty()
            || command.idempotency_key.is_empty()
            || command.idempotency_key.trim() != command.idempotency_key
            || command.idempotency_key.len() > 512
            || command.checkout_path.is_empty()
            || command.branch_name.is_empty()
            || command.normalized_request_hash
                != runtime_core::normalized_json_hash(&command.normalized_request)?
        {
            return Err(RuntimeError::InvalidState(
                "invalid trusted workspace worktree creation intent".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native worktree admission", error))?;

        // Resolve the *original* receipt before any mutable workspace state.
        if let Some(existing) = operation_by_idempotency(
            &transaction,
            command.actor.kind,
            &command.actor.identifier,
            Some(&command.idempotency_key),
        )? {
            return classify_existing_creation(
                existing,
                &command.workspace_id,
                &command.normalized_request_hash,
            );
        }

        let workspace = workspace_by_id(&transaction, &command.workspace_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", command.workspace_id)))?;
        let frozen: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id = ?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking worktree retirement fence", error))?;
        if workspace.lifecycle_state != WorkspaceLifecycleState::Active || frozen {
            return Err(RuntimeError::Conflict(
                "workspace is retired or has unresolved retirement effects".into(),
            ));
        }
        // A workspace may be registered at a subdirectory of its Git root;
        // its persisted authority root must still remain within that repo.
        if !std::path::Path::new(&workspace.canonical_root)
            .starts_with(std::path::Path::new(&command.repository_root))
        {
            return Err(RuntimeError::Conflict(
                "workspace is not within the admitted Git repository".into(),
            ));
        }

        let blocking_operation: Option<String> = transaction
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",
                params![CLAIM_KIND, command.checkout_path],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking native worktree effect claim", error))?;
        if let Some(operation_id) = blocking_operation {
            return Err(RuntimeError::Conflict(format!(
                "native checkout is reserved by operation {operation_id}; inspect its Git effect before retrying"
            )));
        }

        let branch_key = branch_resource_id(&command.repository_root, &command.branch_name);
        let blocking_branch: Option<String> = transaction
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",
                params![BRANCH_CLAIM_KIND, branch_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking native Git branch effect claim", error))?;
        if let Some(operation_id) = blocking_branch {
            return Err(RuntimeError::Conflict(format!(
                "native Git branch is reserved by operation {operation_id}; inspect it before retrying"
            )));
        }

        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: Some(command.workspace_id.clone()),
            kind: CREATE_KIND.into(),
            actor: command.actor.clone(),
            idempotency_key: Some(command.idempotency_key.clone()),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: Some(serde_json::json!({"native_effect":"git_worktree_create"})),
            created_at: command.requested_at,
        })?;
        authority.acquire_claim(
            &command.operation_id,
            CLAIM_KIND,
            &command.checkout_path,
            command.requested_at,
        )?;
        authority.acquire_claim(
            &command.operation_id,
            BRANCH_CLAIM_KIND,
            &branch_key,
            command.requested_at,
        )?;
        authority.insert_effect(&OperationEffectRecord {
            effect_id: format!("fx_{}", command.operation_id),
            operation_id: command.operation_id.clone(),
            effect_kind: EFFECT_KIND.into(),
            target_kind: "git_checkout".into(),
            target_id: command.checkout_path.clone(),
            phase: "intended".into(),
            idempotency_key: "native_git_and_init".into(),
            evidence: Some(serde_json::json!({
                "repository_root": command.repository_root,
                "branch_name": command.branch_name,
                "checkout_path": command.checkout_path,
            })),
            attempt_count: 0,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing native worktree intent", error))?;
        Ok(WorkspaceWorktreeCreationAdmission::Execute {
            operation_id: command.operation_id.clone(),
        })
    }

    /// The exact response is receipted only after its managed row and native
    /// identity have been written, in the same transaction as effect evidence.
    pub fn complete_workspace_worktree_creation(
        &self,
        operation_id: &str,
        response: &WorktreeCreateResponse,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native worktree completion", error))?;
        let operation = operation_by_id(&transaction, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("operation {operation_id}")))?;
        if operation.kind != CREATE_KIND || operation.phase != OperationPhase::Requested {
            return Err(RuntimeError::Conflict(format!(
                "operation {operation_id} cannot be completed from its current phase"
            )));
        }
        let recorded: Option<(String, String)> = transaction
            .query_row(
                "SELECT target_id, evidence_json FROM runtime_operation_effects
                 WHERE operation_id=?1 AND effect_kind=?2 AND phase='intended'",
                params![operation_id, EFFECT_KIND],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading native worktree intent", error))?;
        let (checkout_path, evidence) = recorded.ok_or_else(|| {
            RuntimeError::Conflict("worktree native effect intent is absent or changed".into())
        })?;
        let identity: serde_json::Value = serde_json::from_str(&evidence).map_err(|error| {
            RuntimeError::Bootstrap(format!("corrupt worktree native intent: {error}"))
        })?;
        if response.worktree.worktree_cwd != checkout_path
            || identity["repository_root"].as_str() != Some(response.worktree.repo_root.as_str())
            || identity["branch_name"].as_str() != Some(response.worktree.branch_name.as_str())
        {
            return Err(RuntimeError::Conflict(
                "worktree completion does not match its admitted Git identity".into(),
            ));
        }
        let origin: Option<Option<String>> = transaction
            .query_row(
                "SELECT created_by_operation_id FROM managed_worktrees
                 WHERE id=?1 AND worktree_cwd=?2 AND branch_name=?3",
                params![
                    response.worktree.id,
                    checkout_path,
                    response.worktree.branch_name
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking persisted managed worktree", error))?;
        let origin = origin.ok_or_else(|| {
            RuntimeError::Conflict("managed worktree was not persisted before completion".into())
        })?;
        if response.created && origin.as_deref() != Some(operation_id) {
            return Err(RuntimeError::Conflict(
                "native worktree creation lacks matching operation provenance".into(),
            ));
        }

        let exact = serde_json::to_value(response).map_err(|error| {
            RuntimeError::Bootstrap(format!("failed serializing worktree receipt: {error}"))
        })?;
        let updated = transaction
            .execute(
                "UPDATE runtime_operation_effects
                 SET phase='finalized', evidence_json=?2, attempt_count=1, updated_at=?3
                 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![
                    operation_id,
                    json_to_string(&serde_json::json!({
                        "receipt": exact,
                        "native_identity": identity,
                    }))?,
                    at_ms,
                    EFFECT_KIND
                ],
            )
            .map_err(|error| db_error("failed recording native worktree evidence", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "native worktree effect changed".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&transaction);
        authority.terminalize(
            operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            operation.workspace_id.as_deref(),
            &exact,
            None,
            None,
            at_ms,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({"receipt":"exact_worktree_creation"})),
            created_at: at_ms,
        })?;
        // The Git ref is a second authority dimension. A changed checkout
        // root must never bypass a claim on the same native branch.
        let branch_key =
            branch_resource_id(&response.worktree.repo_root, &response.worktree.branch_name);
        for (kind, resource_id) in [(CLAIM_KIND, checkout_path), (BRANCH_CLAIM_KIND, branch_key)] {
            let claim: Option<(i64, i64)> = transaction
                .query_row(
                    "SELECT fence_generation, acquired_at FROM runtime_operation_resource_claims
                     WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'
                       AND owner_operation_id=?3",
                    params![kind, resource_id, operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed finding exact native worktree claim", error))?;
            let (generation, acquired_at) = claim.ok_or_else(|| {
                RuntimeError::Conflict(format!(
                    "native worktree creation no longer owns its {kind} reservation"
                ))
            })?;
            authority.release_claim(&OperationResourceClaimRecord {
                resource_kind: kind.into(),
                resource_id,
                claim_mode: "exclusive".into(),
                owner_operation_id: operation_id.into(),
                fence_generation: u64::try_from(generation).map_err(|_| {
                    RuntimeError::Bootstrap("invalid native worktree claim generation".into())
                })?,
                acquired_at,
            })?;
        }
        transaction
            .commit()
            .map_err(|error| db_error("failed committing exact worktree receipt", error))
    }

    pub fn mark_workspace_worktree_creation_uncertain(
        &self,
        operation_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native worktree recovery review", error))?;
        let operation = operation_by_id(&transaction, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("operation {operation_id}")))?;
        if operation.kind != CREATE_KIND || operation.phase != OperationPhase::Requested {
            return Err(RuntimeError::Conflict(
                "cannot mark an unrelated or terminal operation uncertain".into(),
            ));
        }
        let diagnostic: String = reason.chars().take(400).collect();
        transaction
            .execute(
                "UPDATE runtime_operation_effects SET phase='uncertain', attempt_count=1, updated_at=?2,
                 evidence_json=json_set(evidence_json, '$.diagnostic', ?3)
                 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![operation_id, at_ms, diagnostic, EFFECT_KIND],
            )
            .map_err(|error| {
                db_error("failed recording uncertain native worktree effect", error)
            })?;
        transaction
            .execute(
                "UPDATE runtime_operations SET phase='manual_review',
                 error_code='outcome_unresolved', error_message=?2, updated_at=?3
                 WHERE operation_id=?1 AND phase='requested'",
                params![operation_id, diagnostic, at_ms],
            )
            .map_err(|error| db_error("failed flagging native worktree manual review", error))?;
        OperationAuthorityTransaction::new(&transaction).append_transition(
            &OperationTransitionRecord {
                operation_id: operation_id.into(),
                sequence: 2,
                from_phase: Some(OperationPhase::Requested),
                to_phase: OperationPhase::ManualReview,
                evidence: Some(serde_json::json!({"reason":"native_outcome_uncertain"})),
                created_at: at_ms,
            },
        )?;
        // Preserve the exclusive claim: a fresh key cannot overwrite unknown
        // Git or user-init-script effects until a trusted recovery reconciles it.
        transaction
            .commit()
            .map_err(|error| db_error("failed committing native worktree review", error))
    }
}

/// This lookup deliberately depends only on the immutable admitted request,
/// never current workspace lifecycle, native checkout state or init scripts.
fn classify_existing_creation(
    existing: OperationRecord,
    workspace_id: &str,
    normalized_request_hash: &str,
) -> Result<WorkspaceWorktreeCreationAdmission, RuntimeError> {
    if existing.kind != CREATE_KIND
        || existing.workspace_id.as_deref() != Some(workspace_id)
        || existing.normalized_request_hash != normalized_request_hash
    {
        return Err(RuntimeError::Conflict(format!(
            "Idempotency-Key belongs to operation {} with different input",
            existing.operation_id
        )));
    }
    match (existing.phase, existing.exact_terminal_result) {
        (OperationPhase::Completed, Some(result)) => {
            Ok(WorkspaceWorktreeCreationAdmission::Replay {
                operation_id: existing.operation_id,
                response: serde_json::from_value(result).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "corrupt durable worktree creation receipt: {error}"
                    ))
                })?,
            })
        }
        _ => Ok(WorkspaceWorktreeCreationAdmission::Unresolved {
            operation_id: existing.operation_id,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runtime_core::{
        normalized_json_hash, prepare_workspace_registration, ManagedWorktreeRecord,
        OperationActor, WorkspaceRegisterRequest,
    };
    use serde_json::json;

    use super::*;

    fn fixture() -> (
        tempfile::TempDir,
        SqliteRuntimeRepository,
        WorkspaceWorktreeCreationCommand,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let database_path = temp.path().join("runtime.sqlite3");
        let repo = SqliteRuntimeRepository::new(database_path);
        repo.initialize_schema().unwrap();
        let root = temp.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let workspace = repo
            .register_workspace(
                &prepare_workspace_registration(
                    WorkspaceRegisterRequest {
                        canonical_root: root.to_str().unwrap().into(),
                        display_name: None,
                    },
                    OperationActor::operator("operator"),
                    None,
                )
                .unwrap(),
            )
            .unwrap()
            .workspace;
        let input = json!({"workspace_id":workspace.workspace_id,"worktree_name":"feature",
            "branch_prefix":null,"base_ref":null,"deletion_policy":"retain_on_last_claim",
            "run_init_script":false});
        let command = WorkspaceWorktreeCreationCommand {
            operation_id: "op_worktree_test_1".into(),
            workspace_id: workspace.workspace_id,
            actor: OperationActor::operator("runtime_operator"),
            idempotency_key: "native-once".into(),
            normalized_request_hash: normalized_json_hash(&input).unwrap(),
            normalized_request: input,
            repository_root: workspace.canonical_root,
            branch_name: "gg/feature".into(),
            checkout_path: temp.path().join("feature").to_string_lossy().to_string(),
            requested_at: 100,
        };
        (temp, repo, command)
    }

    fn response(command: &WorkspaceWorktreeCreationCommand) -> WorktreeCreateResponse {
        WorktreeCreateResponse {
            worktree: ManagedWorktreeRecord {
                id: "wt_native_test_1".into(),
                repo_root: command.repository_root.clone(),
                worktree_root: std::path::Path::new(&command.checkout_path)
                    .parent()
                    .unwrap()
                    .to_string_lossy()
                    .into(),
                worktree_cwd: command.checkout_path.clone(),
                branch_name: command.branch_name.clone(),
                worktree_name: "feature".into(),
                unified_workspace_path: "feature".into(),
                deletion_policy: "retain_on_last_claim".into(),
                created_by_session_id: None,
                created_by_operation_id: Some(command.operation_id.clone()),
                created_at: 101,
                updated_at: 101,
            },
            created: true,
            init_script_status: "skipped_disabled".into(),
        }
    }

    #[test]
    fn native_worktree_write_ahead_claim_rejects_collisions_and_unknown_replay() {
        let (temp, repo, command) = fixture();
        assert!(matches!(
            repo.admit_workspace_worktree_creation(&command).unwrap(),
            WorkspaceWorktreeCreationAdmission::Execute { .. }
        ));
        let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
        assert!(
            reopened
                .replay_workspace_worktree_creation(
                    &OperationActor::operator("other_operator"),
                    &command.idempotency_key,
                    &command.workspace_id,
                    &command.normalized_request_hash,
                )
                .unwrap()
                .is_none(),
            "one operator cannot read another operator's keyed native receipt"
        );
        assert!(
            matches!(
                reopened.replay_workspace_worktree_creation(
                    &command.actor,
                    &command.idempotency_key,
                    "different_workspace",
                    &command.normalized_request_hash,
                ),
                Err(RuntimeError::Conflict(_))
            ),
            "key replay is scoped to the originally admitted workspace"
        );
        let mut another = command.clone();
        another.operation_id = "op_retry".into();
        assert!(matches!(
            reopened.admit_workspace_worktree_creation(&another).unwrap(),
            WorkspaceWorktreeCreationAdmission::Unresolved { operation_id }
            if operation_id == command.operation_id
        ));
        another.normalized_request = json!({"worktree_name":"a-different-request"});
        another.normalized_request_hash =
            normalized_json_hash(&another.normalized_request).unwrap();
        assert!(matches!(
            reopened.admit_workspace_worktree_creation(&another),
            Err(RuntimeError::Conflict(_))
        ));
        another = command.clone();
        another.operation_id = "op_distinct_key".into();
        another.idempotency_key = "distinct-native".into();
        assert!(matches!(
            reopened.admit_workspace_worktree_creation(&another),
            Err(RuntimeError::Bootstrap(_)) | Err(RuntimeError::Conflict(_))
        ));
        another.checkout_path = temp
            .path()
            .join("changed-managed-root/feature")
            .to_string_lossy()
            .to_string();
        assert!(
            matches!(
                reopened.admit_workspace_worktree_creation(&another),
                Err(RuntimeError::Conflict(_))
            ),
            "a changed managed worktree root must not bypass an unresolved Git branch claim"
        );
        assert_eq!(
            reopened
                .get_operation(&command.operation_id)
                .unwrap()
                .unwrap()
                .effects
                .len(),
            1
        );
    }

    #[test]
    fn exact_receipt_is_persisted_across_restart_with_atomic_claim_release() {
        let (temp, repo, command) = fixture();
        repo.admit_workspace_worktree_creation(&command).unwrap();
        let result = response(&command);
        assert!(
            repo.complete_workspace_worktree_creation(&command.operation_id, &result, 102)
                .is_err(),
            "a success receipt requires persisted managed worktree evidence"
        );
        repo.upsert_managed_worktree(&result.worktree).unwrap();
        repo.complete_workspace_worktree_creation(&command.operation_id, &result, 103)
            .unwrap();
        let details = repo.get_operation(&command.operation_id).unwrap().unwrap();
        assert_eq!(details.operation.phase, OperationPhase::Completed);
        assert_eq!(details.effects[0].phase, "finalized");
        assert!(details.claims.is_empty());
        let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
        let replay = reopened
            .admit_workspace_worktree_creation(&command)
            .unwrap();
        match replay {
            WorkspaceWorktreeCreationAdmission::Replay {
                operation_id,
                response,
            } => {
                assert_eq!(operation_id, command.operation_id);
                assert_eq!(
                    serde_json::to_value(&response).unwrap(),
                    serde_json::to_value(&result).unwrap()
                );
                assert_eq!(response.init_script_status, "skipped_disabled");
                assert!(response.created);
            }
            _ => panic!("expected original exact receipt"),
        }

        std::fs::remove_dir_all(&command.repository_root).unwrap();
        assert!(
            matches!(
                reopened
                    .replay_workspace_worktree_creation(
                        &command.actor,
                        &command.idempotency_key,
                        &command.workspace_id,
                        &command.normalized_request_hash,
                    )
                    .unwrap(),
                Some(WorkspaceWorktreeCreationAdmission::Replay { response, .. })
                    if response.worktree.id == result.worktree.id
            ),
            "operator replay does not require a live checkout or repository root"
        );
    }

    #[test]
    fn failed_terminal_write_preserves_unknown_outcome_without_native_redispatch() {
        let (temp, repo, command) = fixture();
        repo.admit_workspace_worktree_creation(&command).unwrap();
        let result = response(&command);
        repo.upsert_managed_worktree(&result.worktree).unwrap();
        let db = temp.path().join("runtime.sqlite3");
        open_connection(&db)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER native_receipt_fail BEFORE UPDATE ON runtime_operation_effects
                 BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END;",
            )
            .unwrap();
        assert!(repo
            .complete_workspace_worktree_creation(&command.operation_id, &result, 102)
            .is_err());
        let details = repo.get_operation(&command.operation_id).unwrap().unwrap();
        assert_eq!(details.operation.phase, OperationPhase::Requested);
        assert_eq!(details.effects[0].phase, "intended");
        assert_eq!(details.claims.len(), 2);
        assert!(matches!(
            repo.admit_workspace_worktree_creation(&command).unwrap(),
            WorkspaceWorktreeCreationAdmission::Unresolved { .. }
        ));
        open_connection(&db)
            .unwrap()
            .execute_batch("DROP TRIGGER native_receipt_fail;")
            .unwrap();
        repo.complete_workspace_worktree_creation(&command.operation_id, &result, 103)
            .unwrap();
    }

    #[test]
    fn failed_native_intent_commit_rolls_back_operation_and_both_reservations() {
        let (temp, repo, command) = fixture();
        let db = temp.path().join("runtime.sqlite3");
        open_connection(&db)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse_native_intent BEFORE INSERT ON runtime_operation_effects
                 BEGIN SELECT RAISE(ABORT, 'injected intent failure'); END;",
            )
            .unwrap();
        assert!(repo.admit_workspace_worktree_creation(&command).is_err());
        assert!(repo.get_operation(&command.operation_id).unwrap().is_none());
        assert_eq!(
            repo.native_worktree_creation_claim_owner(&command.checkout_path)
                .unwrap(),
            None
        );
        assert_eq!(
            repo.native_worktree_creation_branch_owner(
                &command.repository_root,
                &command.branch_name
            )
            .unwrap(),
            None
        );
        open_connection(&db)
            .unwrap()
            .execute_batch("DROP TRIGGER refuse_native_intent;")
            .unwrap();
        assert!(matches!(
            repo.admit_workspace_worktree_creation(&command).unwrap(),
            WorkspaceWorktreeCreationAdmission::Execute { .. }
        ));
    }

    #[test]
    fn uncertain_native_failure_preserves_exclusive_claim_for_operator_review() {
        let (_temp, repo, command) = fixture();
        repo.admit_workspace_worktree_creation(&command).unwrap();
        repo.mark_workspace_worktree_creation_uncertain(
            &command.operation_id,
            "git interrupted",
            102,
        )
        .unwrap();
        let details = repo.get_operation(&command.operation_id).unwrap().unwrap();
        assert_eq!(details.operation.phase, OperationPhase::ManualReview);
        assert_eq!(
            details.operation.error_code.as_deref(),
            Some("outcome_unresolved")
        );
        assert_eq!(details.effects[0].phase, "uncertain");
        assert_eq!(details.claims.len(), 2);
        assert!(matches!(
            repo.admit_workspace_worktree_creation(&command).unwrap(),
            WorkspaceWorktreeCreationAdmission::Unresolved { .. }
        ));
        let mut new_key = command.clone();
        new_key.idempotency_key = "new-request-must-not-overwrite-unknown".into();
        new_key.operation_id = "op_new_while_unknown".into();
        assert!(repo.admit_workspace_worktree_creation(&new_key).is_err());
    }

    #[test]
    fn concurrent_same_key_admits_only_one_native_executor() {
        let (_temp, repo, command) = fixture();
        let shared = Arc::new(repo);
        let workers = (0..8)
            .map(|_| {
                let shared = shared.clone();
                let command = command.clone();
                std::thread::spawn(move || shared.admit_workspace_worktree_creation(&command))
            })
            .collect::<Vec<_>>();
        let mut executors = 0;
        for worker in workers {
            match worker.join().unwrap().unwrap() {
                WorkspaceWorktreeCreationAdmission::Execute { .. } => executors += 1,
                WorkspaceWorktreeCreationAdmission::Unresolved { .. } => {}
                WorkspaceWorktreeCreationAdmission::Replay { .. } => panic!("not yet terminal"),
            }
        }
        assert_eq!(executors, 1);
    }
}
