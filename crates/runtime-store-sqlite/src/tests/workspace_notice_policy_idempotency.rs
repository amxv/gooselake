use super::*;
use runtime_core::{
    prepare_workspace_notice_policy_mutation, prepare_workspace_registration, OperationActor,
    OperationPhase, RuntimeError, WorkspaceNoticePolicy, WorkspaceNoticePolicyMutationCommand,
    WorkspaceNoticePolicyUpdate, WorkspaceRegisterRequest,
};

fn setup_workspace(temp: &tempfile::TempDir) -> (SqliteRuntimeRepository, String) {
    let repository = repo(temp);
    repository.initialize_schema().expect("schema");
    let root = temp.path().join("notices-root");
    std::fs::create_dir(&root).expect("workspace directory");
    let workspace = repository
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.to_string_lossy().into_owned(),
                    display_name: Some("Notice policy tests".into()),
                },
                OperationActor::operator("test_operator"),
                Some("workspace-create-once".into()),
            )
            .expect("registration command"),
        )
        .expect("registration")
        .workspace;
    (repository, workspace.workspace_id)
}

fn policy_command(
    workspace_id: &str,
    revision: u64,
    lead_notices: bool,
    key: Option<&str>,
) -> WorkspaceNoticePolicyMutationCommand {
    let policy = WorkspaceNoticePolicy {
        lead_transitions: lead_notices,
        ..WorkspaceNoticePolicy::default()
    };
    prepare_workspace_notice_policy_mutation(
        workspace_id,
        WorkspaceNoticePolicyUpdate {
            expected_revision: revision,
            policy,
        },
        OperationActor::operator("test_operator"),
        key.map(str::to_string),
    )
    .expect("notice policy command")
}

#[test]
fn durable_notice_policy_receipt_replays_across_reopen_and_conflicts_do_not_mutate() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (repository, workspace_id) = setup_workspace(&temp);
    let command = policy_command(&workspace_id, 0, false, Some("policy-key-1"));
    let completed = repository
        .update_workspace_notice_policy_idempotent(&command)
        .expect("first mutation");
    assert_eq!(completed.operation_id, command.operation_id);
    assert_eq!(completed.policy.revision, 1);
    assert!(!completed.policy.policy.lead_transitions);
    let details = repository
        .get_operation(&command.operation_id)
        .expect("get operation")
        .expect("durable operation");
    assert_eq!(details.operation.phase, OperationPhase::Completed);
    assert_eq!(details.transitions.len(), 2);
    assert!(details.claims.is_empty());
    assert_eq!(
        details.operation.exact_terminal_result,
        Some(serde_json::to_value(&completed).expect("receipt JSON"))
    );

    // A new process/repository instance must replay the original response,
    // even after the original request's expected revision is no longer current.
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let replayed = reopened
        .update_workspace_notice_policy_idempotent(&policy_command(
            &workspace_id,
            0,
            false,
            Some("policy-key-1"),
        ))
        .expect("restart replay");
    assert_eq!(completed, replayed);
    assert_eq!(
        reopened
            .get_workspace_notice_policy(&workspace_id)
            .expect("current policy"),
        completed.policy
    );

    let conflict = reopened
        .update_workspace_notice_policy_idempotent(&policy_command(
            &workspace_id,
            0,
            true,
            Some("policy-key-1"),
        ))
        .expect_err("different body on same key must conflict");
    assert!(matches!(conflict, RuntimeError::Conflict(_)));
    let other_root = temp.path().join("other-root");
    std::fs::create_dir(&other_root).expect("second root");
    let other_workspace = reopened
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: other_root.to_string_lossy().into_owned(),
                    display_name: None,
                },
                OperationActor::operator("test_operator"),
                None,
            )
            .expect("second registration"),
        )
        .expect("second workspace")
        .workspace;
    assert!(matches!(
        reopened.update_workspace_notice_policy_idempotent(&policy_command(
            &other_workspace.workspace_id,
            0,
            false,
            Some("policy-key-1")
        )),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(matches!(
        reopened.update_workspace_notice_policy_idempotent(&policy_command(
            &workspace_id,
            1,
            false,
            Some("workspace-create-once")
        )),
        Err(RuntimeError::Conflict(_))
    ));
    let stale = policy_command(&workspace_id, 0, true, Some("stale-policy-key"));
    assert!(matches!(
        reopened.update_workspace_notice_policy_idempotent(&stale),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(reopened
        .get_operation(&stale.operation_id)
        .expect("stale operation lookup")
        .is_none());
    assert_eq!(
        reopened
            .get_workspace_notice_policy(&workspace_id)
            .expect("after conflicts"),
        completed.policy
    );

    let next = reopened
        .update_workspace_notice_policy_idempotent(&policy_command(
            &workspace_id,
            1,
            true,
            Some("policy-key-2"),
        ))
        .expect("next revision");
    assert_eq!(next.policy.revision, 2);
    assert!(next.policy.policy.lead_transitions);
    assert_eq!(
        reopened
            .update_workspace_notice_policy_idempotent(&command)
            .expect("old receipt remains immutable"),
        completed
    );
    let different_principal = prepare_workspace_notice_policy_mutation(
        &workspace_id,
        WorkspaceNoticePolicyUpdate {
            expected_revision: 2,
            policy: WorkspaceNoticePolicy::default(),
        },
        OperationActor::operator("different_operator"),
        Some("policy-key-1".into()),
    )
    .expect("second principal");
    let independent = reopened
        .update_workspace_notice_policy_idempotent(&different_principal)
        .expect("same key scoped to a different principal");
    assert_ne!(independent.operation_id, completed.operation_id);
    assert_eq!(independent.policy.revision, 3);
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).expect("db");
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_notice_policy_update'",
            [],
            |row| row.get(0),
        )
        .expect("operation count");
    assert_eq!(count, 3, "only successful new writes create operations");
}

#[test]
fn notice_policy_failure_rolls_back_operation_claim_and_policy_then_retries_safely() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (repository, workspace_id) = setup_workspace(&temp);
    let command = policy_command(&workspace_id, 0, false, Some("crash-safe"));
    {
        let connection = open_connection(&temp.path().join("runtime.sqlite3")).expect("db");
        connection
            .execute_batch(
                "CREATE TRIGGER fail_notice_policy BEFORE INSERT ON workspace_notice_policies
                 BEGIN SELECT RAISE(ABORT, 'simulated crash at policy commit'); END;",
            )
            .expect("install failpoint");
    }
    assert!(repository
        .update_workspace_notice_policy_idempotent(&command)
        .is_err());
    assert!(repository
        .get_operation(&command.operation_id)
        .expect("operation lookup")
        .is_none());
    assert_eq!(
        repository
            .get_workspace_notice_policy(&workspace_id)
            .expect("rolled back policy")
            .revision,
        0
    );
    {
        let connection = open_connection(&temp.path().join("runtime.sqlite3")).expect("db");
        let claims: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_operation_resource_claims",
                [],
                |row| row.get(0),
            )
            .expect("claims count");
        assert_eq!(claims, 0);
        connection
            .execute_batch("DROP TRIGGER fail_notice_policy")
            .expect("remove failpoint");
    }
    let recovered = repository
        .update_workspace_notice_policy_idempotent(&command)
        .expect("retry same command");
    assert_eq!(recovered.operation_id, command.operation_id);
    assert_eq!(recovered.policy.revision, 1);
    assert!(repository
        .get_operation(&command.operation_id)
        .expect("operation lookup")
        .is_some());
}

#[test]
fn concurrent_identical_notice_policy_requests_converge_without_revision_loss() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (_, workspace_id) = setup_workspace(&temp);
    let db_path = temp.path().join("runtime.sqlite3");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(6));
    let mut workers = Vec::new();
    for _ in 0..6 {
        let workspace_id = workspace_id.clone();
        let db_path = db_path.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let repository = SqliteRuntimeRepository::new(db_path);
            let command = policy_command(&workspace_id, 0, false, Some("same-concurrent-key"));
            barrier.wait();
            repository.update_workspace_notice_policy_idempotent(&command)
        }));
    }
    let mut operation_ids = std::collections::BTreeSet::new();
    for worker in workers {
        let result = worker.join().expect("worker").expect("idempotent result");
        operation_ids.insert(result.operation_id);
        assert_eq!(result.policy.revision, 1);
    }
    assert_eq!(operation_ids.len(), 1);
    let repository = SqliteRuntimeRepository::new(db_path);
    assert_eq!(
        repository
            .get_workspace_notice_policy(&workspace_id)
            .expect("final policy")
            .revision,
        1
    );
}
