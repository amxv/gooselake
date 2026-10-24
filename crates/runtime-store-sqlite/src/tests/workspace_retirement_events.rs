use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::{
    prepare_workspace_retirement, OperationActor, OperationPhase, ProviderRetirementCloseEvidence,
    ProviderRetirementCloseProofKind, WorkspaceAgentRecord, WorkspaceLifecycleState,
    WorkspaceNoticePolicyUpdate, WorkspaceRecord, WorkspaceRetirementAdmission,
    WorkspaceRetirementCommand, WorkspaceRetirementRequest,
};

fn setup() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    WorkspaceRecord,
    WorkspaceAgentRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) =
        workspace_agent_fixture(&workspace, "sess_retirement_event", "event-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    (temp, repository, workspace, agent)
}

fn command(workspace: &WorkspaceRecord) -> WorkspaceRetirementCommand {
    prepare_workspace_retirement(
        &workspace.workspace_id,
        WorkspaceRetirementRequest {
            expected_revision: workspace.revision,
        },
        OperationActor::operator("retirement-events"),
        Some("retirement-events".into()),
    )
    .unwrap()
}

fn event_count(db: &Connection, operation_id: &str) -> i64 {
    db.query_row(
        "SELECT COUNT(*) FROM runtime_events WHERE json_extract(payload_json,'$.operation_id')=?1",
        [operation_id],
        |row| row.get(0),
    )
    .unwrap()
}

fn evidence(agent: &WorkspaceAgentRecord) -> ProviderRetirementCloseEvidence {
    ProviderRetirementCloseEvidence {
        runtime_session_id: agent.agent_id.clone(),
        provider_session_ref: agent.provider_session_ref.clone().unwrap(),
        proof_kind: ProviderRetirementCloseProofKind::NativeProcessTerminated,
    }
}

#[test]
fn retirement_events_and_cancelled_notices_are_atomic_exact_and_survive_reopen() {
    let (temp, repository, workspace, agent) = setup();
    let command = command(&workspace);
    let plan = match repository.admit_workspace_retirement(&command).unwrap() {
        WorkspaceRetirementAdmission::New(plan) => plan,
        other => panic!("unexpected admission {other:?}"),
    };
    let mut db = open_connection(&repository.database_path).unwrap();
    assert_eq!(event_count(&db, &command.operation_id), 1);
    let notice_id = format!(
        "workspace-lifecycle:{}:workspace.retirement_started:workspace_retirement:{}",
        command.operation_id, agent.agent_id
    );
    let notice = repository
        .get_workspace_system_notice(&notice_id)
        .unwrap()
        .unwrap();
    assert_eq!(notice.state, "cancelled");
    assert_eq!(notice.attempt_count, 0);
    assert_eq!(notice.turn_id, None);
    assert_eq!(
        notice.last_error.as_deref(),
        Some("workspace_retirement_freezes_new_model_input")
    );
    assert!(!repository
        .list_workspace_system_notices(256)
        .unwrap()
        .iter()
        .any(|pending| pending.notice_id == notice_id));

    repository
        .settle_passive_workspace_retirement(&command.operation_id, 201)
        .unwrap();
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &agent.agent_id, 202)
        .unwrap();
    repository
        .finalize_workspace_retirement_native_close(&attempt, &evidence(&agent), 203)
        .unwrap();
    assert_eq!(event_count(&db, &command.operation_id), 2);
    repository
        .finalize_settled_workspace_retirement(&command.operation_id, 204)
        .unwrap()
        .unwrap();
    let original_events = repository.list_runtime_events(None, None, 256).unwrap();
    assert_eq!(event_count(&db, &command.operation_id), 3);
    let workspace_events = repository
        .list_runtime_events(
            Some((RuntimeEventScope::Workspace, &workspace.workspace_id)),
            None,
            256,
        )
        .unwrap();
    assert_eq!(workspace_events.len(), 2);
    assert_eq!(workspace_events[0].kind, "workspace.retirement_started");
    assert_eq!(workspace_events[1].kind, "workspace.retired");
    assert_eq!(
        serde_json::to_value(&workspace_events[0]).unwrap()["scope"],
        "workspace"
    );
    let archived = original_events
        .iter()
        .find(|event| event.kind == "session.archived")
        .unwrap();
    assert_eq!(archived.scope, RuntimeEventScope::Session);
    assert_eq!(archived.scope_id, agent.agent_id);
    assert_eq!(archived.payload["workspace_id"], workspace.workspace_id);
    assert_eq!(archived.payload["native_close_verified"], true);

    // The transaction helper is independently idempotent, including notices
    // and scope sequence allocation, even with a different retry timestamp.
    let tx = db.transaction().unwrap();
    crate::repository_workspace_retirement_events::retirement_started(&tx, &plan, 300).unwrap();
    crate::repository_workspace_retirement_events::retirement_agent_archived(
        &tx,
        &command.operation_id,
        &workspace.workspace_id,
        &agent.agent_id,
        evidence(&agent).proof_kind,
        300,
    )
    .unwrap();
    crate::repository_workspace_retirement_events::retirement_completed(
        &tx,
        &command.operation_id,
        &workspace.workspace_id,
        300,
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        repository.list_runtime_events(None, None, 256).unwrap(),
        original_events
    );
    drop(repository);
    let reopened = repo(&temp);
    reopened.initialize_schema().unwrap();
    assert!(matches!(
        reopened.admit_workspace_retirement(&command).unwrap(),
        WorkspaceRetirementAdmission::Replay(_)
    ));
    assert_eq!(event_count(&db, &command.operation_id), 3);
    assert_eq!(
        reopened
            .get_workspace_system_notice(&notice_id)
            .unwrap()
            .unwrap(),
        notice
    );
}

#[test]
fn retirement_event_failure_rolls_back_each_authority_boundary() {
    let (_temp, repository, workspace, agent) = setup();
    let command = command(&workspace);
    let db = open_connection(&repository.database_path).unwrap();
    db.execute_batch("CREATE TRIGGER reject_retirement_event BEFORE INSERT ON runtime_events
        WHEN NEW.kind='workspace.retirement_started' BEGIN SELECT RAISE(ABORT,'receipt failpoint'); END;").unwrap();
    assert!(repository.admit_workspace_retirement(&command).is_err());
    assert!(repository
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
    assert_eq!(event_count(&db, &command.operation_id), 0);
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM workspace_retirement_fences",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER reject_retirement_event;")
        .unwrap();
    repository.admit_workspace_retirement(&command).unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 201)
        .unwrap();
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &agent.agent_id, 202)
        .unwrap();
    db.execute_batch(
        "CREATE TRIGGER reject_retirement_event BEFORE INSERT ON runtime_events
        WHEN NEW.kind='session.archived' BEGIN SELECT RAISE(ABORT,'receipt failpoint'); END;",
    )
    .unwrap();
    assert!(repository
        .finalize_workspace_retirement_native_close(&attempt, &evidence(&agent), 203)
        .is_err());
    assert_eq!(event_count(&db, &command.operation_id), 1);
    assert_eq!(
        db.query_row(
            "SELECT status FROM sessions WHERE id=?1",
            [&agent.agent_id],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "ready"
    );
    db.execute_batch("DROP TRIGGER reject_retirement_event;")
        .unwrap();
    repository
        .finalize_workspace_retirement_native_close(&attempt, &evidence(&agent), 203)
        .unwrap();
    db.execute_batch(
        "CREATE TRIGGER reject_retirement_event BEFORE INSERT ON runtime_events
        WHEN NEW.kind='workspace.retired' BEGIN SELECT RAISE(ABORT,'receipt failpoint'); END;",
    )
    .unwrap();
    assert!(repository
        .finalize_settled_workspace_retirement(&command.operation_id, 204)
        .is_err());
    assert_eq!(event_count(&db, &command.operation_id), 2);
    assert_eq!(
        repository
            .get_workspace(&workspace.workspace_id)
            .unwrap()
            .unwrap()
            .lifecycle_state,
        WorkspaceLifecycleState::Active
    );
    assert_eq!(
        repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap()
            .operation
            .phase,
        OperationPhase::Requested
    );
    db.execute_batch("DROP TRIGGER reject_retirement_event;")
        .unwrap();
    repository
        .finalize_settled_workspace_retirement(&command.operation_id, 205)
        .unwrap()
        .unwrap();
    assert_eq!(event_count(&db, &command.operation_id), 3);
}

#[test]
fn unknown_native_closure_never_emits_archived_or_retired_and_policy_disables_notices() {
    let (_temp, repository, workspace, agent) = setup();
    let mut policy = repository
        .get_workspace_notice_policy(&workspace.workspace_id)
        .unwrap();
    policy.policy.workspace_retirement = false;
    repository
        .update_workspace_notice_policy(
            &workspace.workspace_id,
            &WorkspaceNoticePolicyUpdate {
                expected_revision: policy.revision,
                policy: policy.policy,
            },
        )
        .unwrap();
    let command = command(&workspace);
    repository.admit_workspace_retirement(&command).unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM workspace_system_notices WHERE kind='workspace_retirement'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &agent.agent_id, 201)
        .unwrap();
    repository
        .quarantine_workspace_retirement_native_close(&attempt, "native_close_unknown", 202)
        .unwrap();
    assert!(repository
        .finalize_settled_workspace_retirement(&command.operation_id, 203)
        .is_err());
    assert_eq!(event_count(&db, &command.operation_id), 1);
    assert_eq!(db.query_row("SELECT COUNT(*) FROM runtime_events WHERE kind IN ('session.archived','workspace.retired')", [], |row|row.get::<_,i64>(0)).unwrap(), 0);
}

#[test]
fn quiescent_retirement_receipts_are_replay_safe_and_old_notice_policies_keep_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    let command = command(&workspace);
    let db = open_connection(&repository.database_path).unwrap();
    db.execute("INSERT INTO workspace_notice_policies(workspace_id,revision,policy_json) VALUES(?1,0,'{\"workspace_interrupt\":false}')",
        [&workspace.workspace_id]).unwrap();
    assert!(
        repository
            .get_workspace_notice_policy(&workspace.workspace_id)
            .unwrap()
            .policy
            .workspace_retirement
    );
    repository.retire_empty_workspace(&command).unwrap();
    repository.retire_empty_workspace(&command).unwrap();
    assert_eq!(event_count(&db, &command.operation_id), 2);
}
