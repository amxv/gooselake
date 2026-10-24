use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::{register, retirement_command, setup};
use super::*;
use runtime_core::{
    prepare_workspace_retirement_review, OperationActor, OperationPhase, ProviderKind,
    WorkspaceRetirementNativeIdentity, WorkspaceRetirementReviewRequest,
    WorkspaceRetirementReviewResolution,
};

#[test]
fn restarted_operator_close_settles_exact_active_turn_without_rewriting_unknown_dispatch() {
    active_turn_review_case("dispatched");
}

#[test]
fn preexisting_uncertain_active_turn_enters_retirement_review_without_native_replay() {
    for dispatch in ["unknown", "dispatching"] {
        active_turn_review_case(dispatch);
    }
}

#[test]
fn orphaned_uncertain_turn_is_rejected_before_retirement_admission() {
    for dispatch in ["unknown", "dispatching"] {
        let (temp, repository, workspace) = setup();
        let (session, agent) =
            workspace_agent_fixture(&workspace, "orphan-review-agent", "orphan-review-heron");
        repository.create_workspace_agent(&session, &agent).unwrap();
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        db.execute(
            "INSERT INTO turns(id,session_id,status,input_json)
            VALUES('orphan-review-turn',?1,'running','[]')",
            [&session.id],
        )
        .unwrap();
        db.execute("INSERT INTO turn_admissions(turn_id,session_id,provider,projection_source,
            user_input_snapshot_json,dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at)
            VALUES('orphan-review-turn',?1,'codex','user_visible','{}','{}','{}',?2,101,101)",
            params![session.id,dispatch]).unwrap();
        let workspace = repository
            .get_workspace(&workspace.workspace_id)
            .unwrap()
            .unwrap();
        let preflight = repository
            .workspace_retirement_preflight(&workspace.workspace_id)
            .unwrap();
        assert!(preflight
            .blockers
            .iter()
            .any(|blocker| blocker.code == "native_turns_unresolved"));
        let command = retirement_command(&workspace, "orphan-review-retirement");
        assert!(repository.admit_workspace_retirement(&command).is_err());
        assert!(repository
            .get_operation(&command.operation_id)
            .unwrap()
            .is_none());
        let fences: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM workspace_retirement_fences",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(fences, 0);
    }
}

fn active_turn_review_case(initial_dispatch: &str) {
    let (temp, repository, workspace) = setup();
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "review-active-agent", "review-active-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO turns(id,session_id,status,input_json,provider_turn_ref)
        VALUES('active-review-turn',?1,'running','[]','native-review-turn')",
        [&session.id],
    )
    .unwrap();
    db.execute("INSERT INTO turn_admissions(turn_id,session_id,provider,projection_source,
        user_input_snapshot_json,dispatch_policy_json,correlation_json,dispatch_state,
        provider_native_turn_id,admitted_at,updated_at)
        VALUES('active-review-turn',?1,'codex','user_visible','{}','{}','{}',?2,'native-review-turn',101,101)",
        params![session.id,initial_dispatch]).unwrap();
    for (id, status) in [
        ("undecided-approval", "pending"),
        ("decided-approval", "decision_pending"),
    ] {
        db.execute("INSERT INTO approvals(id,session_id,turn_id,origin,status,request_json,response_json,created_at)
            VALUES(?1,?2,'active-review-turn','provider',?3,'{}','{\"decision\":\"allow\"}',101)",
            params![id,session.id,status]).unwrap();
    }
    session.active_turn_id = Some("active-review-turn".into());
    session.updated_at = 101;
    repository.upsert_session(&session).unwrap();
    db.execute("INSERT INTO managed_processes(process_id,workspace_id,owner_session_id,command_json,status,
        admission_order,queue_order,capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at,completion_state)
        VALUES('independent-native-process',?1,?2,'{}','running',1,1,1024,'out','err',101,101,'not_required')",
        params![workspace.workspace_id,session.id]).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "active-review-retirement");
    repository.admit_workspace_retirement(&command).unwrap();
    let next = repository
        .progress_workspace_retirement_turn(&command.operation_id, "active-review-turn", 102)
        .unwrap();
    if initial_dispatch == "dispatched" {
        assert!(matches!(
            next,
            runtime_core::WorkspaceRetirementTurnStep::Interrupt { .. }
        ));
    } else {
        assert_eq!(next, runtime_core::WorkspaceRetirementTurnStep::NeedsReview);
        let effects = repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap()
            .effects;
        assert!(
            effects.iter().all(|effect| effect.attempt_count == 0),
            "uncertain preexisting dispatch cannot create a new native attempt"
        );
    }
    repository
        .quarantine_workspace_retirement_interrupt(
            &command.operation_id,
            "active-review-turn",
            "native_interrupt_timeout",
            103,
        )
        .unwrap();
    db.execute("UPDATE turn_admissions SET dispatch_state='unknown',
        dispatch_error_json='{\"code\":\"native_ack_lost\",\"provider_dispatch_outcome\":\"unknown\"}'
        WHERE turn_id='active-review-turn'",[]).unwrap();

    // Reopen without reattaching or replaying any native operation. The
    // operator has independently closed the exact attachment named below.
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    restarted.initialize_schema().unwrap();
    let original = restarted
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let close = original
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    let identity = WorkspaceRetirementNativeIdentity {
        agent_id: session.id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: None,
        agent_revision: Some(agent.revision),
        session_updated_at: session.updated_at,
        active_turn_id: session.active_turn_id.clone(),
    };
    let prepare = |identity, key: &str| {
        prepare_workspace_retirement_review(&workspace.workspace_id,&command.operation_id,
        WorkspaceRetirementReviewRequest {effect_id:close.effect_id.clone(),
            evidence:"Operator verified this exact native attachment has closed and cannot execute its active turn".into(),
            resolution:WorkspaceRetirementReviewResolution::ProviderClosed {identity}},
        OperationActor::operator("native-review-operator"),Some(key.into())).unwrap()
    };
    for active in [None, Some("different-turn".into())] {
        let mut stale = identity.clone();
        stale.active_turn_id = active;
        assert!(restarted
            .reconcile_workspace_retirement(&prepare(stale, "stale-active-turn"))
            .is_err());
    }
    let valid = prepare(identity, "closed-active-attachment");
    assert!(
        restarted.reconcile_workspace_retirement(&valid).is_err(),
        "provider closure cannot imply an independent managed process has exited"
    );
    assert_eq!(
        restarted
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap(),
        original
    );
    db.execute(
        "UPDATE managed_processes SET status='killed',ended_at=110,terminal_recorded_at=110
        WHERE process_id='independent-native-process'",
        [],
    )
    .unwrap();
    let process = original
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_process_settle")
        .unwrap();
    let process_review = prepare_workspace_retirement_review(
        &workspace.workspace_id,
        &command.operation_id,
        WorkspaceRetirementReviewRequest {
            effect_id: process.effect_id.clone(),
            evidence: "Independent managed process terminal receipt is now available".into(),
            resolution: WorkspaceRetirementReviewResolution::RecheckSettled,
        },
        OperationActor::operator("native-review-operator"),
        Some("process-terminal-proof".into()),
    )
    .unwrap();
    assert!(
        restarted
            .reconcile_workspace_retirement(&process_review)
            .unwrap()
            .receipt
            .settled
    );
    let reviewed = restarted.reconcile_workspace_retirement(&valid).unwrap();
    assert_eq!(reviewed.resume_command, Some(command.clone()));
    let (turn_status,dispatch,prior_dispatch,active,status):(String,String,String,Option<String>,String)=db.query_row(
        "SELECT t.status,a.dispatch_state,json_extract(a.dispatch_error_json,'$.prior_dispatch_error.code'),
            s.active_turn_id,s.status FROM turns t JOIN turn_admissions a ON a.turn_id=t.id
            JOIN sessions s ON s.id=t.session_id WHERE t.id='active-review-turn'",[],
        |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).unwrap();
    assert_eq!(turn_status, "interrupted");
    assert_eq!(dispatch, "unknown");
    assert_eq!(prior_dispatch, "native_ack_lost");
    assert_eq!(active, None);
    assert_eq!(status, "closed");
    let approvals: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM approvals WHERE status='cancelled'
        AND json_extract(response_json,'$.prior_response.decision')='allow'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(approvals, 2);
    let proof_count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM workspace_retirement_closed_turns
        WHERE turn_id='active-review-turn'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(proof_count, 1);
    restarted
        .settle_passive_workspace_retirement(&command.operation_id, 112)
        .unwrap();
    restarted
        .finalize_settled_workspace_retirement(&command.operation_id, 113)
        .unwrap()
        .unwrap();
    assert_eq!(
        restarted
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap()
            .operation
            .phase,
        OperationPhase::Completed
    );
    assert_eq!(
        restarted
            .reconcile_workspace_retirement(&valid)
            .unwrap()
            .receipt,
        reviewed.receipt
    );

    // A later explicit restore creates a new attachment generation. It must
    // invalidate current close reuse without reviving a resolved historical turn.
    let active_workspace = register(
        &restarted,
        std::path::Path::new(&workspace.canonical_root),
        "after-active-review",
    );
    db.execute(
        "UPDATE workspace_agents SET lifecycle_state='active',archived_at=NULL,
        revision=revision+1 WHERE session_id=?1",
        [&session.id],
    )
    .unwrap();
    session.active_turn_id = None;
    session.status = "ready".into();
    session.updated_at = valid.reviewed_at + 1;
    restarted.upsert_session(&session).unwrap();
    let next = retirement_command(&active_workspace, "new-generation-retirement");
    restarted.admit_workspace_retirement(&next).unwrap();
    let close = restarted
        .get_operation(&next.operation_id)
        .unwrap()
        .unwrap()
        .effects
        .into_iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    assert_eq!(close.phase, "intended");
}
