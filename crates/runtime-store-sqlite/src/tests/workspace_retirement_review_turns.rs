use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::{register, retirement_command, setup};
use super::*;
use runtime_core::{
    prepare_workspace_retirement_review, OperationActor, OperationPhase, ProviderKind,
    WorkspaceRetirementNativeIdentity, WorkspaceRetirementReviewRequest,
    WorkspaceRetirementReviewResolution,
};

#[test]
fn retirement_turn_claim_requires_signed_attempt_headroom_before_native_interrupt() {
    let (temp, repository, workspace) = setup();
    let (mut session, agent) = workspace_agent_fixture(
        &workspace,
        "synthetic-retirement-turn-agent",
        "synthetic-retirement-heron",
    );
    repository.create_workspace_agent(&session, &agent).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO turns(id,session_id,status,input_json,provider_turn_ref)
         VALUES('signed-retirement-turn',?1,'running','[]','native-signed-turn')",
        [&session.id],
    )
    .unwrap();
    db.execute(
        "INSERT INTO turn_admissions(turn_id,session_id,provider,projection_source,
         user_input_snapshot_json,dispatch_policy_json,correlation_json,dispatch_state,
         provider_native_turn_id,admitted_at,updated_at)
         VALUES('signed-retirement-turn',?1,'codex','user_visible','{}','{}','{}',
                'dispatched','native-signed-turn',101,101)",
        [&session.id],
    )
    .unwrap();
    session.active_turn_id = Some("signed-retirement-turn".into());
    session.updated_at = 101;
    repository.upsert_session(&session).unwrap();
    let command = retirement_command(&workspace, "signed-retirement-turn-attempt");
    repository.admit_workspace_retirement(&command).unwrap();
    let effect = "workspace_turn_interrupt";
    db.execute(
        "UPDATE runtime_operation_effects SET attempt_count=?1
         WHERE operation_id=?2 AND effect_kind=?3 AND target_id='signed-retirement-turn'",
        params![i64::MAX, command.operation_id, effect],
    )
    .unwrap();
    let error = repository
        .progress_workspace_retirement_turn(&command.operation_id, "signed-retirement-turn", 102)
        .unwrap_err();
    assert!(
        matches!(
            error,
            runtime_core::RuntimeError::Conflict(_) | runtime_core::RuntimeError::InvalidState(_)
        ),
        "{error}"
    );
    let (attempt, kind, phase): (i64, String, String) = db
        .query_row(
            "SELECT attempt_count,typeof(attempt_count),phase FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?2 AND target_id='signed-retirement-turn'",
            params![command.operation_id, effect],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (attempt, kind, phase),
        (i64::MAX, "integer".into(), "intended".into())
    );

    db.execute(
        "UPDATE runtime_operation_effects SET attempt_count=?1
         WHERE operation_id=?2 AND effect_kind=?3 AND target_id='signed-retirement-turn'",
        params![i64::MAX - 1, command.operation_id, effect],
    )
    .unwrap();
    assert!(matches!(
        repository
            .progress_workspace_retirement_turn(
                &command.operation_id,
                "signed-retirement-turn",
                103
            )
            .unwrap(),
        runtime_core::WorkspaceRetirementTurnStep::Interrupt { .. }
    ));
    let (attempt, kind, phase): (i64, String, String) = db
        .query_row(
            "SELECT attempt_count,typeof(attempt_count),phase FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?2 AND target_id='signed-retirement-turn'",
            params![command.operation_id, effect],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (attempt, kind, phase),
        (i64::MAX, "integer".into(), "started".into())
    );
}

#[test]
fn restarted_prose_close_cannot_settle_exact_active_turn_or_unknown_dispatch() {
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

    // Reopen without native replay. Prose about the exact attachment is not a close receipt.
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
        assert!(matches!(
            restarted.reconcile_workspace_retirement(&prepare(stale, "stale-active-turn")),
            Err(runtime_core::RuntimeError::Conflict(_))
        ));
    }
    let valid = prepare(identity, "closed-active-attachment");
    assert!(
        matches!(restarted.reconcile_workspace_retirement(&valid),Err(runtime_core::RuntimeError::OperationUnresolved {operation_id,..}) if operation_id==command.operation_id)
    );
    let running_process: String = db
        .query_row(
            "SELECT status FROM managed_processes WHERE process_id='independent-native-process'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(running_process, "running");
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
    let after_process = restarted
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(after_process.operation.phase, OperationPhase::ManualReview);
    let before = super::legacy_native_turn_guards::raw_state(&db);
    for _ in 0..2 {
        assert!(
            matches!(restarted.reconcile_workspace_retirement(&valid),Err(runtime_core::RuntimeError::OperationUnresolved {operation_id,..}) if operation_id==command.operation_id)
        );
        assert_eq!(super::legacy_native_turn_guards::raw_state(&db), before);
    }
    let(turn_status,dispatch,original_error,active,status):(String,String,String,Option<String>,String)=db.query_row("SELECT t.status,a.dispatch_state,json_extract(a.dispatch_error_json,'$.code'),s.active_turn_id,s.status FROM turns t JOIN turn_admissions a ON a.turn_id=t.id JOIN sessions s ON s.id=t.session_id WHERE t.id='active-review-turn'",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).unwrap();
    assert_eq!(turn_status, "running");
    assert_eq!(dispatch, "unknown");
    assert_eq!(original_error, "native_ack_lost");
    assert_eq!(active, Some("active-review-turn".into()));
    assert_eq!(status, session.status);
    let approvals:(i64,i64)=db.query_row("SELECT (SELECT COUNT(*) FROM approvals WHERE id='undecided-approval' AND status='pending' AND json_extract(response_json,'$.decision')='allow'),(SELECT COUNT(*) FROM approvals WHERE id='decided-approval' AND status='decision_pending' AND json_extract(response_json,'$.decision')='allow')",[],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!(approvals, (1, 1));
    let proof_count:i64=db.query_row("SELECT COUNT(*) FROM workspace_retirement_closed_turns WHERE turn_id='active-review-turn'",[],|row|row.get(0)).unwrap();
    assert_eq!(proof_count, 0);
    assert!(matches!(
        restarted.finalize_settled_workspace_retirement(&command.operation_id, 113),
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    assert_eq!(super::legacy_native_turn_guards::raw_state(&db), before);
    assert_eq!(
        restarted.get_operation(&command.operation_id).unwrap(),
        Some(after_process)
    );
}

mod known_close;
