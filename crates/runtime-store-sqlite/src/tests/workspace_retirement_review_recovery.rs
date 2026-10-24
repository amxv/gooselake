use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::{retirement_command, setup};
use super::*;
use runtime_core::{
    prepare_workspace_retirement_review, OperationActor, OperationPhase, ProviderKind,
    ProviderRetirementCloseEvidence, ProviderRetirementCloseProofKind,
    WorkspaceRetirementNativeIdentity, WorkspaceRetirementReviewRequest,
    WorkspaceRetirementReviewResolution,
};

fn recheck(
    repository: &SqliteRuntimeRepository,
    command: &runtime_core::WorkspaceRetirementCommand,
    kind: &str,
    key: &str,
) -> runtime_core::WorkspaceRetirementReviewOutcome {
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let effect = operation
        .effects
        .iter()
        .find(|effect| effect.effect_kind == kind)
        .unwrap();
    let review = prepare_workspace_retirement_review(
        &command.workspace_id,
        &command.operation_id,
        WorkspaceRetirementReviewRequest {
            effect_id: effect.effect_id.clone(),
            evidence: "Operator rechecked persisted dispatch and native terminal facts".into(),
            resolution: WorkspaceRetirementReviewResolution::RecheckSettled,
        },
        OperationActor::operator("recovery-operator"),
        Some(key.into()),
    )
    .unwrap();
    repository.reconcile_workspace_retirement(&review).unwrap()
}

#[test]
fn pre_admission_delivery_and_notice_claims_cancel_only_behind_verified_native_closure() {
    let (temp, repository, workspace) = setup();
    let (session, agent) =
        workspace_agent_fixture(&workspace, "review-input-agent", "review-input-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute("INSERT INTO agent_messages(id,scope,context_kind,workspace_id,sender_agent_id,
        recipient_agent_ids_json,input_json,image_paths_json,priority,policy,created_at)
        VALUES('claimed-message','direct','workspace_team',?1,?2,'[]','[]','[]','normal','non_interrupting',101)",
        params![workspace.workspace_id,session.id]).unwrap();
    db.execute("INSERT INTO agent_deliveries(id,message_id,recipient_agent_id,provider,status,created_at,updated_at)
        VALUES('claimed-delivery','claimed-message',?1,'codex','injecting',101,101)",[&session.id]).unwrap();
    db.execute("INSERT INTO workspace_system_notices(notice_id,workspace_id,recipient_agent_id,kind,
        correlation_id,input_json,state,created_at_ms,updated_at_ms)
        VALUES('claimed-notice',?1,?2,'membership','claimed-notice-correlation','[]','injecting',101,101)",
        params![workspace.workspace_id,session.id]).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "claimed-input-review");
    repository.admit_workspace_retirement(&command).unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 102)
        .unwrap();
    assert!(
        !recheck(
            &repository,
            &command,
            "workspace_delivery_settle",
            "before-closure-delivery"
        )
        .receipt
        .settled
    );
    assert!(
        !recheck(
            &repository,
            &command,
            "workspace_notice_settle",
            "before-closure-notice"
        )
        .receipt
        .settled
    );
    let close = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap()
        .effects
        .into_iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    let review = prepare_workspace_retirement_review(
        &workspace.workspace_id,
        &command.operation_id,
        WorkspaceRetirementReviewRequest {
            effect_id: close.effect_id,
            evidence:
                "Operator independently terminated and verified the exact provider attachment"
                    .into(),
            resolution: WorkspaceRetirementReviewResolution::ProviderClosed {
                identity: WorkspaceRetirementNativeIdentity {
                    agent_id: agent.agent_id.clone(),
                    provider: ProviderKind::Codex,
                    provider_session_ref: session.provider_session_ref.clone().unwrap(),
                    canonical_provider_session_ref: None,
                    agent_revision: Some(agent.revision),
                    session_updated_at: session.updated_at,
                    active_turn_id: None,
                },
            },
        },
        OperationActor::operator("recovery-operator"),
        Some("close-verified".into()),
    )
    .unwrap();
    let result = repository.reconcile_workspace_retirement(&review).unwrap();
    assert_eq!(result.receipt.operation_phase, OperationPhase::ManualReview);
    assert!(result.resume_command.is_none());
    assert!(
        recheck(
            &repository,
            &command,
            "workspace_delivery_settle",
            "after-closure-delivery"
        )
        .receipt
        .settled
    );
    let last = recheck(
        &repository,
        &command,
        "workspace_notice_settle",
        "after-closure-notice",
    );
    assert!(last.receipt.settled);
    assert_eq!(last.resume_command, Some(command.clone()));
    let states: (String, String) = db
        .query_row(
            "SELECT (SELECT status FROM agent_deliveries WHERE id='claimed-delivery'),
        (SELECT state FROM workspace_system_notices WHERE notice_id='claimed-notice')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(states, ("cancelled".into(), "cancelled".into()));
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 200)
        .unwrap();
    assert!(repository
        .finalize_settled_workspace_retirement(&command.operation_id, 201)
        .unwrap()
        .is_some());
    let close = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap()
        .effects
        .into_iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    assert_eq!(
        close.attempt_count, 0,
        "operator verification never issues a runtime native close"
    );
}

#[test]
fn process_completion_review_preserves_deferred_output_receipt_for_retirement_finalization() {
    let (temp, repository, workspace) = setup();
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "review-process-agent", "review-process-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute("INSERT INTO turns(id,session_id,status,input_json) VALUES ('completion-turn',?1,'running','[]')",[&session.id]).unwrap();
    db.execute("INSERT INTO turn_admissions(turn_id,session_id,provider,projection_source,
        user_input_snapshot_json,dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at)
        VALUES('completion-turn',?1,'codex','automation_context','{}','{}',
        '{\"correlation_id\":\"process_completion:review-process\"}','dispatching',101,101)",[&session.id]).unwrap();
    session.active_turn_id = Some("completion-turn".into());
    repository.upsert_session(&session).unwrap();
    db.execute("INSERT INTO managed_processes(process_id,workspace_id,owner_session_id,command_json,status,
        admission_order,queue_order,capture_limit_bytes,stdout_path,stderr_path,admitted_at,updated_at,
        terminal_recorded_at,ended_at,completion_state)
        VALUES('review-process',?1,?2,'{}','completed',1,1,1024,'out','err',100,101,101,101,'injecting')",
        params![workspace.workspace_id,session.id]).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "process-review");
    repository.admit_workspace_retirement(&command).unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 110)
        .unwrap();
    repository
        .quarantine_workspace_retirement_interrupt(
            &command.operation_id,
            "completion-turn",
            "dispatch_unknown",
            111,
        )
        .unwrap();
    assert!(
        !recheck(
            &repository,
            &command,
            "workspace_process_settle",
            "process-unknown"
        )
        .receipt
        .settled
    );
    db.execute("UPDATE turn_admissions SET dispatch_state='not_dispatched' WHERE turn_id='completion-turn'",[]).unwrap();
    db.execute(
        "UPDATE turns SET status='failed',completed_at=120 WHERE id='completion-turn'",
        [],
    )
    .unwrap();
    session.active_turn_id = None;
    session.updated_at = 120;
    repository.upsert_session(&session).unwrap();
    assert!(
        recheck(
            &repository,
            &command,
            "workspace_turn_interrupt",
            "process-turn-terminal"
        )
        .receipt
        .settled
    );
    let reviewed = recheck(
        &repository,
        &command,
        "workspace_process_settle",
        "completion-terminal",
    );
    assert_eq!(reviewed.resume_command, Some(command.clone()));
    let state = repository
        .get_managed_process("review-process")
        .unwrap()
        .unwrap();
    assert_eq!(state.completion_state, "pending");
    assert_eq!(
        state.completion_last_error.as_deref(),
        Some("workspace_retirement_deferred_completion")
    );
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &session.id, 130)
        .unwrap();
    repository
        .finalize_workspace_retirement_native_close(
            &attempt,
            &ProviderRetirementCloseEvidence {
                runtime_session_id: session.id,
                provider_session_ref: session.provider_session_ref.unwrap(),
                proof_kind: ProviderRetirementCloseProofKind::NativeProcessTerminated,
            },
            131,
        )
        .unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 132)
        .unwrap();
    assert!(repository
        .finalize_settled_workspace_retirement(&command.operation_id, 133)
        .unwrap()
        .is_some());
}

#[test]
fn missing_native_attachment_identity_is_rejected_before_a_retirement_fence_exists() {
    let (temp, repository, workspace) = setup();
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "missing-native-agent", "missing-native-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    session.provider_session_ref = None;
    repository.upsert_session(&session).unwrap();
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
        .any(|blocker| blocker.code == "native_attachment_unresolved"));
    let command = retirement_command(&workspace, "missing-native-retirement");
    assert!(repository.admit_workspace_retirement(&command).is_err());
    assert!(repository
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let fences: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM workspace_retirement_fences",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(fences, 0);
}
