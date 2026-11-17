use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::{retirement_command, setup};
use super::*;
use runtime_core::{
    prepare_workspace_registration, prepare_workspace_retirement_review, OperationActor,
    OperationPhase, ProviderKind, ProviderRetirementCloseEvidence,
    ProviderRetirementCloseProofKind, RuntimeError, WorkspaceAgentRecord, WorkspaceRecord,
    WorkspaceRegisterRequest, WorkspaceRetirementAdmission, WorkspaceRetirementCommand,
    WorkspaceRetirementNativeIdentity, WorkspaceRetirementReviewRequest,
    WorkspaceRetirementReviewResolution,
};
use rusqlite::{params, Connection};
const PROOFS: [(Option<&str>, bool); 5] = [
    (Some("native_session_close_acknowledged"), true),
    (Some("native_process_terminated"), true),
    (Some("operator_verified_closed"), false),
    (Some("unrecognized_proof"), false),
    (None, false),
];
fn admitted(
    inputs: bool,
) -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    WorkspaceRecord,
    SessionRecord,
    WorkspaceAgentRecord,
    WorkspaceRetirementCommand,
) {
    let (temp, repository, workspace) = setup();
    let (session, agent) =
        workspace_agent_fixture(&workspace, "d4-review-agent", "d4-review-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    if inputs {
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        db.execute("INSERT INTO agent_messages(id,scope,context_kind,workspace_id,sender_agent_id,recipient_agent_ids_json,input_json,image_paths_json,priority,policy,created_at) VALUES('d4-message','direct','workspace_team',?1,?2,'[]','[]','[]','normal','non_interrupting',101)",params![workspace.workspace_id,session.id]).unwrap();
        db.execute("INSERT INTO agent_deliveries(id,message_id,recipient_agent_id,provider,status,created_at,updated_at) VALUES('d4-delivery','d4-message',?1,'codex','injecting',101,101)",[&session.id]).unwrap();
        db.execute("INSERT INTO workspace_system_notices(notice_id,workspace_id,recipient_agent_id,kind,correlation_id,input_json,state,created_at_ms,updated_at_ms) VALUES('d4-notice',?1,?2,'membership','d4-notice-correlation','[]','injecting',101,101)",params![workspace.workspace_id,session.id]).unwrap();
    }
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "d4-retirement");
    assert!(matches!(
        repository.admit_workspace_retirement(&command).unwrap(),
        WorkspaceRetirementAdmission::New(_)
    ));
    (temp, repository, workspace, session, agent, command)
}
fn strong_close(
    repository: &SqliteRuntimeRepository,
    command: &WorkspaceRetirementCommand,
    session: &SessionRecord,
    proof: Option<&str>,
) {
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &session.id, 200)
        .unwrap();
    let kind = if proof == Some("native_process_terminated") {
        ProviderRetirementCloseProofKind::NativeProcessTerminated
    } else {
        ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged
    };
    repository
        .finalize_workspace_retirement_native_close(
            &attempt,
            &ProviderRetirementCloseEvidence {
                runtime_session_id: session.id.clone(),
                provider_session_ref: session.provider_session_ref.clone().unwrap(),
                proof_kind: kind,
            },
            201,
        )
        .unwrap();
}
// Only owned disposable fixture databases are altered to represent history.
fn historical_proof(db: &Connection, operation_id: &str, proof: Option<&str>) {
    if let Some(proof) = proof {
        db.execute("UPDATE runtime_operation_effects SET evidence_json=json_set(evidence_json,'$.proof_kind',?2) WHERE operation_id=?1 AND effect_kind='workspace_provider_close'",params![operation_id,proof]).unwrap();
    } else {
        db.execute("UPDATE runtime_operation_effects SET evidence_json=json_remove(evidence_json,'$.proof_kind') WHERE operation_id=?1 AND effect_kind='workspace_provider_close'",[operation_id]).unwrap();
    }
}
#[test]
fn direct_retirement_review_refuses_prose_before_any_receipt_write() {
    let (temp, repository, workspace, session, agent, command) = admitted(false);
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &session.id, 200)
        .unwrap();
    repository
        .quarantine_workspace_retirement_native_close(&attempt, "lost_close_ack", 201)
        .unwrap();
    let before = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let effect = before
        .effects
        .iter()
        .find(|e| e.effect_kind == "workspace_provider_close")
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch("CREATE TRIGGER reject_d4_effect_write BEFORE UPDATE ON runtime_operation_effects BEGIN SELECT RAISE(ABORT,'D4 refusal attempted a receipt write'); END;").unwrap();
    for evidence in [
        "Operator claims the exact native attachment is closed",
        "Operator claims the exact native attachment is closed",
        "Another assertion does not provide a native receipt",
    ] {
        let review = prepare_workspace_retirement_review(
            &workspace.workspace_id,
            &command.operation_id,
            WorkspaceRetirementReviewRequest {
                effect_id: effect.effect_id.clone(),
                evidence: evidence.into(),
                resolution: WorkspaceRetirementReviewResolution::ProviderClosed {
                    identity: WorkspaceRetirementNativeIdentity {
                        agent_id: agent.agent_id.clone(),
                        provider: ProviderKind::Codex,
                        provider_session_ref: session.provider_session_ref.clone().unwrap(),
                        canonical_provider_session_ref: session
                            .canonical_provider_session_ref
                            .clone(),
                        agent_revision: Some(agent.revision),
                        session_updated_at: session.updated_at,
                        active_turn_id: None,
                    },
                },
            },
            OperationActor::operator("d4-reviewer"),
            Some("d4-review-key".into()),
        )
        .unwrap();
        assert!(
            matches!(repository.reconcile_workspace_retirement(&review),Err(RuntimeError::OperationUnresolved{operation_id,..})if operation_id==command.operation_id)
        );
        assert_eq!(
            repository
                .get_operation(&command.operation_id)
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            repository
                .get_workspace_agent(&workspace.workspace_id, &agent.agent_id)
                .unwrap()
                .unwrap(),
            agent
        );
    }
    let(reviews,status):(i64,String)=db.query_row("SELECT (SELECT COUNT(*) FROM workspace_retirement_reviews),(SELECT status FROM sessions WHERE id=?1)",[&session.id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(reviews, 0);
    assert_eq!(status, session.status);
}
#[test]
fn retirement_completion_requires_a_supported_strong_close_proof() {
    for (proof, accepted) in PROOFS {
        let (temp, repository, _workspace, session, _agent, command) = admitted(false);
        strong_close(&repository, &command, &session, proof);
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        historical_proof(&db, &command.operation_id, proof);
        let before = repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap();
        let result = repository
            .finalize_settled_workspace_retirement(&command.operation_id, 202)
            .unwrap();
        assert_eq!(result.is_some(), accepted, "{proof:?}");
        if !accepted {
            assert_eq!(
                repository
                    .get_operation(&command.operation_id)
                    .unwrap()
                    .unwrap(),
                before
            );
            assert_eq!(before.operation.phase, OperationPhase::Requested);
            assert!(!before.claims.is_empty());
        }
    }
}
#[test]
fn historical_operator_close_cannot_authorize_workspace_reactivation() {
    for (proof, accepted) in PROOFS {
        let (temp, repository, workspace, session, _agent, command) = admitted(false);
        strong_close(&repository, &command, &session, proof);
        repository
            .finalize_settled_workspace_retirement(&command.operation_id, 202)
            .unwrap()
            .unwrap();
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        historical_proof(&db, &command.operation_id, proof);
        let historical = repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap();
        let retired = repository
            .get_workspace(&workspace.workspace_id)
            .unwrap()
            .unwrap();
        let registration = prepare_workspace_registration(
            WorkspaceRegisterRequest {
                canonical_root: workspace.canonical_root.clone(),
                display_name: Some(workspace.display_name.clone()),
            },
            OperationActor::operator("d4-reviewer"),
            Some("d4-reactivation".into()),
        )
        .unwrap();
        let result = repository.register_workspace(&registration);
        assert_eq!(result.is_ok(), accepted, "{proof:?}: {result:?}");
        assert_eq!(
            repository
                .get_operation(&command.operation_id)
                .unwrap()
                .unwrap(),
            historical,
            "history remains inspectable, never rewritten as fresh proof"
        );
        if !accepted {
            assert!(
                matches!(&result, Err(RuntimeError::Conflict(message)) if message.contains("unverified provider attachments")),
                "{proof:?}: {result:?}"
            );
            assert_eq!(
                repository
                    .get_workspace(&workspace.workspace_id)
                    .unwrap()
                    .unwrap(),
                retired
            );
        }
    }
}
#[test]
fn pending_inputs_settle_only_behind_a_supported_exact_close_receipt() {
    for (proof, accepted) in PROOFS {
        let (temp, repository, workspace, session, _agent, command) = admitted(true);
        strong_close(&repository, &command, &session, proof);
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        historical_proof(&db, &command.operation_id, proof);
        repository
            .settle_passive_workspace_retirement(&command.operation_id, 202)
            .unwrap();
        let original = repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(original.operation.phase, OperationPhase::ManualReview);
        for kind in ["workspace_delivery_settle", "workspace_notice_settle"] {
            let effect = original
                .effects
                .iter()
                .find(|e| e.effect_kind == kind)
                .unwrap();
            let review = prepare_workspace_retirement_review(
                &workspace.workspace_id,
                &command.operation_id,
                WorkspaceRetirementReviewRequest {
                    effect_id: effect.effect_id.clone(),
                    evidence: "Recheck exact durable close and dispatch receipts".into(),
                    resolution: WorkspaceRetirementReviewResolution::RecheckSettled,
                },
                OperationActor::operator("d4-reviewer"),
                Some(format!("d4-recheck-{kind}")),
            )
            .unwrap();
            let result = repository.reconcile_workspace_retirement(&review).unwrap();
            assert_eq!(result.receipt.settled, accepted, "{proof:?}/{kind}");
            if !accepted {
                assert!(result.resume_command.is_none());
                assert_eq!(result.receipt.operation_phase, OperationPhase::ManualReview);
            }
        }
        let states:(String,String)=db.query_row("SELECT (SELECT status FROM agent_deliveries WHERE id='d4-delivery'),(SELECT state FROM workspace_system_notices WHERE notice_id='d4-notice')",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        let expected = if accepted { "cancelled" } else { "injecting" };
        assert_eq!(states, (expected.into(), expected.into()), "{proof:?}");
    }
}
