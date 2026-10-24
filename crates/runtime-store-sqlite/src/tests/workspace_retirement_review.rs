use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::{retirement_command, setup};
use super::*;
use runtime_core::{
    prepare_workspace_retirement_review, OperationActor, OperationActorKind, OperationPhase,
    ProviderKind, WorkspaceRetirementNativeIdentity, WorkspaceRetirementReviewRequest,
    WorkspaceRetirementReviewResolution,
};

fn request(
    effect_id: &str,
    identity: WorkspaceRetirementNativeIdentity,
) -> WorkspaceRetirementReviewRequest {
    WorkspaceRetirementReviewRequest {
        effect_id: effect_id.into(),
        evidence: "Operator inspected the exact native session and verified it is closed".into(),
        resolution: WorkspaceRetirementReviewResolution::ProviderClosed { identity },
    }
}

#[test]
fn operator_review_binds_identity_revision_and_replays_exact_receipt_without_extra_effects() {
    let (temp, repository, workspace) = setup();
    let (session, agent) = workspace_agent_fixture(&workspace, "review-agent", "review-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "unknown-close-review");
    repository.admit_workspace_retirement(&command).unwrap();
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &agent.agent_id, 200)
        .unwrap();
    repository
        .quarantine_workspace_retirement_native_close(
            &attempt,
            "transport_timeout_after_dispatch",
            201,
        )
        .unwrap();
    let original = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let effect = &original.effects[0];
    let identity = WorkspaceRetirementNativeIdentity {
        agent_id: agent.agent_id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        agent_revision: Some(agent.revision),
        session_updated_at: session.updated_at,
        active_turn_id: None,
    };
    let review = prepare_workspace_retirement_review(
        &workspace.workspace_id,
        &command.operation_id,
        request(&effect.effect_id, identity.clone()),
        OperationActor::operator("review-operator"),
        Some("verified-close".into()),
    )
    .unwrap();
    let mut unauthorized = review.clone();
    unauthorized.actor.kind = OperationActorKind::Agent;
    assert!(repository
        .reconcile_workspace_retirement(&unauthorized)
        .is_err());
    for (index, mut changed) in [
        identity.clone(),
        identity.clone(),
        identity.clone(),
        identity.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        match index {
            0 => changed.provider_session_ref = "another-native".into(),
            1 => changed.canonical_provider_session_ref = Some("another-canonical".into()),
            2 => changed.agent_revision = Some(agent.revision + 1),
            _ => changed.session_updated_at += 1,
        }
        let invalid = prepare_workspace_retirement_review(
            &workspace.workspace_id,
            &command.operation_id,
            request(&effect.effect_id, changed),
            OperationActor::operator("review-operator"),
            Some(format!("mismatch-{index}")),
        )
        .unwrap();
        assert!(repository.reconcile_workspace_retirement(&invalid).is_err());
    }
    assert_eq!(
        repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap(),
        original
    );
    let reviewed = repository.reconcile_workspace_retirement(&review).unwrap();
    assert!(reviewed.receipt.settled);
    assert_eq!(reviewed.receipt.operation_phase, OperationPhase::Requested);
    assert_eq!(reviewed.resume_command, Some(command.clone()));
    let replay = repository.reconcile_workspace_retirement(&review).unwrap();
    assert_eq!(replay.receipt, reviewed.receipt);
    assert!(replay.resume_command.is_none());
    let mut changed = request(&effect.effect_id, identity);
    changed.evidence = "Different assertion cannot reuse the original review key".into();
    let conflict = prepare_workspace_retirement_review(
        &workspace.workspace_id,
        &command.operation_id,
        changed,
        OperationActor::operator("review-operator"),
        Some("verified-close".into()),
    )
    .unwrap();
    assert!(repository
        .reconcile_workspace_retirement(&conflict)
        .is_err());
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 203)
        .unwrap();
    repository
        .finalize_settled_workspace_retirement(&command.operation_id, 204)
        .unwrap()
        .unwrap();
    let completed = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(completed.effects.len(), original.effects.len());
    assert_eq!(completed.effects[0].attempt_count, 1);
    let proof = completed.effects[0].evidence.as_ref().unwrap();
    assert_eq!(proof["proof_kind"], "operator_verified_closed");
    assert_eq!(
        proof["prior_evidence"]["reason"],
        "transport_timeout_after_dispatch"
    );
    assert_eq!(proof["prior_evidence"]["prior_evidence"]["started_at"], 200);
    assert_eq!(completed.transitions.len(), 3);
    assert_eq!(
        completed.transitions[1].from_phase,
        Some(OperationPhase::ManualReview)
    );
    assert_eq!(completed.transitions[1].to_phase, OperationPhase::Requested);
    assert_eq!(completed.transitions[2].sequence, 3);
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let (reviews, archives): (i64, i64) = db
        .query_row(
            "SELECT
        (SELECT COUNT(*) FROM workspace_retirement_reviews),
        (SELECT COUNT(*) FROM runtime_events WHERE kind='session.archived')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((reviews, archives), (1, 1));
    let replay = repository.reconcile_workspace_retirement(&review).unwrap();
    assert_eq!(replay.receipt, reviewed.receipt);
    assert_eq!(
        repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap(),
        completed
    );
    assert!(db
        .execute("DELETE FROM workspace_retirement_reviews", [])
        .is_err());
}

#[test]
fn operator_recheck_only_releases_independently_terminal_obligations_and_preserves_other_unknowns()
{
    let (temp, repository, workspace) = setup();
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "review-local-agent", "local-review-heron");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute("INSERT INTO turns(id,session_id,status,input_json) VALUES ('review-turn',?1,'running','[]')",[&session.id]).unwrap();
    db.execute("INSERT INTO turn_admissions(turn_id,session_id,provider,projection_source,
        user_input_snapshot_json,dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at)
        VALUES('review-turn',?1,'codex','user_visible','{}','{}','{}','dispatched',101,101)",[&session.id]).unwrap();
    session.active_turn_id = Some("review-turn".into());
    repository.upsert_session(&session).unwrap();
    db.execute("INSERT INTO agent_messages(id,scope,context_kind,workspace_id,sender_agent_id,
        recipient_agent_ids_json,input_json,image_paths_json,priority,policy,created_at)
        VALUES('review-message','direct','workspace_team',?1,?2,'[]','[]','[]','normal','non_interrupting',101)",
        params![workspace.workspace_id,session.id]).unwrap();
    db.execute("INSERT INTO agent_deliveries(id,message_id,recipient_agent_id,provider,status,created_at,updated_at)
        VALUES('review-delivery','review-message',?1,'codex','injecting',101,101)",[&session.id]).unwrap();
    db.execute("INSERT INTO workspace_system_notices(notice_id,workspace_id,recipient_agent_id,kind,
        correlation_id,input_json,state,created_at_ms,updated_at_ms)
        VALUES('review-notice',?1,?2,'membership','review-notice-correlation','[]','injecting',101,101)",
        params![workspace.workspace_id,session.id]).unwrap();
    db.execute("INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
        VALUES('review-parent','workspace',?1,1,'test',0,'{}',101)",[&workspace.workspace_id]).unwrap();
    db.execute("INSERT INTO workspace_hook_effects(workspace_id,parent_event_id,event_name,hook_name,
        hook_json,input_json,phase,attempt_count,created_at_ms,updated_at_ms)
        VALUES(?1,'review-parent','post_direct_message_send','review-hook','{}','{}','started',1,101,101)",
        [&workspace.workspace_id]).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "local-review");
    repository.admit_workspace_retirement(&command).unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 110)
        .unwrap();
    repository
        .quarantine_workspace_retirement_interrupt(
            &command.operation_id,
            "review-turn",
            "native_interrupt_unknown",
            111,
        )
        .unwrap();
    let original = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let targets = [
        "workspace_turn_interrupt",
        "workspace_delivery_settle",
        "workspace_notice_settle",
        "workspace_hook_settle",
    ];
    for kind in targets {
        let effect = original
            .effects
            .iter()
            .find(|effect| effect.effect_kind == kind)
            .unwrap();
        let request = WorkspaceRetirementReviewRequest {
            effect_id: effect.effect_id.clone(),
            evidence: "Rechecked current durable facts without contacting native adapters".into(),
            resolution: WorkspaceRetirementReviewResolution::RecheckSettled,
        };
        let review = prepare_workspace_retirement_review(
            &workspace.workspace_id,
            &command.operation_id,
            request,
            OperationActor::operator("review-operator"),
            Some(format!("still-unknown-{kind}")),
        )
        .unwrap();
        let result = repository.reconcile_workspace_retirement(&review).unwrap();
        assert!(!result.receipt.settled, "{kind}");
        assert_eq!(result.receipt.operation_phase, OperationPhase::ManualReview);
        assert!(result.resume_command.is_none());
    }
    db.execute(
        "UPDATE turns SET status='interrupted',completed_at=120 WHERE id='review-turn'",
        [],
    )
    .unwrap();
    session.active_turn_id = None;
    session.updated_at = 120;
    repository.upsert_session(&session).unwrap();
    db.execute("UPDATE agent_deliveries SET status='injected',injected_turn_id='review-turn' WHERE id='review-delivery'",[]).unwrap();
    db.execute("UPDATE workspace_system_notices SET state='delivered',turn_id='review-turn' WHERE notice_id='review-notice'",[]).unwrap();
    db.execute("UPDATE workspace_hook_effects SET phase='succeeded',receipt_json='{}',application_state='pending'
        WHERE hook_name='review-hook'",[]).unwrap();
    for (index, kind) in targets.into_iter().enumerate() {
        let effect = original
            .effects
            .iter()
            .find(|effect| effect.effect_kind == kind)
            .unwrap();
        let request = WorkspaceRetirementReviewRequest {
            effect_id: effect.effect_id.clone(),
            evidence: "Independent durable terminal result is now available for this exact effect"
                .into(),
            resolution: WorkspaceRetirementReviewResolution::RecheckSettled,
        };
        let review = prepare_workspace_retirement_review(
            &workspace.workspace_id,
            &command.operation_id,
            request,
            OperationActor::operator("review-operator"),
            Some(format!("terminal-{kind}")),
        )
        .unwrap();
        let result = repository.reconcile_workspace_retirement(&review).unwrap();
        assert!(result.receipt.settled, "{kind}");
        assert_eq!(result.receipt.unresolved_effect_ids.len(), 3 - index);
        assert_eq!(
            result.resume_command,
            if index == 3 {
                Some(command.clone())
            } else {
                None
            }
        );
    }
    let operation = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(operation.operation.phase, OperationPhase::Requested);
    assert_eq!(operation.effects.len(), original.effects.len());
    let close = operation
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    assert_eq!(close.phase, "intended");
    assert_eq!(close.attempt_count, 0);
    let application: String = db
        .query_row(
            "SELECT application_state FROM workspace_hook_effects WHERE hook_name='review-hook'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(application, "rejected");
}
