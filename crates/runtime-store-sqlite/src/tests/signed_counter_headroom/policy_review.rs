use super::*;
use crate::tests::legacy_native_turn_guards::raw_state;

fn review_command(
    command: &OperatorNativePolicyCommand,
    session: &SessionRecord,
    resolution: OperatorNativePolicyReviewResolution,
) -> OperatorNativePolicyReviewCommand {
    OperatorNativePolicyReviewCommand {
        operation_id: command.operation_id.clone(),
        agent_id: command.agent_id.clone(),
        actor: command.actor.clone(),
        input: OperatorNativePolicyReviewInput {
            resolution,
            evidence: "A prose assertion does not establish the native outcome".into(),
            provider: session.provider.clone(),
            provider_session_ref: session.provider_session_ref.clone().unwrap(),
            canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
            expected_revision: command.expected_revision,
            session_updated_at: session.updated_at,
        },
        reviewed_at: 205,
    }
}
fn assert_review_refused_at(resolution: OperatorNativePolicyReviewResolution, revision: i64) {
    let (temp, repository, session, agent, mut command) = legacy_native_policy_fixture();
    repository
        .mark_operator_native_policy_uncertain(
            &command.operation_id,
            "synthetic unknown native outcome",
            102,
        )
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    synthesize_legacy_policy_intent_at_revision(&db, &mut command, revision);
    let review = review_command(&command, &session, resolution);
    let original = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    for candidate in [&repository, &reopened] {
        assert!(
            matches!(candidate.resolve_operator_native_policy_review(&review),
            Err(RuntimeError::OperationUnresolved { operation_id, .. }) if operation_id == command.operation_id)
        );
        assert_eq!(raw_state(&db), before);
        assert_eq!(
            candidate.get_operation(&command.operation_id).unwrap(),
            Some(original.clone())
        );
    }
    let actual: (i64,String,String) = db.query_row(
        "SELECT a.revision,typeof(a.revision),s.status FROM workspace_agents a JOIN sessions s ON s.id=a.session_id WHERE a.session_id=?1",
        [&agent.agent_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
    assert_eq!(actual, (revision, "integer".into(), session.status));
    assert_eq!(
        original.operation.phase,
        runtime_core::OperationPhase::ManualReview
    );
    assert_eq!(original.effects[0].phase, "uncertain");
    assert_eq!(original.claims.len(), 1);
}
#[test]
fn legacy_native_policy_operator_review_applied_refuses_at_both_signed_boundaries() {
    for revision in [i64::MAX, i64::MAX - 1] {
        assert_review_refused_at(OperatorNativePolicyReviewResolution::Applied, revision);
    }
}
#[test]
fn legacy_native_policy_not_applied_review_cannot_clear_unknown_at_signed_max() {
    assert_review_refused_at(OperatorNativePolicyReviewResolution::NotApplied, i64::MAX);
}
#[test]
fn historical_not_applied_review_replays_without_clearing_its_startup_fence() {
    let (temp, repository, session, agent, mut command) = legacy_native_policy_fixture();
    repository
        .mark_operator_native_policy_uncertain(
            &command.operation_id,
            "historical unknown native outcome",
            102,
        )
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    synthesize_legacy_policy_intent_at_revision(&db, &mut command, i64::MAX);
    let review = review_command(
        &command,
        &session,
        OperatorNativePolicyReviewResolution::NotApplied,
    );
    let original = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let response = runtime_core::OperatorNativePolicyReviewResponse {
        operation_id: command.operation_id.clone(),
        agent_id: agent.agent_id.clone(),
        resolution: OperatorNativePolicyReviewResolution::NotApplied,
        agent_revision: i64::MAX as u64,
        restart_required: true,
        reviewed_at_ms: review.reviewed_at,
    };
    // Represent a pre-D4 receipt in this disposable fixture. No fresh review creates it.
    let evidence = serde_json::json!({"outcome":"operator_verified_not_applied","prior_evidence":original.effects[0].evidence,"operator_review":{"input":review.input,"response":response,"actor":review.actor,"reviewed_at_ms":review.reviewed_at}});
    let exact = serde_json::json!({"operation_id":command.operation_id,"code":"operator_verified_not_applied","agent_revision":i64::MAX as u64});
    let tx = db.unchecked_transaction().unwrap();
    tx.execute(
        "UPDATE sessions SET status='policy_recovery_required',updated_at=?2 WHERE id=?1",
        params![agent.agent_id, review.reviewed_at],
    )
    .unwrap();
    tx.execute("UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?2,updated_at=?3 WHERE operation_id=?1",params![command.operation_id,serde_json::to_string(&evidence).unwrap(),review.reviewed_at]).unwrap();
    let authority = crate::operation_tx::OperationAuthorityTransaction::new(&tx);
    authority
        .terminalize(
            &command.operation_id,
            runtime_core::OperationPhase::ManualReview,
            runtime_core::OperationPhase::Failed,
            Some(&agent.workspace_id),
            &exact,
            Some("operator_verified_not_applied"),
            Some("operator verified native policy is not applied"),
            review.reviewed_at,
        )
        .unwrap();
    authority.append_transition(&runtime_core::OperationTransitionRecord { operation_id:command.operation_id.clone(),sequence:3,from_phase:Some(runtime_core::OperationPhase::ManualReview),to_phase:runtime_core::OperationPhase::Failed,evidence:Some(serde_json::json!({"receipt":"operator_verified_native_policy","resolution":"not_applied","reviewed_at_ms":review.reviewed_at})),created_at:review.reviewed_at }).unwrap();
    authority.release_claim(&original.claims[0]).unwrap();
    tx.commit().unwrap();
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    for candidate in [&repository, &reopened] {
        assert_eq!(
            candidate
                .resolve_operator_native_policy_review(&review)
                .unwrap(),
            response
        );
        assert!(candidate
            .operator_native_policy_reattachment_pending(&agent.agent_id)
            .unwrap());
        assert_eq!(raw_state(&db), before);
    }
    let mut different = review.clone();
    different.input.evidence = "Different prose cannot replace a historical receipt".into();
    assert!(matches!(
        reopened.resolve_operator_native_policy_review(&different),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(raw_state(&db), before);
    let actual:(i64,String,String)=db.query_row("SELECT a.revision,typeof(a.revision),s.status FROM workspace_agents a JOIN sessions s ON s.id=a.session_id WHERE a.session_id=?1",[&agent.agent_id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
    assert_eq!(
        actual,
        (
            i64::MAX,
            "integer".into(),
            "policy_recovery_required".into()
        )
    );
    let historical = reopened
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        historical.operation.phase,
        runtime_core::OperationPhase::Failed
    );
    assert_eq!(historical.effects[0].phase, "finalized");
    assert!(historical.claims.is_empty());
}
