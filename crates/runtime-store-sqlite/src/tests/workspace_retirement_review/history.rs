use super::*;
#[test]
fn historical_prose_close_receipt_remains_inspectable_without_new_authority() {
    let (temp, repository, workspace) = setup();
    let (session, agent) = workspace_agent_fixture(
        &workspace,
        "historical-review-agent",
        "historical-review-heron",
    );
    repository.create_workspace_agent(&session, &agent).unwrap();
    let workspace = repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap();
    let command = retirement_command(&workspace, "historical-close-review");
    repository.admit_workspace_retirement(&command).unwrap();
    let original = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    let effect = original
        .effects
        .iter()
        .find(|effect| effect.effect_kind == "workspace_provider_close")
        .unwrap();
    let mut review = prepare_workspace_retirement_review(
        &workspace.workspace_id,
        &command.operation_id,
        request(
            &effect.effect_id,
            WorkspaceRetirementNativeIdentity {
                agent_id: agent.agent_id.clone(),
                provider: ProviderKind::Codex,
                provider_session_ref: session.provider_session_ref.clone().unwrap(),
                canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
                agent_revision: Some(agent.revision),
                session_updated_at: session.updated_at,
                active_turn_id: None,
            },
        ),
        OperationActor::operator("review-operator"),
        Some("historical-verified-close".into()),
    )
    .unwrap();
    review.reviewed_at = 201;
    // Normal ACK establishes coherent archived projections in this separate fixture.
    let attempt = repository
        .claim_workspace_retirement_native_close(&command.operation_id, &session.id, 200)
        .unwrap();
    repository
        .finalize_workspace_retirement_native_close(
            &attempt,
            &runtime_core::ProviderRetirementCloseEvidence {
                runtime_session_id: session.id.clone(),
                provider_session_ref: session.provider_session_ref.clone().unwrap(),
                proof_kind:
                    runtime_core::ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
            },
            201,
        )
        .unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, 202)
        .unwrap();
    repository
        .finalize_settled_workspace_retirement(&command.operation_id, 203)
        .unwrap()
        .unwrap();
    // Model retained pre-D4 history; no fresh prose method produces this state.
    let receipt = runtime_core::WorkspaceRetirementReviewReceipt {
        review_id: review.review_id.clone(),
        workspace_id: workspace.workspace_id.clone(),
        operation_id: command.operation_id.clone(),
        effect_id: effect.effect_id.clone(),
        settled: true,
        operation_phase: OperationPhase::Requested,
        unresolved_effect_ids: Vec::new(),
        reviewed_at: review.reviewed_at,
    };
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let historical_review = serde_json::json!({"review_id":review.review_id,"operator":review.actor.identifier,"evidence":review.request.evidence,"resolution":review.request.resolution,"reviewed_at":review.reviewed_at});
    db.execute("UPDATE runtime_operation_effects SET evidence_json=json_set(evidence_json,'$.proof_kind','operator_verified_closed','$.operator_review',json(?2)) WHERE operation_id=?1 AND effect_kind='workspace_provider_close'",params![command.operation_id,serde_json::to_string(&historical_review).unwrap()]).unwrap();
    db.execute("INSERT INTO workspace_retirement_reviews(review_id,operation_id,effect_id,actor_id,idempotency_key,normalized_request_hash,normalized_request_json,receipt_json,reviewed_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![review.review_id,command.operation_id,effect.effect_id,review.actor.identifier,review.idempotency_key,review.normalized_request_hash,serde_json::to_string(&review.normalized_request).unwrap(),serde_json::to_string(&receipt).unwrap(),review.reviewed_at]).unwrap();
    let before = crate::tests::legacy_native_turn_guards::raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    for candidate in [&repository, &reopened] {
        let replay = candidate.reconcile_workspace_retirement(&review).unwrap();
        assert_eq!(replay.receipt, receipt);
        assert!(replay.resume_command.is_none());
        assert_eq!(
            crate::tests::legacy_native_turn_guards::raw_state(&db),
            before
        );
    }
    let mut different_request = review.request.clone();
    different_request.evidence = "Different prose cannot replace historical review".into();
    let different = prepare_workspace_retirement_review(
        &workspace.workspace_id,
        &command.operation_id,
        different_request,
        review.actor.clone(),
        Some(review.idempotency_key.clone()),
    )
    .unwrap();
    assert!(matches!(
        reopened.reconcile_workspace_retirement(&different),
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    assert_eq!(
        crate::tests::legacy_native_turn_guards::raw_state(&db),
        before
    );
    assert!(db
        .execute("DELETE FROM workspace_retirement_reviews", [])
        .is_err());
    assert_eq!(
        crate::tests::legacy_native_turn_guards::raw_state(&db),
        before
    );
}
