use super::*;
use seeds::{approval_decision, Seeds};

pub(super) fn refuse_dispatch_decision_and_completion(
    repository: &SqliteRuntimeRepository,
    seeds: &Seeds,
) {
    no_write(repository, || {
        for admitted in [&seeds.turn, &seeds.process_turn] {
            assert!(matches!(
                repository.upsert_turn_admission(admitted),
                Err(RuntimeError::TurnDispatchNotClaimed { .. })
            ));
        }
        for attempt in ["n07_attempt_1", "n07_attempt_2"] {
            let (reserved, event) = approval_decision(&seeds.approval.pending, attempt);
            assert!(matches!(
                repository.claim_provider_approval_decision(
                    &seeds.approval.pending,
                    &reserved,
                    &seeds.approval.turn,
                    &seeds.approval.session,
                    &event
                ),
                Err(RuntimeError::Conflict(_))
            ));
        }
        for state in ["injecting", "delivered"] {
            assert!(repository
                .update_managed_process_completion(
                    "n07_process",
                    &ProcessCompletionUpdate {
                        state: state.into(),
                        turn_id: Some(seeds.process_turn.turn_id.clone()),
                        attempt_count: 1,
                        last_error: None,
                        updated_at: 900
                    }
                )
                .unwrap()
                .is_none());
        }
    });
}

pub(super) fn quarantine_and_refuse(repository: &SqliteRuntimeRepository, seeds: &Seeds) {
    let before = image(&repository.database_path);
    assert_eq!(
        repository
            .quarantine_started_team_hook_effects(1000)
            .unwrap(),
        1
    );
    let edits = repository.recover_session_edit_reruns(1000).unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].operation_id, seeds.edit.operation_id);
    assert_eq!(edits[0].phase, "manual_review");
    assert_eq!(
        repository
            .quarantine_started_workspace_agent_rebinds(1000)
            .unwrap(),
        vec![seeds.rebind.operation.operation_id.clone()]
    );
    assert_eq!(
        repository
            .quarantine_ambiguous_retirement_effects_after_restart(1000)
            .unwrap(),
        1
    );
    let after = image(&repository.database_path);
    assert_eq!(
        before.tables.keys().collect::<Vec<_>>(),
        after.tables.keys().collect::<Vec<_>>()
    );
    assert_eq!(before.schema, after.schema);
    assert_eq!(before.user_version, after.user_version);
    assert_eq!(before.application_id, after.application_id);
    let quarantine_carriers = [
        "workspace_hook_effects",
        "session_edit_reruns",
        "workspace_agent_rebinds",
        "runtime_operations",
        "runtime_operation_effects",
        "workspace_retirement_fences",
        "runtime_operation_transitions",
        "runtime_events",
        "runtime_event_subjects",
        "sqlite_sequence",
    ];
    for (name, rows) in &before.tables {
        if !quarantine_carriers.contains(&name.as_str()) {
            assert_eq!(
                &after.tables[name], rows,
                "quarantine changed unrelated authority carrier {name}"
            );
        }
    }
    for table in [
        "managed_worktree_claims",
        "runtime_operation_resource_claims",
        "runtime_operation_resource_fences",
    ] {
        assert_eq!(after.tables[table], before.tables[table], "{table}");
    }
    no_write(repository, || {
        assert_eq!(
            repository
                .quarantine_started_team_hook_effects(1001)
                .unwrap(),
            0
        );
        assert!(repository
            .recover_session_edit_reruns(1001)
            .unwrap()
            .is_empty());
        assert!(repository
            .quarantine_started_workspace_agent_rebinds(1001)
            .unwrap()
            .is_empty());
        assert_eq!(
            repository
                .quarantine_ambiguous_retirement_effects_after_restart(1001)
                .unwrap(),
            0
        );
    });
    no_write(repository, || {
        assert!(repository
            .claim_team_hook_effect(
                &seeds.hook.workspace_id,
                &seeds.hook.parent_event_id,
                seeds.hook.hook.event_name,
                &seeds.hook.hook.name,
                1100
            )
            .is_err());
        assert!(matches!(
            repository.resolve_session_edit_rerun_review(
                &seeds.edit.operation_id,
                "I verified the provider",
                1100
            ),
            Err(RuntimeError::Conflict(_))
        ));
        assert_eq!(
            repository
                .begin_workspace_agent_rebind(&seeds.rebind.operation)
                .unwrap()
                .phase,
            "manual_review"
        );
        assert!(repository
            .claim_workspace_retirement_native_close(
                &seeds.retirement.operation_id,
                &seeds.retiring_session,
                1100
            )
            .is_err());
        assert!(repository
            .finalize_settled_workspace_retirement(&seeds.retirement.operation_id, 1100)
            .is_err());
    });
    let hook = repository
        .get_team_hook_effect(
            &seeds.hook.workspace_id,
            &seeds.hook.parent_event_id,
            seeds.hook.hook.event_name,
            &seeds.hook.hook.name,
        )
        .unwrap()
        .unwrap();
    assert_eq!(hook.phase, TeamHookEffectPhase::ManualReview);
    assert_eq!(hook.attempt_count, 1);
    assert!(hook.receipt.is_none());
    let edit = repository
        .get_session_edit_rerun(&seeds.edit.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(edit.phase, "manual_review");
    assert!(edit.child_provider_session_ref.is_none());
    assert!(edit.child_canonical_provider_session_ref.is_none());
    assert!(edit.replacement_turn_id.is_none());
    assert!(repository
        .unresolved_session_edit_rerun(&edit.agent_id)
        .unwrap());
    let rebind = repository
        .get_workspace_agent_rebind(&seeds.rebind.operation.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(rebind.phase, "manual_review");
    assert!(rebind.provider_evidence.is_none());
    let session = repository
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.id == seeds.rebind.session.id)
        .unwrap();
    assert_eq!(session, seeds.rebind.session);
    assert_eq!(
        repository
            .get_workspace_agent(
                &seeds.rebind.operation.workspace_id,
                &seeds.rebind.session.id
            )
            .unwrap()
            .unwrap(),
        seeds.rebind.agent
    );
    let retirement = repository
        .get_operation(&seeds.retirement.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(retirement.operation.phase, OperationPhase::ManualReview);
    let close = retirement
        .effects
        .iter()
        .find(|e| {
            e.effect_kind == "workspace_provider_close" && e.target_id == seeds.retiring_session
        })
        .unwrap();
    assert_eq!(close.phase, "manual_review");
    assert_eq!(close.attempt_count, 1);
    assert!(repository
        .list_recoverable_workspace_retirements()
        .unwrap()
        .is_empty());
    assert_eq!(
        repository
            .retiring_operation_for_session(&seeds.retiring_session)
            .unwrap(),
        Some(seeds.retirement.operation_id.clone())
    );
    refuse_dispatch_decision_and_completion(repository, seeds);
}
