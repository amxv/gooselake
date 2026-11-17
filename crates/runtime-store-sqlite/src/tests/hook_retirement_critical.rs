use super::legacy_native_turn_guards::raw_state;
use super::workspace_agent::workspace_agent_fixture;
use super::workspace_retirement::{register, retirement_command, setup};
use super::*;
use runtime_core::{
    OperationPhase, TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase,
    TeamHookEffectReceipt, TeamHookEffectRecord, TeamHookEventName, TeamHookResolvedEntry,
    TeamHookScope, WorkspaceRetirementReviewRequest, WorkspaceRetirementReviewResolution,
};
use serde_json::json;

fn hook() -> TeamHookResolvedEntry {
    TeamHookResolvedEntry {
        event_name: TeamHookEventName::PostTeammateAdd,
        name: "retirement_hook".into(),
        command: "printf '{}'".into(),
        timeout_ms: 500,
        scope: TeamHookScope::Project,
    }
}
fn approve(repo: &SqliteRuntimeRepository, workspace: &str) {
    repo.update_team_hook_approval(&TeamHookApprovalUpdate {
        workspace_id: workspace.into(),
        expected_revision: 0,
        hooks: vec![hook()],
        approved_at_ms: 10,
    })
    .unwrap();
}
fn admit_hook(
    repo: &SqliteRuntimeRepository,
    workspace: &str,
    parent: &str,
) -> TeamHookEffectRecord {
    repo.append_runtime_event(&NewRuntimeEvent {
        event_id: parent.into(),
        scope: RuntimeEventScope::Workspace,
        scope_id: workspace.into(),
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: "workspace.member_added".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({"workspace_id":workspace}),
        provider: None,
        provider_seq: None,
        created_at: 10,
    })
    .unwrap();
    repo.admit_team_hook_effect(&TeamHookEffectAdmission {
        workspace_id: workspace.into(),
        parent_event_id: parent.into(),
        hook: hook(),
        input: json!({"event_id":parent,"hook_event":"post_teammate_add"}),
        created_at_ms: 11,
    })
    .unwrap()
}
fn start(repo: &SqliteRuntimeRepository, effect: &TeamHookEffectRecord) {
    repo.claim_team_hook_effect(
        &effect.workspace_id,
        &effect.parent_event_id,
        effect.hook.event_name,
        &effect.hook.name,
        12,
    )
    .unwrap();
}

#[test]
fn passive_hook_and_target_receipt_failure_roll_back_together() {
    let (_temp, repo, workspace) = setup();
    approve(&repo, &workspace.workspace_id);
    let effect = admit_hook(&repo, &workspace.workspace_id, "passive_hook_parent");
    let command = retirement_command(&workspace, "hook-passive");
    repo.admit_workspace_retirement(&command).unwrap();
    let db = open_connection(&repo.database_path).unwrap();
    let before = raw_state(&db);
    db.execute_batch(
        "CREATE TRIGGER fail_retirement_hook BEFORE INSERT ON runtime_events
        WHEN NEW.kind='workspace.retirement_hook_settled'
        BEGIN SELECT RAISE(ABORT,'retirement hook receipt fault'); END;",
    )
    .unwrap();
    assert!(repo
        .settle_passive_workspace_retirement(&command.operation_id, 30)
        .is_err());
    assert_eq!(raw_state(&db), before);
    db.execute_batch("DROP TRIGGER fail_retirement_hook")
        .unwrap();
    repo.settle_passive_workspace_retirement(&command.operation_id, 30)
        .unwrap();
    let saved = repo
        .get_team_hook_effect(
            &effect.workspace_id,
            &effect.parent_event_id,
            effect.hook.event_name,
            &effect.hook.name,
        )
        .unwrap()
        .unwrap();
    assert_eq!(saved.phase, TeamHookEffectPhase::Failed);
    assert_eq!(saved.attempt_count, 0);
    assert!(saved.receipt.is_none());
    assert_eq!(saved.application_state, "rejected");
    let operation = repo.get_operation(&command.operation_id).unwrap().unwrap();
    assert!(operation
        .effects
        .iter()
        .filter(|effect| effect.effect_kind == "workspace_hook_settle")
        .all(|effect| effect.phase == "finalized"));
    let before = raw_state(&db);
    repo.settle_passive_workspace_retirement(&command.operation_id, 99)
        .unwrap();
    assert_eq!(raw_state(&db), before);
}

fn passive_hook_receipt_preserves_selected_attempts(later_target: bool) {
    let (_temp, repo, workspace) = setup();
    approve(&repo, &workspace.workspace_id);
    let first = admit_hook(&repo, &workspace.workspace_id, "a_passive_authority");
    let second = admit_hook(&repo, &workspace.workspace_id, "z_passive_authority");
    let command = retirement_command(&workspace, "passive-authority");
    repo.admit_workspace_retirement(&command).unwrap();
    let affected = if later_target { &second } else { &first };
    let target = json!([
        affected.parent_event_id,
        affected.hook.event_name.as_str(),
        affected.hook.name
    ])
    .to_string();
    let db = open_connection(&repo.database_path).unwrap();
    let before = raw_state(&db);
    db.execute_batch(&format!(
        "CREATE TRIGGER drift_selected_target AFTER INSERT ON runtime_events
         WHEN NEW.kind='workspace.hook_effect_transitioned'
           AND json_extract(NEW.payload_json,'$.origin')='passive_retirement'
           AND json_extract(NEW.payload_json,'$.parent_event_id')='a_passive_authority'
         BEGIN UPDATE runtime_operation_effects SET attempt_count=attempt_count+1
           WHERE operation_id='{}' AND effect_kind='workspace_hook_settle'
             AND target_id='{target}'; END;",
        command.operation_id
    ))
    .unwrap();
    assert!(
        repo.settle_passive_workspace_retirement(&command.operation_id, 30)
            .is_err(),
        "a hook receipt must not change a selected retirement attempt"
    );
    assert_eq!(raw_state(&db), before);
    db.execute_batch("DROP TRIGGER drift_selected_target")
        .unwrap();
    repo.settle_passive_workspace_retirement(&command.operation_id, 30)
        .unwrap();
    let operation = repo.get_operation(&command.operation_id).unwrap().unwrap();
    for effect in operation
        .effects
        .iter()
        .filter(|effect| effect.effect_kind == "workspace_hook_settle")
    {
        assert_eq!(effect.attempt_count, 0);
        assert_eq!(effect.phase, "finalized");
    }
    let before = raw_state(&db);
    repo.settle_passive_workspace_retirement(&command.operation_id, 99)
        .unwrap();
    assert_eq!(raw_state(&db), before);
}

#[test]
fn passive_hook_receipt_cannot_rebase_its_retirement_attempt() {
    passive_hook_receipt_preserves_selected_attempts(false);
}

#[test]
fn passive_hook_receipt_cannot_rebase_a_later_retirement_attempt() {
    passive_hook_receipt_preserves_selected_attempts(true);
}

#[test]
fn passive_requested_to_manual_review_critical_fault_rolls_back_every_prior_local_change() {
    for fault in 0..5 {
        let (_temp, repo, workspace) = setup();
        approve(&repo, &workspace.workspace_id);
        admit_hook(&repo, &workspace.workspace_id, "a_intended_hook");
        let started = admit_hook(&repo, &workspace.workspace_id, "z_started_hook");
        start(&repo, &started);
        let command = retirement_command(&workspace, "hook-passive-quarantine");
        repo.admit_workspace_retirement(&command).unwrap();
        let db = open_connection(&repo.database_path).unwrap();
        let before = raw_state(&db);
        let (when,body)=match fault {
        0=>("BEFORE","SELECT RAISE(ABORT,'quarantine transition fault');"),
        1=>("AFTER","UPDATE runtime_events SET seq=seq+100 WHERE event_id=NEW.event_id;"),
        2=>("AFTER","UPDATE workspace_hook_effects SET application_state='applied' WHERE parent_event_id='a_intended_hook';"),
        3=>("AFTER","UPDATE runtime_events SET seq=seq+100 WHERE kind='workspace.hook_effect_transitioned' AND json_extract(payload_json,'$.origin')='passive_retirement' AND json_extract(payload_json,'$.parent_event_id')='a_intended_hook';"),
        _=>("AFTER","UPDATE runtime_operation_effects SET phase='intended' WHERE effect_kind='workspace_hook_settle' AND json_extract(target_id,'$[0]')='a_intended_hook';"),
    };
        db.execute_batch(&format!(
            "CREATE TRIGGER fail_retirement_quarantine {when} INSERT ON runtime_events
        WHEN NEW.kind='operation.transitioned'
          AND json_extract(NEW.payload_json,'$.to_phase')='manual_review'
        BEGIN {body} END;"
        ))
        .unwrap();
        assert!(repo
            .settle_passive_workspace_retirement(&command.operation_id, 30)
            .is_err());
        assert_eq!(raw_state(&db), before);
        db.execute_batch("DROP TRIGGER fail_retirement_quarantine")
            .unwrap();
        repo.settle_passive_workspace_retirement(&command.operation_id, 30)
            .unwrap();
        let operation = repo.get_operation(&command.operation_id).unwrap().unwrap();
        assert_eq!(operation.operation.phase, OperationPhase::ManualReview);
        assert_eq!(
            operation.transitions.last().unwrap().from_phase,
            Some(OperationPhase::Requested)
        );
        assert_eq!(
            operation.transitions.last().unwrap().to_phase,
            OperationPhase::ManualReview
        );
        let before = raw_state(&db);
        repo.settle_passive_workspace_retirement(&command.operation_id, 99)
            .unwrap();
        assert_eq!(raw_state(&db), before);
    }
}

#[test]
fn retirement_recheck_receipt_is_atomic_when_another_unknown_hook_remains() {
    for fault in 0..5 {
        let (_temp, repo, workspace) = setup();
        approve(&repo, &workspace.workspace_id);
        let first = admit_hook(&repo, &workspace.workspace_id, "first_review_hook");
        let second = admit_hook(&repo, &workspace.workspace_id, "second_review_hook");
        start(&repo, &first);
        start(&repo, &second);
        let command = retirement_command(&workspace, "hook-review");
        repo.admit_workspace_retirement(&command).unwrap();
        repo.settle_passive_workspace_retirement(&command.operation_id, 30)
            .unwrap();
        repo.finish_team_hook_effect(
            &first.workspace_id,
            &first.parent_event_id,
            first.hook.event_name,
            &first.hook.name,
            TeamHookEffectPhase::Succeeded,
            TeamHookEffectReceipt {
                validated_output: Some(json!({})),
                error_code: None,
            },
            31,
        )
        .unwrap();
        let operation = repo.get_operation(&command.operation_id).unwrap().unwrap();
        let target = json!([
            first.parent_event_id,
            first.hook.event_name.as_str(),
            first.hook.name
        ])
        .to_string();
        let effect = operation
            .effects
            .iter()
            .find(|effect| {
                effect.effect_kind == "workspace_hook_settle" && effect.target_id == target
            })
            .unwrap();
        let review = runtime_core::prepare_workspace_retirement_review(
            &workspace.workspace_id,
            &command.operation_id,
            WorkspaceRetirementReviewRequest {
                effect_id: effect.effect_id.clone(),
                evidence: "Recheck the original durable terminal hook receipt".into(),
                resolution: WorkspaceRetirementReviewResolution::RecheckSettled,
            },
            runtime_core::OperationActor::operator("hook-reviewer"),
            Some("exact-hook-review".into()),
        )
        .unwrap();
        let db = open_connection(&repo.database_path).unwrap();
        let before = raw_state(&db);
        if fault < 2 {
            let (when, body) = if fault == 1 {
                ("AFTER","UPDATE workspace_hook_effects SET application_state='pending' WHERE parent_event_id='first_review_hook';")
            } else {
                ("BEFORE", "SELECT RAISE(ABORT,'review receipt fault');")
            };
            db.execute_batch(&format!(
                "CREATE TRIGGER fail_unreleased_review {when} INSERT ON runtime_events
                WHEN NEW.kind='workspace.retirement_review_recorded' BEGIN {body} END;"
            ))
            .unwrap();
        } else if fault < 4 {
            let body = if fault == 2 {
                "UPDATE runtime_operation_effects SET attempt_count=attempt_count+1 WHERE effect_id=NEW.effect_id;"
            } else {
                "UPDATE runtime_operation_effects SET evidence_json='{}' WHERE effect_id=NEW.effect_id;"
            };
            db.execute_batch(&format!(
                "CREATE TRIGGER fail_unreleased_review AFTER UPDATE ON runtime_operation_effects
                WHEN NEW.effect_id='{}' AND NEW.phase='finalized'
                  AND NEW.attempt_count=OLD.attempt_count
                  AND json_extract(NEW.evidence_json,'$.operator_review.review_id') IS NOT NULL
                BEGIN {body} END;",
                effect.effect_id
            ))
            .unwrap();
        } else {
            db.execute_batch(&format!(
                "CREATE TRIGGER fail_unreleased_review AFTER INSERT ON runtime_events
                 WHEN NEW.kind='workspace.hook_effect_transitioned'
                   AND json_extract(NEW.payload_json,'$.origin')='retirement_recheck'
                 BEGIN UPDATE runtime_operation_effects SET attempt_count=attempt_count+1
                   WHERE operation_id='{}' AND effect_id='{}'; END;",
                command.operation_id, effect.effect_id
            ))
            .unwrap();
        }
        assert!(
            repo.reconcile_workspace_retirement(&review).is_err(),
            "fault {fault}"
        );
        assert_eq!(raw_state(&db), before);
        db.execute_batch("DROP TRIGGER fail_unreleased_review")
            .unwrap();
        let outcome = repo.reconcile_workspace_retirement(&review).unwrap();
        assert!(outcome.receipt.settled);
        assert_eq!(
            outcome.receipt.operation_phase,
            OperationPhase::ManualReview
        );
        assert_eq!(outcome.receipt.unresolved_effect_ids.len(), 1);
        assert!(outcome.resume_command.is_none());
        let saved = repo
            .get_team_hook_effect(
                &first.workspace_id,
                &first.parent_event_id,
                first.hook.event_name,
                &first.hook.name,
            )
            .unwrap()
            .unwrap();
        assert_eq!(saved.application_state, "rejected");
        assert_eq!(
            repo.get_operation(&command.operation_id)
                .unwrap()
                .unwrap()
                .effects
                .iter()
                .find(|current| current.effect_id == effect.effect_id)
                .unwrap()
                .attempt_count,
            effect.attempt_count
        );
        let before = raw_state(&db);
        let replay = repo.reconcile_workspace_retirement(&review).unwrap();
        assert_eq!(replay.receipt, outcome.receipt);
        assert!(replay.resume_command.is_none());
        assert_eq!(raw_state(&db), before);
    }
}

#[test]
fn retirement_restart_late_receipt_fault_rolls_back_batch_and_never_reclaims_native_close() {
    for fault in 0..5 {
        let (temp, repo, first_workspace) = setup();
        let second_root = temp.path().join("second");
        std::fs::create_dir_all(&second_root).unwrap();
        let second_workspace = register(&repo, &second_root, "second-workspace");
        let mut commands = Vec::new();
        for (index, workspace) in [first_workspace, second_workspace].into_iter().enumerate() {
            let id = format!("sess_restart_native_{index}");
            let (session, agent) =
                workspace_agent_fixture(&workspace, &id, &format!("restart-heron-{index}"));
            repo.create_workspace_agent(&session, &agent).unwrap();
            let current = repo
                .get_workspace(&workspace.workspace_id)
                .unwrap()
                .unwrap();
            let command = retirement_command(&current, &format!("restart-retirement-{index}"));
            repo.admit_workspace_retirement(&command).unwrap();
            repo.claim_workspace_retirement_native_close(&command.operation_id, &session.id, 25)
                .unwrap();
            commands.push((command, session.id));
        }
        commands.sort_by(|a, b| a.0.operation_id.cmp(&b.0.operation_id));
        let db = open_connection(&repo.database_path).unwrap();
        let before = raw_state(&db);
        let first = &commands[0].0.operation_id;
        let (when,body)=match fault {
        0=>("BEFORE","SELECT RAISE(ABORT,'last restart receipt fault');".into()),
        1=>("AFTER",format!("UPDATE runtime_operation_effects SET phase='started' WHERE operation_id='{first}' AND effect_kind='workspace_provider_close';")),
        2=>("AFTER",format!("UPDATE runtime_events SET seq=seq+100 WHERE kind='workspace.retirement_restart_classified' AND json_extract(payload_json,'$.operation_id')='{first}';")),
        3=>("AFTER",format!("UPDATE runtime_operation_transitions SET evidence_json='{{}}' WHERE operation_id='{first}' AND to_phase='manual_review';")),
        _=>("AFTER",format!("UPDATE runtime_operation_effects SET attempt_count=attempt_count+1 WHERE operation_id='{}' AND effect_kind='workspace_provider_close';",commands[1].0.operation_id)),
    };
        let journal_owner = if fault == 4 {
            first
        } else {
            &commands[1].0.operation_id
        };
        db.execute_batch(&format!(
            "CREATE TRIGGER fail_last_restart {when} INSERT ON runtime_events
         WHEN NEW.kind='workspace.retirement_restart_classified'
           AND json_extract(NEW.payload_json,'$.operation_id')='{}'
         BEGIN {body} END;",
            journal_owner
        ))
        .unwrap();
        assert!(
            repo.quarantine_ambiguous_retirement_effects_after_restart(40)
                .is_err(),
            "fault {fault}"
        );
        assert_eq!(raw_state(&db), before);
        db.execute_batch("DROP TRIGGER fail_last_restart").unwrap();
        assert_eq!(
            repo.quarantine_ambiguous_retirement_effects_after_restart(40)
                .unwrap(),
            2
        );
        for (command, agent) in &commands {
            let operation = repo.get_operation(&command.operation_id).unwrap().unwrap();
            assert_eq!(operation.operation.phase, OperationPhase::ManualReview);
            let effect = operation
                .effects
                .iter()
                .find(|effect| effect.effect_kind == "workspace_provider_close")
                .unwrap();
            assert_eq!(effect.phase, "manual_review");
            assert_eq!(effect.attempt_count, 1);
            assert!(!operation.claims.is_empty());
            assert!(repo
                .claim_workspace_retirement_native_close(&command.operation_id, agent, 41)
                .is_err());
        }
        let before = raw_state(&db);
        assert_eq!(
            repo.quarantine_ambiguous_retirement_effects_after_restart(99)
                .unwrap(),
            0
        );
        assert_eq!(raw_state(&db), before);
    }
}
