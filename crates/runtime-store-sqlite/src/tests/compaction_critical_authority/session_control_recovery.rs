use super::*;
use crate::tests::edit_rerun::session_control_recovery::{
    blocked, check_damaged_pairs, check_owner_binding, check_pair_replay, check_pair_rollbacks,
    PairFixture,
};
use crate::tests::legacy_native_turn_guards::raw_state;
use runtime_core::{
    ProviderCompactionObservation, SessionControlCoordinate, TeamHookApprovalUpdate,
    TeamHookEventName, TeamHookResolvedEntry, TeamHookScope,
};
use serde_json::{json, Value};

fn fixture() -> PairFixture {
    let (temp, repo, workspace) = setup();
    let run = run_for(
        &repo,
        &workspace,
        "session_evidence",
        "cmp_evidence",
        "evidence-key",
    );
    repo.begin_session_compaction(&run).unwrap();
    let owner = SessionControlCoordinate::Compaction {
        agent_id: run.agent_id,
        run_id: run.run_id,
    };
    (temp, repo, owner)
}
#[test]
fn compaction_pair_is_immutable_and_survives_reopen() {
    check_pair_replay(fixture);
}
#[test]
fn compaction_pair_rolls_back_carrier_and_critical_event_faults() {
    check_pair_rollbacks(fixture);
}
#[test]
fn compaction_pair_rejects_orphans_mismatches_and_nested_unknown_fields() {
    check_damaged_pairs(fixture);
}
#[test]
fn compaction_pair_requires_exact_family_agent_and_owner() {
    check_owner_binding(fixture);
}

#[test]
fn fresh_compaction_cannot_supply_its_own_recovery_observation() {
    let (_temp, repo, workspace) = setup();
    let mut run = run_for(
        &repo,
        &workspace,
        "session_incoming_evidence",
        "cmp_incoming_evidence",
        "incoming-evidence-key",
    );
    let db = open_connection(&repo.database_path).unwrap();
    let before = raw_state(&db);
    for value in [
        json!({"recovery":blocked("forged"),"observed_at_ms":110}),
        json!({"unverified":true}),
        Value::Null,
    ] {
        run.legacy_native_recovery_evidence = Some(value);
        assert!(matches!(
            repo.begin_session_compaction_with_receipt(&run),
            Err(RuntimeError::InvalidState(_))
        ));
        assert_eq!(raw_state(&db), before);
        assert!(repo.get_session_compaction(&run.run_id).unwrap().is_none());
    }
}

fn terminal_observation(run: &SessionCompactionRun, phase: &str) -> ProviderCompactionObservation {
    ProviderCompactionObservation {
        proposed_run_id: "cmp_unused_proposed_run".into(),
        agent_id: run.agent_id.clone(),
        provider: run.provider,
        provider_session_ref: run.provider_session_ref.clone(),
        canonical_provider_session_ref: run.canonical_provider_session_ref.clone(),
        turn_id: None,
        phase: phase.into(),
        trigger: Some("manual".into()),
        pre_tokens: Some(125),
        post_tokens: (phase == "completed").then_some(60),
        context_window_size: Some(200),
        observed_at_ms: 120,
    }
}

#[test]
fn native_terminal_results_retain_raw_evidence_without_authorizing_a_new_refusal() {
    for phase in ["not_performed", "completed", "failed", "cancelled"] {
        for (save_pair, damage_pair) in [(false, false), (true, false), (true, true)] {
            let (_temp, repo, owner) = fixture();
            let original = repo.get_session_compaction(owner.id()).unwrap().unwrap();
            let recovery = blocked("history");
            let db = open_connection(&repo.database_path).unwrap();
            if save_pair {
                repo.record_session_control_legacy_recovery(&owner, &recovery, 110)
                    .unwrap();
            }
            if damage_pair {
                db.execute("UPDATE session_compaction_runs SET record_json=json_set(record_json,'$.legacy_native_recovery_evidence',json('{\"damaged\":true}')) WHERE run_id=?1", [owner.id()]).unwrap();
            }
            let retained = repo
                .get_session_compaction(owner.id())
                .unwrap()
                .unwrap()
                .legacy_native_recovery_evidence;
            let terminal = if phase == "not_performed" {
                repo.transition_session_compaction(
                    owner.id(),
                    "not_performed",
                    Some(ProviderCompactSessionOutcome::NotPerformed),
                    None,
                    120,
                )
                .unwrap()
            } else {
                repo.observe_provider_compaction(&terminal_observation(&original, phase))
                    .unwrap()
                    .run
            };
            assert_eq!(terminal.run_id, original.run_id);
            assert_eq!(terminal.phase, phase);
            assert_eq!(terminal.legacy_native_recovery_evidence, retained);
            assert!(!repo
                .unresolved_session_compaction(owner.agent_id())
                .unwrap());
            match phase {
                "not_performed" => assert_eq!(
                    terminal.outcome,
                    Some(ProviderCompactSessionOutcome::NotPerformed)
                ),
                "completed" => assert_eq!(
                    terminal.outcome,
                    Some(ProviderCompactSessionOutcome::Accepted)
                ),
                "failed" => assert_eq!(
                    terminal.error_code.as_deref(),
                    Some("provider_compaction_failed")
                ),
                "cancelled" => assert_eq!(
                    terminal.error_code.as_deref(),
                    Some("provider_compaction_cancelled")
                ),
                _ => unreachable!(),
            }
            let settled = raw_state(&db);
            if save_pair && !damage_pair {
                assert_eq!(
                    repo.read_session_control_legacy_recovery(&owner).unwrap(),
                    Some(recovery.clone())
                );
                assert!(repo
                    .record_session_control_legacy_recovery(&owner, &recovery, 900)
                    .unwrap()
                    .is_none());
            } else if damage_pair {
                assert!(repo.read_session_control_legacy_recovery(&owner).is_err());
                assert!(repo
                    .record_session_control_legacy_recovery(&owner, &recovery, 900)
                    .is_err());
            } else {
                assert_eq!(
                    repo.read_session_control_legacy_recovery(&owner).unwrap(),
                    None
                );
                assert!(repo
                    .record_session_control_legacy_recovery(&owner, &recovery, 900)
                    .is_err());
            }
            assert!(repo
                .record_session_control_legacy_recovery(
                    &owner,
                    &blocked("new-terminal-refusal"),
                    901
                )
                .is_err());
            let replay = repo
                .begin_session_compaction_with_receipt(&original)
                .unwrap();
            assert!(!replay.changed);
            assert!(replay.journal_event.is_none());
            assert_eq!(replay.run, terminal);
            if phase == "completed" {
                let late = repo
                    .transition_session_compaction_with_receipt(
                        owner.id(),
                        "accepted",
                        Some(ProviderCompactSessionOutcome::Accepted),
                        None,
                        902,
                    )
                    .unwrap();
                assert!(!late.changed);
                assert!(late.journal_event.is_none());
                assert_eq!(late.run, terminal);
            }
            assert_eq!(raw_state(&db), settled);
        }
    }
}

#[test]
fn prose_resolution_refuses_without_changing_any_database_table() {
    let (_temp, repo, workspace) = setup();
    let (lead_session, lead) = workspace_agent_fixture(&workspace, "session_d4_lead", "d4-lead");
    repo.create_workspace_agent(&lead_session, &lead).unwrap();
    let run = run_for(&repo, &workspace, "session_d4", "cmp_d4", "d4-key");
    let db = open_connection(&repo.database_path).unwrap();
    db.execute(
        "UPDATE workspaces SET lead_agent_id=?1 WHERE workspace_id=?2",
        params![lead.agent_id, workspace.workspace_id],
    )
    .unwrap();
    repo.update_team_hook_approval(&TeamHookApprovalUpdate {
        workspace_id: workspace.workspace_id.clone(),
        expected_revision: 0,
        hooks: vec![TeamHookResolvedEntry {
            event_name: TeamHookEventName::PostCompaction,
            name: "d4-completion-hook".into(),
            command: "printf approved".into(),
            timeout_ms: 1000,
            scope: TeamHookScope::User,
        }],
        approved_at_ms: 100,
    })
    .unwrap();
    repo.begin_session_compaction(&run).unwrap();
    let owner = SessionControlCoordinate::Compaction {
        agent_id: run.agent_id.clone(),
        run_id: run.run_id.clone(),
    };
    let recovery = blocked("history");
    repo.record_session_control_legacy_recovery(&owner, &recovery, 110)
        .unwrap();
    repo.recover_session_compactions(200).unwrap();
    let unresolved = repo.get_session_compaction(&run.run_id).unwrap().unwrap();
    assert_eq!(unresolved.phase, "manual_review");
    assert!(unresolved.review_evidence.is_none());
    assert!(repo.list_workspace_system_notices(10).unwrap().is_empty());
    assert!(repo
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap()
        .is_empty());
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    for phase in ["completed", "failed"] {
        for at in [201, 202] {
            assert!(matches!(
                reopened.resolve_session_compaction_review_with_receipt(
                    &run.run_id,
                    phase,
                    "Operator independently inspected the native provider outcome",
                    at
                ),
                Err(RuntimeError::Unsupported(_))
            ));
            assert_eq!(raw_state(&db), before);
            assert_eq!(
                reopened.get_session_compaction(&run.run_id).unwrap(),
                Some(unresolved.clone())
            );
            assert!(reopened
                .unresolved_session_compaction(&run.agent_id)
                .unwrap());
            assert_eq!(
                reopened
                    .read_session_control_legacy_recovery(&owner)
                    .unwrap(),
                Some(recovery.clone())
            );
        }
    }
}
