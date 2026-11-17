use super::critical_receipts::{events, raw};
use super::*;

fn pre_add(
    repo: &SqliteRuntimeRepository,
    id: &str,
    completed: bool,
) -> runtime_core::TeamHookEffectRecord {
    let mut effect = repo
        .admit_pre_add_team_hooks(&runtime_core::TeamHookPreAddAdmission {
            workspace_id: "ws_one".into(),
            reserved_agent_id: id.into(),
            actor_agent_id: None,
            provider: "codex".into(),
            model: None,
            planned_cwd: "/tmp/ws_one".into(),
            member_title: None,
            onboarding_prompt: None,
            created_at_ms: 20,
        })
        .unwrap()
        .remove(0);
    if completed {
        repo.claim_team_hook_effect(
            "ws_one",
            &effect.parent_event_id,
            effect.hook.event_name,
            &effect.hook.name,
            21,
        )
        .unwrap();
        effect = repo
            .finish_team_hook_effect(
                "ws_one",
                &effect.parent_event_id,
                effect.hook.event_name,
                &effect.hook.name,
                TeamHookEffectPhase::Succeeded,
                TeamHookEffectReceipt {
                    validated_output: Some(json!({})),
                    error_code: None,
                },
                22,
            )
            .unwrap();
    }
    effect
}

#[test]
fn hook_startup_batch_rolls_back_later_event_and_repeat_startup_does_nothing() {
    for fault in 0..3 {
        let (_temp, repo) = fixture();
        let mut pre = hook();
        pre.event_name = TeamHookEventName::PreTeammateAdd;
        approve(&repo, 0, vec![hook(), pre]);
        let post = repo.admit_team_hook_effect(&admission()).unwrap();
        repo.claim_team_hook_effect(
            "ws_one",
            &post.parent_event_id,
            post.hook.event_name,
            &post.hook.name,
            10,
        )
        .unwrap();
        let orphan = pre_add(&repo, "sess_a_orphan", false);
        let pending = pre_add(&repo, "sess_z_pending", true);
        let before = raw(&repo);
        assert!(
            post.parent_event_id < orphan.parent_event_id
                && orphan.parent_event_id < pending.parent_event_id
        );
        let (when,body)=match fault {
        0=>("BEFORE","SELECT RAISE(ABORT,'last classification failed');"),
        1=>("AFTER","UPDATE workspace_hook_effects SET phase='started' WHERE parent_event_id='evt_one';"),
        _=>("AFTER","UPDATE runtime_events SET seq=seq+100 WHERE kind='workspace.hook_effect_transitioned' AND json_extract(payload_json,'$.origin')='startup' AND json_extract(payload_json,'$.parent_event_id')='evt_one';"),
    };
        open_connection(&repo.database_path)
            .unwrap()
            .execute_batch(&format!(
                "CREATE TRIGGER refuse_last_startup {when} INSERT ON runtime_events
         WHEN NEW.kind='workspace.hook_effect_transitioned'
           AND json_extract(NEW.payload_json,'$.origin')='startup'
           AND json_extract(NEW.payload_json,'$.parent_event_id')='{}'
         BEGIN {body} END;",
                pending.parent_event_id
            ))
            .unwrap();
        assert!(repo.quarantine_started_team_hook_effects(30).is_err());
        assert_eq!(raw(&repo), before);
        open_connection(&repo.database_path)
            .unwrap()
            .execute_batch("DROP TRIGGER refuse_last_startup")
            .unwrap();
        assert_eq!(
            repo.quarantine_started_team_hook_effects(30).unwrap(),
            1,
            "return count still counts only previously Started effects"
        );
        let read = |effect: &runtime_core::TeamHookEffectRecord| {
            repo.get_team_hook_effect(
                &effect.workspace_id,
                &effect.parent_event_id,
                effect.hook.event_name,
                &effect.hook.name,
            )
            .unwrap()
            .unwrap()
        };
        assert_eq!(read(&post).phase, TeamHookEffectPhase::ManualReview);
        assert_eq!(
            read(&orphan).application_error_code.as_deref(),
            Some("pre_add_orphaned_before_execution")
        );
        assert_eq!(read(&pending).phase, TeamHookEffectPhase::Succeeded);
        assert_eq!(
            read(&pending).application_error_code.as_deref(),
            Some("pre_add_provider_outcome_unknown")
        );
        assert_eq!(
            events(&repo)
                .iter()
                .filter(|event| event["origin"] == "startup")
                .count(),
            3
        );
        let before = raw(&repo);
        let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
        assert_eq!(
            reopened.quarantine_started_team_hook_effects(99).unwrap(),
            0
        );
        assert_eq!(raw(&repo), before);
    }
}

#[test]
fn hook_startup_pins_every_selected_target_before_the_first_receipt() {
    for mutation in ["attempt_count=attempt_count+1", "phase='succeeded'"] {
        let (_temp, repo) = fixture();
        let mut second_hook = hook();
        second_hook.name = "z_later_hook".into();
        approve(&repo, 0, vec![hook(), second_hook.clone()]);
        let first = repo.admit_team_hook_effect(&admission()).unwrap();
        let mut request = admission();
        request.hook = second_hook;
        let second = repo.admit_team_hook_effect(&request).unwrap();
        for effect in [&first, &second] {
            repo.claim_team_hook_effect(
                &effect.workspace_id,
                &effect.parent_event_id,
                effect.hook.event_name,
                &effect.hook.name,
                10,
            )
            .unwrap();
        }
        let before = raw(&repo);
        let db = open_connection(&repo.database_path).unwrap();
        db.execute_batch(&format!(
            "CREATE TRIGGER drift_later_startup AFTER INSERT ON runtime_events
             WHEN NEW.kind='workspace.hook_effect_transitioned'
               AND json_extract(NEW.payload_json,'$.origin')='startup'
               AND json_extract(NEW.payload_json,'$.hook_name')='bootstrap'
             BEGIN UPDATE workspace_hook_effects SET {mutation}
               WHERE workspace_id='ws_one' AND parent_event_id='evt_one'
                 AND hook_name='z_later_hook'; END;"
        ))
        .unwrap();
        assert!(
            repo.quarantine_started_team_hook_effects(30).is_err(),
            "{mutation}"
        );
        assert_eq!(raw(&repo), before);
        db.execute_batch("DROP TRIGGER drift_later_startup")
            .unwrap();
        assert_eq!(repo.quarantine_started_team_hook_effects(30).unwrap(), 2);
        for effect in [&first, &second] {
            let saved = repo
                .get_team_hook_effect(
                    &effect.workspace_id,
                    &effect.parent_event_id,
                    effect.hook.event_name,
                    &effect.hook.name,
                )
                .unwrap()
                .unwrap();
            assert_eq!(saved.phase, TeamHookEffectPhase::ManualReview);
            assert_eq!(saved.attempt_count, 1);
            assert!(saved.receipt.is_none());
        }
        let before = raw(&repo);
        assert_eq!(repo.quarantine_started_team_hook_effects(99).unwrap(), 0);
        assert_eq!(raw(&repo), before);
    }
}

#[test]
fn hook_startup_keeps_exact_admitted_create_output_and_ignores_inert_history_bodies() {
    let (_temp, repo) = fixture();
    let mut pre = hook();
    pre.event_name = TeamHookEventName::PreTeammateAdd;
    approve(&repo, 0, vec![pre]);
    for (index, (phase, effect_phase)) in [
        ("requested", "intended"),
        ("requested", "uncertain"),
        ("manual_review", "intended"),
        ("manual_review", "uncertain"),
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("sess_owned_{index}");
        let effect = pre_add(&repo, &id, true);
        let db = open_connection(&repo.database_path).unwrap();
        let op = format!("op_owned_{index}");
        db.execute(
            "INSERT INTO runtime_operations(operation_id,workspace_id,kind,actor_kind,actor_id,
            normalized_request_hash,normalized_request_json,phase,created_at,updated_at)
            VALUES(?1,'ws_one','operator_agent_create','operator','fixture','hash','{}',?2,1,1)",
            rusqlite::params![op, phase],
        )
        .unwrap();
        db.execute(
            "INSERT INTO runtime_operation_effects(effect_id,operation_id,effect_kind,target_kind,
            target_id,phase,idempotency_key,attempt_count,created_at,updated_at)
            VALUES(?1,?2,'provider_session_create','session',?3,?4,'native',1,1,1)",
            rusqlite::params![format!("effect_owned_{index}"), op, id, effect_phase],
        )
        .unwrap();
        assert_eq!(effect.application_state, "pending");
    }
    // Unselected historical rows need not decode into a current hook structure.
    open_connection(&repo.database_path).unwrap().execute_batch(
        "INSERT INTO workspace_hook_effects(workspace_id,parent_event_id,event_name,hook_name,
         hook_json,input_json,phase,attempt_count,created_at_ms,updated_at_ms,application_state)
         VALUES('ws_one','evt_one','post_teammate_add','inert_history','{}','{}','failed',1,1,1,'rejected');"
    ).unwrap();
    let before = raw(&repo);
    assert_eq!(repo.quarantine_started_team_hook_effects(40).unwrap(), 0);
    assert_eq!(raw(&repo), before);
    assert_eq!(
        events(&repo)
            .iter()
            .filter(|event| event["origin"] == "startup")
            .count(),
        0
    );
}
