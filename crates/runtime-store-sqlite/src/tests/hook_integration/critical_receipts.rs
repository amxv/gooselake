use super::super::legacy_native_turn_guards::raw_state;
use super::*;

fn pending() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    runtime_core::TeamHookEffectRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (leader, lead) = workspace_agent_fixture(&workspace, "sess_hook_leader", "leader-heron");
    repository.create_workspace_agent(&leader, &lead).unwrap();
    grant(
        &repository,
        &workspace.workspace_id,
        &[TeamHookEventName::PostTeammateAdd],
    );
    let (session, mut member) =
        workspace_agent_fixture(&workspace, "sess_hook_member", "member-otter");
    member.profile.added_by = lead.agent_id.clone();
    repository
        .create_workspace_agent(&session, &member)
        .unwrap();
    let effect = finish(
        &repository,
        TeamHookEventName::PostTeammateAdd,
        json!({
            "inject":{"caller":{"text":"private_hook_context"},"targets":[
                {"agent_id":member.agent_id,"text":"private_member_context"}]}
        }),
    );
    (temp, repository, effect)
}

#[test]
fn hook_output_application_event_failure_rolls_back_notices_and_exact_application() {
    for fault in 0..4 {
        let (_temp, repository, effect) = pending();
        let db = open_connection(&repository.database_path).unwrap();
        let before = raw_state(&db);
        let (timing,body)=match fault {
            0=>("BEFORE","SELECT RAISE(ABORT,'application receipt fault');"),
            1=>("AFTER","UPDATE workspace_hook_effects SET application_state='pending';"),
            2=>("AFTER","DELETE FROM workspace_system_notices WHERE notice_id=(SELECT MIN(notice_id) FROM workspace_system_notices WHERE kind='hook_context');"),
            _=>("AFTER","UPDATE workspace_system_notices SET input_json='{}' WHERE notice_id=(SELECT MIN(notice_id) FROM workspace_system_notices WHERE kind='hook_context');"),
        };
        db.execute_batch(&format!(
            "CREATE TRIGGER fail_hook_application {timing} INSERT ON runtime_events
             WHEN NEW.kind='workspace.hook_effect_transitioned'
               AND json_extract(NEW.payload_json,'$.origin')='output_application'
             BEGIN {body} END;"
        ))
        .unwrap();
        assert!(repository
            .apply_team_hook_effect_output(&effect, 303)
            .is_err());
        assert_eq!(raw_state(&db), before);
        db.execute_batch("DROP TRIGGER fail_hook_application")
            .unwrap();
        let applied = repository
            .apply_team_hook_effect_output(&effect, 303)
            .unwrap();
        assert_eq!(applied.application_state, "applied");
        assert_eq!(
            repository
                .list_workspace_system_notices(30)
                .unwrap()
                .iter()
                .filter(|notice| notice.kind == "hook_context")
                .count(),
            2
        );
        let after = raw_state(&db);
        assert_eq!(
            repository
                .apply_team_hook_effect_output(&effect, 999)
                .unwrap(),
            applied
        );
        assert_eq!(raw_state(&db), after);
        let raw:String=db.query_row(
            "SELECT payload_json FROM runtime_events WHERE kind='workspace.hook_effect_transitioned'
             AND json_extract(payload_json,'$.origin')='output_application'",[],|row|row.get(0)
        ).unwrap();
        assert!(!raw.contains("private_hook_context"));
        assert!(!raw.contains("private_member_context"));
    }
}

#[test]
fn revoked_hook_output_rejection_is_an_atomic_receipt_without_context_delivery() {
    let (_temp, repository, effect) = pending();
    repository
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: effect.workspace_id.clone(),
            expected_revision: 1,
            hooks: vec![],
            approved_at_ms: 300,
        })
        .unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    let before = raw_state(&db);
    db.execute_batch(
        "CREATE TRIGGER fail_hook_rejection BEFORE INSERT ON runtime_events
        WHEN NEW.kind='workspace.hook_effect_transitioned'
          AND json_extract(NEW.payload_json,'$.origin')='output_application'
        BEGIN SELECT RAISE(ABORT,'rejection receipt fault'); END;",
    )
    .unwrap();
    assert!(repository
        .apply_team_hook_effect_output(&effect, 303)
        .is_err());
    assert_eq!(raw_state(&db), before);
    db.execute_batch("DROP TRIGGER fail_hook_rejection")
        .unwrap();
    let rejected = repository
        .apply_team_hook_effect_output(&effect, 303)
        .unwrap();
    assert_eq!(rejected.application_state, "rejected");
    assert_eq!(
        rejected.application_error_code.as_deref(),
        Some("parent_or_grant_changed")
    );
    assert_eq!(
        repository
            .list_workspace_system_notices(30)
            .unwrap()
            .iter()
            .filter(|notice| notice.kind == "hook_context")
            .count(),
        0
    );
    let before = raw_state(&db);
    assert_eq!(
        repository
            .apply_team_hook_effect_output(&effect, 404)
            .unwrap(),
        rejected
    );
    assert_eq!(raw_state(&db), before);
}

#[test]
fn critical_hook_intent_fault_rolls_back_its_causing_roster_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    grant(
        &repository,
        &workspace.workspace_id,
        &[TeamHookEventName::PostTeammateAdd],
    );
    let (session, member) =
        workspace_agent_fixture(&workspace, "sess_no_partial_hook", "no-partial-heron");
    let db = open_connection(&repository.database_path).unwrap();
    let before = raw_state(&db);
    db.execute_batch(
        "CREATE TRIGGER fail_hook_intent BEFORE INSERT ON runtime_events
        WHEN NEW.kind='workspace.hook_effect_transitioned'
          AND json_extract(NEW.payload_json,'$.origin')='parent_trigger'
        BEGIN SELECT RAISE(ABORT,'parent intent receipt fault'); END;",
    )
    .unwrap();
    assert!(repository
        .create_workspace_agent(&session, &member)
        .is_err());
    assert_eq!(raw_state(&db), before);
    db.execute_batch("DROP TRIGGER fail_hook_intent").unwrap();
    repository
        .create_workspace_agent(&session, &member)
        .unwrap();
    assert_eq!(
        repository
            .list_dispatchable_team_hook_effects(10)
            .unwrap()
            .len(),
        1
    );
}
