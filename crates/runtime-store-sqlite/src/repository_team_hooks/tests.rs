use std::sync::Arc;

use runtime_core::{
    RuntimeError, TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase,
    TeamHookEffectReceipt, TeamHookEventName, TeamHookResolvedEntry, TeamHookScope,
};
use serde_json::json;

use crate::db::open_connection;
use crate::SqliteRuntimeRepository;

fn fixture() -> (tempfile::TempDir, SqliteRuntimeRepository) {
    let temp = tempfile::tempdir().unwrap();
    let repo = SqliteRuntimeRepository::new(temp.path().join("hooks.sqlite"));
    repo.initialize_schema().unwrap();
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "INSERT INTO workspaces(workspace_id,canonical_root,display_name,lifecycle_state,revision,created_at,updated_at)
         VALUES('ws_one','/tmp/ws_one','One','active',0,1,1),('ws_two','/tmp/ws_two','Two','active',0,1,1);
         INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES('evt_one','system','system',1,'workspace.member_added',1,'{\"workspace_id\":\"ws_one\"}',1),
               ('evt_two','system','system',2,'workspace.member_added',1,'{\"workspace_id\":\"ws_two\"}',2);",
    )
    .unwrap();
    (temp, repo)
}

#[test]
fn schema_upgrade_never_retroactively_injects_old_terminal_script_output() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("upgrade.sqlite");
    let mut connection = open_connection(&path).unwrap();
    for migration in crate::schema::MIGRATIONS
        .iter()
        .filter(|migration| migration.version <= 16)
    {
        let tx = connection.transaction().unwrap();
        tx.execute_batch(migration.sql).unwrap();
        tx.execute(
            "INSERT INTO schema_migrations(version,applied_at) VALUES(?1,1)",
            [migration.version],
        )
        .unwrap();
        tx.commit().unwrap();
    }
    connection.execute_batch(
        "INSERT INTO workspaces(workspace_id,canonical_root,display_name,lifecycle_state,revision,created_at,updated_at)
         VALUES('ws_upgrade','/tmp/upgrade','Upgrade','active',0,1,1);
         INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES('evt_upgrade','workspace','ws_upgrade',1,'workspace.hook_parent',0,'{}',1);
         INSERT INTO workspace_hook_effects
         (workspace_id,parent_event_id,event_name,hook_name,hook_json,input_json,
          phase,attempt_count,receipt_json,created_at_ms,updated_at_ms)
         VALUES('ws_upgrade','evt_upgrade','post_teammate_add','old','{}','{}',
          'succeeded',1,'{\"validated_output\":{},\"error_code\":null}',1,2);",
    ).unwrap();
    drop(connection);

    let repository = SqliteRuntimeRepository::new(path);
    repository.initialize_schema().unwrap();
    let upgraded = open_connection(&repository.database_path).unwrap();
    let status: (String, Option<String>) = upgraded
        .query_row(
            "SELECT application_state,application_error_code FROM workspace_hook_effects",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        status,
        ("rejected".into(), Some("pre_activation_effect".into()))
    );
    assert!(repository
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
}

fn hook() -> TeamHookResolvedEntry {
    TeamHookResolvedEntry {
        event_name: TeamHookEventName::PostTeammateAdd,
        name: "bootstrap".into(),
        command: "printf ok".into(),
        timeout_ms: 500,
        scope: TeamHookScope::Project,
    }
}

fn approve(repo: &SqliteRuntimeRepository, revision: u64, hooks: Vec<TeamHookResolvedEntry>) {
    let approved = repo
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: "ws_one".into(),
            expected_revision: revision,
            hooks,
            approved_at_ms: 7,
        })
        .unwrap();
    assert_eq!(approved.revision, revision + 1);
}

fn admission() -> TeamHookEffectAdmission {
    TeamHookEffectAdmission {
        workspace_id: "ws_one".into(),
        parent_event_id: "evt_one".into(),
        hook: hook(),
        input: json!({"schema_version":1,"event_id":"evt_one","hook_event":"post_teammate_add"}),
        created_at_ms: 9,
    }
}

#[test]
fn pre_add_reservation_and_restart_never_replay_an_unattached_script() {
    let (_temp, repo) = fixture();
    let mut pre_add_hook = hook();
    pre_add_hook.event_name = TeamHookEventName::PreTeammateAdd;
    approve(&repo, 0, vec![pre_add_hook.clone()]);
    let admission = runtime_core::TeamHookPreAddAdmission {
        workspace_id: "ws_one".into(),
        reserved_agent_id: "sess_planned_one".into(),
        actor_agent_id: None,
        provider: "codex".into(),
        model: Some("gpt-test".into()),
        planned_cwd: "/tmp/ws_one".into(),
        member_title: Some("Writer".into()),
        onboarding_prompt: Some("Draft paper".into()),
        created_at_ms: 42,
    };
    let effects = repo.admit_pre_add_team_hooks(&admission).unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(repo.admit_pre_add_team_hooks(&admission).unwrap(), effects);
    let mut conflicting = admission.clone();
    conflicting.planned_cwd = "/some/other/root".into();
    assert!(matches!(
        repo.admit_pre_add_team_hooks(&conflicting),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(effects[0].phase, TeamHookEffectPhase::Intended);
    assert_eq!(
        effects[0].input["subject"]["member_agent_id"],
        "sess_planned_one"
    );
    let effect = &effects[0];
    let claimed = repo
        .claim_team_hook_effect(
            "ws_one",
            &effect.parent_event_id,
            TeamHookEventName::PreTeammateAdd,
            &pre_add_hook.name,
            43,
        )
        .unwrap();
    assert_eq!(claimed.phase, TeamHookEffectPhase::Started);
    repo.finish_team_hook_effect(
        "ws_one",
        &effect.parent_event_id,
        TeamHookEventName::PreTeammateAdd,
        &pre_add_hook.name,
        TeamHookEffectPhase::Succeeded,
        TeamHookEffectReceipt {
            validated_output: Some(json!({})),
            error_code: None,
        },
        44,
    )
    .unwrap();
    repo.quarantine_started_team_hook_effects(45).unwrap();
    let after = repo
        .get_team_hook_effect(
            "ws_one",
            &effect.parent_event_id,
            TeamHookEventName::PreTeammateAdd,
            &pre_add_hook.name,
        )
        .unwrap()
        .unwrap();
    assert_eq!(after.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(after.application_state, "rejected");
    assert_eq!(
        after.application_error_code.as_deref(),
        Some("pre_add_provider_outcome_unknown")
    );
    assert!(repo
        .list_dispatchable_team_hook_effects(10)
        .unwrap()
        .is_empty());
    assert!(repo
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
}

#[test]
fn approved_hooks_are_revisioned_workspace_local_and_revoke_cleanly() {
    let (_temp, repo) = fixture();
    let initial = repo.get_team_hook_approval("ws_one").unwrap();
    assert_eq!(initial.revision, 0);
    assert!(initial.hooks.is_empty());
    assert!(matches!(
        repo.get_team_hook_approval("unknown"),
        Err(RuntimeError::NotFound(_))
    ));

    approve(&repo, 0, vec![hook()]);
    let approved = repo.get_team_hook_approval("ws_one").unwrap();
    assert_eq!(approved.hooks, vec![hook()]);
    assert_eq!(repo.get_team_hook_approval("ws_two").unwrap().revision, 0);
    assert!(matches!(
        repo.update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: "ws_one".into(),
            expected_revision: 0,
            hooks: vec![hook()],
            approved_at_ms: 8,
        }),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(matches!(
        repo.update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: "ws_one".into(),
            expected_revision: 1,
            hooks: vec![hook(), hook()],
            approved_at_ms: 8,
        }),
        Err(RuntimeError::InvalidState(_))
    ));
    approve(&repo, 1, Vec::new());
    assert!(repo
        .get_team_hook_approval("ws_one")
        .unwrap()
        .hooks
        .is_empty());
}

#[test]
fn effects_require_exact_grant_and_persisted_workspace_parent() {
    let (_temp, repo) = fixture();
    let intended = admission();
    assert!(matches!(
        repo.admit_team_hook_effect(&intended),
        Err(RuntimeError::Conflict(_))
    ));
    approve(&repo, 0, vec![hook()]);

    let mut forged = intended.clone();
    forged.parent_event_id = "evt_two".into();
    assert!(matches!(
        repo.admit_team_hook_effect(&forged),
        Err(RuntimeError::InvalidState(_))
    ));
    forged.parent_event_id = "invented".into();
    assert!(matches!(
        repo.admit_team_hook_effect(&forged),
        Err(RuntimeError::InvalidState(_))
    ));
    forged = intended.clone();
    forged.hook.command = "rm -rf /".into();
    assert!(matches!(
        repo.admit_team_hook_effect(&forged),
        Err(RuntimeError::Conflict(_))
    ));
    forged = intended.clone();
    forged.input = json!(["not a structured hook input"]);
    assert!(matches!(
        repo.admit_team_hook_effect(&forged),
        Err(RuntimeError::InvalidState(_))
    ));
    forged = intended.clone();
    forged.input["event_id"] = json!("evt_two");
    assert!(matches!(
        repo.admit_team_hook_effect(&forged),
        Err(RuntimeError::InvalidState(_))
    ));
    forged = intended.clone();
    forged.input["hook_event"] = json!("post_broadcast_message_send");
    assert!(matches!(
        repo.admit_team_hook_effect(&forged),
        Err(RuntimeError::InvalidState(_))
    ));

    let created = repo.admit_team_hook_effect(&intended).unwrap();
    assert_eq!(created.phase, TeamHookEffectPhase::Intended);
    assert_eq!(created.attempt_count, 0);
    assert_eq!(repo.admit_team_hook_effect(&intended).unwrap(), created);
    let mut changed = intended.clone();
    changed.input = json!({"different":true});
    assert!(matches!(
        repo.admit_team_hook_effect(&changed),
        Err(RuntimeError::Conflict(_))
    ));

    // Revoking after admission fences an as-yet unstarted shell effect.
    approve(&repo, 1, Vec::new());
    assert!(matches!(
        repo.claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            10
        ),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repo.admit_team_hook_effect(&intended).unwrap(), created);
}

#[test]
fn claimed_effect_receipt_is_exact_and_never_replayed() {
    let (_temp, repo) = fixture();
    approve(&repo, 0, vec![hook()]);
    let intended = admission();
    repo.admit_team_hook_effect(&intended).unwrap();
    let started = repo
        .claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            10,
        )
        .unwrap();
    assert_eq!(started.phase, TeamHookEffectPhase::Started);
    assert_eq!(started.attempt_count, 1);
    assert!(matches!(
        repo.claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            11
        ),
        Err(RuntimeError::Conflict(_))
    ));
    let receipt = TeamHookEffectReceipt {
        validated_output: Some(json!({"inject": {"caller": {"text":"done"}}})),
        error_code: None,
    };
    let terminal = repo
        .finish_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            TeamHookEffectPhase::Succeeded,
            receipt.clone(),
            12,
        )
        .unwrap();
    assert_eq!(terminal.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(terminal.receipt.as_ref(), Some(&receipt));
    assert_eq!(terminal.attempt_count, 1);
    assert_eq!(
        repo.finish_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            TeamHookEffectPhase::Succeeded,
            receipt.clone(),
            13,
        )
        .unwrap(),
        terminal
    );
    assert!(matches!(
        repo.finish_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            TeamHookEffectPhase::Failed,
            receipt,
            14,
        ),
        Err(RuntimeError::Conflict(_))
    ));
    let reopen = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(reopen.admit_team_hook_effect(&intended).unwrap(), terminal);
    assert_eq!(reopen.quarantine_started_team_hook_effects(15).unwrap(), 0);
    assert_eq!(
        reopen.list_team_hook_effects("ws_one", 100).unwrap(),
        vec![terminal]
    );
}

#[test]
fn message_hook_waits_for_delivery_and_terminal_script_before_context_application() {
    let (_temp, repo) = fixture();
    let mut direct_hook = hook();
    direct_hook.event_name = TeamHookEventName::PostDirectMessageSend;
    approve(&repo, 0, vec![direct_hook.clone()]);
    let input = json!({
        "schema_version": 1,
        "workspace_id": "ws_one",
        "event_id": "evt_pending_direct",
        "hook_event": "post_direct_message_send",
        "actor_agent_id": "sess_sender",
        "subject": {
            "kind": "message",
            "message_id": "msg_pending",
            "sender_agent_id": "sess_sender",
            "scope": "direct",
            "recipient_agent_ids": ["sess_target"],
            "recipient_count": 1,
            "recipients_truncated": false
        },
        "eligible_target_agent_ids": [],
        "eligible_targets_truncated": false
    });
    {
        let connection = open_connection(&repo.database_path).unwrap();
        connection.execute(
            "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
             VALUES('evt_pending_direct','workspace','ws_one',1,'workspace.hook_parent',0,?1,12)",
            [input.to_string()],
        ).unwrap();
        connection.execute_batch(
            "INSERT INTO agent_messages(id,scope,context_kind,workspace_id,sender_agent_id,
             recipient_agent_ids_json,input_json,priority,policy,created_at)
             VALUES('msg_pending','direct','workspace_team','ws_one','sess_sender',
             '[\"sess_target\"]','{}','normal','immediate',12);
             INSERT INTO agent_deliveries(id,message_id,recipient_agent_id,provider,status,created_at,updated_at)
             VALUES('delivery_pending','msg_pending','sess_target','codex','pending',12,12);",
        ).unwrap();
    }
    let admission = TeamHookEffectAdmission {
        workspace_id: "ws_one".into(),
        parent_event_id: "evt_pending_direct".into(),
        hook: direct_hook,
        input,
        created_at_ms: 12,
    };
    let intended = repo.admit_team_hook_effect(&admission).unwrap();
    assert_eq!(intended.phase, TeamHookEffectPhase::Intended);
    // Canonical admission deliberately starts as not_ready; the successful
    // terminal receipt is responsible for enabling application.
    assert_eq!(intended.application_state, "not_ready");
    assert!(repo
        .list_dispatchable_team_hook_effects(10)
        .unwrap()
        .is_empty());
    assert!(repo
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
    assert_eq!(
        repo.apply_team_hook_effect_output(&intended, 13).unwrap(),
        intended
    );

    // If an older or corrupted writer ever prematurely marks an intended
    // script as pending, the application boundary must fail closed instead
    // of consuming the receipt before the script runs.
    {
        let conn = open_connection(&repo.database_path).unwrap();
        conn.execute(
            "UPDATE workspace_hook_effects SET application_state='pending'
             WHERE parent_event_id='evt_pending_direct'",
            [],
        )
        .unwrap();
        assert!(matches!(
            repo.apply_team_hook_effect_output(&intended, 13),
            Err(RuntimeError::Conflict(_))
        ));
        conn.execute(
            "UPDATE workspace_hook_effects SET application_state='not_ready'
             WHERE parent_event_id='evt_pending_direct'",
            [],
        )
        .unwrap();
    }

    // Restart while the original message is pending. No worker can consume
    // the output before the script even becomes dispatchable.
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    reopened.initialize_schema().unwrap();
    assert!(reopened
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "UPDATE agent_deliveries SET status='delivered' WHERE id='delivery_pending'",
            [],
        )
        .unwrap();
    assert_eq!(
        reopened
            .list_dispatchable_team_hook_effects(10)
            .unwrap()
            .len(),
        1
    );
    let started = reopened
        .claim_team_hook_effect(
            "ws_one",
            "evt_pending_direct",
            TeamHookEventName::PostDirectMessageSend,
            "bootstrap",
            14,
        )
        .unwrap();
    assert_eq!(started.phase, TeamHookEffectPhase::Started);
    assert!(reopened
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened
            .apply_team_hook_effect_output(&started, 14)
            .unwrap(),
        started
    );
    let finished = reopened
        .finish_team_hook_effect(
            "ws_one",
            "evt_pending_direct",
            TeamHookEventName::PostDirectMessageSend,
            "bootstrap",
            TeamHookEffectPhase::Succeeded,
            TeamHookEffectReceipt {
                validated_output: Some(json!({})),
                error_code: None,
            },
            15,
        )
        .unwrap();
    assert_eq!(
        reopened.list_unapplied_team_hook_effects(10).unwrap().len(),
        1
    );
    let applied = reopened
        .apply_team_hook_effect_output(&finished, 16)
        .unwrap();
    assert_eq!(applied.application_state, "applied");
    assert!(reopened
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened
            .apply_team_hook_effect_output(&finished, 17)
            .unwrap(),
        applied
    );
}

#[test]
fn restart_quarantines_started_shell_without_redispatching() {
    let (_temp, repo) = fixture();
    approve(&repo, 0, vec![hook()]);
    let input = admission();
    repo.admit_team_hook_effect(&input).unwrap();
    repo.claim_team_hook_effect(
        "ws_one",
        "evt_one",
        TeamHookEventName::PostTeammateAdd,
        "bootstrap",
        10,
    )
    .unwrap();
    let reopen = SqliteRuntimeRepository::new(repo.database_path.clone());
    reopen.initialize_schema().unwrap();
    assert_eq!(reopen.quarantine_started_team_hook_effects(20).unwrap(), 1);
    assert_eq!(reopen.quarantine_started_team_hook_effects(21).unwrap(), 0);
    let state = reopen.admit_team_hook_effect(&input).unwrap();
    assert_eq!(state.phase, TeamHookEffectPhase::ManualReview);
    assert_eq!(state.attempt_count, 1);
    assert!(matches!(
        reopen.claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            22
        ),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn competing_claims_converge_on_one_shell_dispatch_authority() {
    let (_temp, repo) = fixture();
    approve(&repo, 0, vec![hook()]);
    repo.admit_team_hook_effect(&admission()).unwrap();
    let repo = Arc::new(repo);
    let workers = (0..6)
        .map(|_| {
            let repo = Arc::clone(&repo);
            std::thread::spawn(move || {
                repo.claim_team_hook_effect(
                    "ws_one",
                    "evt_one",
                    TeamHookEventName::PostTeammateAdd,
                    "bootstrap",
                    30,
                )
            })
        })
        .collect::<Vec<_>>();
    let successes = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().is_ok())
        .filter(|success| *success)
        .count();
    assert_eq!(successes, 1);
    assert_eq!(
        repo.get_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap"
        )
        .unwrap()
        .unwrap()
        .attempt_count,
        1
    );
}

#[test]
fn workspace_retirement_fences_an_approved_pending_hook() {
    let (_temp, repo) = fixture();
    approve(&repo, 0, vec![hook()]);
    repo.admit_team_hook_effect(&admission()).unwrap();
    let connection = open_connection(&repo.database_path).unwrap();
    connection
        .execute(
            "UPDATE workspaces SET lifecycle_state='retired' WHERE workspace_id='ws_one'",
            [],
        )
        .unwrap();
    assert!(matches!(
        repo.claim_team_hook_effect(
            "ws_one",
            "evt_one",
            TeamHookEventName::PostTeammateAdd,
            "bootstrap",
            90
        ),
        Err(RuntimeError::Conflict(_))
    ));
}
