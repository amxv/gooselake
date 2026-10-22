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
