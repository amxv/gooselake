use super::*;
use crate::team_hook_execution::{MAX_DURABLE_RECEIPT_BYTES, MAX_SCRIPT_STDOUT};
use runtime_core::{
    TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase, TeamHookEffectReceipt,
    TeamHookEventName, TeamHookResolvedEntry, TeamHookScope,
};

fn fixture(
    command: String,
    timeout_ms: u64,
) -> (
    tempfile::TempDir,
    SqliteRuntimeStore,
    TeamHookEffectAdmission,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    store.repository().initialize_schema().unwrap();
    let conn = rusqlite::Connection::open(store.database_path()).unwrap();
    conn.execute(
        "INSERT INTO workspaces(workspace_id,canonical_root,display_name,lifecycle_state,revision,created_at,updated_at)
         VALUES('ws_hooks',?1,'Hook test','active',0,1,1)",
        [root.to_string_lossy().as_ref()],
    )
    .unwrap();
    conn.execute_batch(
        "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES('evt_hooks','system','system',1,'workspace.member_added',1,'{\"workspace_id\":\"ws_hooks\"}',1)",
    )
    .unwrap();
    let hook = TeamHookResolvedEntry {
        event_name: TeamHookEventName::PostTeammateAdd,
        name: "runtime_check".into(),
        command,
        timeout_ms,
        scope: TeamHookScope::User,
    };
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: "ws_hooks".into(),
            expected_revision: 0,
            hooks: vec![hook.clone()],
            approved_at_ms: 1,
        })
        .unwrap();
    let admission = TeamHookEffectAdmission {
        workspace_id: "ws_hooks".into(),
        parent_event_id: "evt_hooks".into(),
        hook,
        input: json!({"schema_version":1,"event_id":"evt_hooks","hook_event":"post_teammate_add"}),
        created_at_ms: 2,
    };
    (temp, store, admission)
}

#[tokio::test]
async fn persisted_workspace_parent_dispatches_once_and_reconciles_after_restart() {
    let marker_dir = tempfile::tempdir().unwrap();
    let marker = marker_dir.path().join("script-runs");
    let (temp, store, original) = fixture(
        format!("printf x >> '{}'; printf '{{}}'", marker.display()),
        5_000,
    );
    let mut admitted = original;
    admitted.parent_event_id = "evt_workspace_hook_dispatch".into();
    admitted.input = json!({
        "schema_version":1,
        "workspace_id":"ws_hooks",
        "event_id":admitted.parent_event_id,
        "hook_event":"post_teammate_add",
        "actor_agent_id":null,
        "subject":{"kind":"member","member_agent_id":"sess_test"},
        "eligible_target_agent_ids":[],
    });
    let sql = rusqlite::Connection::open(store.database_path()).unwrap();
    sql.execute(
        "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES(?1,'workspace','ws_hooks',1,'workspace.hook_parent',0,?2,1)",
        rusqlite::params![admitted.parent_event_id, admitted.input.to_string()],
    ).unwrap();
    store.admit_team_hook_effect(&admitted).unwrap();

    assert_eq!(dispatch_team_hook_effects(&store).await.unwrap(), 2);
    let complete = store
        .get_team_hook_effect(
            "ws_hooks",
            &admitted.parent_event_id,
            admitted.hook.event_name,
            &admitted.hook.name,
        )
        .unwrap()
        .unwrap();
    assert_eq!(complete.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(complete.application_state, "applied");
    assert_eq!(complete.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");

    let restarted = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    restarted.repository().initialize_schema().unwrap();
    assert_eq!(dispatch_team_hook_effects(&restarted).await.unwrap(), 0);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
}

#[tokio::test]
async fn operator_granted_hook_runs_once_with_durable_terminal_receipt() {
    let marker = tempfile::tempdir().unwrap();
    let marker_path = marker.path().join("executions");
    let (temp, store, admission) = fixture(
        format!("printf x >> '{}'; printf '{{}}'", marker_path.display()),
        5_000,
    );
    let first = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(first.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(first.attempt_count, 1);
    assert_eq!(first.receipt.unwrap().validated_output, Some(json!({})));
    let replay = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(replay.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(replay.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(marker_path).unwrap(), "x");
    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    let durable = execute_team_hook_effect(&reopened, &admission)
        .await
        .unwrap();
    assert_eq!(durable.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(durable.attempt_count, 1);
}

#[tokio::test]
async fn revoked_or_mismatched_hook_cannot_launch_shell() {
    let marker = tempfile::tempdir().unwrap();
    let marker_path = marker.path().join("must_not_exist");
    let (_temp, store, admission) = fixture(
        format!("printf x > '{}'; printf '{{}}'", marker_path.display()),
        5_000,
    );
    let mut spoof = admission.clone();
    spoof.hook.command = "printf '{}'".into();
    assert!(matches!(
        execute_team_hook_effect(&store, &spoof).await,
        Err(RuntimeError::Conflict(_))
    ));
    store
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: "ws_hooks".into(),
            expected_revision: 1,
            hooks: vec![],
            approved_at_ms: 3,
        })
        .unwrap();
    assert!(matches!(
        execute_team_hook_effect(&store, &admission).await,
        Err(RuntimeError::Conflict(_))
    ));
    assert!(!marker_path.exists());
}

#[tokio::test]
async fn timeouts_and_invalid_output_never_leak_script_stdout() {
    let (_temp, store, admission) = fixture("sleep 2; printf '{}'".into(), 100);
    let timed = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(timed.phase, TeamHookEffectPhase::TimedOut);
    assert_eq!(
        timed.receipt.unwrap().error_code.as_deref(),
        Some("timeout")
    );

    // Keep the secret bytes outside the approved command snapshot. A grant is
    // intentionally auditable (including its command), but neither a failed
    // receipt nor the effect history may retain the *script's output*.
    let secret_dir = tempfile::tempdir().unwrap();
    let secret_file = secret_dir.path().join("invalid-output.json");
    std::fs::write(
        &secret_file,
        br#"{"caller_agent_id":"forged","secret":"not_stored"}"#,
    )
    .unwrap();
    let (_temp, store, admission) = fixture(format!("cat '{}'", secret_file.display()), 5_000);
    let invalid = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(invalid.phase, TeamHookEffectPhase::Failed);
    assert_eq!(
        invalid.receipt.unwrap().error_code.as_deref(),
        Some("invalid_output")
    );
    let durable = store.list_team_hook_effects("ws_hooks", 10).unwrap();
    let encoded = serde_json::to_string(&durable).unwrap();
    assert!(!encoded.contains("not_stored"));
    assert!(!encoded.contains("forged"));
}

#[tokio::test]
async fn near_limit_valid_stdout_cannot_leave_an_unpersistable_started_receipt() {
    let secret_dir = tempfile::tempdir().unwrap();
    let output_file = secret_dir.path().join("bounded-output.json");
    let mut near_limit = None;
    for second_len in (7_900..=8_192).rev() {
        let payload = serde_json::to_vec(&json!({
            "inject": {
                "caller": {"text": "x".repeat(8_192)},
                "targets": [{"agent_id": "sess_other", "text": "y".repeat(second_len)}]
            }
        }))
        .unwrap();
        if payload.len() > MAX_SCRIPT_STDOUT {
            continue;
        }
        let validated = runtime_core::team_hooks::validate_team_hook_script_output(
            TeamHookEventName::PostTeammateAdd,
            &payload,
        )
        .unwrap();
        let receipt = TeamHookEffectReceipt {
            validated_output: Some(serde_json::to_value(validated).unwrap()),
            error_code: None,
        };
        if serde_json::to_vec(&receipt).unwrap().len() > MAX_DURABLE_RECEIPT_BYTES {
            near_limit = Some(payload);
            break;
        }
    }
    let near_limit = near_limit.expect("valid stdout whose durable receipt exceeds 16 KiB");
    std::fs::write(&output_file, near_limit).unwrap();
    let (temp, store, admission) = fixture(format!("cat '{}'", output_file.display()), 5_000);
    let effect = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(effect.phase, TeamHookEffectPhase::Failed);
    assert_eq!(
        effect.receipt.unwrap().error_code.as_deref(),
        Some("output_too_large")
    );

    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite"),
    });
    let recovered = execute_team_hook_effect(&reopened, &admission)
        .await
        .unwrap();
    assert_eq!(recovered.phase, TeamHookEffectPhase::Failed);
    assert_eq!(recovered.attempt_count, 1);
    assert_eq!(
        reopened
            .repository()
            .quarantine_started_team_hook_effects(9)
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn competing_dispatchers_can_launch_at_most_one_hook() {
    let marker = tempfile::tempdir().unwrap();
    let marker_path = marker.path().join("executions");
    let (_temp, store, admission) = fixture(
        format!("printf x >> '{}'; printf '{{}}'", marker_path.display()),
        5_000,
    );
    let (a, b) = tokio::join!(
        execute_team_hook_effect(&store, &admission),
        execute_team_hook_effect(&store, &admission),
    );
    assert!(a.is_ok(), "first execution returned {a:?}");
    assert!(b.is_ok(), "second execution returned {b:?}");
    let latest = store
        .get_team_hook_effect(
            "ws_hooks",
            "evt_hooks",
            admission.hook.event_name,
            &admission.hook.name,
        )
        .unwrap()
        .unwrap();
    assert_eq!(latest.phase, TeamHookEffectPhase::Succeeded);
    assert_eq!(latest.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(marker_path).unwrap(), "x");
}
