use super::*;
use runtime_core::{
    TeamHookApprovalUpdate, TeamHookEffectAdmission, TeamHookEffectPhase, TeamHookEventName,
    TeamHookResolvedEntry, TeamHookScope,
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

    let (_temp, store, admission) = fixture(
        "printf '{\"caller_agent_id\":\"forged\",\"secret\":\"not_stored\"}'".into(),
        5_000,
    );
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
