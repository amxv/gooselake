use super::*;

#[tokio::test]
async fn critical_hook_claim_and_terminal_faults_preserve_zero_then_one_shell_attempt() {
    let (_temp, store, admission) = fixture("printf x >> native_runs; printf '{}'".into(), 5_000);
    let root = std::path::PathBuf::from(
        store
            .get_workspace(&admission.workspace_id)
            .unwrap()
            .unwrap()
            .canonical_root,
    );
    let marker = root.join("native_runs");
    let intended = store.admit_team_hook_effect(&admission).unwrap();
    let db = rusqlite::Connection::open(store.database_path()).unwrap();
    db.execute_batch(
        "CREATE TRIGGER fail_native_claim BEFORE INSERT ON runtime_events
        WHEN NEW.kind='workspace.hook_effect_transitioned'
          AND json_extract(NEW.payload_json,'$.origin')='execution_claim'
        BEGIN SELECT RAISE(ABORT,'claim event unavailable'); END;",
    )
    .unwrap();
    assert!(execute_team_hook_effect(&store, &admission).await.is_err());
    assert!(!marker.exists());
    assert_eq!(
        store
            .get_team_hook_effect(
                &admission.workspace_id,
                &admission.parent_event_id,
                admission.hook.event_name,
                &admission.hook.name
            )
            .unwrap()
            .unwrap(),
        intended
    );
    db.execute_batch(
        "DROP TRIGGER fail_native_claim;
        CREATE TRIGGER fail_native_terminal BEFORE INSERT ON runtime_events
        WHEN NEW.kind='workspace.hook_effect_transitioned'
          AND json_extract(NEW.payload_json,'$.origin')='execution_receipt'
        BEGIN SELECT RAISE(ABORT,'terminal event unavailable'); END;",
    )
    .unwrap();
    assert!(execute_team_hook_effect(&store, &admission).await.is_err());
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
    let started = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(started.phase, TeamHookEffectPhase::Started);
    assert_eq!(started.attempt_count, 1);
    assert!(started.receipt.is_none());
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
    db.execute_batch("DROP TRIGGER fail_native_terminal")
        .unwrap();
    assert_eq!(store.quarantine_started_team_hook_effects(99).unwrap(), 1);
    let held = execute_team_hook_effect(&store, &admission).await.unwrap();
    assert_eq!(held.phase, TeamHookEffectPhase::ManualReview);
    assert_eq!(held.attempt_count, 1);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
}

#[tokio::test]
async fn supported_create_completion_event_remains_the_atomic_pre_add_application_receipt() {
    for damage_post_hook in [false, true] {
        let (_temp, store, workspace, manager, provider) =
            pre_add_fixture("printf x >> pre_add_runs; printf '{}'".into());
        let root = std::path::PathBuf::from(&workspace.canonical_root);
        let mut hooks = store
            .get_team_hook_approval(&workspace.workspace_id)
            .unwrap()
            .hooks;
        hooks.push(TeamHookResolvedEntry {
            event_name: TeamHookEventName::PostTeammateAdd,
            name: "observe_member".into(),
            command: "printf '{}'".into(),
            timeout_ms: 5_000,
            scope: TeamHookScope::Project,
        });
        store
            .update_team_hook_approval(&TeamHookApprovalUpdate {
                workspace_id: workspace.workspace_id.clone(),
                expected_revision: 1,
                hooks,
                approved_at_ms: 2,
            })
            .unwrap();
        manager
            .install_pre_add_hook_executor(Arc::new(NativeTeamHookExecutor::new(store.clone())))
            .await;
        let db = rusqlite::Connection::open(store.database_path()).unwrap();
        let (when, body, cause) = if damage_post_hook {
            ("AFTER","UPDATE workspace_hook_effects SET application_state='applied' WHERE event_name='post_teammate_add';","hook authority")
        } else {
            (
                "BEFORE",
                "SELECT RAISE(ABORT,'completion event fault');",
                "completion event fault",
            )
        };
        db.execute_batch(&format!(
            "CREATE TRIGGER fail_real_create_completion {when} INSERT ON runtime_events
        WHEN NEW.kind='operation.transitioned'
          AND json_extract(NEW.payload_json,'$.to_phase')='completed'
          AND EXISTS(SELECT 1 FROM runtime_operations op
            WHERE op.operation_id=json_extract(NEW.payload_json,'$.operation_id')
              AND op.kind='operator_agent_create')
        BEGIN {body} END;"
        ))
        .unwrap();
        let result = manager
            .create_workspace_agent_journaled_fixture(
                &workspace.workspace_id,
                pre_add_request(&root),
                "critical-pre-add",
            )
            .await;
        let original = assert_admitted_pre_add_failure(&store, result, cause);
        assert_eq!(
            provider.state.lock().await.sessions.len(),
            1,
            "the fake-provider attachment remains"
        );
        assert_eq!(
            provider
                .effect_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "exactly one provider create and no cleanup/resume/send"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("pre_add_runs")).unwrap(),
            "x"
        );
        let effects = store
            .list_team_hook_effects(&workspace.workspace_id, 20)
            .unwrap();
        assert!(
            effects
                .iter()
                .all(|effect| effect.hook.event_name == TeamHookEventName::PreTeammateAdd),
            "the later failed parent commit also rolls back its post-hook intent"
        );
        let pre = effects
            .iter()
            .find(|effect| effect.hook.event_name == TeamHookEventName::PreTeammateAdd)
            .unwrap();
        assert_eq!(pre.phase, TeamHookEffectPhase::Succeeded);
        assert_eq!(
            pre.application_state, "pending",
            "create rollback includes pre-add application"
        );
        assert_eq!(pre.attempt_count, 1);
        let target = &original.effects[0].target_id;
        let roster: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM workspace_agents WHERE session_id=?1",
                [target],
                |row| row.get(0),
            )
            .unwrap();
        let session: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id=?1",
                [target],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!((roster, session), (0, 0));
        let notices: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM workspace_system_notices WHERE recipient_agent_id=?1",
                [target],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(notices, 0);
        assert!(original.operation.exact_terminal_result.is_none());
        assert!(!original.claims.is_empty());
        db.execute_batch("DROP TRIGGER fail_real_create_completion")
            .unwrap();
        assert_eq!(store.quarantine_started_team_hook_effects(999).unwrap(), 0);
        assert_eq!(
            store
                .get_team_hook_effect(
                    &pre.workspace_id,
                    &pre.parent_event_id,
                    pre.hook.event_name,
                    &pre.hook.name
                )
                .unwrap()
                .unwrap(),
            *pre
        );
        assert_eq!(provider.state.lock().await.sessions.len(), 1);
        assert_eq!(
            provider
                .effect_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.join("pre_add_runs")).unwrap(),
            "x"
        );
    }
}
