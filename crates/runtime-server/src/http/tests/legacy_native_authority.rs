use super::workspace_control_routes::{
    create_control_agent_with_permission, register_control_workspace,
};
use super::workspace_worktree_routes::call;
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn workspace_interrupt_lost_ack_retains_original_target_across_replay_and_restart() {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    let root = temp.path().join("interrupt-uncertain");
    fs::create_dir_all(&root).unwrap();
    let workspace =
        register_control_workspace(&router, &token, &root, "Interrupt uncertainty").await;
    let agent = create_control_agent_with_permission(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Active",
        "workspace_write",
    )
    .await;
    let turn_id = start_native_interrupt_fixture(
        &router,
        &token,
        &runtime,
        &provider,
        &temp,
        &workspace.workspace_id,
        &agent.agent_id,
    )
    .await;
    provider.interrupt_failure_mode.store(1, Ordering::SeqCst);
    let uri = format!("/v2/workspaces/{}/interrupt", workspace.workspace_id);
    let (status, response) = call(
        &router,
        &token,
        "POST",
        &uri,
        None,
        Some("interrupt-lost-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");
    let operation_id = response["error"]["details"]["operation_id"]
        .as_str()
        .expect("original operation coordinate")
        .to_string();
    assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 1);
    let native = provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: agent.agent_id.clone(),
            turn_id: turn_id.clone(),
            timeout_ms: Some(100),
        })
        .await
        .unwrap();
    assert_eq!(
        native.status,
        ProviderTurnStatus::Interrupted,
        "independent native effect preceded lost ACK"
    );

    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    let operation = store.get_operation(&operation_id).unwrap().unwrap();
    assert!(operation.operation.exact_terminal_result.is_none());
    assert_eq!(operation.effects.len(), 1);
    assert_eq!(operation.effects[0].target_id, agent.agent_id);
    assert_eq!(operation.effects[0].phase, "uncertain");
    assert_eq!(
        operation.effects[0].evidence.as_ref().unwrap()["turn_id"],
        turn_id
    );
    for _ in 0..2 {
        let (status, replay) = call(
            &router,
            &token,
            "POST",
            &uri,
            None,
            Some("interrupt-lost-ack"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{replay}");
        assert_eq!(replay["error"]["details"]["operation_id"], operation_id);
    }
    provider.interrupt_failure_mode.store(0, Ordering::SeqCst);
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let restarted =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128).unwrap());
    let resumes_before = provider.restore_call_count().await;
    let recovery = restarted.recover_startup().await.unwrap();
    assert_eq!(
        provider.restore_call_count().await,
        resumes_before,
        "{recovery:?}"
    );
    assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 1);
    // A later terminal projection does not retrospectively prove that the
    // original dispatched interrupt never acted.
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET active_turn_id=NULL WHERE id=?1",
        [&agent.agent_id],
    )
    .unwrap();
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &uri,
        None,
        Some("interrupt-lost-ack"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{replay}");
    assert_eq!(replay["error"]["details"]["operation_id"], operation_id);
    assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 1);
    db.execute(
        "UPDATE sessions SET status='ready' WHERE id=?1",
        [&agent.agent_id],
    )
    .unwrap();
    let sends_before = provider.send_invocations.load(Ordering::SeqCst);
    let resumes_before = provider.restore_call_count().await;
    let admissions_before = store.repository().list_turn_admissions().unwrap().len();
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let fresh =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128).unwrap());
    fresh.recover_startup().await.unwrap();
    for keyed in [false, true] {
        let input: SendTurnInput =
            serde_json::from_value(json!({"input":[{"type":"text","text":"must remain fenced"}]}))
                .unwrap();
        let error = if keyed {
            fresh
                .send_operator_user_turn(
                    &agent.agent_id,
                    input,
                    runtime_core::OperationActor::operator("fixture"),
                    "must-not-bypass-interrupt".into(),
                )
                .await
                .unwrap_err()
        } else {
            fresh.send_turn(&agent.agent_id, input).await.unwrap_err()
        };
        assert!(
            matches!(error, RuntimeError::OperationUnresolved {operation_id:id,..} if id==operation_id)
        );
    }
    let before_resume = fresh.get_session(&agent.agent_id).await.unwrap();
    let error = fresh
        .resume_session(
            &agent.agent_id,
            ResumeSessionInput {
                provider_session_ref: None,
                canonical_provider_session_ref: None,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Unsupported(_)));
    assert_eq!(
        fresh.get_session(&agent.agent_id).await.unwrap(),
        before_resume
    );
    assert_eq!(
        provider.send_invocations.load(Ordering::SeqCst),
        sends_before
    );
    assert_eq!(provider.restore_call_count().await, resumes_before);
    assert_eq!(
        store.repository().list_turn_admissions().unwrap().len(),
        admissions_before
    );
    assert!(store
        .get_operation(&operation_id)
        .unwrap()
        .unwrap()
        .operation
        .exact_terminal_result
        .is_none());
}

#[tokio::test]
async fn raw_legacy_native_session_creation_is_refused_without_provider_or_store_effects() {
    let (router, token, temp, provider, runtime) =
        build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let events_before: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();
    let (status, response) = call(
        &router,
        &token,
        "POST",
        "/v1/sessions",
        Some(json!({"provider":"codex","model":"test-model","metadata":{}})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(provider.create_call_count(), 0);
    assert!(runtime.list_sessions().await.is_empty());
    let events_after: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events_after, events_before);
}

#[tokio::test]
async fn raw_legacy_lifecycle_and_interrupt_refuse_for_existing_and_missing_targets() {
    let (router, token, temp, provider, runtime, _app, history) =
        build_test_router_with_history(TeamMcpPolicy::default(), |_| {
            vec![json!({"provider":"codex","model":"test-model"})]
        })
        .await;
    provider.allow_lifecycle_close_for_test();
    let legacy = historical_session_json(&history[0]);
    let id = legacy["id"].as_str().unwrap();
    let before = runtime.get_session(id).await.unwrap();
    let root = temp.path().join("canonical-refusal-target");
    fs::create_dir_all(&root).unwrap();
    let workspace = register_control_workspace(&router, &token, &root, "Canonical refusal").await;
    let canonical = create_control_agent_with_permission(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Canonical",
        "workspace_write",
    )
    .await;
    let canonical_before = runtime.get_session(&canonical.agent_id).await.unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let event_count: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();
    for target in [id, canonical.agent_id.as_str(), "missing-target"] {
        for suffix in ["resume", "close", "turns/unknown-turn/interrupt"] {
            let (status, body) = call(
                &router,
                &token,
                "POST",
                &format!("/v1/sessions/{target}/{suffix}"),
                Some(json!({})),
                Some("cannot-authorize-raw-native"),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert!(body["error"]
                .as_str()
                .unwrap()
                .contains("legacy native mutation is unavailable"));
        }
    }
    let (status, body) = call(
        &router,
        &token,
        "POST",
        "/v1/teams/missing-team/interrupt-all",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("legacy native mutation is unavailable"));
    assert_eq!(runtime.get_session(id).await.unwrap(), before);
    assert_eq!(
        runtime.get_session(&canonical.agent_id).await.unwrap(),
        canonical_before
    );
    assert_eq!(provider.restore_call_count().await, 0);
    assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 0);
    let after: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after, event_count);
}

#[tokio::test]
async fn raw_legacy_worktree_refuses_before_git_hooks_or_claims() {
    let (router, token, temp, provider, _runtime, _app, history) =
        build_test_router_with_history(TeamMcpPolicy::default(), |root| {
            let repo = root.join("raw-refusal-repo");
            super::workspace_worktree_routes::seeded_repo(&repo);
            vec![json!({"provider":"codex","model":"test-model","cwd":repo})]
        })
        .await;
    let repo = temp.path().join("raw-refusal-repo");
    let source = historical_session_json(&history[0]);
    let native = || {
        std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["worktree", "list", "--porcelain"])
            .output()
            .unwrap()
            .stdout
    };
    let before = native();
    let calls = provider.create_call_count();
    let (status, body) = call(&router, &token, "POST", "/v1/worktrees", Some(json!({
        "source_session_id":source["id"], "worktree_name":"must-not-exist", "branch_prefix":"gg",
        "run_init_script":true, "operation_id":"caller-supplied-native-reservation-owner"
    })), Some("caller-key")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("legacy native mutation is unavailable"));
    assert_eq!(native(), before);
    assert_eq!(provider.create_call_count(), calls);
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    for table in [
        "managed_worktrees",
        "managed_worktree_claims",
        "runtime_operation_effects",
    ] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
}

#[tokio::test]
async fn workspace_interrupt_late_receipt_failures_keep_original_coordinate_and_never_redispatch() {
    for fault in ["event", "effect"] {
        let (router, token, temp, provider, runtime) =
            build_test_router_with_provider_and_runtime(TeamMcpPolicy::default()).await;
        let root = temp.path().join("interrupt-late-fault");
        fs::create_dir_all(&root).unwrap();
        let workspace =
            register_control_workspace(&router, &token, &root, "Interrupt receipt").await;
        let agent = create_control_agent_with_permission(
            &router,
            &token,
            &workspace.workspace_id,
            &root,
            "Active",
            "workspace_write",
        )
        .await;
        let _turn_id = start_native_interrupt_fixture(
            &router,
            &token,
            &runtime,
            &provider,
            &temp,
            &workspace.workspace_id,
            &agent.agent_id,
        )
        .await;
        let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
        let sql = if fault == "event" {
            "CREATE TRIGGER fail_interrupt_receipt BEFORE INSERT ON runtime_events WHEN NEW.kind='turn.interrupt_requested' BEGIN SELECT RAISE(ABORT,'late interrupt event fault'); END;"
        } else {
            "CREATE TRIGGER fail_interrupt_receipt BEFORE UPDATE ON runtime_operation_effects WHEN NEW.effect_kind='workspace_turn_interrupt' AND NEW.phase='finalized' BEGIN SELECT RAISE(ABORT,'late interrupt effect fault'); END;"
        };
        db.execute_batch(sql).unwrap();
        let uri = format!("/v2/workspaces/{}/interrupt", workspace.workspace_id);
        let (status, body) = call(
            &router,
            &token,
            "POST",
            &uri,
            None,
            Some("original-late-fault"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{fault}: {body}");
        let id = body["error"]["details"]["operation_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 1);
        let store = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: temp.path().join("runtime.sqlite3"),
        });
        let pending = store.get_operation(&id).unwrap().unwrap();
        assert!(pending.operation.exact_terminal_result.is_none());
        assert_eq!(pending.effects[0].target_id, agent.agent_id);
        assert_eq!(
            pending.effects[0].phase,
            if fault == "event" {
                "uncertain"
            } else {
                "started"
            }
        );
        let (retry_status, retry_body) = call(
            &router,
            &token,
            "POST",
            &uri,
            None,
            Some("original-late-fault"),
        )
        .await;
        assert_eq!(retry_status, StatusCode::CONFLICT, "{fault}: {retry_body}");
        assert_eq!(retry_body["error"]["details"]["operation_id"], id);
        assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 1);
        db.execute_batch("DROP TRIGGER fail_interrupt_receipt")
            .unwrap();
        let (status, replay) = call(
            &router,
            &token,
            "POST",
            &uri,
            None,
            Some("original-late-fault"),
        )
        .await;
        if fault == "event" {
            assert_eq!(status, StatusCode::CONFLICT, "{replay}");
            assert_eq!(replay["error"]["details"]["operation_id"], id);
        } else {
            assert_eq!(status, StatusCode::OK, "{replay}");
            assert_eq!(replay["operation_id"], id);
            assert_eq!(replay["interrupted_agent_ids"], json!([agent.agent_id]));
        }
        assert_eq!(provider.interrupt_invocations.load(Ordering::SeqCst), 1);
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_native_interrupt_fixture(
    router: &Router,
    token: &str,
    runtime: &Arc<RuntimeSessionManager>,
    provider: &Arc<TestProvider>,
    temp: &tempfile::TempDir,
    workspace_id: &str,
    agent_id: &str,
) -> String {
    finish_test_onboarding(runtime, workspace_id, agent_id).await;
    let before = provider.send_invocations.load(Ordering::SeqCst);
    let (status, response) = call(
        router,
        token,
        "POST",
        &format!("/v2/agents/{agent_id}/turns"),
        Some(json!({"input":[{"type":"text","text":"model_assign_wait_for_interrupt"}]})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(provider.send_invocations.load(Ordering::SeqCst), before + 1);
    let turn_id = response["turn_id"].as_str().unwrap().to_string();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    let admission = store
        .repository()
        .list_turn_admissions()
        .unwrap()
        .into_iter()
        .find(|row| row.turn_id == turn_id)
        .unwrap();
    assert_eq!(
        admission.dispatch_state,
        runtime_core::TurnDispatchState::Dispatched
    );
    turn_id
}
