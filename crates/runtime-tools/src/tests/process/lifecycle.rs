use super::*;

#[tokio::test]
async fn durable_admission_precedes_spawn_and_queue_wait_does_not_consume_execution_timeout() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    store
        .replace_process_scheduler_settings(&scheduler_settings(1, 1_000_000, true))
        .expect("persist paused scheduler");

    let manager =
        RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 1_000_000))
            .await
            .expect("process manager");
    let timeout_marker = temp_dir.path().join("timeout-marker");
    let admitted = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: Some("call_1".to_string()),
            command: format!(
                "printf started > {}; sleep 2; printf finished >> {}",
                timeout_marker.display(),
                timeout_marker.display()
            ),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: Some(100),
        })
        .await
        .expect("admit process");

    assert_eq!(admitted.process.status, "queued");
    assert!(admitted.process.pid.is_none());
    let durable = store
        .get_managed_process(&admitted.process.process_id)
        .expect("read durable process")
        .expect("durable process row");
    assert_eq!(durable.status, "queued");
    assert_eq!(durable.timeout_ms, Some(100));

    tokio::time::sleep(Duration::from_millis(120)).await;
    let still_queued = store
        .get_managed_process(&admitted.process.process_id)
        .expect("read queued process")
        .expect("queued process row");
    assert_eq!(still_queued.status, "queued");
    assert!(still_queued.execution_started_at.is_none());

    manager.resume_scheduler().await.expect("resume scheduler");
    let terminal = wait_for_terminal(&manager, &admitted.process.process_id).await;
    assert_eq!(terminal.process.status, "timed_out");
    assert_eq!(
        std::fs::read_to_string(&timeout_marker).expect("timeout marker"),
        "started"
    );
    let durable = store
        .get_managed_process(&admitted.process.process_id)
        .expect("read terminal process")
        .expect("terminal process row");
    assert!(durable.execution_started_at.is_some());
    assert!(
        durable.execution_duration_ms.expect("execution duration") >= 80,
        "execution timeout should begin only after actual launch"
    );
}

#[tokio::test]
async fn queued_cancel_is_idempotent_silent_and_never_launches() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    store
        .replace_process_scheduler_settings(&scheduler_settings(1, 1_000_000, true))
        .expect("persist paused scheduler");

    let manager =
        RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 1_000_000))
            .await
            .expect("process manager");
    let marker = temp_dir.path().join("should-not-exist");
    let admitted = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: None,
            command: format!("printf launched > {}", marker.display()),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit process");

    let first = manager
        .kill_process(ProcessKillRequest {
            process_id: admitted.process.process_id.clone(),
            caller_session_id: Some("sess_owner".to_string()),
            reason: Some("test".to_string()),
        })
        .await
        .expect("first cancel");
    let second = manager
        .kill_process(ProcessKillRequest {
            process_id: admitted.process.process_id.clone(),
            caller_session_id: Some("sess_owner".to_string()),
            reason: Some("test_again".to_string()),
        })
        .await
        .expect("idempotent cancel");
    assert_eq!(first.process.status, "canceled");
    assert_eq!(second.process.status, "canceled");

    manager.resume_scheduler().await.expect("resume scheduler");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!marker.exists());

    let durable = store
        .get_managed_process(&admitted.process.process_id)
        .expect("read canceled process")
        .expect("canceled process row");
    assert_eq!(durable.status, "canceled");
    assert!(durable.pid.is_none());
    assert_eq!(durable.completion_state, PROCESS_COMPLETION_NOT_REQUIRED);
}

#[tokio::test]
async fn spawn_failure_is_terminalized_from_durable_admission() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let manager =
        RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 1_000_000))
            .await
            .expect("process manager");

    let missing_cwd = temp_dir.path().join("missing-cwd");
    let admitted = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: Some("spawn_failure".to_string()),
            command: "printf never-runs".to_string(),
            cwd: Some(missing_cwd.display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("durable admission succeeds before native spawn");
    assert_eq!(admitted.process.status, "queued");

    let terminal = wait_for_terminal(&manager, &admitted.process.process_id).await;
    assert_eq!(terminal.process.status, "failed");
    let durable = store
        .get_managed_process(&admitted.process.process_id)
        .expect("read spawn failure")
        .expect("spawn failure row");
    assert!(
        durable
            .terminal_reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("spawn_failed:")),
        "unexpected terminal reason: {:?}",
        durable.terminal_reason
    );
    assert!(durable.pid.is_none());
}

#[tokio::test]
async fn nonzero_exit_preserves_exit_code_and_running_cancel_is_idempotent() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let manager =
        RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 1_000_000))
            .await
            .expect("process manager");

    let failed = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: None,
            command: "exit 7".to_string(),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit failing process");
    let failed = wait_for_terminal(&manager, &failed.process.process_id).await;
    assert_eq!(failed.process.status, "failed");
    assert_eq!(failed.exit_code, Some(7));

    let running = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: None,
            command: "sleep 30".to_string(),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit long process");
    let process_id = running.process.process_id.clone();
    for _ in 0..200 {
        let record = store
            .get_managed_process(&process_id)
            .expect("read long process")
            .expect("long process row");
        if record.status == "running" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        store
            .get_managed_process(&process_id)
            .expect("read running process")
            .expect("running row")
            .status,
        "running"
    );

    manager
        .kill_process(ProcessKillRequest {
            process_id: process_id.clone(),
            caller_session_id: Some("sess_owner".to_string()),
            reason: Some("phase7_running_cancel".to_string()),
        })
        .await
        .expect("request running cancel");
    let killed = wait_for_terminal(&manager, &process_id).await;
    assert_eq!(killed.process.status, "killed");
    let replay = manager
        .kill_process(ProcessKillRequest {
            process_id: process_id.clone(),
            caller_session_id: Some("sess_owner".to_string()),
            reason: Some("idempotent_replay".to_string()),
        })
        .await
        .expect("idempotent terminal cancel");
    assert_eq!(replay.process.status, "killed");
}

#[tokio::test]
async fn process_visibility_is_workspace_scoped_but_model_cancellation_remains_owner_only() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let workspace_a_root = temp_dir.path().join("workspace-a");
    let workspace_b_root = temp_dir.path().join("workspace-b");
    std::fs::create_dir_all(&workspace_a_root).expect("workspace a root");
    std::fs::create_dir_all(&workspace_b_root).expect("workspace b root");
    let workspace_a = register_workspace(&store, &workspace_a_root, "Workspace A");
    let workspace_b = register_workspace(&store, &workspace_b_root, "Workspace B");
    let (runtime, _team_comms) = build_runtime_and_team_comms(store.clone()).await;
    let owner = runtime
        .create_workspace_agent(
            &workspace_a.workspace_id,
            workspace_agent_request(&workspace_a_root, "Owner"),
            "operator",
        )
        .await
        .expect("create owner agent");
    let peer = runtime
        .create_workspace_agent(
            &workspace_a.workspace_id,
            workspace_agent_request(&workspace_a_root, "Peer"),
            "operator",
        )
        .await
        .expect("create peer agent");
    let outsider = runtime
        .create_workspace_agent(
            &workspace_b.workspace_id,
            workspace_agent_request(&workspace_b_root, "Outsider"),
            "operator",
        )
        .await
        .expect("create outsider agent");

    store
        .replace_process_scheduler_settings(&scheduler_settings(1, 1_000_000, true))
        .expect("pause scheduler");
    let manager =
        RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 1_000_000))
            .await
            .expect("process manager");
    let admitted = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some(owner.agent_id.clone()),
            tool_call_id: None,
            command: "sleep 30".to_string(),
            cwd: Some(workspace_a_root.display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit workspace process");
    let process_id = admitted.process.process_id.clone();

    let peer_view = manager
        .get_process(ProcessGetRequest {
            process_id: process_id.clone(),
            caller_session_id: Some(peer.agent_id.clone()),
        })
        .await
        .expect("same-workspace peer can observe process");
    assert_eq!(peer_view.process.status, "queued");
    let peer_list = manager
        .list_processes(runtime_core::ProcessListRequest {
            caller_session_id: Some(peer.agent_id.clone()),
            include_completed: false,
        })
        .await
        .expect("peer list");
    assert_eq!(peer_list.len(), 1);
    assert_eq!(peer_list[0].process_id, process_id);

    let outsider_get = manager
        .get_process(ProcessGetRequest {
            process_id: process_id.clone(),
            caller_session_id: Some(outsider.agent_id.clone()),
        })
        .await;
    assert!(
        matches!(outsider_get, Err(RuntimeError::InvalidState(_))),
        "cross-workspace get unexpectedly succeeded/failed differently: {outsider_get:?}; workspace_a={}, workspace_b={}, owner_workspace={:?}",
        workspace_a.workspace_id,
        workspace_b.workspace_id,
        store
            .get_managed_process(&process_id)
            .expect("owner process read")
            .and_then(|row| row.workspace_id)
    );
    assert!(manager
        .list_processes(runtime_core::ProcessListRequest {
            caller_session_id: Some(outsider.agent_id.clone()),
            include_completed: false,
        })
        .await
        .expect("outsider list")
        .is_empty());
    assert!(matches!(
        manager
            .kill_process(ProcessKillRequest {
                process_id: process_id.clone(),
                caller_session_id: Some(peer.agent_id.clone()),
                reason: Some("peer_should_not_cancel".to_string()),
            })
            .await,
        Err(RuntimeError::InvalidState(_))
    ));

    let operator_cancel = manager
        .kill_process(ProcessKillRequest {
            process_id: process_id.clone(),
            caller_session_id: None,
            reason: Some("trusted_operator".to_string()),
        })
        .await
        .expect("trusted operator can cancel");
    assert_eq!(operator_cancel.process.status, "canceled");
}

#[tokio::test]
async fn capture_limit_is_per_stream_and_persisted_as_authoritative_truncation_metadata() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");

    let manager = RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 32))
        .await
        .expect("process manager");
    let admitted = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: None,
            command: "printf 'abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'"
                .to_string(),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit process");
    let terminal = wait_for_terminal(&manager, &admitted.process.process_id).await;
    assert_eq!(terminal.process.status, "completed");
    assert_eq!(terminal.stdout_bytes, 32);
    assert!(terminal.stdout_truncated);

    let durable = store
        .get_managed_process(&admitted.process.process_id)
        .expect("read terminal process")
        .expect("terminal process row");
    assert_eq!(durable.stdout_captured_bytes, 32);
    assert!(durable.stdout_truncated);
    assert_eq!(
        std::fs::metadata(&durable.stdout_path)
            .expect("stdout metadata")
            .len(),
        32
    );

    let logs = manager
        .read_process_logs(ProcessLogReadRequest {
            process_id: admitted.process.process_id,
            caller_session_id: Some("sess_owner".to_string()),
            stream: Some("stdout".to_string()),
            head_lines: Some(0),
            tail_lines: Some(80),
            max_bytes: None,
        })
        .await
        .expect("read process logs");
    assert_eq!(logs.len(), 1);
    assert!(logs[0].truncated);
}
