use super::*;

#[tokio::test]
async fn queue_order_pause_and_settings_survive_manager_restart() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    store
        .replace_process_scheduler_settings(&scheduler_settings(1, 1_000_000, true))
        .expect("persist paused scheduler");

    let config = process_config(&temp_dir, 8, 99_999);
    let manager = RuntimeProcessManager::new(store.clone(), config.clone())
        .await
        .expect("first process manager");
    let first = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: None,
            command: "printf '1\n' >> order.txt".to_string(),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit first");
    let second = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_owner".to_string()),
            tool_call_id: None,
            command: "printf '2\n' >> order.txt".to_string(),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit second");
    manager
        .reorder_queue(
            &second.process.process_id,
            Some(&first.process.process_id),
            None,
        )
        .await
        .expect("reorder queue");
    drop(manager);

    let manager = RuntimeProcessManager::new(store.clone(), config)
        .await
        .expect("restarted process manager");
    let persisted_settings = manager.scheduler_settings().await;
    assert!(persisted_settings.paused);
    assert_eq!(persisted_settings.max_concurrent, 1);
    assert_eq!(persisted_settings.capture_limit_bytes, 1_000_000);

    let first_row = store
        .get_managed_process(&first.process.process_id)
        .expect("first row")
        .expect("first exists");
    let second_row = store
        .get_managed_process(&second.process.process_id)
        .expect("second row")
        .expect("second exists");
    assert!(second_row.queue_order < first_row.queue_order);

    manager.resume_scheduler().await.expect("resume scheduler");
    wait_for_terminal(&manager, &second.process.process_id).await;
    wait_for_terminal(&manager, &first.process.process_id).await;
    assert_eq!(
        std::fs::read_to_string(temp_dir.path().join("order.txt")).expect("order file"),
        "2\n1\n"
    );
    let events = store
        .list_runtime_events(
            Some((
                runtime_core::RuntimeEventScope::Process,
                second.process.process_id.as_str(),
            )),
            None,
            100,
        )
        .expect("process events after restart");
    let event_kinds = events
        .iter()
        .map(|event| event.kind.as_str())
        .collect::<Vec<_>>();
    assert!(event_kinds.contains(&"process.queued"));
    assert!(event_kinds.contains(&"process.started"));
    assert!(event_kinds.contains(&"process.completed"));
}

#[tokio::test]
async fn launch_reservation_is_requeued_after_restart_without_native_spawn() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let persisted_settings = scheduler_settings(1, 1024, true);
    store
        .replace_process_scheduler_settings(&persisted_settings)
        .expect("persist paused scheduler");

    let stdout = temp_dir.path().join("claim.stdout");
    let stderr = temp_dir.path().join("claim.stderr");
    std::fs::write(&stdout, []).expect("stdout");
    std::fs::write(&stderr, []).expect("stderr");
    store
        .admit_managed_process(&ManagedProcessAdmission {
            process_id: "proc_claim_recovery".to_string(),
            owner_session_id: None,
            workspace_id: None,
            tool_call_id: None,
            command: json!({ "command": "printf should-not-run" }),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
            stdout_path: stdout.display().to_string(),
            stderr_path: stderr.display().to_string(),
            capture_limit_bytes: 1024,
            admitted_at: 1,
        })
        .expect("admit process");
    let mut claim_settings = persisted_settings.clone();
    claim_settings.paused = false;
    let claimed = store
        .claim_managed_processes(&claim_settings, 2)
        .expect("claim process");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].status, "launch_reserved");

    let manager = RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 1024))
        .await
        .expect("restarted manager");
    assert!(manager.scheduler_settings().await.paused);
    let recovered = store
        .get_managed_process("proc_claim_recovery")
        .expect("read recovered claim")
        .expect("recovered claim row");
    assert_eq!(recovered.status, "queued");
    assert!(recovered.pid.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn startup_matching_os_identity_is_safely_terminated_and_reconciled() {
    use std::os::unix::process::CommandExt;

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let persisted_settings = scheduler_settings(1, 1024, true);
    store
        .replace_process_scheduler_settings(&persisted_settings)
        .expect("persist paused scheduler");

    let mut child = std::process::Command::new("sh");
    child.arg("-c").arg("sleep 30").process_group(0);
    let mut child = child.spawn().expect("spawn recovery child");
    let pid = child.id();
    let identity = crate::os_process::capture_managed_process_identity(pid)
        .expect("capture managed process identity");
    let stdout = temp_dir.path().join("survivor.stdout");
    let stderr = temp_dir.path().join("survivor.stderr");
    std::fs::write(&stdout, []).expect("stdout");
    std::fs::write(&stderr, []).expect("stderr");
    store
        .admit_managed_process(&ManagedProcessAdmission {
            process_id: "proc_survivor".to_string(),
            owner_session_id: None,
            workspace_id: None,
            tool_call_id: None,
            command: json!({ "command": "sleep 30" }),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
            stdout_path: stdout.display().to_string(),
            stderr_path: stderr.display().to_string(),
            capture_limit_bytes: 1024,
            admitted_at: 1,
        })
        .expect("admit survivor");
    let mut claim_settings = persisted_settings.clone();
    claim_settings.paused = false;
    let claim = store
        .claim_managed_processes(&claim_settings, 2)
        .expect("claim survivor")
        .into_iter()
        .next()
        .expect("survivor claim");
    store
        .mark_managed_process_running(
            &claim.process_id,
            claim.claim_generation,
            i64::from(pid),
            &identity,
            3,
        )
        .expect("mark survivor running")
        .expect("running survivor row");

    let _manager = RuntimeProcessManager::new(store.clone(), process_config(&temp_dir, 1, 1024))
        .await
        .expect("startup recovery succeeds");
    let status = child.wait().expect("recovered child exits");
    assert!(!status.success());
    let recovered = store
        .get_managed_process("proc_survivor")
        .expect("read survivor")
        .expect("survivor row");
    assert_eq!(recovered.status, "interrupted");
    assert_eq!(
        recovered.terminal_reason.as_deref(),
        Some("startup_reconciliation")
    );
}

#[tokio::test]
async fn workspace_concurrency_override_is_enforced_by_atomic_claims() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");

    for (process_id, workspace_id, order) in [
        ("proc_a1", "ws_a", 1_i64),
        ("proc_a2", "ws_a", 2_i64),
        ("proc_b1", "ws_b", 3_i64),
    ] {
        let stdout = temp_dir.path().join(format!("{process_id}.stdout"));
        let stderr = temp_dir.path().join(format!("{process_id}.stderr"));
        std::fs::write(&stdout, []).expect("stdout");
        std::fs::write(&stderr, []).expect("stderr");
        store
            .admit_managed_process(&ManagedProcessAdmission {
                process_id: process_id.to_string(),
                owner_session_id: Some(format!("sess_{process_id}")),
                workspace_id: Some(workspace_id.to_string()),
                tool_call_id: None,
                command: json!({ "command": "true" }),
                cwd: Some(temp_dir.path().display().to_string()),
                timeout_ms: None,
                stdout_path: stdout.display().to_string(),
                stderr_path: stderr.display().to_string(),
                capture_limit_bytes: 1024,
                admitted_at: order,
            })
            .expect("admit durable process");
    }

    let mut settings = scheduler_settings(2, 1024, false);
    settings
        .workspace_max_concurrent
        .insert("ws_a".to_string(), 1);
    let claimed = store
        .claim_managed_processes(&settings, 10)
        .expect("claim processes");
    let claimed_ids = claimed
        .iter()
        .map(|record| record.process_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(claimed_ids, vec!["proc_a1", "proc_b1"]);
    assert_eq!(
        store
            .get_managed_process("proc_a2")
            .expect("a2 row")
            .expect("a2 exists")
            .status,
        "queued"
    );
}

#[tokio::test]
async fn startup_identity_mismatch_fails_closed_without_signaling_reused_pid() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let settings = scheduler_settings(1, 1024, true);
    store
        .replace_process_scheduler_settings(&settings)
        .expect("persist scheduler");

    let stdout = temp_dir.path().join("identity.stdout");
    let stderr = temp_dir.path().join("identity.stderr");
    std::fs::write(&stdout, []).expect("stdout");
    std::fs::write(&stderr, []).expect("stderr");
    store
        .admit_managed_process(&ManagedProcessAdmission {
            process_id: "proc_identity".to_string(),
            owner_session_id: None,
            workspace_id: None,
            tool_call_id: None,
            command: json!({ "command": "sleep 30" }),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
            stdout_path: stdout.display().to_string(),
            stderr_path: stderr.display().to_string(),
            capture_limit_bytes: 1024,
            admitted_at: 1,
        })
        .expect("admit process");

    let mut claim_settings = settings.clone();
    claim_settings.paused = false;
    let claim = store
        .claim_managed_processes(&claim_settings, 2)
        .expect("claim process")
        .into_iter()
        .next()
        .expect("one claim");
    store
        .mark_managed_process_running(
            &claim.process_id,
            claim.claim_generation,
            i64::from(std::process::id()),
            "spoofed-process-identity",
            3,
        )
        .expect("mark running")
        .expect("running row");

    let error = RuntimeProcessManager::new(store, process_config(&temp_dir, 1, 1024))
        .await
        .err()
        .expect("identity mismatch must fail closed");
    assert!(
        error.to_string().contains("different OS start identity"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn completion_turn_is_exactly_once_across_post_admission_restart_boundary() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let (runtime, _team_comms) = build_runtime_and_team_comms(store.clone()).await;
    let session = create_test_session(&runtime, temp_dir.path().to_string_lossy().as_ref()).await;
    let config = process_config(&temp_dir, 1, 1_000_000);

    let manager =
        RuntimeProcessManager::new_with_runtime(store.clone(), runtime.clone(), config.clone())
            .await
            .expect("process manager");
    let admitted = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some(session.id.clone()),
            tool_call_id: Some("call_completion".to_string()),
            command: "printf completion-ok".to_string(),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit process");
    let terminal = wait_for_terminal(&manager, &admitted.process.process_id).await;
    assert_eq!(terminal.process.status, "completed");

    let delivered = wait_for_completion_delivery(&store, &admitted.process.process_id).await;
    let delivered_turn_id = delivered
        .completion_turn_id
        .clone()
        .expect("completion turn id");
    let turns = runtime
        .list_session_turns(&session.id)
        .await
        .expect("list turns");
    let completion_turns = turns
        .iter()
        .filter(|turn| turn.source.as_deref() == Some("automation_context"))
        .collect::<Vec<_>>();
    assert_eq!(completion_turns.len(), 1);
    assert_eq!(completion_turns[0].id, delivered_turn_id);

    store
        .update_managed_process_completion(
            &admitted.process.process_id,
            &ProcessCompletionUpdate {
                state: PROCESS_COMPLETION_INJECTING.to_string(),
                turn_id: None,
                attempt_count: delivered.completion_attempt_count,
                last_error: None,
                updated_at: delivered.updated_at.saturating_add(1),
            },
        )
        .expect("simulate crash after turn admission");
    drop(manager);

    let restarted = RuntimeProcessManager::new_with_runtime(store.clone(), runtime.clone(), config)
        .await
        .expect("restarted process manager");
    let recovered = store
        .get_managed_process(&admitted.process.process_id)
        .expect("read recovered process")
        .expect("recovered process");
    assert_eq!(recovered.completion_state, PROCESS_COMPLETION_DELIVERED);
    assert_eq!(
        recovered.completion_turn_id.as_deref(),
        Some(delivered_turn_id.as_str())
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let turns = runtime
        .list_session_turns(&session.id)
        .await
        .expect("list turns after restart");
    assert_eq!(
        turns
            .iter()
            .filter(|turn| turn.source.as_deref() == Some("automation_context"))
            .count(),
        1
    );
    drop(restarted);
}

#[tokio::test]
async fn completion_retry_survives_proven_provider_unavailability_without_duplicate_dispatch() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let provider = Arc::new(FlakyCompletionProvider::default());
    let mut registry = ProviderRegistry::new();
    registry
        .register(provider.clone())
        .expect("register flaky provider");
    let runtime = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512)
            .expect("runtime manager"),
    );
    let session = create_test_session(&runtime, temp_dir.path().to_string_lossy().as_ref()).await;
    let manager = RuntimeProcessManager::new_with_runtime(
        store.clone(),
        runtime.clone(),
        process_config(&temp_dir, 1, 1_000_000),
    )
    .await
    .expect("process manager");

    let admitted = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some(session.id.clone()),
            tool_call_id: Some("flaky_completion".to_string()),
            command: "printf outage-retry".to_string(),
            cwd: Some(temp_dir.path().display().to_string()),
            timeout_ms: None,
        })
        .await
        .expect("admit process");
    let terminal = wait_for_terminal(&manager, &admitted.process.process_id).await;
    assert_eq!(terminal.process.status, "completed");

    let delivered = wait_for_completion_delivery(&store, &admitted.process.process_id).await;
    assert_eq!(
        provider.send_attempts.load(Ordering::SeqCst),
        2,
        "proven-not-dispatched completion should be retried exactly once"
    );
    assert_eq!(
        provider.accepted_sends.load(Ordering::SeqCst),
        1,
        "only one completion turn may reach the provider"
    );
    let correlation = format!("process_completion:{}", admitted.process.process_id);
    let mut correlated = store
        .list_turn_admissions()
        .expect("list turn admissions")
        .into_iter()
        .filter(|admission| admission.correlation.correlation_id.as_deref() == Some(&correlation))
        .collect::<Vec<_>>();
    correlated.sort_by(|left, right| {
        left.admitted_at
            .cmp(&right.admitted_at)
            .then(left.turn_id.cmp(&right.turn_id))
    });
    assert_eq!(correlated.len(), 2);
    assert!(correlated.iter().any(
        |admission| admission.dispatch_state == runtime_core::TurnDispatchState::NotDispatched
    ));
    assert!(correlated
        .iter()
        .any(|admission| admission.dispatch_state == runtime_core::TurnDispatchState::Dispatched));
    assert_eq!(
        delivered.completion_turn_id.as_deref(),
        correlated
            .iter()
            .find(
                |admission| admission.dispatch_state == runtime_core::TurnDispatchState::Dispatched
            )
            .map(|admission| admission.turn_id.as_str())
    );
}
