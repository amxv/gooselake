use super::*;
use runtime_core::{ProcessManager, RuntimeError};

#[tokio::test]
async fn spawn_failure_after_launch_tears_down_child_and_leaves_no_ghost_process() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(FailingProcessUpsertStore::default());
    let manager = RuntimeProcessManager::new(
        store.clone(),
        ProcessManagerConfig {
            enabled: true,
            max_concurrent: 1,
            default_timeout_ms: 60_000,
            max_output_bytes_per_process: 1_000_000,
            allow_shell: true,
            completed_retention_ms: 60_000,
            output_event_sample_bytes: 1024,
            log_dir: temp_dir.path().join("logs"),
        },
    )
    .await
    .expect("build process manager");

    let result = manager
        .run_process(runtime_core::ProcessRunRequest {
            caller_session_id: Some("sess_test".to_string()),
            tool_call_id: None,
            command: "sleep 5".to_string(),
            cwd: None,
            timeout_ms: None,
        })
        .await;
    assert!(matches!(result, Err(RuntimeError::Io(_))));

    let rows = manager
        .list_processes(ProcessListRequest {
            caller_session_id: Some("sess_test".to_string()),
            include_completed: true,
        })
        .await
        .expect("list processes");
    assert!(rows.is_empty());
    assert_eq!(store.upsert_process_calls.load(AtomicOrdering::Relaxed), 1);
}
