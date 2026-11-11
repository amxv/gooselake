use super::workspace_agent::apply_schema_through;
use super::*;
use runtime_core::{
    ManagedProcessAdmission, ManagedProcessTerminalUpdate, ProcessSchedulerSettings,
};

fn admission(process_id: &str, tool_call_id: &str) -> ManagedProcessAdmission {
    ManagedProcessAdmission {
        process_id: process_id.into(),
        owner_session_id: Some("session_1".into()),
        workspace_id: Some("workspace_events".into()),
        tool_call_id: Some(tool_call_id.into()),
        command: serde_json::json!({"command":"printf fixture"}),
        cwd: Some("/tmp/isolated-process".into()),
        timeout_ms: Some(1000),
        stdout_path: format!("/tmp/{process_id}.stdout"),
        stderr_path: format!("/tmp/{process_id}.stderr"),
        capture_limit_bytes: 1024,
        admitted_at: 100,
    }
}

fn scheduler() -> ProcessSchedulerSettings {
    ProcessSchedulerSettings {
        max_concurrent: 2,
        workspace_max_concurrent: std::collections::BTreeMap::new(),
        capture_limit_bytes: 1024,
        paused: false,
        pause_reason: None,
        updated_at: 50,
    }
}

fn terminal() -> ManagedProcessTerminalUpdate {
    ManagedProcessTerminalUpdate {
        status: "completed".into(),
        terminal_reason: Some("exited".into()),
        exit_code: Some(0),
        signal: None,
        ended_at: 300,
        execution_duration_ms: Some(100),
        stdout_captured_bytes: 16,
        stderr_captured_bytes: 0,
        stdout_truncated: false,
        stderr_truncated: false,
        completion_required: false,
    }
}

fn scoped(
    repository: &SqliteRuntimeRepository,
    process_id: &str,
) -> Vec<runtime_core::RuntimeEventRecord> {
    repository
        .list_runtime_events(Some((RuntimeEventScope::Process, process_id)), None, 30)
        .unwrap()
}

#[test]
fn process_lifecycle_authority_and_critical_events_commit_together_and_survive_restart() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("runtime.sqlite3");
    let repository = SqliteRuntimeRepository::new(db_path.clone());
    repository.initialize_schema().unwrap();
    let mut session = sample_session();
    session.worktree_id = None;
    repository.upsert_session(&session).unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: db_path.clone(),
    });
    let mut receiver = store.subscribe_event_commits().unwrap();

    repository
        .admit_managed_process(&admission("proc_atomic", "call_atomic"))
        .unwrap();
    assert!(
        receiver.try_recv().is_ok(),
        "committed queued event wakes canonical SSE"
    );
    let queued = scoped(&repository, "proc_atomic");
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].kind, "process.queued");
    assert_eq!(queued[0].payload["queue_order"], 1);
    assert_eq!(queued[0].event_id, "evt_proc_proc_atomic_process.queued");

    // A legacy live-event bridge looks up that deterministic event rather
    // than inserting a second event with a random identity.
    let same = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: queued[0].event_id.clone(),
            scope: RuntimeEventScope::Process,
            scope_id: "proc_atomic".into(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "process.queued".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"replacement":true}),
            provider: None,
            provider_seq: None,
            created_at: 999,
        })
        .unwrap();
    assert_eq!(same, queued[0]);
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    let claim = repository
        .claim_managed_processes(&scheduler(), 150)
        .unwrap();
    assert_eq!(claim.len(), 1);
    let running = repository
        .mark_managed_process_running(
            "proc_atomic",
            claim[0].claim_generation,
            4242,
            "pid-start-identity",
            200,
        )
        .unwrap()
        .unwrap();
    assert_eq!(running.status, "running");
    assert!(receiver.try_recv().is_ok());
    let requested = repository
        .request_managed_process_cancel("proc_atomic", Some("session_1"), 220)
        .unwrap()
        .unwrap();
    assert!(requested.cancel_requested);
    assert!(receiver.try_recv().is_ok());
    assert!(
        repository
            .request_managed_process_cancel("proc_atomic", Some("session_1"), 230)
            .unwrap()
            .unwrap()
            .cancel_requested
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let terminal_record = repository
        .terminalize_managed_process("proc_atomic", &terminal())
        .unwrap()
        .unwrap();
    assert_eq!(terminal_record.status, "completed");
    assert!(receiver.try_recv().is_ok());
    assert_eq!(
        repository
            .terminalize_managed_process("proc_atomic", &terminal())
            .unwrap()
            .unwrap(),
        terminal_record
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    let restarted = SqliteRuntimeRepository::new(db_path);
    let events = scoped(&restarted, "proc_atomic");
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        [
            "process.queued",
            "process.started",
            "process.kill_requested",
            "process.completed"
        ]
    );
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        4
    );
    assert!(events
        .iter()
        .all(|event| event.criticality == RuntimeEventCriticality::Critical));
    assert_eq!(events[1].payload["pid"], 4242);
    assert_eq!(events[3].payload["exit_code"], 0);
    assert_eq!(events[3].payload["stdout_truncated"], false);
    assert_eq!(
        restarted
            .get_managed_process("proc_atomic")
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
}

#[test]
fn process_admission_and_running_and_terminal_event_faults_rollback_authority() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: repository.database_path.clone(),
    });
    let mut rx = store.subscribe_event_commits().unwrap();
    let db = open_connection(&repository.database_path).unwrap();

    db.execute_batch(
        "CREATE TRIGGER fail_process_queued BEFORE INSERT ON runtime_events
        WHEN NEW.kind='process.queued'
        BEGIN SELECT RAISE(FAIL, 'injected queue journal failure'); END;",
    )
    .unwrap();
    assert!(repository
        .admit_managed_process(&admission("proc_fault", "fault_one"))
        .is_err());
    assert!(repository
        .get_managed_process("proc_fault")
        .unwrap()
        .is_none());
    assert!(scoped(&repository, "proc_fault").is_empty());
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    db.execute_batch("DROP TRIGGER fail_process_queued;")
        .unwrap();
    repository
        .admit_managed_process(&admission("proc_fault", "fault_one"))
        .unwrap();
    assert!(rx.try_recv().is_ok());
    let claim = repository
        .claim_managed_processes(&scheduler(), 150)
        .unwrap();
    assert_eq!(claim.len(), 1);

    db.execute_batch(
        "CREATE TRIGGER fail_process_running BEFORE INSERT ON runtime_events
        WHEN NEW.kind='process.started'
        BEGIN SELECT RAISE(FAIL, 'injected running journal failure'); END;",
    )
    .unwrap();
    assert!(repository
        .mark_managed_process_running(
            "proc_fault",
            claim[0].claim_generation,
            202,
            "identity",
            200
        )
        .is_err());
    let pending = repository
        .get_managed_process("proc_fault")
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, "launch_reserved");
    assert!(pending.pid.is_none());
    assert_eq!(scoped(&repository, "proc_fault").len(), 1);
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    db.execute_batch("DROP TRIGGER fail_process_running;")
        .unwrap();
    repository
        .mark_managed_process_running(
            "proc_fault",
            claim[0].claim_generation,
            202,
            "identity",
            200,
        )
        .unwrap()
        .unwrap();
    assert!(rx.try_recv().is_ok());

    db.execute_batch(
        "CREATE TRIGGER fail_process_terminal BEFORE INSERT ON runtime_events
        WHEN NEW.kind='process.completed'
        BEGIN SELECT RAISE(FAIL, 'injected terminal journal failure'); END;",
    )
    .unwrap();
    assert!(repository
        .terminalize_managed_process("proc_fault", &terminal())
        .is_err());
    assert_eq!(
        repository
            .get_managed_process("proc_fault")
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert_eq!(scoped(&repository, "proc_fault").len(), 2);
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    db.execute_batch("DROP TRIGGER fail_process_terminal;")
        .unwrap();
    repository
        .terminalize_managed_process("proc_fault", &terminal())
        .unwrap();
    assert!(rx.try_recv().is_ok());
    let events = scoped(&repository, "proc_fault");
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        ["process.queued", "process.started", "process.completed"]
    );
}

#[test]
fn cancellation_and_restart_upgrade_preserve_old_state_without_fabricating_native_history() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&db_path, 28);
    let repository = SqliteRuntimeRepository::new(db_path.clone());
    repository
        .admit_managed_process(&admission("proc_upgrade", "old_admission"))
        .unwrap();
    assert!(
        scoped(&repository, "proc_upgrade").is_empty(),
        "version28 has no atomic process event trigger"
    );
    repository.initialize_schema().unwrap();
    assert!(
        scoped(&repository, "proc_upgrade").is_empty(),
        "migration must not invent prior queued native proof"
    );
    let canceled = repository
        .cancel_queued_managed_process("proc_upgrade", Some("session_1"), 210)
        .unwrap()
        .unwrap();
    assert_eq!(canceled.status, "canceled");
    let restarted = SqliteRuntimeRepository::new(db_path.clone());
    let events = scoped(&restarted, "proc_upgrade");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "process.canceled");
    assert_eq!(events[0].payload["before_launch"], true);
    assert_eq!(events[0].seq, 1);
    assert!(restarted
        .cancel_queued_managed_process("proc_upgrade", Some("session_1"), 215)
        .unwrap()
        .is_some());
    assert_eq!(scoped(&restarted, "proc_upgrade").len(), 1);
    let db = open_connection(&db_path).unwrap();
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
}

#[test]
fn independent_process_admission_writers_replay_one_event_for_identical_caller_tool_id() {
    use std::sync::{Arc, Barrier};
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut writers = Vec::new();
    for n in 0..2 {
        let peer = SqliteRuntimeRepository::new(repository.database_path.clone());
        let barrier = barrier.clone();
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            peer.admit_managed_process(&admission(&format!("proc_writer_{n}"), "call_shared"))
        }));
    }
    let results = writers
        .into_iter()
        .map(|writer| writer.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results[0].process_id, results[1].process_id);
    assert_eq!(repository.list_managed_processes().unwrap().len(), 1);
    let events = scoped(&repository, &results[0].process_id);
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].event_id,
        format!("evt_proc_{}_process.queued", results[0].process_id)
    );
    assert_eq!(
        repository
            .list_runtime_events(None, None, 100)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn queued_and_running_cancellation_journal_faults_rollback_and_replay_once() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let db = open_connection(&repository.database_path).unwrap();

    repository
        .admit_managed_process(&admission("proc_queue_fault", "queue_fault"))
        .unwrap();
    db.execute_batch(
        "CREATE TRIGGER block_process_cancel_event BEFORE INSERT ON runtime_events
         WHEN NEW.kind = 'process.canceled'
         BEGIN SELECT RAISE(ABORT, 'injected cancel event failure'); END;",
    )
    .unwrap();
    assert!(repository
        .cancel_queued_managed_process("proc_queue_fault", Some("session_1"), 150)
        .is_err());
    assert_eq!(
        repository
            .get_managed_process("proc_queue_fault")
            .unwrap()
            .unwrap()
            .status,
        "queued"
    );
    assert_eq!(scoped(&repository, "proc_queue_fault").len(), 1);
    db.execute_batch("DROP TRIGGER block_process_cancel_event;")
        .unwrap();
    assert_eq!(
        repository
            .cancel_queued_managed_process("proc_queue_fault", Some("session_1"), 160)
            .unwrap()
            .unwrap()
            .status,
        "canceled"
    );
    repository
        .cancel_queued_managed_process("proc_queue_fault", Some("session_1"), 170)
        .unwrap();
    let events = scoped(&repository, "proc_queue_fault");
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        ["process.queued", "process.canceled"]
    );
    assert_eq!(events[1].payload["before_launch"], true);

    repository
        .admit_managed_process(&admission("proc_running_fault", "running_fault"))
        .unwrap();
    let claim = repository
        .claim_managed_processes(&scheduler(), 180)
        .unwrap();
    assert_eq!(claim.len(), 1);
    repository
        .mark_managed_process_running(
            "proc_running_fault",
            claim[0].claim_generation,
            4242,
            "identity",
            200,
        )
        .unwrap();
    db.execute_batch(
        "CREATE TRIGGER block_process_kill_event BEFORE INSERT ON runtime_events
         WHEN NEW.kind = 'process.kill_requested'
         BEGIN SELECT RAISE(ABORT, 'injected kill event failure'); END;",
    )
    .unwrap();
    assert!(repository
        .request_managed_process_cancel("proc_running_fault", Some("session_1"), 220)
        .is_err());
    assert!(
        !repository
            .get_managed_process("proc_running_fault")
            .unwrap()
            .unwrap()
            .cancel_requested
    );
    assert_eq!(scoped(&repository, "proc_running_fault").len(), 2);
    db.execute_batch("DROP TRIGGER block_process_kill_event;")
        .unwrap();
    assert!(
        repository
            .request_managed_process_cancel("proc_running_fault", Some("session_1"), 230)
            .unwrap()
            .unwrap()
            .cancel_requested
    );
    repository
        .request_managed_process_cancel("proc_running_fault", Some("session_1"), 240)
        .unwrap();
    let events = scoped(&repository, "proc_running_fault");
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        [
            "process.queued",
            "process.started",
            "process.kill_requested"
        ]
    );
    assert_eq!(events[2].payload["cancel_requested"], true);
    assert_eq!(events[2].seq, 3);
}

#[test]
fn running_cancel_and_terminal_race_has_one_terminal_and_at_most_one_kill_intent() {
    use std::sync::{Arc, Barrier};
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    repository
        .admit_managed_process(&admission("proc_cancel_race", "race"))
        .unwrap();
    let claim = repository
        .claim_managed_processes(&scheduler(), 150)
        .unwrap();
    repository
        .mark_managed_process_running(
            "proc_cancel_race",
            claim[0].claim_generation,
            4242,
            "identity",
            200,
        )
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let mut writers = Vec::new();
    for cancel in [true, false] {
        let peer = SqliteRuntimeRepository::new(repository.database_path.clone());
        let barrier = barrier.clone();
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            if cancel {
                peer.request_managed_process_cancel("proc_cancel_race", Some("session_1"), 220)
                    .map(|result| result.unwrap().status)
            } else {
                peer.terminalize_managed_process("proc_cancel_race", &terminal())
                    .map(|result| result.unwrap().status)
            }
        }));
    }
    barrier.wait();
    for result in writers {
        assert!(matches!(
            result.join().unwrap().unwrap().as_str(),
            "running" | "completed"
        ));
    }
    let restarted = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        restarted
            .get_managed_process("proc_cancel_race")
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    let events = scoped(&restarted, "proc_cancel_race");
    assert_eq!(events[0].kind, "process.queued");
    assert_eq!(events[1].kind, "process.started");
    assert_eq!(events.last().unwrap().kind, "process.completed");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "process.completed")
            .count(),
        1
    );
    assert!(
        events
            .iter()
            .filter(|event| event.kind == "process.kill_requested")
            .count()
            <= 1
    );
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        (1..=events.len() as i64).collect::<Vec<_>>()
    );
}
