use crate::{SqliteRuntimeRepository, SqliteRuntimeStore, SqliteStoreConfig};
use runtime_core::{
    ManagedProcessAdmission, ProcessSchedulerSettings, RuntimeError, RuntimeEventCriticality,
    RuntimeEventRecord, RuntimeEventScope, RuntimeStore,
};
use rusqlite::Connection;
use serde_json::json;
use std::collections::BTreeMap;

struct Fixture {
    _temp: tempfile::TempDir,
    repo: SqliteRuntimeRepository,
    path: std::path::PathBuf,
}

fn defaults() -> ProcessSchedulerSettings {
    ProcessSchedulerSettings {
        max_concurrent: 2,
        workspace_max_concurrent: BTreeMap::new(),
        capture_limit_bytes: 1024,
        paused: false,
        pause_reason: None,
        updated_at: 50,
    }
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runtime.sqlite");
        let repo = SqliteRuntimeRepository::new(path.clone());
        repo.initialize_schema().unwrap();
        assert_eq!(
            repo.load_or_initialize_process_scheduler_settings(&defaults())
                .unwrap(),
            defaults()
        );
        Self {
            _temp: temp,
            repo,
            path,
        }
    }

    fn db(&self) -> Connection {
        Connection::open(&self.path).unwrap()
    }

    fn settings(&self) -> ProcessSchedulerSettings {
        self.repo
            .load_or_initialize_process_scheduler_settings(&defaults())
            .unwrap()
    }

    fn events(&self) -> Vec<RuntimeEventRecord> {
        self.repo
            .list_runtime_events(
                Some((RuntimeEventScope::System, "process_scheduler")),
                None,
                30,
            )
            .unwrap()
    }

    fn queue(&self) {
        for id in ["a", "b", "c"] {
            self.repo
                .admit_managed_process(&ManagedProcessAdmission {
                    process_id: id.into(),
                    owner_session_id: None,
                    workspace_id: None,
                    tool_call_id: None,
                    command: json!({"command":"fixture-never-executed"}),
                    cwd: None,
                    timeout_ms: Some(1000),
                    stdout_path: self
                        .path
                        .with_file_name(format!("{id}.stdout"))
                        .to_string_lossy()
                        .into_owned(),
                    stderr_path: self
                        .path
                        .with_file_name(format!("{id}.stderr"))
                        .to_string_lossy()
                        .into_owned(),
                    capture_limit_bytes: 1024,
                    admitted_at: 100,
                })
                .unwrap();
        }
    }

    fn fail_event(&self, kind: &str) {
        assert!(matches!(
            kind,
            "process.scheduler_settings_changed" | "process.queue_reordered"
        ));
        self.db()
            .execute_batch(&format!(
                "CREATE TRIGGER reject_scheduler_control BEFORE INSERT ON runtime_events
                 WHEN NEW.kind='{kind}' AND NEW.scope='system'
                   AND NEW.scope_id='process_scheduler'
                 BEGIN SELECT RAISE(ABORT,'fixture scheduler event failure'); END;"
            ))
            .unwrap();
    }

    fn allow_event(&self) {
        self.db()
            .execute_batch("DROP TRIGGER reject_scheduler_control;")
            .unwrap();
    }
}

#[test]
fn scheduler_control_change_is_critical_and_timestamp_only_retry_survives_restart() {
    let fixture = Fixture::new();
    assert!(
        fixture.events().is_empty(),
        "bootstrap does not invent operator history"
    );
    let previous = fixture.settings();
    let mut requested = previous.clone();
    requested.paused = true;
    requested.pause_reason = Some("maintenance".into());
    requested.updated_at = 100;
    let saved = fixture
        .repo
        .replace_process_scheduler_settings(&requested)
        .unwrap();
    assert_eq!(saved, requested);
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "process.scheduler_settings_changed");
    assert_eq!(events[0].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(events[0].payload["previous_settings"], json!(previous));
    assert_eq!(events[0].payload["settings"], json!(saved));
    assert_eq!(events[0].created_at, 100);

    let reopened = SqliteRuntimeRepository::new(fixture.path.clone());
    let mut different_bootstrap = defaults();
    different_bootstrap.max_concurrent = 99;
    assert_eq!(
        reopened
            .load_or_initialize_process_scheduler_settings(&different_bootstrap)
            .unwrap(),
        saved
    );
    let mut retry = requested;
    retry.updated_at = 999;
    assert_eq!(
        reopened.replace_process_scheduler_settings(&retry).unwrap(),
        saved
    );
    assert_eq!(fixture.settings(), saved);
    assert_eq!(fixture.events(), events);
}

#[test]
fn scheduler_control_event_fault_rolls_back_settings_and_emits_no_commit_notification() {
    let fixture = Fixture::new();
    let previous = fixture.settings();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: fixture.path.clone(),
    });
    let mut notifications = store.subscribe_event_commits().unwrap();
    fixture.fail_event("process.scheduler_settings_changed");
    let mut requested = previous.clone();
    requested.max_concurrent = 3;
    requested.updated_at = 100;
    let error = fixture
        .repo
        .replace_process_scheduler_settings(&requested)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("fixture scheduler event failure"),
        "{error}"
    );
    assert_eq!(fixture.settings(), previous);
    assert!(fixture.events().is_empty());
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    fixture.allow_event();
    assert_eq!(
        fixture
            .repo
            .replace_process_scheduler_settings(&requested)
            .unwrap(),
        requested
    );
    assert!(notifications.try_recv().is_ok());
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    requested.updated_at = 900;
    let saved = fixture
        .repo
        .replace_process_scheduler_settings(&requested)
        .unwrap();
    assert_eq!(saved.updated_at, 100);
    assert_eq!(fixture.events(), events);
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn queue_control_event_fault_rolls_back_every_row_then_replay_is_an_exact_noop() {
    let fixture = Fixture::new();
    fixture.queue();
    let before = fixture.repo.list_managed_processes().unwrap();
    fixture.fail_event("process.queue_reordered");
    let error = fixture
        .repo
        .reorder_queued_managed_process("c", Some("a"), None, 200)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("fixture scheduler event failure"),
        "{error}"
    );
    assert_eq!(fixture.repo.list_managed_processes().unwrap(), before);
    assert!(fixture.events().is_empty());

    fixture.allow_event();
    let expected = vec!["c".to_string(), "a".to_string(), "b".to_string()];
    assert_eq!(
        fixture
            .repo
            .reorder_queued_managed_process("c", Some("a"), None, 200)
            .unwrap(),
        expected
    );
    let after = fixture.repo.list_managed_processes().unwrap();
    for (id, position) in [("c", 1), ("a", 2), ("b", 3)] {
        let row = after.iter().find(|row| row.process_id == id).unwrap();
        assert_eq!(row.queue_order, position);
        assert_eq!(row.updated_at, 200);
        assert_eq!(row.status, "queued");
        assert_eq!(row.claim_generation, 0);
    }
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "process.queue_reordered");
    assert_eq!(events[0].criticality, RuntimeEventCriticality::Critical);
    assert_eq!(events[0].payload["previous_order"], json!(["a", "b", "c"]));
    assert_eq!(events[0].payload["order"], json!(["c", "a", "b"]));
    assert_eq!(events[0].payload["process_id"], "c");
    assert_eq!(events[0].payload["before_process_id"], "a");
    assert!(events[0].payload["after_process_id"].is_null());

    let reopened = SqliteRuntimeRepository::new(fixture.path.clone());
    assert_eq!(
        reopened
            .reorder_queued_managed_process("c", Some("a"), None, 999)
            .unwrap(),
        expected
    );
    assert_eq!(fixture.repo.list_managed_processes().unwrap(), after);
    assert_eq!(fixture.events(), events);
}

#[test]
fn independent_queue_claim_of_moving_or_anchor_target_refuses_without_an_order_event() {
    for (moving, anchor) in [("a", "b"), ("b", "a")] {
        let fixture = Fixture::new();
        fixture.queue();
        let independent = SqliteRuntimeRepository::new(fixture.path.clone());
        let mut scheduler = defaults();
        scheduler.max_concurrent = 1;
        let claimed = independent
            .claim_managed_processes(&scheduler, 150)
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].process_id, "a");
        assert_eq!(claimed[0].status, "launch_reserved");
        assert_eq!(claimed[0].claim_generation, 1);
        let after_claim = fixture.repo.list_managed_processes().unwrap();
        assert!(matches!(
            fixture
                .repo
                .reorder_queued_managed_process(moving, Some(anchor), None, 200),
            Err(RuntimeError::NotFound(_))
        ));
        assert_eq!(fixture.repo.list_managed_processes().unwrap(), after_claim);
        assert!(fixture.events().is_empty());
    }
}

#[test]
fn scheduler_control_signed_event_exhaustion_rolls_back_settings_and_queue_changes() {
    let fixture = Fixture::new();
    fixture.queue();
    fixture
        .db()
        .execute(
            "INSERT INTO runtime_events
         (event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES('fixture_scheduler_max','system','process_scheduler',?1,
                'fixture.scheduler_history',1,'{}',1)",
            [i64::MAX],
        )
        .unwrap();
    let previous = fixture.settings();
    let before = fixture.repo.list_managed_processes().unwrap();
    let events = fixture.events();
    let mut requested = previous.clone();
    requested.max_concurrent = 7;
    requested.updated_at = 200;
    for result in [
        fixture
            .repo
            .replace_process_scheduler_settings(&requested)
            .map(|_| ()),
        fixture
            .repo
            .reorder_queued_managed_process("c", Some("a"), None, 200)
            .map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(RuntimeError::Conflict(ref message))
                if message.contains("process scheduler event sequence")
        ));
    }
    assert_eq!(fixture.settings(), previous);
    assert_eq!(fixture.repo.list_managed_processes().unwrap(), before);
    assert_eq!(fixture.events(), events);
}

#[test]
fn direct_settings_replacement_preserves_last_write_semantics_and_records_actual_prior_state() {
    let fixture = Fixture::new();
    let mut stale_replacement = fixture.settings();
    let independent = SqliteRuntimeRepository::new(fixture.path.clone());
    let mut first = stale_replacement.clone();
    first.paused = true;
    first.pause_reason = Some("independent writer".into());
    first.updated_at = 100;
    independent
        .replace_process_scheduler_settings(&first)
        .unwrap();

    // The existing direct API supplies a full replacement, not expected_revision.
    // Its event must describe the persisted prior state, not this caller's stale view.
    stale_replacement.max_concurrent = 7;
    stale_replacement.updated_at = 200;
    assert_eq!(
        fixture
            .repo
            .replace_process_scheduler_settings(&stale_replacement)
            .unwrap(),
        stale_replacement
    );
    assert_eq!(fixture.settings(), stale_replacement);
    let events = fixture.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].payload["previous_settings"], json!(first));
    assert_eq!(events[1].payload["settings"], json!(stale_replacement));
    assert!(events
        .iter()
        .all(|event| event.criticality == RuntimeEventCriticality::Critical));
}

#[test]
fn scheduler_ignored_or_damaged_control_receipts_roll_back_both_mutations() {
    for kind in [
        "process.scheduler_settings_changed",
        "process.queue_reordered",
    ] {
        for damage in [
            None,
            Some("delete"),
            Some("critical=0"),
            Some("payload_json='{}'"),
            Some("seq=seq+100"),
            Some("session_id='foreign'"),
            Some("event_id=event_id||':changed'"),
        ] {
            let fixture = Fixture::new();
            fixture.queue();
            let before_settings = fixture.settings();
            let before_processes = fixture.repo.list_managed_processes().unwrap();
            let before_events = fixture.events();
            let store = SqliteRuntimeStore::new(SqliteStoreConfig {
                database_path: fixture.path.clone(),
            });
            let mut notifications = store.subscribe_event_commits().unwrap();
            let (timing, body) = match damage {
                None => ("BEFORE", "SELECT RAISE(IGNORE);".to_string()),
                Some("delete") => (
                    "AFTER",
                    "DELETE FROM runtime_events WHERE event_id=NEW.event_id;".to_string(),
                ),
                Some(change) => (
                    "AFTER",
                    format!("UPDATE runtime_events SET {change} WHERE event_id=NEW.event_id;"),
                ),
            };
            fixture
                .db()
                .execute_batch(&format!(
                    "CREATE TRIGGER reject_scheduler_control {timing} INSERT ON runtime_events
                 WHEN NEW.kind='{kind}' AND NEW.scope='system'
                   AND NEW.scope_id='process_scheduler'
                 BEGIN {body} END;"
                ))
                .unwrap();
            let mut requested = before_settings.clone();
            requested.max_concurrent = 7;
            requested.updated_at = 200;
            let mutate = || {
                if kind == "process.scheduler_settings_changed" {
                    fixture
                        .repo
                        .replace_process_scheduler_settings(&requested)
                        .map(|_| ())
                } else {
                    fixture
                        .repo
                        .reorder_queued_managed_process("c", Some("a"), None, 200)
                        .map(|_| ())
                }
            };
            let error = mutate().unwrap_err();
            assert!(
                matches!(error, RuntimeError::Conflict(ref message)
                if message.contains("scheduler control event")),
                "{damage:?}: {error}"
            );
            assert_eq!(fixture.settings(), before_settings, "{damage:?}");
            assert_eq!(
                fixture.repo.list_managed_processes().unwrap(),
                before_processes
            );
            assert_eq!(fixture.events(), before_events);
            assert!(matches!(
                notifications.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));

            fixture.allow_event();
            mutate().unwrap();
            let saved_settings = fixture.settings();
            let saved_processes = fixture.repo.list_managed_processes().unwrap();
            let saved_events = fixture.events();
            assert_eq!(saved_events.len(), 1);
            assert_eq!(saved_events[0].kind, kind);
            assert_eq!(
                saved_events[0].criticality,
                RuntimeEventCriticality::Critical
            );
            assert!(notifications.try_recv().is_ok());
            mutate().unwrap();
            assert_eq!(fixture.settings(), saved_settings);
            assert_eq!(
                fixture.repo.list_managed_processes().unwrap(),
                saved_processes
            );
            assert_eq!(fixture.events(), saved_events);
            assert!(matches!(
                notifications.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));
        }
    }
}

#[test]
fn process_control_event_trigger_cannot_change_its_settings_or_queue_carrier() {
    for (kind, changes) in [
        ("process.scheduler_settings_changed", vec![
            "UPDATE process_scheduler_state SET max_concurrent=9 WHERE singleton=1;",
            "UPDATE process_scheduler_state SET updated_at=updated_at+1 WHERE singleton=1;",
        ]),
        ("process.queue_reordered", vec![
            "UPDATE managed_processes SET queue_order=queue_order+100 WHERE status='queued';",
            "UPDATE managed_processes SET updated_at=updated_at+1 WHERE status='queued';",
            "UPDATE managed_processes SET claim_generation=claim_generation+1 WHERE status='queued';",
        ]),
    ] {
        for damage in changes {
            let fixture = Fixture::new();
            fixture.queue();
            let before_settings = fixture.settings();
            let before_processes = fixture.repo.list_managed_processes().unwrap();
            let events = fixture.events();
            let store = SqliteRuntimeStore::new(SqliteStoreConfig {
                database_path: fixture.path.clone(),
            });
            let mut notifications = store.subscribe_event_commits().unwrap();
            fixture.db().execute_batch(&format!(
                "CREATE TRIGGER reject_scheduler_control AFTER INSERT ON runtime_events
                 WHEN NEW.kind='{kind}' AND NEW.scope='system'
                   AND NEW.scope_id='process_scheduler'
                 BEGIN {damage} END;"
            )).unwrap();
            let mut requested = before_settings.clone();
            requested.max_concurrent = 7;
            requested.updated_at = 200;
            let mutate = || {
                if kind == "process.scheduler_settings_changed" {
                    fixture.repo.replace_process_scheduler_settings(&requested).map(|_| ())
                } else {
                    fixture.repo.reorder_queued_managed_process("c", Some("a"), None, 200)
                        .map(|_| ())
                }
            };
            let error = mutate().unwrap_err();
            assert!(matches!(error, RuntimeError::Conflict(ref message)
                if message.contains("event carrier")), "{damage}: {error}");
            assert_eq!(fixture.settings(), before_settings);
            assert_eq!(fixture.repo.list_managed_processes().unwrap(), before_processes);
            assert_eq!(fixture.events(), events);
            assert!(matches!(notifications.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)));
            fixture.allow_event();
            mutate().unwrap();
            let saved_settings = fixture.settings();
            let saved_processes = fixture.repo.list_managed_processes().unwrap();
            let saved_events = fixture.events();
            assert_eq!(saved_events.len(), 1);
            assert!(notifications.try_recv().is_ok());
            mutate().unwrap();
            assert_eq!(fixture.settings(), saved_settings);
            assert_eq!(fixture.repo.list_managed_processes().unwrap(), saved_processes);
            assert_eq!(fixture.events(), saved_events);
            assert!(matches!(notifications.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)));
        }
    }
}
