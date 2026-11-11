use super::*;

fn record() -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: "wt_native".into(),
        repo_root: "/tmp/native-repository".into(),
        worktree_root: "/tmp/native-managed".into(),
        worktree_cwd: "/tmp/native-managed/gg--native".into(),
        branch_name: "gg/native".into(),
        worktree_name: "native".into(),
        unified_workspace_path: "native-repository".into(),
        deletion_policy: "retain_on_last_claim".into(),
        created_by_session_id: None,
        created_by_operation_id: Some("op_native_intent".into()),
        created_at: 100,
        updated_at: 100,
    }
}

#[test]
fn native_created_managed_row_and_critical_event_rollback_together() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let subscriber = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    // Keep the store alive so the journal notification channel is retained.
    let mut notifications = subscriber.subscribe_event_commits().unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch("CREATE TRIGGER reject_create_event BEFORE INSERT ON runtime_events
        WHEN NEW.kind = 'worktree.created' BEGIN SELECT RAISE(FAIL, 'injected event failure'); END;").unwrap();
    assert!(repository
        .record_managed_worktree_event(&record(), "skipped_disabled", false, None, None)
        .is_err());
    assert!(repository
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktrees
        .is_empty());
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    db.execute_batch("DROP TRIGGER reject_create_event;")
        .unwrap();
    let effective = repository
        .record_managed_worktree_event(&record(), "skipped_disabled", false, None, None)
        .unwrap();
    assert_eq!(effective, record());
    assert!(notifications.try_recv().is_ok());
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        restarted.hydrate_runtime_state().unwrap().managed_worktrees,
        [record()]
    );
    let events = restarted
        .list_runtime_events(Some((RuntimeEventScope::Worktree, "wt_native")), None, 10)
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "worktree.created");
    assert_eq!(events[0].payload["worktree"]["id"], "wt_native");
    assert_eq!(events[0].payload["init_script_status"], "skipped_disabled");
    assert_eq!(events[0].seq, 1);
}

#[test]
fn recovered_native_checkout_uses_persisted_natural_key_id_for_journal() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    repository.upsert_managed_worktree(&record()).unwrap();
    let original_revision = repository.managed_worktree_revision("wt_native").unwrap();
    let mut proposed = record();
    proposed.id = "wt_uncommitted_proposal".into();
    proposed.updated_at = 200;
    let effective = repository
        .record_managed_worktree_event(&proposed, "recovered_existing_checkout", true, None, None)
        .unwrap();
    assert_eq!(effective.id, "wt_native");
    let events = repository.list_runtime_events(None, None, 10).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "worktree.recovered");
    assert_eq!(events[0].scope_id, "wt_native");
    assert_eq!(events[0].payload["worktree"]["id"], "wt_native");
    assert_eq!(
        repository
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktrees
            .len(),
        1
    );
    assert_eq!(
        repository.managed_worktree_revision("wt_native").unwrap(),
        original_revision + 1
    );
}
