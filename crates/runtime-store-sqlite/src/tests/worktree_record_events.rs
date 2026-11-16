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
fn historical_managed_worktree_merge_requires_exact_original_row_under_writer_lock() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let original = record();
    repository.upsert_managed_worktree(&original).unwrap();
    let changed = ManagedWorktreeRecord {
        worktree_name: "native_normalized".into(),
        deletion_policy: "delete_on_last_claim".into(),
        ..original.clone()
    };
    let independent = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let newer = ManagedWorktreeRecord {
        unified_workspace_path: "independent_route".into(),
        updated_at: 250,
        ..original.clone()
    };
    independent.upsert_managed_worktree(&newer).unwrap();
    let durable_revision = independent.managed_worktree_revision(&original.id).unwrap();

    assert!(matches!(
        repository.repair_managed_worktree_if_unchanged(&original, &changed),
        Err(runtime_core::RuntimeError::Conflict(_)),
    ));
    assert_eq!(
        independent.get_managed_worktree(&original.id).unwrap(),
        Some(newer.clone())
    );
    assert_eq!(
        independent.managed_worktree_revision(&original.id).unwrap(),
        durable_revision
    );

    // A genuinely unchanged input is an exact no-op even with an UPDATE
    // trigger, and the changed legacy repair can later commit with the
    // refreshed original without any synthetic lifecycle event.
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER forbid_worktree_noop BEFORE UPDATE ON managed_worktrees
         WHEN NEW.worktree_name=OLD.worktree_name
         BEGIN SELECT RAISE(FAIL,'historical exact retry must not UPDATE'); END;",
    )
    .unwrap();
    repository
        .repair_managed_worktree_if_unchanged(&newer, &newer)
        .unwrap();
    db.execute_batch("DROP TRIGGER forbid_worktree_noop;")
        .unwrap();
    assert_eq!(
        independent.managed_worktree_revision(&original.id).unwrap(),
        durable_revision,
    );
    let valid_normalized = ManagedWorktreeRecord {
        worktree_name: "native_normalized".into(),
        ..newer.clone()
    };
    repository
        .repair_managed_worktree_if_unchanged(&newer, &valid_normalized)
        .unwrap();
    assert_eq!(
        independent.get_managed_worktree(&original.id).unwrap(),
        Some(valid_normalized.clone())
    );
    assert_eq!(
        independent.managed_worktree_revision(&original.id).unwrap(),
        durable_revision + 1,
    );
    let wrong_identity = ManagedWorktreeRecord {
        id: "wt_unrelated_identity".into(),
        ..valid_normalized.clone()
    };
    assert!(matches!(
        repository.repair_managed_worktree_if_unchanged(&valid_normalized, &wrong_identity),
        Err(runtime_core::RuntimeError::Conflict(_)),
    ));
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn historical_worktree_normalization_cannot_rewrite_another_natural_key_identity() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let raw = ManagedWorktreeRecord {
        repo_root: " /tmp/native-repository ".into(),
        ..record()
    };
    let foreign = ManagedWorktreeRecord {
        id: "wt_foreign".into(),
        ..record()
    };
    repository.upsert_managed_worktree(&raw).unwrap();
    repository.upsert_managed_worktree(&foreign).unwrap();
    let raw_revision = repository.managed_worktree_revision(&raw.id).unwrap();
    let foreign_revision = repository.managed_worktree_revision(&foreign.id).unwrap();
    let normalized = ManagedWorktreeRecord {
        repo_root: raw.repo_root.trim().into(),
        ..raw.clone()
    };
    // The proposed identity collides with a different pre-existing row.
    // The helper may temporarily target it, but the CAS must roll back the
    // entire writer transaction before touching either durable revision.
    assert!(matches!(
        repository.repair_managed_worktree_if_unchanged(&raw, &normalized),
        Err(runtime_core::RuntimeError::Conflict(_)),
    ));
    assert_eq!(
        repository.get_managed_worktree(&raw.id).unwrap(),
        Some(raw.clone())
    );
    assert_eq!(
        repository.get_managed_worktree(&foreign.id).unwrap(),
        Some(foreign.clone())
    );
    assert_eq!(
        repository.managed_worktree_revision(&raw.id).unwrap(),
        raw_revision
    );
    assert_eq!(
        repository.managed_worktree_revision(&foreign.id).unwrap(),
        foreign_revision,
    );
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn historical_worktree_repair_batch_rolls_back_earlier_winner_on_later_write_fault() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let winner = record();
    let loser = ManagedWorktreeRecord {
        id: "wt_batch_loser".into(),
        repo_root: " /tmp/native-repository ".into(),
        worktree_cwd: " /tmp/native-managed/gg--native ".into(),
        branch_name: " gg/native ".into(),
        ..winner.clone()
    };
    repository.upsert_managed_worktree(&winner).unwrap();
    repository.upsert_managed_worktree(&loser).unwrap();
    let winner_rev = repository.managed_worktree_revision(&winner.id).unwrap();
    let loser_rev = repository.managed_worktree_revision(&loser.id).unwrap();
    let merged = ManagedWorktreeRecord {
        deletion_policy: "delete_on_last_claim".into(),
        ..winner.clone()
    };
    let tombstoned = ManagedWorktreeRecord {
        repo_root: "__gg_tombstoned__/wt_batch_loser".into(),
        ..loser.clone()
    };
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch(
        "CREATE TRIGGER reject_later_legacy_repair BEFORE UPDATE ON managed_worktrees
         WHEN OLD.id='wt_batch_loser'
         BEGIN SELECT RAISE(FAIL,'injected losing row write failure'); END;",
    )
    .unwrap();
    assert!(repository
        .repair_managed_worktrees_if_unchanged(&[
            (&winner, merged.clone()),
            (&loser, tombstoned.clone()),
        ])
        .is_err());
    assert_eq!(
        repository.get_managed_worktree(&winner.id).unwrap(),
        Some(winner.clone())
    );
    assert_eq!(
        repository.get_managed_worktree(&loser.id).unwrap(),
        Some(loser.clone())
    );
    assert_eq!(
        repository.managed_worktree_revision(&winner.id).unwrap(),
        winner_rev
    );
    assert_eq!(
        repository.managed_worktree_revision(&loser.id).unwrap(),
        loser_rev
    );
    assert!(repository
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());

    db.execute_batch("DROP TRIGGER reject_later_legacy_repair;")
        .unwrap();
    repository
        .repair_managed_worktrees_if_unchanged(&[
            (&winner, merged.clone()),
            (&loser, tombstoned.clone()),
        ])
        .unwrap();
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        reopened.get_managed_worktree(&winner.id).unwrap(),
        Some(merged)
    );
    assert_eq!(
        reopened.get_managed_worktree(&loser.id).unwrap(),
        Some(tombstoned)
    );
    assert_eq!(
        reopened.managed_worktree_revision(&winner.id).unwrap(),
        winner_rev + 1
    );
    assert_eq!(
        reopened.managed_worktree_revision(&loser.id).unwrap(),
        loser_rev + 1
    );
    assert!(reopened
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());
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
