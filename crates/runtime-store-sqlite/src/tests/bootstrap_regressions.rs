use super::*;
use std::io::Write;

#[test]
fn source_bootstrap_ignores_large_internal_only_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    let mut connection = open_connection(&repository.database_path).unwrap();
    let transaction = connection.transaction().unwrap();
    {
        let mut credential = transaction.prepare(
            "INSERT INTO credentials (id, provider, profile, kind, encrypted_secret, metadata_json, created_at, updated_at)
             VALUES (?1, 'codex', ?1, 'token', 'secret', '{}', 1, 1)").unwrap();
        let mut diagnostic = transaction.prepare(
            "INSERT INTO team_operation_diagnostics (operation_id, team_id, code, message, payload_json, created_at)
             VALUES (NULL, NULL, 'test', 'excluded', '{}', 1)").unwrap();
        for index in 0..=crate::repository_bootstrap::MAX_SOURCE_BOOTSTRAP_ROWS_PER_TABLE {
            credential.execute([format!("credential_{index}")]).unwrap();
            diagnostic.execute([]).unwrap();
        }
    }
    transaction.commit().unwrap();
    let bootstrap = repository
        .source_bootstrap()
        .expect("excluded records do not participate");
    assert_eq!(bootstrap.high_watermark, 0);
    assert!(bootstrap.records.sessions.is_empty());
}

#[test]
fn mid_mutation_batch_failure_rolls_back_records_and_event() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    let mut session = sample_session();
    session.id = "session_atomic".into();
    let mut invalid_turn = sample_turn();
    invalid_turn.id = "turn_invalid".into();
    invalid_turn.session_id = "missing_session".into();

    let error = append_session_event(
        &repository,
        "evt_atomic_failure",
        "session_atomic",
        1,
        &[
            runtime_core::RuntimeRecordMutation::Session(session),
            runtime_core::RuntimeRecordMutation::Turn(invalid_turn),
        ],
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .to_ascii_lowercase()
        .contains("foreign key"));
    let bootstrap = repository.source_bootstrap().unwrap();
    assert_eq!(bootstrap.high_watermark, 0);
    assert!(bootstrap.records.sessions.is_empty());
}

#[test]
fn event_insert_failure_rolls_back_mutations_and_restart_needs_no_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    open_connection(&repository.database_path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_test_event BEFORE INSERT ON runtime_events
             BEGIN SELECT RAISE(ABORT, 'forced event insert failure'); END;",
        )
        .unwrap();

    let error = append_session_event(
        &repository,
        "evt_rejected",
        "session_1",
        1,
        &[runtime_core::RuntimeRecordMutation::Session(
            sample_session(),
        )],
    )
    .unwrap_err();
    assert!(error.to_string().contains("forced event insert failure"));
    drop(repository);

    let restarted = repo(&temp_dir);
    restarted.initialize_schema().unwrap();
    let bootstrap = restarted.source_bootstrap().unwrap();
    assert_eq!(bootstrap.high_watermark, 0);
    assert!(bootstrap.records.sessions.is_empty());
}

#[test]
fn authority_write_failure_prevents_database_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    let epoch = repository.source_bootstrap().unwrap().source_epoch;
    crate::db::install_before_authority_write_hook(
        repository.database_path.clone(),
        Box::new(|| {
            Err(runtime_core::RuntimeError::Bootstrap(
                "forced authority write failure".into(),
            ))
        }),
    );

    let error = append_session_event(
        &repository,
        "evt_authority_failure",
        "session_1",
        1,
        &[runtime_core::RuntimeRecordMutation::Session(
            sample_session(),
        )],
    )
    .unwrap_err();
    assert!(error.to_string().contains("forced authority write failure"));
    let bootstrap = repository.source_bootstrap().unwrap();
    assert_eq!(bootstrap.high_watermark, 0);
    assert!(bootstrap.records.sessions.is_empty());
    drop(repository);

    let restarted = repo(&temp_dir);
    restarted.initialize_schema().unwrap();
    assert_eq!(restarted.source_bootstrap().unwrap().source_epoch, epoch);
}

#[test]
fn authority_ahead_of_aborted_database_commit_is_never_downgraded() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    let epoch = repository.source_bootstrap().unwrap().source_epoch;
    let connection = open_connection(&repository.database_path).unwrap();
    crate::db::checkpoint_source_identity_at(
        &connection,
        &repository.database_path,
        repository.authority_root.as_deref(),
        1,
    )
    .unwrap();
    crate::db::checkpoint_source_identity_at(
        &connection,
        &repository.database_path,
        repository.authority_root.as_deref(),
        0,
    )
    .unwrap();
    drop(connection);
    drop(repository);

    let restarted = repo(&temp_dir);
    restarted.initialize_schema().unwrap();
    let bootstrap = restarted.source_bootstrap().unwrap();
    assert_eq!(bootstrap.high_watermark, 0);
    assert_ne!(bootstrap.source_epoch, epoch);
}

#[test]
fn committed_record_event_pair_survives_restart_as_one_generation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    append_session_event(
        &repository,
        "evt_committed",
        "session_1",
        1,
        &[runtime_core::RuntimeRecordMutation::Session(
            sample_session(),
        )],
    )
    .unwrap();
    drop(repository);

    let restarted = repo(&temp_dir);
    restarted.initialize_schema().unwrap();
    let bootstrap = restarted.source_bootstrap().unwrap();
    assert_eq!(bootstrap.high_watermark, 1);
    assert_eq!(bootstrap.records.sessions, vec![sample_session()]);
}

#[test]
fn bootstrap_preflight_counts_utf8_bytes_in_previously_omitted_fields() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    let mut session = sample_session();
    session.system_prompt = Some(
        "🪿".repeat(crate::repository_bootstrap::MAX_SOURCE_BOOTSTRAP_TEXT_BYTES as usize / 4 + 1),
    );
    repository.upsert_session(&session).unwrap();
    let error = repository.source_bootstrap().unwrap_err();
    assert!(error.to_string().contains("text bytes"));
}

#[test]
fn same_inode_database_rollback_rotates_source_epoch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    append_system_event(&repository, "evt_before_snapshot", 1);
    let original_epoch = repository.source_bootstrap().unwrap().source_epoch;
    let snapshot_path = temp_dir.path().join("older.sqlite3");
    checkpoint(&repository);
    std::fs::copy(&repository.database_path, &snapshot_path).unwrap();
    append_system_event(&repository, "evt_after_snapshot", 2);
    checkpoint(&repository);
    let inode_before = std::fs::metadata(&repository.database_path).unwrap();
    let older = std::fs::read(&snapshot_path).unwrap();
    let mut target = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&repository.database_path)
        .unwrap();
    target.write_all(&older).unwrap();
    target.sync_all().unwrap();
    drop(target);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            inode_before.ino(),
            std::fs::metadata(&repository.database_path).unwrap().ino()
        );
    }
    let restored = SqliteRuntimeRepository::new_with_authority_root(
        repository.database_path.clone(),
        temp_dir.path().join("authority"),
    );
    restored.initialize_schema().unwrap();
    let bootstrap = restored.source_bootstrap().unwrap();
    assert_eq!(bootstrap.high_watermark, 1);
    assert_ne!(bootstrap.source_epoch, original_epoch);
}

fn append_system_event(repository: &SqliteRuntimeRepository, event_id: &str, created_at: i64) {
    repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: event_id.into(),
            scope: RuntimeEventScope::System,
            scope_id: "runtime".into(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "runtime.checkpoint".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({}),
            provider: None,
            provider_seq: None,
            created_at,
        })
        .unwrap();
}

fn append_session_event(
    repository: &SqliteRuntimeRepository,
    event_id: &str,
    session_id: &str,
    created_at: i64,
    mutations: &[runtime_core::RuntimeRecordMutation],
) -> Result<runtime_core::RuntimeEventRecord, runtime_core::RuntimeError> {
    repository.append_runtime_event_with_mutations(
        &NewRuntimeEvent {
            event_id: event_id.into(),
            scope: RuntimeEventScope::Session,
            scope_id: session_id.into(),
            session_id: Some(session_id.into()),
            team_id: None,
            turn_id: None,
            kind: "session.updated".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({}),
            provider: None,
            provider_seq: None,
            created_at,
        },
        mutations,
    )
}

fn checkpoint(repository: &SqliteRuntimeRepository) {
    open_connection(&repository.database_path)
        .unwrap()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
}
