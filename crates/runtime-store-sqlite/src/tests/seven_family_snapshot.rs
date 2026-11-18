//! A disposable backup/reopen proof for seven unresolved families.
//! This is repository recovery, not manager startup or native execution.
//! No provider, process manager, hook executor, or HTTP router is constructed.
use crate::SqliteRuntimeRepository;
use runtime_core::*;
use rusqlite::Connection;
use serde_json::json;
use std::fs;

mod image;
mod manifest;
mod recovery;
mod seeds;
use image::*;
use recovery::*;

#[test]
fn seven_unresolved_families_survive_migration_sqlite_backup_and_prepared_target_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let seed_path = temp.path().join("seed.sqlite3");
    let sealed_path = temp.path().join("sealed.sqlite3");
    let target_path = temp.path().join("prepared.sqlite3");
    let repository = SqliteRuntimeRepository::new(seed_path.clone());
    super::event_epoch::schema_52(&seed_path);
    let wal_anchor = Connection::open(&seed_path).unwrap();
    wal_anchor
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    let seeds = seeds::seed_all(&repository, &temp);
    let prior = image(&seed_path);
    repository.initialize_schema().unwrap();
    let current = image(&seed_path);
    // The additive epoch migration must leave every old authority table,
    // raw JSON value, event, subject, rowid and existing schema object intact.
    for (name, rows) in &prior.tables {
        if name != "schema_migrations" {
            assert_eq!(&current.tables[name], rows, "migration rewrote {name}");
        }
    }
    for row in &prior.schema.rows {
        assert!(current.schema.rows.binary_search(row).is_ok());
    }
    assert_eq!(prior.event_heads, current.event_heads);
    assert_eq!(current.tables["schema_migrations"].rows.len(), 53);
    assert_eq!(prior.tables["schema_migrations"].rows.len(), 52);
    for row in &prior.tables["schema_migrations"].rows {
        assert!(current.tables["schema_migrations"]
            .rows
            .binary_search(row)
            .is_ok());
    }

    let source = readonly(&seed_path);
    source.execute_batch("BEGIN DEFERRED").unwrap();
    let expected = image_in_read_transaction(&source);
    assert_eq!(expected, current);
    for table in [
        "turn_admissions",
        "managed_processes",
        "approvals",
        "workspace_hook_effects",
        "session_edit_reruns",
        "workspace_agent_rebinds",
        "workspace_retirement_fences",
        "runtime_operation_effects",
        "runtime_operation_transitions",
        "runtime_events",
        "runtime_event_subjects",
    ] {
        assert!(!expected.tables[table].rows.is_empty(), "{table}");
    }
    let epoch = crate::repository_event_cursor::read_journal_epoch(&source).unwrap();
    backup_once(&source, &sealed_path);
    assert_eq!(image_in_read_transaction(&source), expected);
    source.execute_batch("ROLLBACK").unwrap();
    drop(source);
    drop(wal_anchor);
    drop(repository);
    let sealed_bytes = fs::read(&sealed_path).unwrap();
    let sealed_sha256 = sha256(&sealed_bytes);
    assert_eq!(image(&sealed_path), expected);
    assert_no_sidecars(&sealed_path);
    let witness = manifest::capture(&sealed_path, &temp.path().join("sealed-manifest.json"));
    let families = witness.document["observations"]["seven_families"]
        .as_object()
        .unwrap();
    assert_eq!(families.len(), 7);
    let expected_counts = json!({
        "turn":{"turns.status":3,"turn_admissions.dispatch_state":2},
        "process_completion":{"managed_processes.completion_state":1},
        "approval":{"approvals.status":1},
        "hook":{"workspace_hook_effects.phase":1,"workspace_hook_effects.application_state":0},
        "edit":{"session_edit_reruns.phase":1},
        "rebind":{"workspace_agent_rebinds.phase":1},
        "retirement":{"workspace_retirement_fences.phase":1}
    });
    for family in [
        "turn",
        "process_completion",
        "approval",
        "hook",
        "edit",
        "rebind",
        "retirement",
    ] {
        assert_eq!(
            families[family]["unresolved_rows_by_carrier"], expected_counts[family],
            "{family}"
        );
        assert_eq!(families[family]["all_carriers_observed"], true, "{family}");
        assert_eq!(
            families[family]["has_observed_unresolved_rows"], true,
            "{family}"
        );
    }
    manifest::verify(&sealed_path, &witness, true);
    let sealed_source = readonly(&sealed_path);
    sealed_source.execute_batch("BEGIN DEFERRED").unwrap();
    assert_eq!(image_in_read_transaction(&sealed_source), expected);
    backup_once(&sealed_source, &target_path);
    sealed_source.execute_batch("ROLLBACK").unwrap();
    drop(sealed_source);
    assert_ne!(
        fs::canonicalize(&sealed_path).unwrap(),
        fs::canonicalize(&target_path).unwrap()
    );
    assert_eq!(image(&target_path), expected);
    let reopened = SqliteRuntimeRepository::new(target_path.clone());
    reopened.initialize_schema().unwrap();
    assert_eq!(
        image(&target_path),
        expected,
        "repository reopen must not classify or replay effects"
    );
    let collection = EventCollection::history(None).unwrap();
    let page = reopened
        .list_operator_events_page_bound(&collection, None, 1)
        .unwrap();
    let first = &page.rows[0];
    let saved = EventCursor::for_event(
        &page.epoch,
        &collection,
        first.row_id,
        first.seq,
        &first.event_id,
    )
    .unwrap();
    assert_eq!(page.epoch, epoch);

    // Test-only preparation of this distinct inactive target. Production has
    // no restore writer, automatic epoch rotation or activation operation.
    let db = Connection::open(&target_path).unwrap();
    db.execute("UPDATE runtime_event_journal_identity SET epoch=lower(hex(randomblob(16))) WHERE singleton=1",[]).unwrap();
    let prepared_epoch = crate::repository_event_cursor::read_journal_epoch(&db).unwrap();
    assert_ne!(prepared_epoch, epoch);
    drop(db);
    reopened.initialize_schema().unwrap();
    let prepared = image(&target_path);
    let mut expected_prepared = expected.clone();
    expected_prepared.tables.insert(
        "runtime_event_journal_identity".into(),
        prepared.tables["runtime_event_journal_identity"].clone(),
    );
    assert_eq!(
        prepared, expected_prepared,
        "preparation changes only the target's original epoch cell"
    );
    seal_owned_database(&target_path);
    let prepared_target_sha256 = sha256(&fs::read(&target_path).unwrap());
    assert_ne!(prepared_target_sha256, sealed_sha256);
    manifest::verify(&target_path, &witness, false);
    let prepared_witness =
        manifest::capture(&target_path, &temp.path().join("prepared-manifest.json"));
    assert_ne!(prepared_witness.sha256, witness.sha256);
    assert_eq!(
        prepared_witness.document["observations"]["schema"]["journal_epoch"],
        prepared_epoch.as_str()
    );
    assert_eq!(
        prepared_witness.document["observations"]["events"],
        witness.document["observations"]["events"]
    );
    manifest::verify(&target_path, &prepared_witness, true);
    manifest::verify(&sealed_path, &witness, true);
    assert_eq!(fs::read(&sealed_path).unwrap(), sealed_bytes);
    assert!(matches!(
        reopened.list_operator_events_page_bound(&collection, Some(&saved), 1),
        Err(EventReadError::Cursor(EventCursorRejection::EpochMismatch))
    ));
    assert_eq!(image(&target_path), prepared);

    refuse_dispatch_decision_and_completion(&reopened, &seeds);
    quarantine_and_refuse(&reopened, &seeds);
    let quarantined = image(&target_path);
    assert_ne!(quarantined, prepared);
    for table in ["runtime_events", "runtime_event_subjects"] {
        assert_eq!(
            quarantined.tables[table].columns,
            expected.tables[table].columns
        );
        for row in &expected.tables[table].rows {
            assert!(
                quarantined.tables[table].rows.binary_search(row).is_ok(),
                "original {table} changed"
            );
        }
    }
    drop(reopened);
    seal_owned_database(&target_path);
    let quarantined_target_sha256 = sha256(&fs::read(&target_path).unwrap());
    assert_ne!(prepared_target_sha256, quarantined_target_sha256);
    assert_eq!(fs::read(&sealed_path).unwrap(), sealed_bytes);
    assert_eq!(sha256(&fs::read(&sealed_path).unwrap()), sealed_sha256);
    assert_eq!(image(&sealed_path), expected);
    assert_no_sidecars(&sealed_path);
    eprintln!(
        "{}",
        json!({"fixture":"seven_family_snapshot","schema_version":crate::schema::SCHEMA_VERSION,
        "sealed_snapshot_sha256":sealed_sha256,"sealed_logical_image_sha256":image_hash(&expected),"source_epoch":epoch.as_str(),"prepared_epoch":prepared_epoch.as_str(),
        "prepared_target_sha256":prepared_target_sha256,"quarantined_target_sha256":quarantined_target_sha256,"quarantined_logical_image_sha256":image_hash(&quarantined),
        "sealed_manifest_sha256":witness.sha256,"prepared_manifest_sha256":prepared_witness.sha256,"shipped_manifest_observer_verified":true,
        "families":["turn","process_completion","approval_decision","hook","edit","rebind","retirement"],"repository_reopen_only":true,"target_epoch_rotated":true,
        "provider_or_process_or_hook_executor_constructed":false,"cutover_authorized":false})
    );
}
