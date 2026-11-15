use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::*;

const OPERATION_ID: &str = "edit-replacement-proof";
const TARGET: &str = "edit_correlation_target";
const FOREIGN: &str = "edit_correlation_foreign";

fn plan(db: &Connection, query: &str) -> String {
    db.prepare(query)
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ")
}

fn first_query_plan(db: &Connection) -> String {
    plan(
        db,
        "EXPLAIN QUERY PLAN SELECT turn_id,session_id,provider,projection_source,
                user_input_snapshot_json,dispatch_policy_json,correlation_json,
                dispatch_state,provider_native_turn_id,dispatch_error_json,admitted_at,updated_at
         FROM turn_admissions
         WHERE session_id='edit_correlation_target'
           AND json_extract(correlation_json,'$.correlation_id')='edit-replacement-proof'
         ORDER BY admitted_at ASC,turn_id ASC LIMIT 1",
    )
}

fn restart_query_plan(db: &Connection) -> String {
    plan(
        db,
        "EXPLAIN QUERY PLAN SELECT turn_id FROM turn_admissions
         WHERE session_id='edit_correlation_target'
           AND json_extract(correlation_json,'$.correlation_id')='edit-replacement-proof'
         ORDER BY admitted_at DESC,turn_id DESC LIMIT 1",
    )
}

#[test]
fn schema47_exact_edit_correlation_replaces_global_history_scan_without_native_replay() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 46);
    let repository = SqliteRuntimeRepository::new(path.clone());
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    for (id, alias) in [(TARGET, "target-edit"), (FOREIGN, "foreign-edit")] {
        let (session, agent) = workspace_agent_fixture(&workspace, id, alias);
        repository.create_workspace_agent(&session, &agent).unwrap();
    }

    let mut db = open_connection(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    let transaction = db.transaction().unwrap();
    for i in 0..3_600 {
        let session = if i % 9 == 0 { TARGET } else { FOREIGN };
        let correlation = if matches!(i, 1800 | 1890 | 1880) {
            OPERATION_ID.to_string()
        } else if i == 1879 {
            "only-foreign-edit".into()
        } else {
            format!("historical-{i:05}")
        };
        let turn = format!("historical_edit_turn_{i:05}");
        // Two matching turns share the same timestamp, so both the first
        // fallback and reversed recovery must use turn ID as a tiebreaker.
        let admitted_at = if i == 1890 {
            200
        } else {
            100 + (i / 18) as i64
        };
        let dispatch_state = match i % 5 {
            0 => "dispatched",
            1 => "pending",
            2 => "unknown",
            3 => "not_dispatched",
            _ => "dispatching",
        };
        transaction
            .execute(
                "INSERT INTO turns(id,session_id,status,input_json,source,started_at)
                 VALUES(?1,?2,'completed','[]','user',?3)",
                params![turn, session, admitted_at],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO turn_admissions(
                turn_id,session_id,provider,projection_source,user_input_snapshot_json,
                dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at)
             VALUES(?1,?2,'codex','user_visible',?3,?4,?5,?6,?7,?8)",
                params![
                    turn,
                    session,
                    serde_json::json!({
                        "prompt_text":format!("historical input {i}"),
                        "image_refs":[],
                        "invocation":null
                    })
                    .to_string(),
                    serde_json::json!({
                        "permission_mode":null,
                        "pre_dispatch_approval_required":false,
                        "turn_settings":{}
                    })
                    .to_string(),
                    serde_json::json!({"correlation_id":correlation}).to_string(),
                    dispatch_state,
                    admitted_at,
                    admitted_at + (i % 3) as i64,
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let old_first_plan = first_query_plan(&db);
    let old_restart_plan = restart_query_plan(&db);
    assert!(
        old_first_plan.contains("SEARCH turn_admissions"),
        "{old_first_plan}"
    );
    assert!(
        old_restart_plan.contains("SEARCH turn_admissions"),
        "{old_restart_plan}"
    );
    assert!(!old_first_plan.contains("idx_turn_admissions_session_correlation_order"));
    assert!(!old_restart_plan.contains("idx_turn_admissions_session_correlation_order"));
    // Before schema47 the manager's fallback had to decode the entire history,
    // including foreign sessions, and chose the earliest committed match.
    let original = repository
        .list_turn_admissions()
        .unwrap()
        .into_iter()
        .find(|admission| {
            admission.session_id == TARGET
                && admission.correlation.correlation_id.as_deref() == Some(OPERATION_ID)
        })
        .unwrap();
    assert_eq!(original.turn_id, "historical_edit_turn_01800");
    assert_eq!(
        repository
            .get_persisted_turn_admission_for_correlation(TARGET, OPERATION_ID)
            .unwrap(),
        Some(original.clone()),
        "new direct lookup must preserve the original first ordered admission even on old schema"
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM turn_admissions", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        3_600
    );
    drop(db);

    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    let new_first_plan = first_query_plan(&db);
    let new_restart_plan = restart_query_plan(&db);
    assert!(
        new_first_plan.contains(
            "SEARCH turn_admissions USING INDEX idx_turn_admissions_session_correlation_order"
        ),
        "{new_first_plan}"
    );
    assert!(new_first_plan.contains("<expr>=?"), "{new_first_plan}");
    assert!(
        !new_first_plan.contains("USE TEMP B-TREE"),
        "{new_first_plan}"
    );
    assert!(
        new_restart_plan.contains(
            "SEARCH turn_admissions USING INDEX idx_turn_admissions_session_correlation_order"
        ),
        "{new_restart_plan}"
    );
    assert!(
        !new_restart_plan.contains("USE TEMP B-TREE"),
        "{new_restart_plan}"
    );
    let recovered_latest: String = db
        .query_row(
            "SELECT turn_id FROM turn_admissions
             WHERE session_id=?1 AND json_extract(correlation_json,'$.correlation_id')=?2
             ORDER BY admitted_at DESC,turn_id DESC LIMIT 1",
            params![TARGET, OPERATION_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recovered_latest, "historical_edit_turn_01890");
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert_eq!(
        repository
            .get_persisted_turn_admission_for_correlation(TARGET, OPERATION_ID)
            .unwrap(),
        Some(original.clone())
    );
    assert!(repository
        .get_persisted_turn_admission_for_correlation(TARGET, "only-foreign-edit")
        .unwrap()
        .is_none());
    assert!(repository
        .get_persisted_turn_admission_for_correlation(TARGET, "nonexistent-edit")
        .unwrap()
        .is_none());

    // Valid SQLite JSON with invalid admission *types* makes the old global
    // fallback return an error after native fork, but the exact owning-session
    // projection still reads its immutable matching receipt after reopen.
    db.execute(
        "UPDATE turn_admissions SET user_input_snapshot_json='{}' WHERE session_id=?1",
        [FOREIGN],
    )
    .unwrap();
    assert!(repository.list_turn_admissions().is_err());
    assert_eq!(
        repository
            .get_persisted_turn_admission_for_correlation(TARGET, OPERATION_ID)
            .unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM turn_admissions", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        3_600
    );
    // A malformed *selected* admission still fails closed, rather than
    // silently skipping it for the later matching correlation. Repair only
    // this synthetic row before checking clean restart parity.
    db.execute(
        "UPDATE turn_admissions SET user_input_snapshot_json='{}'
         WHERE turn_id='historical_edit_turn_01800'",
        [],
    )
    .unwrap();
    assert!(repository
        .get_persisted_turn_admission_for_correlation(TARGET, OPERATION_ID)
        .is_err());
    db.execute(
        "UPDATE turn_admissions SET user_input_snapshot_json=?1
         WHERE turn_id='historical_edit_turn_01800'",
        [serde_json::json!({
            "prompt_text":"historical input 1800",
            "image_refs":[],
            "invocation":null
        })
        .to_string()],
    )
    .unwrap();
    let reopened = SqliteRuntimeRepository::new(path);
    reopened.initialize_schema().unwrap();
    assert_eq!(
        reopened
            .get_persisted_turn_admission_for_correlation(TARGET, OPERATION_ID)
            .unwrap(),
        Some(original)
    );
    assert!(reopened.list_turn_admissions().is_err());
}
