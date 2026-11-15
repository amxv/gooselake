use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::*;

fn query_plan(db: &Connection, query: &str) -> String {
    let mut statement = db.prepare(query).unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ")
}

fn approval_plan(db: &Connection) -> String {
    query_plan(
        db,
        "EXPLAIN QUERY PLAN SELECT id,session_id,turn_id,origin,tool_call_id,
                provider_approval_ref,status,request_json,response_json,created_at,resolved_at
         FROM approvals WHERE session_id='retirement_target'
         ORDER BY created_at ASC,id ASC",
    )
}

fn admission_plan(db: &Connection) -> String {
    query_plan(
        db,
        "EXPLAIN QUERY PLAN SELECT turn_id,session_id,provider,projection_source,
                user_input_snapshot_json,dispatch_policy_json,correlation_json,
                dispatch_state,provider_native_turn_id,dispatch_error_json,
                admitted_at,updated_at
         FROM turn_admissions WHERE session_id='retirement_target'
         ORDER BY admitted_at ASC,turn_id ASC",
    )
}

#[test]
fn schema46_retirement_review_session_reads_ordered_approvals_and_admissions_without_sort() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 45);
    let repository = SqliteRuntimeRepository::new(path.clone());
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    for (id, alias) in [
        ("retirement_target", "target-review"),
        ("retirement_other_a", "foreign-review"),
    ] {
        let (session, agent) = workspace_agent_fixture(&workspace, id, alias);
        repository.create_workspace_agent(&session, &agent).unwrap();
    }

    let mut db = open_connection(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    let tx = db.transaction().unwrap();
    for i in 0..3_600 {
        let (owner, status) = if i % 9 == 0 {
            ("retirement_target", "pending")
        } else {
            ("retirement_other_a", "approved")
        };
        let dispatch_state = match i % 5 {
            0 => "dispatched",
            1 => "pending",
            2 => "unknown",
            3 => "not_dispatched",
            _ => "dispatching",
        };
        let turn = format!("review_turn_{i:05}");
        let admitted_at = 100 + (i / 18) as i64;
        tx.execute(
            "INSERT INTO turns (
                id,session_id,status,input_json,source,started_at,completed_at
             ) VALUES (?1,?2,'completed','[]','user',?3,?3)",
            params![turn, owner, admitted_at],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO approvals (
                id,session_id,turn_id,status,request_json,created_at
             ) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                format!("approval_{i:05}"),
                owner,
                turn,
                status,
                serde_json::json!({"historical_approval":i}).to_string(),
                admitted_at,
            ],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO turn_admissions (
                turn_id,session_id,provider,projection_source,user_input_snapshot_json,
                dispatch_policy_json,correlation_json,dispatch_state,admitted_at,updated_at
             ) VALUES (?1,?2,'codex','user_visible',?3,?4,?5,?6,?7,?7)",
            params![
                turn,
                owner,
                serde_json::json!({
                    "prompt_text":format!("history_{i}"),
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
                serde_json::json!({
                    "expected_turn_id":null,
                    "correlation_id":null
                })
                .to_string(),
                dispatch_state,
                admitted_at,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let old_approvals = approval_plan(&db);
    let old_admissions = admission_plan(&db);
    assert!(
        old_approvals.contains("SEARCH approvals"),
        "{old_approvals}"
    );
    assert!(old_approvals.contains("USE TEMP B-TREE"), "{old_approvals}");
    assert!(
        old_admissions.contains("SEARCH turn_admissions"),
        "{old_admissions}"
    );
    assert!(
        old_admissions.contains("USE TEMP B-TREE"),
        "{old_admissions}"
    );
    let original = repository
        .get_persisted_retirement_review_session("retirement_target")
        .unwrap()
        .unwrap();
    assert_eq!(original.turns.len(), 400);
    assert_eq!(original.approvals.len(), 400);
    assert_eq!(original.admissions.len(), 400);
    assert_eq!(original.approvals.first().unwrap().id, "approval_00000");
    assert_eq!(original.approvals.last().unwrap().id, "approval_03591");
    assert_eq!(
        original.admissions.first().unwrap().turn_id,
        "review_turn_00000"
    );
    assert_eq!(
        original.admissions.last().unwrap().turn_id,
        "review_turn_03591"
    );
    for table in ["turns", "approvals", "turn_admissions"] {
        assert_eq!(
            db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            3_600,
            "{table} must retain the exact historical population"
        );
    }
    drop(db);

    // A genuinely populated schema45 DB upgrades transactionally: neither
    // new ordering index changes record identity, content or time tie order.
    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    let new_approvals = approval_plan(&db);
    let new_admissions = admission_plan(&db);
    assert!(
        new_approvals
            .contains("SEARCH approvals USING INDEX idx_approvals_retirement_session_order"),
        "{new_approvals}"
    );
    assert!(
        !new_approvals.contains("USE TEMP B-TREE"),
        "{new_approvals}"
    );
    assert!(
        new_admissions.contains(
            "SEARCH turn_admissions USING INDEX idx_turn_admissions_retirement_session_order"
        ),
        "{new_admissions}"
    );
    assert!(
        !new_admissions.contains("USE TEMP B-TREE"),
        "{new_admissions}"
    );
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert_eq!(
        repository
            .get_persisted_retirement_review_session("retirement_target")
            .unwrap(),
        Some(original.clone())
    );
    for table in ["turns", "approvals", "turn_admissions"] {
        assert_eq!(
            db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            3_600
        );
    }

    // This is strictly read-side index work: malformed unrelated historical
    // authority remains malformed and cannot obscure the owned review.
    db.execute(
        "UPDATE approvals SET request_json='{broken-json' WHERE session_id='retirement_other_a'",
        [],
    )
    .unwrap();
    db.execute(
        "UPDATE turn_admissions SET user_input_snapshot_json='{}'
         WHERE session_id='retirement_other_a'",
        [],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    assert!(repository.list_turn_admissions().is_err());
    let reopened = SqliteRuntimeRepository::new(path);
    reopened.initialize_schema().unwrap();
    assert_eq!(
        reopened
            .get_persisted_retirement_review_session("retirement_target")
            .unwrap(),
        Some(original)
    );
    assert!(reopened.hydrate_runtime_state().is_err());
    assert!(reopened.list_turn_admissions().is_err());
}
