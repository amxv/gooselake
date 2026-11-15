use super::workspace_agent::{apply_schema_through, registered_workspace, workspace_agent_fixture};
use super::*;

fn settled_turn_approval_plan(db: &Connection) -> String {
    let mut statement = db
        .prepare(
            "EXPLAIN QUERY PLAN SELECT id,session_id,turn_id,origin,tool_call_id,
                    provider_approval_ref,status,request_json,response_json,created_at,resolved_at
             FROM approvals
             WHERE session_id='retirement_target' AND turn_id='turn_retirement_target'
             ORDER BY created_at ASC,id ASC",
        )
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ")
}

#[test]
fn schema45_retirement_approval_projection_seeks_only_owned_turn_after_schema44_upgrade() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    apply_schema_through(&path, 44);
    let repository = SqliteRuntimeRepository::new(path.clone());
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root);
    for (id, alias) in [
        ("retirement_target", "approval-target"),
        ("retirement_other_a", "approval-foreign"),
    ] {
        let (session, agent) = workspace_agent_fixture(&workspace, id, alias);
        repository.create_workspace_agent(&session, &agent).unwrap();
    }

    let mut db = open_connection(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL").unwrap();
    let tx = db.transaction().unwrap();
    for (turn, owner) in [
        ("turn_retirement_target", "retirement_target"),
        ("turn_retirement_other", "retirement_other_a"),
    ] {
        tx.execute(
            "INSERT INTO turns (id,session_id,status,input_json,source,started_at,completed_at)
             VALUES (?1,?2,'completed','{}','user',100,100)",
            params![turn, owner],
        )
        .unwrap();
    }
    for i in 0..3_600 {
        let (owner, turn) = if i % 9 == 0 {
            ("retirement_target", "turn_retirement_target")
        } else {
            ("retirement_other_a", "turn_retirement_other")
        };
        // Keep all provider_approval_ref values NULL and mix status values:
        // schema44's partial indexes cannot satisfy this complete read.
        let status = match i % 4 {
            0 => "pending",
            1 => "approved",
            2 => "denied",
            _ => "cancelled",
        };
        tx.execute(
            "INSERT INTO approvals (
                 id,session_id,turn_id,status,request_json,created_at
             ) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                format!("approval_{i:05}"),
                owner,
                turn,
                status,
                serde_json::json!({"fixture_approval":i}).to_string(),
                i as i64 + 100,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let old_plan = settled_turn_approval_plan(&db);
    assert!(old_plan.contains("SCAN approvals"), "{old_plan}");
    assert!(old_plan.contains("USE TEMP B-TREE"), "{old_plan}");
    let original = repository
        .get_persisted_retirement_settled_turn("turn_retirement_target")
        .unwrap()
        .unwrap();
    assert_eq!(original.approvals.len(), 400);
    assert_eq!(original.approvals.first().unwrap().id, "approval_00000");
    assert_eq!(original.approvals.last().unwrap().id, "approval_03591");
    assert_eq!(original.admission, None);
    let review = repository
        .get_persisted_retirement_review_session("retirement_target")
        .unwrap()
        .unwrap();
    assert_eq!(review.approvals, original.approvals);
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM approvals", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        3_600
    );
    drop(db);

    // Real populated historical schema44 DB: additive schema45 index does
    // not rewrite approval payloads, identities, or their settled ordering.
    repository.initialize_schema().unwrap();
    let db = open_connection(&path).unwrap();
    let indexed_plan = settled_turn_approval_plan(&db);
    assert!(
        indexed_plan.contains("SEARCH approvals USING INDEX idx_approvals_session_turn_projection"),
        "targeted retirement approvals must seek owned rows: {indexed_plan}"
    );
    assert!(!indexed_plan.contains("USE TEMP B-TREE"), "{indexed_plan}");
    assert_eq!(
        db.query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        crate::schema::SCHEMA_VERSION
    );
    assert_eq!(
        repository
            .get_persisted_retirement_settled_turn("turn_retirement_target")
            .unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        repository
            .get_persisted_retirement_review_session("retirement_target")
            .unwrap()
            .unwrap()
            .approvals,
        original.approvals
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM approvals", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        3_600
    );

    db.execute(
        "UPDATE approvals SET request_json='{broken-json' WHERE session_id='retirement_other_a'",
        [],
    )
    .unwrap();
    assert!(repository.hydrate_runtime_state().is_err());
    let reopened = SqliteRuntimeRepository::new(path);
    reopened.initialize_schema().unwrap();
    assert_eq!(
        reopened
            .get_persisted_retirement_settled_turn("turn_retirement_target")
            .unwrap(),
        Some(original)
    );
    assert!(reopened.hydrate_runtime_state().is_err());
}
