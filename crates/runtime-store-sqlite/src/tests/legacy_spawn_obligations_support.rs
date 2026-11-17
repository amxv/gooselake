//! Shared test-only fixture for the unapplied legacy-obligation tests.
//! Minimal in-memory tables exercise classification only. Production schema
//! migration and actual query-plan coverage live in the separate schema module.
use crate::repository_legacy_spawn_obligations::classify_legacy_spawn_obligations;
use runtime_core::legacy_spawn_obligations::{
    LegacyNativeTarget, LegacyScanIncomplete, LegacyScanLimit, LegacyScanTermination,
    LegacySpawnCoordinate, LegacySpawnScan, LegacySpawnScanBudget, LegacyTargetBlockReason,
    LegacyTargetDecision,
};
use runtime_core::{
    ManagedWorktreeRecord, RuntimeError, TeamMemberRecord, TeamMemberSpawnResponse, TeamRecord,
    TeamWithMembers,
};
use rusqlite::{params, types::Value as SqlValue, Connection, TransactionBehavior};

pub(super) const KIND: &str = "spawn_member_with_worktree";
pub(super) const TEAM: &str = "original-team";
pub(super) const JSON_LIMIT: usize = 65536;
pub(super) const METADATA_LIMIT: usize = 4096;

pub(super) struct Fixture {
    pub db: Connection,
}
impl Fixture {
    pub(super) fn new() -> Self {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE team_operation_journal(
                operation_id TEXT PRIMARY KEY, team_id TEXT NOT NULL,
                kind TEXT NOT NULL, stage TEXT NOT NULL, payload_json TEXT NOT NULL,
                created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);",
        )
        .unwrap();
        db.execute_batch(include_str!("../schema_migration_30.sql"))
            .unwrap();
        db.execute_batch(include_str!("../schema_migration_31.sql"))
            .unwrap();
        db.execute_batch(include_str!("../schema_migration_51.sql"))
            .unwrap();
        Self { db }
    }
    pub(super) fn scan(
        &mut self,
        targets: &[LegacyNativeTarget],
        budget: LegacySpawnScanBudget,
    ) -> LegacySpawnScan {
        scan(&mut self.db, targets, budget).unwrap()
    }
}

pub(super) fn scan(
    db: &mut Connection,
    targets: &[LegacyNativeTarget],
    budget: LegacySpawnScanBudget,
) -> Result<LegacySpawnScan, RuntimeError> {
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    let result = classify_legacy_spawn_obligations(&tx, targets, budget);
    assert!(
        !tx.is_autocommit(),
        "classifier must not end its caller's snapshot"
    );
    tx.rollback().unwrap();
    result
}
pub(super) fn session(id: &str) -> LegacyNativeTarget {
    LegacyNativeTarget::Session(id.into())
}
pub(super) fn checkout(id: Option<&str>, path: Option<&str>) -> LegacyNativeTarget {
    LegacyNativeTarget::Checkout {
        worktree_id: id.map(str::to_owned),
        checkout_path: path.map(str::to_owned),
        repository_root: None,
        branch_name: None,
    }
}
pub(super) fn budget(rows: usize, bytes: usize) -> LegacySpawnScanBudget {
    LegacySpawnScanBudget {
        max_rows: rows,
        max_bytes: bytes,
    }
}
pub(super) fn journal(
    db: &Connection,
    op: &str,
    team: &str,
    kind: &str,
    stage: &str,
    payload: &str,
) {
    db.execute(
        "INSERT INTO team_operation_journal
         (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
         VALUES(?1,?2,?3,?4,?5,10,20)",
        params![op, team, kind, stage, payload],
    )
    .unwrap();
}
pub(super) fn admission(db: &Connection, op: &str, team: &str, receipt: Option<&str>) {
    db.execute(
        "INSERT INTO team_spawn_admissions
         (team_id,source_session_id,idempotency_key,request_hash,operation_id,
          completed_response_json,created_at,updated_at)
         VALUES(?1,'source',?2,'original-request-hash',?3,?4,10,20)",
        params![team, format!("key:{op}"), op, receipt],
    )
    .unwrap();
}
pub(super) fn saved_response(op: &str, team: &str, child: &str) -> TeamMemberSpawnResponse {
    let mut child_session = super::sample_session();
    child_session.id = child.into();
    child_session.status = "ready".into();
    child_session.active_turn_id = None;
    child_session.worktree_id = None;
    child_session.provider_session_ref = Some(format!("historical:{child}"));
    child_session.canonical_provider_session_ref = None;
    child_session.closed_at = None;
    let member = TeamMemberRecord {
        team_id: team.into(),
        agent_id: child.into(),
        title: None,
        joined_at: 12,
        added_by: "source".into(),
        creator_agent_id: None,
        creator_compaction_subscription: "auto".into(),
        worktree_id: None,
    };
    let source_member = TeamMemberRecord {
        agent_id: "source".into(),
        added_by: "operator".into(),
        ..member.clone()
    };
    TeamMemberSpawnResponse {
        operation_id: op.into(),
        team: TeamWithMembers {
            team: TeamRecord {
                id: team.into(),
                name: "Saved historical team".into(),
                lead_agent_id: "source".into(),
                created_by: "operator".into(),
                created_at: 1,
                updated_at: 12,
                deleted_at: None,
            },
            members: vec![source_member, member.clone()],
        },
        spawned_session: child_session,
        spawned_member: member,
        worktree: None,
        worktree_assignment_mode: "none".into(),
        worktree_created_by_operation: false,
        onboarding: serde_json::json!({
            "status":"sent", "message_id":"original-message",
            "delivery_ids":["original-delivery"]
        }),
        journal_stage: "completed".into(),
    }
}
pub(super) fn receipt(op: &str, team: &str, child: &str) -> String {
    serde_json::to_string(&saved_response(op, team, child)).unwrap()
}
pub(super) fn certified(db: &Connection, op: &str, payload: &str) -> String {
    let receipt = receipt(op, TEAM, "historical-child");
    journal(db, op, TEAM, KIND, "completed", payload);
    admission(db, op, TEAM, Some(&receipt));
    receipt
}
pub(super) fn child_payload(child: &str) -> String {
    serde_json::json!({"spawned_session_id":child}).to_string()
}
pub(super) fn worktree() -> ManagedWorktreeRecord {
    ManagedWorktreeRecord {
        id: "historical-worktree".into(),
        repo_root: "/saved/repository".into(),
        worktree_root: "/saved/old-link".into(),
        worktree_cwd: "/saved/old-link/checkout".into(),
        branch_name: "gg/original".into(),
        worktree_name: "original".into(),
        unified_workspace_path: "saved-repository".into(),
        deletion_policy: "delete_on_last_claim".into(),
        created_by_session_id: Some("source-not-child".into()),
        // Native legacy create could adopt another operation's managed record.
        created_by_operation_id: Some("prior-managed-operation".into()),
        created_at: 3,
        updated_at: 4,
    }
}
pub(super) fn assert_clear(scan: &LegacySpawnScan) {
    assert!(
        matches!(
            scan.termination,
            LegacyScanTermination::ExhaustedBothStreams
        ),
        "{scan:?}"
    );
    assert!(
        scan.decisions
            .iter()
            .all(|d| matches!(d, LegacyTargetDecision::Clear)),
        "{scan:?}"
    );
}
pub(super) fn assert_blocked<'a>(
    scan: &'a LegacySpawnScan,
    index: usize,
    operation: &str,
    reason: LegacyTargetBlockReason,
) -> &'a LegacySpawnCoordinate {
    match &scan.decisions[index] {
        LegacyTargetDecision::Blocked {
            coordinate,
            reason: found,
        } => {
            assert_eq!(coordinate.operation_id, operation);
            assert_eq!(*found, reason);
            coordinate
        }
        other => panic!("expected original blocker for {operation}, got {other:?}"),
    }
}
pub(super) fn assert_incomplete(
    scan: &LegacySpawnScan,
    index: usize,
    reason: LegacyScanLimit,
) -> &LegacyScanIncomplete {
    let LegacyScanTermination::Incomplete(global) = &scan.termination else {
        panic!("expected incomplete scan, got {scan:?}")
    };
    assert_eq!(global.reason, reason);
    let LegacyTargetDecision::ScanIncomplete(target) = &scan.decisions[index] else {
        panic!("incomplete scan must not return clear or invent a target operation: {scan:?}")
    };
    assert_eq!(target.as_ref(), global.as_ref());
    target
}
pub(super) fn raw_rows(db: &Connection, table: &str) -> Vec<Vec<SqlValue>> {
    assert!(matches!(
        table,
        "team_operation_journal" | "team_spawn_admissions"
    ));
    let columns = db
        .prepare(&format!("PRAGMA table_info('{table}')"))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let projection = columns
        .into_iter()
        .map(|name| {
            let quoted = name.replace('"', "\"\"");
            format!("typeof(\"{quoted}\"), CAST(\"{quoted}\" AS BLOB)")
        })
        .collect::<Vec<_>>()
        .join(",");
    // Record both storage class and bytes without decoding arbitrary TEXT.
    let mut statement = db
        .prepare(&format!(
            "SELECT {projection} FROM {table} ORDER BY operation_id"
        ))
        .unwrap();
    let count = statement.column_count();
    statement
        .query_map([], |row| {
            (0..count)
                .map(|index| row.get(index))
                .collect::<rusqlite::Result<Vec<SqlValue>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}
pub(super) fn metadata_bytes(op: &str, team: &str, stage: &str) -> usize {
    // Both candidate projections copy op + team + journal team + kind + stage.
    op.len() + team.len() * 2 + KIND.len() + stage.len()
}
