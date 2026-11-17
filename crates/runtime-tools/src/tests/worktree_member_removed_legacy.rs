//! Member removal over disposable managed-checkout authority.
//! Git is used only for setup and independent before/after inspection.
use super::worktree_spawn_recovery::{fixture, worktree_input, SpawnFixture};
use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeClaimTransition, ManagedWorktreeRecord,
    RuntimeStore, TeamOperationDiagnosticRecord, WorktreeMemberRemovedRequest, WorktreeService,
};
use rusqlite::{types::Value as SqlValue, Connection};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering};
const ORIGINAL: &str = "historical/removal ?#%é";
const ORIGINAL_TEAM: &str = "original-blocker-team-b";
#[derive(Clone, Copy, Debug)]
enum History {
    Blocked,
    Incomplete,
}
struct Case {
    base: SpawnFixture,
    record: ManagedWorktreeRecord,
    claim: ManagedWorktreeClaimRecord,
}
impl Case {
    fn db(&self) -> Connection {
        Connection::open(&self.base.database_path).unwrap()
    }
    fn request(&self) -> WorktreeMemberRemovedRequest {
        WorktreeMemberRemovedRequest {
            team_id: self.base.team_id.clone(),
            agent_id: self.claim.session_id.clone(),
            removed_by: Some("member-removal-fixture".into()),
        }
    }
    fn diagnostics(&self) -> Vec<TeamOperationDiagnosticRecord> {
        self.base
            .store
            .list_team_operation_diagnostics(None, None)
            .unwrap()
    }
    fn native_snapshot(&self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        (
            self.base.git(&["worktree", "list", "--porcelain"]),
            self.base.git(&["show-ref", "--heads"]),
            std::fs::read(std::path::Path::new(&self.record.worktree_cwd).join(".git")).unwrap(),
        )
    }
}
async fn case(history: History) -> Case {
    let base = fixture().await;
    assert_ne!(base.team_id, ORIGINAL_TEAM);
    base.init_git();
    let mut spawn = base.request();
    spawn.worktree = Some(worktree_input("create", "removal-legacy-boundary"));
    let record = base.prepared_checkout(&spawn, false);
    assert_eq!(record.deletion_policy, "delete_on_last_claim");
    base.store.upsert_managed_worktree(&record).unwrap();
    let claim = base
        .store
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                worktree_id: record.id.clone(),
                session_id: spawn.source_session_id.clone(),
                claim_role: "owner".into(),
                created_at: 1000,
                released_at: None,
            },
            ManagedWorktreeClaimTransition::Claim,
        )
        .unwrap();
    let payload = match history {
        History::Blocked => json!({"spawned_session_id":claim.session_id}),
        History::Incomplete => {
            json!({"spawned_session_id":claim.session_id,"padding":"x".repeat(65_537)})
        }
    };
    Connection::open(&base.database_path)
        .unwrap()
        .execute(
            "INSERT INTO team_operation_journal
        (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES(?1,?2,'spawn_member_with_worktree','session_created',?3,10,20)",
            rusqlite::params![ORIGINAL, ORIGINAL_TEAM, payload.to_string()],
        )
        .unwrap();
    Case {
        base,
        record,
        claim,
    }
}
type ProtectedRows = BTreeMap<String, Vec<Vec<SqlValue>>>;
fn protected_rows(db: &Connection) -> ProtectedRows {
    // Diagnostic rows and their AUTOINCREMENT bookkeeping are separate.
    [
        "managed_worktrees",
        "managed_worktree_claims",
        "managed_worktree_cleanup_receipts",
        "sessions",
        "runtime_operations",
        "runtime_operation_effects",
        "runtime_operation_resource_claims",
        "runtime_events",
        "team_operation_journal",
        "team_spawn_admissions",
    ]
    .into_iter()
    .map(|table| {
        let mut statement = db.prepare(&format!("SELECT * FROM {table}")).unwrap();
        let columns = statement.column_count();
        let mut rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|index| row.get::<_, SqlValue>(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        rows.sort_by_cached_key(|row| format!("{row:?}"));
        (table.to_owned(), rows)
    })
    .collect()
}
fn assert_recovery(value: &Value, history: History) {
    assert_eq!(value["native_retry_authorized"], false, "{value}");
    match history {
        History::Blocked => {
            assert_eq!(value["kind"], "blocked", "{value}");
            assert_eq!(value["coordinate"]["operation_id"], ORIGINAL);
            assert_eq!(value["coordinate"]["team_id"], ORIGINAL_TEAM);
            assert_eq!(value["coordinate"]["stage"], "session_created");
            assert_eq!(value["reason"], "exact_session");
            assert_eq!(
                value["inspection_path"],
                runtime_core::error::legacy_team_operation_inspection_path(ORIGINAL)
            );
        }
        History::Incomplete => {
            assert_eq!(value["kind"], "scan_incomplete", "{value}");
            assert_eq!(
                value["incomplete"]["reason"], "payload_cell_limit",
                "{value}"
            );
            assert!(value.get("coordinate").is_none(), "{value}");
            assert!(value.get("operation_id").is_none(), "{value}");
            assert!(value.get("inspection_path").is_none(), "{value}");
        }
    }
}
fn assert_diagnostic(case: &Case, diagnostic: &TeamOperationDiagnosticRecord, history: History) {
    assert_eq!(diagnostic.operation_id, None);
    assert_eq!(
        diagnostic.team_id.as_deref(),
        Some(case.base.team_id.as_str())
    );
    assert_eq!(diagnostic.code, "worktree_claim_release_failed");
    assert_eq!(diagnostic.payload["worktree_id"], case.record.id);
    assert_eq!(diagnostic.payload["session_id"], case.claim.session_id);
    assert_eq!(diagnostic.payload["removal_team_id"], case.base.team_id);
    assert_recovery(&diagnostic.payload["legacy_recovery"], history);
}
fn assert_unchanged(
    case: &Case,
    before: &ProtectedRows,
    native_before: &(Vec<u8>, Vec<u8>, Vec<u8>),
    provider_before: usize,
) {
    assert_eq!(&protected_rows(&case.db()), before);
    assert_eq!(&case.native_snapshot(), native_before);
    assert_eq!(
        case.base.provider.effect_calls.load(Ordering::SeqCst),
        provider_before
    );
    assert_eq!(
        case.base
            .store
            .get_managed_worktree(&case.record.id)
            .unwrap()
            .as_ref(),
        Some(&case.record)
    );
    assert_eq!(
        case.base
            .store
            .get_managed_worktree_claim(&case.claim.worktree_id, &case.claim.session_id)
            .unwrap()
            .as_ref(),
        Some(&case.claim)
    );
    assert_eq!(case.claim.released_at, None);
}
#[tokio::test]
async fn member_removal_keeps_original_legacy_team_and_preserves_claims() {
    for history in [History::Blocked, History::Incomplete] {
        let case = case(history).await;
        let before = protected_rows(&case.db());
        let native_before = case.native_snapshot();
        let provider_before = case.base.provider.effect_calls.load(Ordering::SeqCst);
        let diagnostics_before = case.diagnostics();
        let result = case
            .base
            .service
            .on_member_removed(case.request())
            .await
            .unwrap();
        assert!(result.released_claims.is_empty());
        assert!(result.cleanup_results.is_empty());
        assert_eq!(result.diagnostics.len(), 1);
        assert_diagnostic(&case, &result.diagnostics[0], history);
        let diagnostics_after = case.diagnostics();
        assert_eq!(diagnostics_after.len(), diagnostics_before.len() + 1);
        assert_eq!(
            &diagnostics_after[..diagnostics_before.len()],
            diagnostics_before.as_slice()
        );
        assert_eq!(diagnostics_after.last(), Some(&result.diagnostics[0]));
        assert_unchanged(&case, &before, &native_before, provider_before);
    }
}
#[tokio::test]
async fn member_removal_diagnostic_fault_retains_original_typed_refusal() {
    for history in [History::Blocked, History::Incomplete] {
        let case = case(history).await;
        let before = protected_rows(&case.db());
        let native_before = case.native_snapshot();
        let provider_before = case.base.provider.effect_calls.load(Ordering::SeqCst);
        let diagnostics_before = case.diagnostics();
        case.db().execute_batch("CREATE TRIGGER refuse_removal_diagnostic BEFORE INSERT ON team_operation_diagnostics
            WHEN NEW.code='worktree_claim_release_failed' BEGIN SELECT RAISE(ABORT,'injected removal diagnostic fault'); END;").unwrap();
        let error = case
            .base
            .service
            .on_member_removed(case.request())
            .await
            .unwrap_err();
        let recovery = error.legacy_native_recovery().unwrap_or_else(|| {
            panic!("diagnostic failure erased the original typed refusal: {error:?}")
        });
        let original = recovery.error_details();
        assert_recovery(&original, history);
        assert_eq!(case.diagnostics(), diagnostics_before);
        assert_unchanged(&case, &before, &native_before, provider_before);
        // Restore only the failed diagnostic writer. The blocker remains.
        case.db()
            .execute_batch("DROP TRIGGER refuse_removal_diagnostic;")
            .unwrap();
        let retry = case
            .base
            .service
            .on_member_removed(case.request())
            .await
            .unwrap();
        assert!(retry.released_claims.is_empty());
        assert!(retry.cleanup_results.is_empty());
        assert_eq!(retry.diagnostics.len(), 1);
        assert_diagnostic(&case, &retry.diagnostics[0], history);
        assert_eq!(retry.diagnostics[0].payload["legacy_recovery"], original);
        assert_unchanged(&case, &before, &native_before, provider_before);
    }
}
