//! First maintenance-authority oracles. These disposable SQLite fixtures do
//! not run Git, a provider, startup native replay or filesystem cleanup.
use super::legacy_native_turn_guards::{legacy, raw_state, LEGACY_ID};
use super::*;
use runtime_core::{
    ManagedWorktreeClaimTransition::{Claim, Release},
    ManagedWorktreeCleanupAdmission, ManagedWorktreeCleanupCommand, RuntimeError,
    WorktreeCleanupResponse,
};

fn blocked(error: RuntimeError) {
    assert!(
        matches!(error, RuntimeError::LegacyNativeObligationBlocked { ref coordinate, .. }
        if coordinate.operation_id == LEGACY_ID),
        "{error:?}"
    );
}

fn historical_claim(repo: &SqliteRuntimeRepository, original: &ManagedWorktreeClaimRecord) {
    // Explicit unjournaled historical data for the normalization authority.
    // No invocation of this helper is a claim about native provider state.
    repo.upsert_managed_worktree_claim(original).unwrap();
}

#[test]
fn fresh_claim_and_changed_release_recheck_late_legacy_history() {
    for release in [false, true] {
        let (temp, repo, claim) = super::worktree_claim_events::fixture();
        if release {
            repo.transition_managed_worktree_claim(&claim, Claim)
                .unwrap();
        }
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        legacy(&db, "rolled_back", "{}", false);
        let before = raw_state(&db);
        let requested = if release {
            ManagedWorktreeClaimRecord {
                released_at: Some(200),
                ..claim.clone()
            }
        } else {
            claim.clone()
        };
        let result = repo
            .transition_managed_worktree_claim(&requested, if release { Release } else { Claim });
        blocked(result.unwrap_err());
        assert_eq!(raw_state(&db), before);
    }
}

#[test]
fn exact_active_claim_and_terminal_release_replay_remain_write_free() {
    for release in [false, true] {
        let (temp, repo, claim) = super::worktree_claim_events::fixture();
        let active = repo
            .transition_managed_worktree_claim(&claim, Claim)
            .unwrap();
        let expected = if release {
            let released = ManagedWorktreeClaimRecord {
                released_at: Some(200),
                ..active
            };
            repo.transition_managed_worktree_claim(&released, Release)
                .unwrap()
        } else {
            active
        };
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        legacy(&db, "planned", "not-json", false);
        let before = raw_state(&db);
        let replay = repo
            .transition_managed_worktree_claim(&expected, if release { Release } else { Claim })
            .unwrap();
        assert_eq!(replay, expected);
        assert_eq!(raw_state(&db), before);
    }
}

#[test]
fn changed_historical_claim_normalization_refuses_before_writing() {
    let (temp, repo, mut claim) = super::worktree_claim_events::fixture();
    claim.claim_role = " OWNER ".into();
    historical_claim(&repo, &claim);
    let repaired = ManagedWorktreeClaimRecord {
        claim_role: "owner".into(),
        ..claim.clone()
    };
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "planned", "{}", true);
    let before = raw_state(&db);
    blocked(
        repo.repair_managed_worktree_claims_if_unchanged_atomically(&[claim], &[repaired])
            .unwrap_err(),
    );
    assert_eq!(raw_state(&db), before);
}

#[test]
fn changed_record_batch_keeps_every_original_revision_and_row() {
    let (temp, repo, claim) = super::worktree_claim_events::fixture();
    let original = repo
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    let second = ManagedWorktreeRecord {
        id: "second-maintenance-target".into(),
        worktree_name: "second".into(),
        worktree_cwd: "/another/retained/path".into(),
        branch_name: "gg/second".into(),
        ..original.clone()
    };
    repo.upsert_managed_worktree(&second).unwrap();
    let updated = ManagedWorktreeRecord {
        worktree_name: "normalized".into(),
        ..original.clone()
    };
    let updated_second = ManagedWorktreeRecord {
        worktree_name: "normalized-second".into(),
        ..second.clone()
    };
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(
        &db,
        "rolled_back",
        "{\"session_id\":\"some-other-child\"}",
        false,
    );
    let before = raw_state(&db);
    blocked(
        repo.repair_managed_worktrees_if_unchanged(&[
            (&original, updated),
            (&second, updated_second),
        ])
        .unwrap_err(),
    );
    assert_eq!(raw_state(&db), before);
}

#[test]
fn combined_startup_record_and_claim_plan_refuses_before_either_repair() {
    let (temp, repo, mut claim) = super::worktree_claim_events::fixture();
    claim.claim_role = " OWNER ".into();
    historical_claim(&repo, &claim);
    let original = repo
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    let changed = ManagedWorktreeRecord {
        worktree_name: "normalized".into(),
        ..original.clone()
    };
    let claim_changed = ManagedWorktreeClaimRecord {
        claim_role: "owner".into(),
        ..claim.clone()
    };
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "planned", "{", false);
    let before = raw_state(&db);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    blocked(
        reopened
            .repair_historical_worktree_startup_atomically(
                &[(&original, changed)],
                &[claim],
                &[claim_changed],
            )
            .unwrap_err(),
    );
    assert_eq!(raw_state(&db), before);
}

#[test]
fn exact_historical_repairs_are_noops_even_with_unresolved_obligation() {
    let (temp, repo, claim) = super::worktree_claim_events::fixture();
    historical_claim(&repo, &claim);
    let original = repo
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "rolled_back", "{}", false);
    let before = raw_state(&db);
    repo.repair_managed_worktree_if_unchanged(&original, &original)
        .unwrap();
    repo.repair_managed_worktree_claims_if_unchanged_atomically(&[claim.clone()], &[claim.clone()])
        .unwrap();
    repo.repair_historical_worktree_startup_atomically(
        &[(&original, original.clone())],
        &[claim.clone()],
        &[claim],
    )
    .unwrap();
    assert_eq!(raw_state(&db), before);
}

fn cleanup_fixture() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    ManagedWorktreeCleanupCommand,
) {
    let (temp, repo, claim) = super::worktree_claim_events::fixture();
    let mut worktree = repo
        .get_managed_worktree(&claim.worktree_id)
        .unwrap()
        .unwrap();
    worktree.deletion_policy = "delete_on_last_claim".into();
    repo.upsert_managed_worktree(&worktree).unwrap();
    let command = ManagedWorktreeCleanupCommand {
        operation_id: "op_disposable_cleanup_authority".into(),
        worktree,
        reason: Some("offline-database-only-fixture".into()),
        requested_at: 200,
    };
    (temp, repo, command)
}

#[test]
fn first_native_cleanup_authority_refuses_without_releasing_any_history() {
    let (temp, repo, command) = cleanup_fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "rolled_back", "{}", false);
    let before = raw_state(&db);
    blocked(repo.admit_managed_worktree_cleanup(&command).unwrap_err());
    assert_eq!(raw_state(&db), before);
}

#[test]
fn known_cleanup_result_and_exact_receipt_do_not_require_native_reexecution() {
    let (temp, repo, command) = cleanup_fixture();
    assert!(matches!(
        repo.admit_managed_worktree_cleanup(&command).unwrap(),
        ManagedWorktreeCleanupAdmission::Execute { .. }
    ));
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    legacy(&db, "planned", "not-json", false);
    // Inject a known native result through the real settlement writer. This
    // asserts database receipt behavior, never actual filesystem absence.
    let receipt = WorktreeCleanupResponse {
        worktree_id: command.worktree.id.clone(),
        status: "deleted".into(),
        deletion_policy: "delete_on_last_claim".into(),
        active_claim_count: 0,
        worktree_path_deleted: true,
        branch_deleted: true,
        diagnostics: vec![],
    };
    repo.complete_managed_worktree_cleanup(&command.operation_id, &receipt, 300)
        .unwrap();
    let before = raw_state(&db);
    match repo.admit_managed_worktree_cleanup(&command).unwrap() {
        ManagedWorktreeCleanupAdmission::Replay {
            operation_id,
            response,
        } => {
            assert_eq!(operation_id, command.operation_id);
            assert_eq!(
                serde_json::to_value(response).unwrap(),
                serde_json::to_value(receipt).unwrap()
            );
        }
        other => panic!("expected exact original cleanup receipt: {other:?}"),
    }
    assert_eq!(raw_state(&db), before);
}

#[test]
fn incomplete_history_scan_is_not_assigned_the_last_examined_operation() {
    let (temp, repo, claim) = super::worktree_claim_events::fixture();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let oversized = "x".repeat(4097);
    db.execute("INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES(?1,'history','spawn_member_with_worktree','planned','{}',1,1)", [&oversized]).unwrap();
    let before = raw_state(&db);
    assert!(matches!(
        repo.transition_managed_worktree_claim(&claim, Claim),
        Err(RuntimeError::LegacyNativeScanIncomplete { .. })
    ));
    assert_eq!(raw_state(&db), before);
}
