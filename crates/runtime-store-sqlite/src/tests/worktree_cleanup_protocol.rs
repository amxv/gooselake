//! Ordered original acknowledgements and grant-loss boundaries, using SQLite only.
use super::legacy_native_turn_guards::raw_state;
use super::worktree_cleanup_receipts::{fixture, original_branch_ack};
use super::*;
use runtime_core::{
    BranchDeletionAck, BranchDeletionGrant, CheckoutRemovalAck, CheckoutRemovalGrant,
    ManagedCleanupAttempt, ManagedCleanupBranchAdmission, ManagedCleanupObservation,
    ManagedWorktreeCleanupAdmission, ManagedWorktreeCleanupCommand, OperationPhase,
};

fn admit(
    repository: &SqliteRuntimeRepository,
    command: &ManagedWorktreeCleanupCommand,
) -> CheckoutRemovalGrant {
    match repository.admit_managed_worktree_cleanup(command).unwrap() {
        ManagedWorktreeCleanupAdmission::Execute { grant } => grant,
        other => panic!("{other:?}"),
    }
}

#[test]
fn original_action_acknowledgements_are_ordered_and_cannot_change_their_grants() {
    let (_temp, repository, command) = fixture();
    let checkout = admit(&repository, &command);
    let db = open_connection(&repository.database_path).unwrap();
    let before = raw_state(&db);
    assert!(repository
        .acknowledge_managed_branch_deletion(&BranchDeletionAck {
            grant: BranchDeletionGrant {
                binding: checkout.binding.clone(),
                checkout_receipt_hash: "not-observed".into(),
                granted_at: 150
            },
            exit_code: 0,
            observed_at: 200
        })
        .is_err());
    for change in 0..4 {
        let mut ack = CheckoutRemovalAck {
            grant: checkout.clone(),
            exit_code: 0,
            observed_at: 150,
        };
        match change {
            0 => ack.exit_code = 1,
            1 => ack.grant.binding.checkout_claim.fence_generation += 1,
            2 => ack.grant.binding.target.checkout_path.push_str("/wrong"),
            _ => ack.grant.binding.requested_at += 1,
        }
        assert!(repository
            .acknowledge_managed_checkout_removal(&ack)
            .is_err());
        assert_eq!(raw_state(&db), before);
    }
    let branch = match repository
        .acknowledge_managed_checkout_removal(&CheckoutRemovalAck {
            grant: checkout,
            exit_code: 0,
            observed_at: 150,
        })
        .unwrap()
    {
        ManagedCleanupBranchAdmission::Execute { grant } => grant,
        other => panic!("{other:?}"),
    };
    let before = raw_state(&db);
    for change in 0..4 {
        let mut ack = BranchDeletionAck {
            grant: branch.clone(),
            exit_code: 0,
            observed_at: 200,
        };
        match change {
            0 => ack.exit_code = 1,
            1 => ack.grant.checkout_receipt_hash = "wrong".into(),
            2 => ack.grant.granted_at += 1,
            _ => ack.grant.binding.branch_claim.fence_generation += 1,
        }
        assert!(repository
            .acknowledge_managed_branch_deletion(&ack)
            .is_err());
        assert_eq!(raw_state(&db), before);
    }
    repository
        .acknowledge_managed_branch_deletion(&BranchDeletionAck {
            grant: branch,
            exit_code: 0,
            observed_at: 200,
        })
        .unwrap();
}

#[test]
fn duplicate_checkout_receipt_never_reissues_a_lost_branch_grant() {
    let (_temp, repository, command) = fixture();
    let checkout = admit(&repository, &command);
    let ack = CheckoutRemovalAck {
        grant: checkout.clone(),
        exit_code: 0,
        observed_at: 150,
    };
    assert!(matches!(
        repository
            .acknowledge_managed_checkout_removal(&ack)
            .unwrap(),
        ManagedCleanupBranchAdmission::Execute { .. }
    ));
    let db = open_connection(&repository.database_path).unwrap();
    let before = raw_state(&db);
    for _ in 0..2 {
        assert!(
            matches!(repository.acknowledge_managed_checkout_removal(&ack).unwrap(),
            ManagedCleanupBranchAdmission::Unresolved{operation_id} if operation_id==command.operation_id)
        );
        assert!(
            matches!(repository.admit_managed_worktree_cleanup(&command).unwrap(),
            ManagedWorktreeCleanupAdmission::Unresolved{operation_id} if operation_id==command.operation_id)
        );
    }
    assert!(repository
        .acknowledge_managed_checkout_removal(&CheckoutRemovalAck {
            observed_at: 151,
            ..ack
        })
        .is_err());
    assert_eq!(raw_state(&db), before);
    let original_claims = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap()
        .claims;
    let attempt = ManagedCleanupAttempt::Checkout(checkout);
    let observation = ManagedCleanupObservation::Unknown {
        diagnostic: "branch grant reply was lost".into(),
    };
    repository
        .hold_managed_worktree_cleanup(&attempt, &observation, "lost response", 200)
        .unwrap();
    let detail = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(detail.operation.phase, OperationPhase::ManualReview);
    assert_eq!(detail.claims, original_claims);
    assert_eq!(
        detail
            .effects
            .iter()
            .find(|e| e.effect_kind == "git_worktree_remove")
            .unwrap()
            .phase,
        "finalized"
    );
    assert_eq!(
        detail
            .effects
            .iter()
            .find(|e| e.effect_kind == "git_branch_delete")
            .unwrap()
            .phase,
        "started"
    );
    let held = raw_state(&db);
    repository
        .hold_managed_worktree_cleanup(&attempt, &observation, "lost response", 200)
        .unwrap();
    assert_eq!(raw_state(&db), held);
}

#[test]
fn known_checkout_receipt_is_retained_when_late_eligibility_blocks_branch_grant() {
    let (_temp, repository, command) = fixture();
    let checkout = admit(&repository, &command);
    let db = open_connection(&repository.database_path).unwrap();
    // A historical native obligation can appear after checkout admission.
    // Supported claim/session writers already honor the cleanup fence.
    super::legacy_native_turn_guards::legacy(&db, "planned", "{}", false);
    assert!(
        matches!(repository.acknowledge_managed_checkout_removal(&CheckoutRemovalAck{
        grant:checkout,exit_code:0,observed_at:150}).unwrap(),ManagedCleanupBranchAdmission::Unresolved{operation_id}
        if operation_id==command.operation_id)
    );
    let detail = repository
        .get_operation(&command.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(detail.operation.phase, OperationPhase::ManualReview);
    assert_eq!(detail.claims.len(), 2);
    let removal = detail
        .effects
        .iter()
        .find(|e| e.effect_kind == "git_worktree_remove")
        .unwrap();
    let branch = detail
        .effects
        .iter()
        .find(|e| e.effect_kind == "git_branch_delete")
        .unwrap();
    assert_eq!((&*removal.phase, removal.attempt_count), ("finalized", 1));
    assert_eq!((&*branch.phase, branch.attempt_count), ("intended", 0));
    assert_eq!(detail.transitions.len(), 2);
    assert_eq!(detail.transitions[1].to_phase, OperationPhase::ManualReview);
    assert!(!repository
        .list_runtime_events(None, None, 100)
        .unwrap()
        .iter()
        .any(|e| e.kind == "worktree.checkout_removed"));
    let before = raw_state(&db);
    assert!(matches!(
        repository.admit_managed_worktree_cleanup(&command).unwrap(),
        ManagedWorktreeCleanupAdmission::Unresolved { .. }
    ));
    assert_eq!(raw_state(&db), before);
}

#[test]
fn completed_replay_permits_later_resource_generations_and_preserves_new_reservations() {
    let (_temp, repository, old) = fixture();
    let (ack, _) = original_branch_ack(&repository, &old);
    let result = repository
        .acknowledge_managed_branch_deletion(&ack)
        .unwrap();
    let mut next = old.clone();
    next.operation_id = "op_cleanup_next".into();
    next.worktree.created_at += 1;
    next.worktree.updated_at += 1;
    repository.upsert_managed_worktree(&next.worktree).unwrap();
    let grant = admit(&repository, &next);
    assert!(
        grant.binding.checkout_claim.fence_generation
            > ack.grant.binding.checkout_claim.fence_generation
    );
    assert!(
        grant.binding.branch_claim.fence_generation
            > ack.grant.binding.branch_claim.fence_generation
    );
    let db = open_connection(&repository.database_path).unwrap();
    let before = raw_state(&db);
    assert!(
        matches!(repository.replay_managed_worktree_cleanup(&old.worktree).unwrap(),Some(ManagedWorktreeCleanupAdmission::Replay{response,..})
        if serde_json::to_value(&response).unwrap()==serde_json::to_value(&result).unwrap())
    );
    repository
        .acknowledge_managed_branch_deletion(&ack)
        .unwrap();
    assert_eq!(raw_state(&db), before);
}

const CHILD_DIR: &str = "GOOSELAKE_N06_CLEANUP_RACE_DIR";
const CHILD_INDEX: &str = "GOOSELAKE_N06_CLEANUP_RACE_INDEX";

#[test]
fn cleanup_admission_child() {
    let Some(root) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let index = std::env::var(CHILD_INDEX).unwrap();
    let repository = SqliteRuntimeRepository::new(root.join("runtime.sqlite3"));
    let command = ManagedWorktreeCleanupCommand {
        operation_id: format!("op_racer_{index}"),
        worktree: repository
            .get_managed_worktree("wt_cleanup")
            .unwrap()
            .unwrap(),
        reason: Some("test_release".into()),
        requested_at: 100,
    };
    std::fs::write(root.join(format!("ready-{index}")), b"ready").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !root.join("go").exists() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let result = match repository.admit_managed_worktree_cleanup(&command).unwrap() {
        ManagedWorktreeCleanupAdmission::Execute { grant } => {
            serde_json::json!({"kind":"execute","operation_id":grant.binding.operation_id})
        }
        ManagedWorktreeCleanupAdmission::Unresolved { operation_id } => {
            serde_json::json!({"kind":"unresolved","operation_id":operation_id})
        }
        other => panic!("{other:?}"),
    };
    std::fs::write(
        root.join(format!("result-{index}.json")),
        serde_json::to_vec(&result).unwrap(),
    )
    .unwrap();
}

#[test]
fn independent_processes_admit_only_one_original_checkout_grant() {
    let (temp, repository, _command) = fixture();
    let mut children = Vec::new();
    for index in 0..2 {
        children.push(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::worktree_cleanup_protocol::cleanup_admission_child",
                    "--nocapture",
                ])
                .env(CHILD_DIR, temp.path())
                .env(CHILD_INDEX, index.to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !(0..2).all(|i| temp.path().join(format!("ready-{i}")).exists()) {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    std::fs::write(temp.path().join("go"), b"go").unwrap();
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    }
    let results: Vec<serde_json::Value> = (0..2)
        .map(|i| {
            serde_json::from_slice(
                &std::fs::read(temp.path().join(format!("result-{i}.json"))).unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(results.iter().filter(|v| v["kind"] == "execute").count(), 1);
    assert_eq!(
        results.iter().filter(|v| v["kind"] == "unresolved").count(),
        1
    );
    assert_eq!(results[0]["operation_id"], results[1]["operation_id"]);
    let detail = repository
        .get_operation(results[0]["operation_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(detail.claims.len(), 2);
    assert_eq!(detail.effects.len(), 2);
    assert_eq!(
        detail
            .effects
            .iter()
            .find(|e| e.effect_kind == "git_worktree_remove")
            .unwrap()
            .attempt_count,
        1
    );
    assert_eq!(
        detail
            .effects
            .iter()
            .find(|e| e.effect_kind == "git_branch_delete")
            .unwrap()
            .attempt_count,
        0
    );
}
