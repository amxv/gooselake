//! Disposable damaged-carrier and injected-event oracles. No native actions.
use super::legacy_native_turn_guards::raw_state;
use super::worktree_cleanup_receipts::{fixture, original_branch_ack};
use super::*;
use runtime_core::{
    ManagedCleanupAttempt, ManagedCleanupObservation, ManagedWorktreeCleanupCommand,
};

fn refused(
    repository: &SqliteRuntimeRepository,
    command: &ManagedWorktreeCleanupCommand,
    db: &Connection,
) {
    let before = raw_state(db);
    assert!(repository
        .replay_managed_worktree_cleanup(&command.worktree)
        .is_err());
    assert!(repository.admit_managed_worktree_cleanup(command).is_err());
    assert_eq!(raw_state(db), before, "damaged replay must be read-only");
}

#[test]
fn typed_completed_replay_cannot_downgrade_to_legacy() {
    for replace_hash in [false, true] {
        let (_temp, repository, command) = fixture();
        let (ack, _) = original_branch_ack(&repository, &command);
        repository
            .acknowledge_managed_branch_deletion(&ack)
            .unwrap();
        let db = open_connection(&repository.database_path).unwrap();
        let mut request = repository
            .get_operation(&command.operation_id)
            .unwrap()
            .unwrap()
            .operation
            .normalized_request;
        request.as_object_mut().unwrap().remove("protocol_version");
        db.execute(
            "UPDATE runtime_operations SET normalized_request_json=?2 WHERE operation_id=?1",
            params![
                command.operation_id,
                serde_json::to_string(&request).unwrap()
            ],
        )
        .unwrap();
        if replace_hash {
            db.execute(
                "UPDATE runtime_operations SET normalized_request_hash=?2 WHERE operation_id=?1",
                params![
                    command.operation_id,
                    runtime_core::normalized_json_hash(&request).unwrap()
                ],
            )
            .unwrap();
        }
        refused(&repository, &command, &db);
    }
}

#[test]
fn completed_replay_rejects_another_operations_acknowledgement() {
    let (_temp, repository, first) = fixture();
    let (ack_a, _) = original_branch_ack(&repository, &first);
    repository
        .acknowledge_managed_branch_deletion(&ack_a)
        .unwrap();
    let mut second = first.clone();
    second.operation_id = "op_cleanup_2".into();
    second.worktree.id = "wt_cleanup_2".into();
    second.worktree.worktree_cwd = "/disposable/worktrees/second".into();
    second.worktree.branch_name = "gg/second".into();
    second.worktree.worktree_name = "second".into();
    repository
        .upsert_managed_worktree(&second.worktree)
        .unwrap();
    let (ack_b, _) = original_branch_ack(&repository, &second);
    let result_b = repository
        .acknowledge_managed_branch_deletion(&ack_b)
        .unwrap();
    let db = open_connection(&repository.database_path).unwrap();
    assert_eq!(db.execute("UPDATE runtime_operation_effects SET evidence_json=json_set(evidence_json,'$.ack',json(?1)) WHERE effect_id=?2",
        params![serde_json::to_string(&ack_b).unwrap(),format!("{}:branch-delete",first.operation_id)]).unwrap(),1);
    refused(&repository, &first, &db);
    let before = raw_state(&db);
    assert!(
        matches!(repository.replay_managed_worktree_cleanup(&second.worktree).unwrap(),
        Some(runtime_core::ManagedWorktreeCleanupAdmission::Replay{response,..})
        if serde_json::to_value(&response).unwrap()==serde_json::to_value(&result_b).unwrap())
    );
    assert_eq!(raw_state(&db), before);
}

const PREFIX_FAULTS:[&str;2]=[
    "DELETE FROM runtime_events WHERE event_id='op_cleanup_1:event:worktree:checkout-acknowledged';",
    "UPDATE runtime_operation_transitions SET evidence_json='{}' WHERE operation_id='op_cleanup_1' AND sequence=2;",
];

#[test]
fn hold_rolls_back_if_prior_checkout_evidence_changes_during_event() {
    for fault in PREFIX_FAULTS {
        let (_temp, repository, command) = fixture();
        let (ack, _) = original_branch_ack(&repository, &command);
        let db = open_connection(&repository.database_path).unwrap();
        db.execute_batch(&format!(
            "CREATE TRIGGER damage_hold_prefix AFTER INSERT ON runtime_events
            WHEN NEW.event_id='op_cleanup_1:event:worktree:held' BEGIN {fault} END;"
        ))
        .unwrap();
        let before = raw_state(&db);
        assert!(repository
            .hold_managed_worktree_cleanup(
                &ManagedCleanupAttempt::Branch(ack.grant),
                &ManagedCleanupObservation::Exited { code: 1 },
                "branch has unmerged history",
                200
            )
            .is_err());
        assert_eq!(
            raw_state(&db),
            before,
            "hold must restore all prior evidence: {fault}"
        );
    }
}

#[test]
fn exact_hold_replay_rejects_damaged_prior_checkout_evidence() {
    for fault in PREFIX_FAULTS {
        let (_temp, repository, command) = fixture();
        let (ack, _) = original_branch_ack(&repository, &command);
        let attempt = ManagedCleanupAttempt::Branch(ack.grant);
        let observation = ManagedCleanupObservation::Exited { code: 1 };
        repository
            .hold_managed_worktree_cleanup(
                &attempt,
                &observation,
                "branch has unmerged history",
                200,
            )
            .unwrap();
        let db = open_connection(&repository.database_path).unwrap();
        db.execute_batch(fault).unwrap();
        let before = raw_state(&db);
        let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
        assert!(reopened
            .hold_managed_worktree_cleanup(
                &attempt,
                &observation,
                "branch has unmerged history",
                200
            )
            .is_err());
        assert_eq!(raw_state(&db), before);
    }
}

#[test]
fn terminal_receipt_rolls_back_if_resource_counter_is_damaged() {
    for branch in [false, true] {
        for delete in [false, true] {
            let (_temp, repository, command) = fixture();
            let (ack, _) = original_branch_ack(&repository, &command);
            let claim = if branch {
                &ack.grant.binding.branch_claim
            } else {
                &ack.grant.binding.checkout_claim
            };
            let verb = if delete {
                "DELETE FROM runtime_operation_resource_fences"
            } else {
                "UPDATE runtime_operation_resource_fences SET last_generation=0"
            };
            let db = open_connection(&repository.database_path).unwrap();
            db.execute_batch(&format!(
                "CREATE TRIGGER damage_settled_counter AFTER INSERT ON runtime_events
                WHEN NEW.event_id='op_cleanup_1:event:worktree:settled' BEGIN
                {verb} WHERE resource_kind='{}' AND resource_id='{}'; END;",
                claim.resource_kind, claim.resource_id
            ))
            .unwrap();
            let before = raw_state(&db);
            assert!(repository
                .acknowledge_managed_branch_deletion(&ack)
                .is_err());
            assert_eq!(raw_state(&db), before, "branch={branch},delete={delete}");
        }
    }
}

#[test]
fn cleanup_action_receipts_reject_changed_original_carriers_without_writes() {
    for fault in [
        "UPDATE runtime_operation_resource_claims SET acquired_at=101;",
        "UPDATE runtime_operation_resource_claims SET fence_generation=fence_generation+1;",
        "DELETE FROM runtime_operation_resource_fences;",
        "UPDATE runtime_operations SET normalized_request_hash='changed';",
        "UPDATE managed_worktree_cleanup_receipts SET branch_name='another-branch';",
        "UPDATE runtime_operation_effects SET attempt_count=2 WHERE effect_kind='git_branch_delete';",
        "UPDATE runtime_operation_effects SET target_id='another-target' WHERE effect_kind='git_branch_delete';",
        "DELETE FROM runtime_operation_effects WHERE effect_kind='git_worktree_remove';",
        "UPDATE runtime_operation_transitions SET evidence_json='{}' WHERE sequence=1;",
    ] {
        let (_temp,repository,command)=fixture();
        let (ack,_)=original_branch_ack(&repository,&command);
        let db=open_connection(&repository.database_path).unwrap();
        db.execute_batch(fault).unwrap();
        let before=raw_state(&db);
        assert!(repository.acknowledge_managed_branch_deletion(&ack).is_err(),"{fault}");
        assert_eq!(raw_state(&db),before,"{fault}");
    }
}

#[test]
fn admission_and_checkout_event_faults_roll_back_the_complete_transaction() {
    for checkout in [false, true] {
        for fault in ["ignore", "reservation", "effect"] {
            let (_temp, repository, command) = fixture();
            let grant = if checkout {
                match repository.admit_managed_worktree_cleanup(&command).unwrap() {
                    runtime_core::ManagedWorktreeCleanupAdmission::Execute { grant } => Some(grant),
                    other => panic!("{other:?}"),
                }
            } else {
                None
            };
            let db = open_connection(&repository.database_path).unwrap();
            let kind = if checkout {
                "worktree.checkout_removed"
            } else {
                "worktree.cleanup_admitted"
            };
            let (timing, body) = match fault {
                "ignore" => ("BEFORE", "SELECT RAISE(IGNORE);"),
                "reservation" => (
                    "AFTER",
                    "UPDATE runtime_operation_resource_claims SET acquired_at=101;",
                ),
                _ => (
                    "AFTER",
                    "UPDATE runtime_operation_effects SET target_id='changed';",
                ),
            };
            db.execute_batch(&format!("CREATE TRIGGER damage_action_event {timing} INSERT ON runtime_events WHEN NEW.kind='{kind}' BEGIN {body} END;")).unwrap();
            let before = raw_state(&db);
            if let Some(grant) = grant {
                assert!(repository
                    .acknowledge_managed_checkout_removal(&runtime_core::CheckoutRemovalAck {
                        grant,
                        exit_code: 0,
                        observed_at: 150
                    })
                    .is_err());
            } else {
                assert!(repository.admit_managed_worktree_cleanup(&command).is_err());
            }
            assert_eq!(raw_state(&db), before, "checkout={checkout},fault={fault}");
        }
    }
}
