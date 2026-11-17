use super::support::*;
use crate::db::open_connection;
use runtime_core::{
    RuntimeError, StartupAttachmentScope, StartupReattachAdmission, StartupReattachHold,
};
use rusqlite::params;
use serde_json::json;

#[test]
fn managed_and_stored_startup_have_one_exact_grant_and_receipt() {
    for managed in [true, false] {
        let fixture = Fixture::new(managed);
        let command = fixture.command("startup_exact", "instance_exact");
        let grant = execute(&fixture.repository, &command, 300);
        assert_eq!(grant.intent.command, command);
        assert_eq!(grant.intent.authority.request, command.request);
        assert_eq!(grant.claim.owner_operation_id, command.operation_id);
        assert_eq!(grant.claim.resource_id, command.session_id);
        assert_eq!(grant.claim.resource_kind, RESUME_CLAIM);
        assert_eq!(grant.claim.acquired_at, 300);
        assert!(grant.claim.fence_generation > 0);
        assert_eq!(
            matches!(
                grant.intent.authority.scope,
                StartupAttachmentScope::WorkspaceAgent { .. }
            ),
            managed
        );
        assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        let before = snapshot(&fixture.repository);
        assert!(matches!(
            fixture.repository.admit_startup_reattach(&command, 301).unwrap(),
            StartupReattachAdmission::Unresolved { operation_id } if operation_id == command.operation_id
        ));
        assert_eq!(snapshot(&fixture.repository), before);

        let acknowledgement = ack(&grant, true);
        let completed = fixture
            .repository
            .complete_startup_reattach(&grant, &acknowledgement, 400)
            .unwrap();
        assert_eq!(
            completed.session.canonical_provider_session_ref.as_deref(),
            Some(acknowledgement.provider_session_ref.as_str())
        );
        assert_eq!(completed.receipt.operation_id, command.operation_id);
        assert_eq!(
            completed.receipt.normalized_request_hash,
            grant.normalized_request_hash
        );
        assert_eq!(completed.receipt.acknowledged_at, 400);
        assert_eq!(completed.receipt.metadata_terminal_event_id, None);
        assert_attempt(&fixture.repository, &grant, "completed", "finalized", false);
        let before = snapshot(&fixture.repository);
        let StartupReattachAdmission::Replay(replayed) = fixture
            .repository
            .admit_startup_reattach(&command, 999)
            .unwrap()
        else {
            panic!("completed startup did not replay");
        };
        assert_eq!(
            serde_json::to_value(replayed).unwrap(),
            serde_json::to_value(&completed.receipt).unwrap()
        );
        assert_eq!(snapshot(&fixture.repository), before);

        // Lost-ACK replay belongs to the old attempt even after a later owner is admitted.
        let next = fixture.grant("startup_later_owner");
        let before = snapshot(&fixture.repository);
        let replay = fixture
            .repository
            .complete_startup_reattach(&grant, &acknowledgement, 1000)
            .unwrap();
        assert_eq!(
            serde_json::to_value(replay.receipt).unwrap(),
            serde_json::to_value(&completed.receipt).unwrap()
        );
        assert!(matches!(
            fixture
                .repository
                .hold_startup_reattach(&grant, None, "lost local response", 1001)
                .unwrap(),
            StartupReattachHold::Completed(_)
        ));
        assert_eq!(snapshot(&fixture.repository), before);
        assert_attempt(&fixture.repository, &next, "requested", "started", true);
    }
}

#[test]
fn frozen_request_and_attempt_key_reject_different_authority_before_any_effect() {
    for field in 0..6 {
        let fixture = Fixture::new(true);
        let mut command = fixture.command("startup_request", "instance_request");
        match field {
            0 => command.request.model = Some("different-model".into()),
            1 => command.request.cwd = Some("/different-root".into()),
            2 => command
                .request
                .launch_policy
                .allowed_tools
                .push("write".into()),
            3 => command.request.metadata = Some(json!({"forged":true})),
            4 => command.request.provider_session_ref.push_str("-different"),
            5 => command.request.launch_policy.system_prompt = Some("different-prompt".into()),
            _ => unreachable!(),
        }
        let before = snapshot(&fixture.repository);
        assert!(
            matches!(
                fixture.repository.admit_startup_reattach(&command, 300),
                Err(RuntimeError::Conflict(_))
            ),
            "frozen request field {field} was accepted"
        );
        assert_eq!(snapshot(&fixture.repository), before);
    }
    let fixture = Fixture::new(true);
    let original = fixture.command("startup_same_key", "instance_same_key");
    let grant = execute(&fixture.repository, &original, 300);
    let mut collision = original.clone();
    collision.operation_id = "startup_other_id".into();
    let before = snapshot(&fixture.repository);
    assert!(matches!(
        fixture.repository.admit_startup_reattach(&collision, 301),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(snapshot(&fixture.repository), before);
    assert_attempt(&fixture.repository, &grant, "requested", "started", true);
}

#[test]
fn submitted_grant_tampering_never_completes_or_holds_the_real_attempt() {
    for field in 0..10 {
        let fixture = Fixture::new(true);
        let original = fixture.grant("startup_bound");
        let acknowledgement = ack(&original, false);
        let mut forged = original.clone();
        match field {
            0 => forged.normalized_request_hash.push('x'),
            1 => forged.intent.command.startup_instance_id.push('x'),
            2 => forged.intent.authority.request.metadata = Some(json!({"forged":true})),
            3 => {
                forged.intent.authority.scope = StartupAttachmentScope::WorkspaceAgent {
                    workspace_id: "wrong-workspace".into(),
                    agent_revision: 0,
                }
            }
            4 => forged.claim.fence_generation += 1,
            5 => forged.claim.owner_operation_id = "different-owner".into(),
            6 => forged.claim.resource_id = "different-session".into(),
            7 => forged.claim.acquired_at += 1,
            8 => forged.claim.resource_kind = "different-kind".into(),
            9 => forged.intent.version += 1,
            _ => unreachable!(),
        }
        let before = snapshot(&fixture.repository);
        assert!(
            fixture
                .repository
                .complete_startup_reattach(&forged, &acknowledgement, 400)
                .is_err(),
            "forged grant field {field} completed"
        );
        assert!(
            fixture
                .repository
                .hold_startup_reattach(&forged, Some(&acknowledgement), "forged", 400)
                .is_err(),
            "forged grant field {field} held the real attempt"
        );
        assert_eq!(snapshot(&fixture.repository), before);
        fixture
            .repository
            .complete_startup_reattach(&original, &acknowledgement, 401)
            .unwrap();
    }
}

#[test]
fn durable_operation_effect_and_claim_damage_cannot_be_repaired_by_acknowledgement() {
    let mutations = [
        "UPDATE runtime_operations SET actor_id='different-system' WHERE operation_id=?1",
        "UPDATE runtime_operations SET normalized_request_hash='different-hash' WHERE operation_id=?1",
        "UPDATE runtime_operation_effects SET attempt_count=2 WHERE operation_id=?1",
        "UPDATE runtime_operation_effects SET target_id='different-session' WHERE operation_id=?1",
        "UPDATE runtime_operation_effects SET evidence_json=json_set(evidence_json,'$.grant.claim.acquired_at',301) WHERE operation_id=?1",
        "UPDATE runtime_operation_resource_claims SET fence_generation=fence_generation+1 WHERE owner_operation_id=?1",
        "UPDATE runtime_operation_resource_claims SET acquired_at=acquired_at+1 WHERE owner_operation_id=?1",
    ];
    for sql in mutations {
        let fixture = Fixture::new(true);
        let grant = fixture.grant("startup_durable_binding");
        let acknowledgement = ack(&grant, false);
        open_connection(&fixture.repository.database_path)
            .unwrap()
            .execute(sql, [&grant.intent.command.operation_id])
            .unwrap();
        let before = snapshot(&fixture.repository);
        assert!(
            fixture
                .repository
                .complete_startup_reattach(&grant, &acknowledgement, 400)
                .is_err(),
            "{sql}"
        );
        assert!(
            fixture
                .repository
                .hold_startup_reattach(&grant, Some(&acknowledgement), "damaged", 400)
                .is_err(),
            "{sql}"
        );
        assert_eq!(snapshot(&fixture.repository), before);
    }
}

#[test]
fn native_ack_identity_mismatch_never_records_positive_completion() {
    for field in 0..3 {
        let fixture = Fixture::new(true);
        let grant = fixture.grant("startup_wrong_ack");
        let mut acknowledgement = ack(&grant, false);
        match field {
            0 => acknowledgement.runtime_session_id.push('x'),
            1 => acknowledgement.provider_session_ref.push('x'),
            2 => {
                acknowledgement.canonical_provider_session_ref = Some("different-canonical".into())
            }
            _ => unreachable!(),
        }
        let before = snapshot(&fixture.repository);
        assert!(matches!(
            fixture
                .repository
                .complete_startup_reattach(&grant, &acknowledgement, 400),
            Err(RuntimeError::ProtocolViolation(_))
        ));
        assert_eq!(snapshot(&fixture.repository), before);
        fixture
            .repository
            .hold_startup_reattach(&grant, Some(&acknowledgement), "identity mismatch", 401)
            .unwrap();
        assert_attempt(
            &fixture.repository,
            &grant,
            "manual_review",
            "uncertain",
            true,
        );
        let before = snapshot(&fixture.repository);
        assert_unresolved(
            fixture
                .repository
                .admit_startup_reattach(&fixture.command("startup_retry", "restart"), 500),
            &grant.intent.command.operation_id,
        );
        assert_eq!(snapshot(&fixture.repository), before);
    }
}

#[test]
fn first_attempt_admission_rolls_back_all_authority_on_each_insert_fault() {
    for table in [
        "runtime_operations",
        "runtime_operation_resource_claims",
        "runtime_operation_effects",
        "runtime_operation_transitions",
    ] {
        let fixture = Fixture::new(true);
        let command = fixture.command("startup_fault", "instance_fault");
        let db = open_connection(&fixture.repository.database_path).unwrap();
        db.execute_batch(&format!(
            "CREATE TRIGGER startup_insert_fault BEFORE INSERT ON {table}
             BEGIN SELECT RAISE(ABORT,'startup admission injection'); END;"
        ))
        .unwrap();
        let before = snapshot(&fixture.repository);
        assert!(
            fixture
                .repository
                .admit_startup_reattach(&command, 300)
                .is_err(),
            "{table}"
        );
        assert_eq!(snapshot(&fixture.repository), before);
        db.execute_batch("DROP TRIGGER startup_insert_fault")
            .unwrap();
        // No grant ever escaped and no native call was authorized.
        let grant = execute(&fixture.repository, &command, 301);
        assert_attempt(&fixture.repository, &grant, "requested", "started", true);
    }
}

#[test]
fn late_completion_receipt_fault_rolls_back_canonical_fill_and_retains_observed_ack() {
    let fixture = Fixture::new(true);
    let grant = fixture.grant("startup_receipt_fault");
    let acknowledgement = ack(&grant, true);
    let db = open_connection(&fixture.repository.database_path).unwrap();
    db.execute_batch(
        "CREATE TRIGGER startup_receipt_fault BEFORE INSERT ON runtime_operation_transitions
         WHEN NEW.to_phase='completed' AND NEW.operation_id='startup_receipt_fault'
         BEGIN SELECT RAISE(ABORT,'startup receipt injection'); END;",
    )
    .unwrap();
    let before = snapshot(&fixture.repository);
    assert!(fixture
        .repository
        .complete_startup_reattach(&grant, &acknowledgement, 400)
        .is_err());
    assert_eq!(snapshot(&fixture.repository), before);
    assert_eq!(fixture.session().canonical_provider_session_ref, None);
    hold_observed(&fixture.repository, &grant, &acknowledgement);
    let before = snapshot(&fixture.repository);
    db.execute_batch("DROP TRIGGER startup_receipt_fault")
        .unwrap();
    assert!(fixture
        .repository
        .complete_startup_reattach(&grant, &acknowledgement, 600)
        .is_err());
    assert_unresolved(
        fixture
            .repository
            .admit_startup_reattach(&fixture.command("startup_retry", "restart"), 601),
        &grant.intent.command.operation_id,
    );
    assert_eq!(snapshot(&fixture.repository), before);
    assert!(matches!(
        fixture.repository.admit_startup_reattach(&grant.intent.command, 602).unwrap(),
        StartupReattachAdmission::Unresolved { operation_id } if operation_id == grant.intent.command.operation_id
    ));
}

#[test]
fn all_historical_d4_review_statuses_refuse_before_grant_and_during_ack() {
    for status in [
        "policy_recovery_required",
        "restore_recovery_required",
        "archive_recovery_required",
        "create_recovery_required",
    ] {
        for after_grant in [false, true] {
            let fixture = Fixture::new(true);
            let command = fixture.command("startup_d4", "instance_d4");
            let grant = after_grant.then(|| execute(&fixture.repository, &command, 300));
            open_connection(&fixture.repository.database_path)
                .unwrap()
                .execute(
                    "UPDATE sessions SET status=?2,updated_at=400 WHERE id=?1",
                    params![SESSION, status],
                )
                .unwrap();
            let before = snapshot(&fixture.repository);
            if let Some(grant) = grant {
                let acknowledgement = ack(&grant, false);
                assert!(
                    fixture
                        .repository
                        .complete_startup_reattach(&grant, &acknowledgement, 401)
                        .is_err(),
                    "late historical review status {status} completed"
                );
                assert_eq!(snapshot(&fixture.repository), before);
                hold_observed(&fixture.repository, &grant, &acknowledgement);
                assert_eq!(fixture.session().status, status);
            } else {
                assert!(
                    fixture
                        .repository
                        .admit_startup_reattach(&command, 401)
                        .is_err(),
                    "historical review status {status} authorized native resume"
                );
                assert_eq!(snapshot(&fixture.repository), before);
            }
        }
    }
}

#[test]
fn real_provider_unknown_receipt_refuses_startup_before_or_during_native_ack() {
    for after_grant in [false, true] {
        let fixture = Fixture::new(true);
        let running = running(&fixture);
        let command = fixture.command("startup_quarantine", "instance_quarantine");
        let grant = after_grant.then(|| execute(&fixture.repository, &command, 300));
        let quarantined = quarantine(&fixture.repository, &running);
        let before = snapshot(&fixture.repository);
        if let Some(grant) = grant {
            let acknowledgement = ack(&grant, false);
            assert!(fixture
                .repository
                .complete_startup_reattach(&grant, &acknowledgement, 401)
                .is_err());
            assert_eq!(snapshot(&fixture.repository), before);
            hold_observed(&fixture.repository, &grant, &acknowledgement);
        } else {
            assert!(fixture
                .repository
                .admit_startup_reattach(&command, 401)
                .is_err());
            assert_eq!(snapshot(&fixture.repository), before);
        }
        let saved = fixture
            .repository
            .get_persisted_retirement_review_session(SESSION)
            .unwrap()
            .unwrap();
        assert_eq!(saved.session, quarantined.session);
        assert_eq!(saved.turns, vec![quarantined.turn]);
        assert_eq!(saved.admissions, vec![running.admission]);
    }
}
