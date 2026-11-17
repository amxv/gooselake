use super::*;
use crate::operation_tx::OperationAuthorityTransaction;
use runtime_core::legacy_spawn_obligations::{LegacySpawnCoordinate, LegacyTargetBlockReason};
use runtime_core::{OperationActor, OperationEffectRecord, OperationRecord};

const OP: &str = "op_existing_canonical_owner";
const EFFECT: &str = "fx_existing_native_target";
const KIND: &str = "operator_turn_dispatch";
const TARGET: &str = "sess_original_target";

fn observation(id: &str) -> LegacyNativeRecovery {
    LegacyNativeRecovery::Blocked {
        coordinate: LegacySpawnCoordinate {
            operation_id: id.into(),
            team_id: "original-team".into(),
            stage: Some("rolled_back".into()),
            evidence_problem: None,
        },
        reason: LegacyTargetBlockReason::ExactSession,
    }
}

/// Synthetic existing carrier only. This fixture never claims to dispatch a
/// provider or to satisfy a canonical native completion verifier.
fn fixture() -> (tempfile::TempDir, SqliteRuntimeRepository) {
    let temp = tempfile::tempdir().unwrap();
    let repo = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    repo.initialize_schema().unwrap();
    let mut connection = open_connection(&repo.database_path).unwrap();
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let authority = OperationAuthorityTransaction::new(&tx);
    let request = json!({"agent_id":TARGET,"input":[]});
    authority
        .insert_operation(&OperationRecord {
            operation_id: OP.into(),
            workspace_id: None,
            kind: "operator_user_turn_send".into(),
            actor: OperationActor::operator("operator"),
            idempotency_key: Some("original-key".into()),
            normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
            normalized_request: request,
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: 10,
            updated_at: 10,
        })
        .unwrap();
    authority
        .insert_effect(&OperationEffectRecord {
            effect_id: EFFECT.into(),
            operation_id: OP.into(),
            effect_kind: KIND.into(),
            target_kind: "provider_session".into(),
            target_id: TARGET.into(),
            phase: "intended".into(),
            idempotency_key: "native:0".into(),
            attempt_count: 0,
            created_at: 10,
            updated_at: 10,
            evidence: Some(json!({"turn_id":"turn-original","snapshot":{"immutable":"original"}})),
        })
        .unwrap();
    authority
        .acquire_claim(OP, "operator_user_turn_native", TARGET, 10)
        .unwrap();
    tx.commit().unwrap();
    (temp, repo)
}

fn event_count(repo: &SqliteRuntimeRepository) -> i64 {
    open_connection(&repo.database_path)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn observation_survives_reopen_without_changing_authority_or_effect_count() {
    let (temp, repo) = fixture();
    let before = repo.get_operation(OP).unwrap().unwrap();
    let count = event_count(&repo);
    let recovery = observation("custom/historical ?#%é");
    repo.record_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET, &recovery, 20)
        .unwrap();
    let saved = repo.get_operation(OP).unwrap().unwrap();
    assert_eq!(saved.operation, before.operation);
    assert_eq!(saved.claims, before.claims);
    assert_eq!(saved.effects.len(), before.effects.len());
    let mut unchanged = saved.clone();
    let evidence = unchanged.effects[0]
        .evidence
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap();
    assert!(evidence.remove("legacy_native_recovery").is_some());
    assert_eq!(
        evidence.remove("legacy_native_recovery_observed_at"),
        Some(json!(20))
    );
    assert_eq!(unchanged, before);
    assert_eq!(event_count(&repo), count + 1);
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let original = reopened.get_operation(OP).unwrap().unwrap();
    let decoded =
        LegacyNativeRecovery::from_effect_evidence(original.effects[0].evidence.as_ref().unwrap())
            .unwrap()
            .unwrap();
    assert_eq!(decoded, recovery);
    assert_eq!(
        reopened
            .read_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET)
            .unwrap(),
        Some(recovery.clone())
    );
    assert_eq!(
        decoded.error_details()["inspection_path"],
        "/v1/diagnostics/team-operations/custom%2Fhistorical%20%3F%23%25%C3%A9"
    );
    reopened
        .record_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET, &recovery, 30)
        .unwrap();
    assert_eq!(reopened.get_operation(OP).unwrap().unwrap(), saved);
    assert_eq!(event_count(&reopened), count + 1);
}

#[test]
fn owner_effect_kind_and_target_are_all_required_and_first_observation_is_immutable() {
    let (_temp, repo) = fixture();
    let before = repo.get_operation(OP).unwrap().unwrap();
    let count = event_count(&repo);
    for (owner, effect, kind, target) in [
        ("unknown-owner", EFFECT, KIND, TARGET),
        (OP, "wrong-effect", KIND, TARGET),
        (OP, EFFECT, "different-kind", TARGET),
        (OP, EFFECT, KIND, "other-session"),
    ] {
        assert!(repo
            .record_operation_effect_legacy_recovery(
                owner,
                effect,
                kind,
                target,
                &observation("old-a"),
                20
            )
            .is_err());
        assert_eq!(repo.get_operation(OP).unwrap().unwrap(), before);
        assert_eq!(event_count(&repo), count);
    }
    repo.record_operation_effect_legacy_recovery(
        OP,
        EFFECT,
        KIND,
        TARGET,
        &observation("old-a"),
        20,
    )
    .unwrap();
    let first = repo.get_operation(OP).unwrap().unwrap();
    assert!(repo
        .record_operation_effect_legacy_recovery(
            OP,
            EFFECT,
            KIND,
            TARGET,
            &observation("old-b"),
            21
        )
        .is_err());
    assert_eq!(repo.get_operation(OP).unwrap().unwrap(), first);
    assert_eq!(event_count(&repo), count + 1);
}

#[test]
fn descriptor_or_critical_event_failure_rolls_back_the_whole_observation() {
    for trigger in [
        "CREATE TRIGGER refuse_recovery BEFORE UPDATE OF evidence_json ON runtime_operation_effects BEGIN SELECT RAISE(ABORT,'carrier fault'); END;",
        "CREATE TRIGGER refuse_recovery BEFORE INSERT ON runtime_events WHEN NEW.kind='operation.legacy_native_recovery_observed' BEGIN SELECT RAISE(ABORT,'event fault'); END;",
    ] {
        let (_temp, repo) = fixture();
        let before = repo.get_operation(OP).unwrap().unwrap();
        let count = event_count(&repo);
        open_connection(&repo.database_path).unwrap().execute_batch(trigger).unwrap();
        assert!(repo.record_operation_effect_legacy_recovery(OP,EFFECT,KIND,TARGET,&observation("old-original"),20).is_err());
        assert_eq!(repo.get_operation(OP).unwrap().unwrap(),before);
        assert_eq!(event_count(&repo),count);
        assert!(before.operation.exact_terminal_result.is_none());
        assert_eq!(before.effects[0].attempt_count,0);
        assert_eq!(before.claims.len(),1);
    }
}

#[test]
fn original_observation_remains_inspectable_inside_known_prior_evidence_receipts() {
    let (_temp, repo) = fixture();
    let recovery = observation("historical-unresolved");
    repo.record_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET, &recovery, 20)
        .unwrap();
    let connection = open_connection(&repo.database_path).unwrap();
    connection.execute("UPDATE runtime_operation_effects SET evidence_json=json_object('prior_evidence',json(evidence_json),'outcome','synthetic-later-receipt') WHERE effect_id=?1",[EFFECT]).unwrap();
    let saved = repo.get_operation(OP).unwrap().unwrap();
    let count = event_count(&repo);
    repo.record_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET, &recovery, 30)
        .unwrap();
    assert_eq!(repo.get_operation(OP).unwrap().unwrap(), saved);
    assert_eq!(event_count(&repo), count);
    assert_eq!(
        LegacyNativeRecovery::from_effect_evidence(saved.effects[0].evidence.as_ref().unwrap())
            .unwrap(),
        Some(recovery)
    );
}

fn observation_events(repo: &SqliteRuntimeRepository) -> Vec<(String, String, i64, String, i64)> {
    let connection = open_connection(&repo.database_path).unwrap();
    let mut statement = connection.prepare(
        "SELECT event_id,scope_id,critical,payload_json,created_at FROM runtime_events ORDER BY id",
    ).unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap();
    rows.map(Result::unwrap).collect()
}

#[test]
fn damaged_carrier_event_pairs_are_neither_restored_nor_repaired() {
    for damage in [
        "DELETE FROM runtime_events WHERE kind='operation.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET critical=0 WHERE kind='operation.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET scope_id='foreign-owner' WHERE kind='operation.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET payload_json=json_set(payload_json,'$.operation_id','foreign-owner') WHERE kind='operation.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET payload_json=json_set(payload_json,'$.effect_id','foreign-effect') WHERE kind='operation.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET payload_json=json_set(payload_json,'$.legacy_recovery.coordinate.operation_id','foreign-history') WHERE kind='operation.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET created_at=21 WHERE kind='operation.legacy_native_recovery_observed'",
        "UPDATE runtime_operation_effects SET evidence_json=json_remove(evidence_json,'$.legacy_native_recovery','$.legacy_native_recovery_observed_at')",
        "UPDATE runtime_operation_effects SET evidence_json=json_remove(evidence_json,'$.legacy_native_recovery_observed_at')",
    ] {
        let (_temp,repo) = fixture();
        let recovery = observation("original-history");
        repo.record_operation_effect_legacy_recovery(OP,EFFECT,KIND,TARGET,&recovery,20).unwrap();
        open_connection(&repo.database_path).unwrap().execute_batch(damage).unwrap();
        let damaged = repo.get_operation(OP).unwrap().unwrap();
        let events = observation_events(&repo);
        assert!(repo.read_operation_effect_legacy_recovery(OP,EFFECT,KIND,TARGET).is_err(),"{damage}");
        assert!(repo.record_operation_effect_legacy_recovery(OP,EFFECT,KIND,TARGET,&recovery,30).is_err(),"{damage}");
        assert_eq!(repo.get_operation(OP).unwrap().unwrap(),damaged,"{damage}");
        assert_eq!(observation_events(&repo),events,"{damage}");
        assert_eq!(damaged.claims.len(),1);
        assert_eq!(damaged.effects.len(),1);
        assert_eq!(damaged.effects[0].attempt_count,0);
        assert!(damaged.operation.exact_terminal_result.is_none());
    }
}

#[test]
fn healthy_pair_replay_keeps_original_time_and_absent_observation_stays_absent() {
    let (_temp, repo) = fixture();
    let initial_event_count = event_count(&repo);
    assert_eq!(
        repo.read_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET)
            .unwrap(),
        None
    );
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "UPDATE runtime_operation_effects SET evidence_json=NULL WHERE effect_id=?1",
            [EFFECT],
        )
        .unwrap();
    assert_eq!(
        repo.read_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET)
            .unwrap(),
        None
    );
    let empty = repo.get_operation(OP).unwrap().unwrap();
    assert!(repo
        .record_operation_effect_legacy_recovery(
            OP,
            EFFECT,
            KIND,
            TARGET,
            &observation("history"),
            20
        )
        .is_err());
    assert_eq!(repo.get_operation(OP).unwrap().unwrap(), empty);
    assert_eq!(event_count(&repo), initial_event_count);

    let (_temp, repo) = fixture();
    let recovery = observation("history");
    repo.record_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET, &recovery, 20)
        .unwrap();
    let before = observation_events(&repo);
    repo.record_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET, &recovery, 900)
        .unwrap();
    assert_eq!(
        repo.read_operation_effect_legacy_recovery(OP, EFFECT, KIND, TARGET)
            .unwrap(),
        Some(recovery)
    );
    assert_eq!(observation_events(&repo), before);
    let original_event_id = event_id(OP, EFFECT).unwrap();
    let original_observation = before
        .iter()
        .find(|row| row.0 == original_event_id)
        .unwrap();
    assert_eq!(original_observation.4, 20);
}
