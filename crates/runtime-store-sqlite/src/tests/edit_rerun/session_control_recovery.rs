use super::*;
use crate::tests::legacy_native_turn_guards::raw_state;
use runtime_core::legacy_spawn_obligations::{
    LegacyNativeRecovery, LegacyScanIncomplete, LegacyScanLimit, LegacySpawnCoordinate,
    LegacySpawnScanStats, LegacySpawnStream, LegacyTargetBlockReason,
};
use runtime_core::{
    ProviderSession, RuntimeEventCriticality, RuntimeEventScope, SessionControlCoordinate,
    SessionEditRerunOperation, TurnAdmissionRecord, TurnCorrelationState,
    TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource, TurnRecord,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

pub(in crate::tests) type PairFixture = (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    SessionControlCoordinate,
);
const SLOT: &str = "legacy_native_recovery_evidence";
const KIND: &str = "session_control.legacy_native_recovery_observed";

fn complete_with_real_replacement(
    repo: &SqliteRuntimeRepository,
    operation: &SessionEditRerunOperation,
) -> SessionEditRerunOperation {
    repo.advance_session_edit_rerun(
        &operation.operation_id,
        "native_started",
        None,
        None,
        None,
        120,
    )
    .unwrap();
    let child = ProviderSession {
        runtime_session_id: operation.agent_id.clone(),
        provider_session_ref: "verified-child-for-evidence-test".into(),
        canonical_provider_session_ref: Some("verified-child-for-evidence-test".into()),
    };
    repo.advance_session_edit_rerun(
        &operation.operation_id,
        "fork_observed",
        Some(&child),
        None,
        None,
        121,
    )
    .unwrap();
    let replacement_id = "actual-correlated-replacement";
    let mut session = repo
        .hydrate_runtime_state()
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.id == operation.agent_id)
        .unwrap();
    session.status = "turn_admitted".into();
    session.active_turn_id = Some(replacement_id.into());
    session.updated_at = 123;
    let turn = TurnRecord {
        id: replacement_id.into(),
        session_id: operation.agent_id.clone(),
        provider_turn_ref: None,
        status: "admitted".into(),
        input: json!([{"type":"text","text":operation.edited_snapshot.prompt_text}]),
        source: Some("user".into()),
        started_at: Some(123),
        completed_at: None,
        usage: None,
        error: None,
    };
    let admission = TurnAdmissionRecord {
        turn_id: replacement_id.into(),
        session_id: operation.agent_id.clone(),
        provider: operation.provider.as_str().into(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: operation.edited_snapshot.clone(),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState {
            expected_turn_id: None,
            correlation_id: Some(operation.operation_id.clone()),
        },
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 123,
        updated_at: 123,
    };
    repo.admit_turn(&admission, &turn, &session, None).unwrap();
    assert_eq!(
        repo.get_persisted_turn_admission_for_correlation(
            &operation.agent_id,
            &operation.operation_id
        )
        .unwrap(),
        Some(admission)
    );
    repo.advance_session_edit_rerun(
        &operation.operation_id,
        "completed",
        None,
        Some(replacement_id),
        None,
        124,
    )
    .unwrap()
}

#[test]
fn actual_edit_completion_retains_raw_evidence_and_only_replays_an_existing_pair() {
    for (save_pair, damage_pair) in [(false, false), (true, false), (true, true)] {
        let (_temp, repo, owner) = fixture();
        let original = repo.get_session_edit_rerun(owner.id()).unwrap().unwrap();
        let recovery = blocked("history");
        let db = open_connection(&repo.database_path).unwrap();
        if save_pair {
            repo.record_session_control_legacy_recovery(&owner, &recovery, 110)
                .unwrap();
        }
        if damage_pair {
            db.execute("UPDATE session_edit_reruns SET record_json=json_set(record_json,'$.legacy_native_recovery_evidence',json('{\"damaged\":true}')) WHERE operation_id=?1", [owner.id()]).unwrap();
        }
        let retained = raw_evidence(&repo, &owner);
        let completed = complete_with_real_replacement(&repo, &original);
        assert_eq!(completed.phase, "completed");
        assert_eq!(
            completed.replacement_turn_id.as_deref(),
            Some("actual-correlated-replacement")
        );
        assert_eq!(completed.legacy_native_recovery_evidence, retained);
        assert!(!repo
            .unresolved_session_edit_rerun(owner.agent_id())
            .unwrap());
        let settled = raw_state(&db);
        if save_pair && !damage_pair {
            assert_eq!(
                repo.read_session_control_legacy_recovery(&owner).unwrap(),
                Some(recovery.clone())
            );
            assert!(repo
                .record_session_control_legacy_recovery(&owner, &recovery, 900)
                .unwrap()
                .is_none());
        } else if damage_pair {
            assert!(repo.read_session_control_legacy_recovery(&owner).is_err());
            assert!(repo
                .record_session_control_legacy_recovery(&owner, &recovery, 900)
                .is_err());
        } else {
            assert_eq!(
                repo.read_session_control_legacy_recovery(&owner).unwrap(),
                None
            );
            assert!(repo
                .record_session_control_legacy_recovery(&owner, &recovery, 900)
                .is_err());
        }
        assert!(repo
            .record_session_control_legacy_recovery(&owner, &blocked("new-terminal-refusal"), 901)
            .is_err());
        assert_eq!(repo.begin_session_edit_rerun(&original).unwrap(), completed);
        assert_eq!(raw_state(&db), settled);
    }
}
pub(in crate::tests) fn blocked(id: &str) -> LegacyNativeRecovery {
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
fn incomplete() -> LegacyNativeRecovery {
    LegacyNativeRecovery::ScanIncomplete {
        incomplete: LegacyScanIncomplete {
            stream: Some(LegacySpawnStream::SpawnJournal),
            after_operation_id: Some("last-completely-inspected".into()),
            reason: LegacyScanLimit::RowBudget,
            stats: LegacySpawnScanStats {
                source_rows_read: 2,
                materialized_bytes: 123,
                pages_read: 1,
                certified_receipts: 0,
            },
        },
    }
}
fn layout(owner: &SessionControlCoordinate) -> (&'static str, &'static str) {
    match owner {
        SessionControlCoordinate::EditRerun { .. } => ("session_edit_reruns", "operation_id"),
        SessionControlCoordinate::Compaction { .. } => ("session_compaction_runs", "run_id"),
    }
}
fn carrier(db: &Connection, owner: &SessionControlCoordinate) -> (String, String, i64, i64) {
    let (table, id) = layout(owner);
    db.query_row(
        &format!("SELECT phase,record_json,created_at_ms,updated_at_ms FROM {table} WHERE {id}=?1"),
        [owner.id()],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .unwrap()
}
fn raw_evidence(repo: &SqliteRuntimeRepository, owner: &SessionControlCoordinate) -> Option<Value> {
    match owner {
        SessionControlCoordinate::EditRerun { operation_id, .. } => {
            repo.get_session_edit_rerun(operation_id)
                .unwrap()
                .unwrap()
                .legacy_native_recovery_evidence
        }
        SessionControlCoordinate::Compaction { run_id, .. } => {
            repo.get_session_compaction(run_id)
                .unwrap()
                .unwrap()
                .legacy_native_recovery_evidence
        }
    }
}
fn fixture() -> PairFixture {
    let temp = tempfile::tempdir().unwrap();
    let (repo, session, _, operation) = signed_edit_revision_fixture(&temp);
    repo.begin_session_edit_rerun(&operation).unwrap();
    let owner = SessionControlCoordinate::EditRerun {
        agent_id: session.id,
        operation_id: operation.operation_id,
    };
    (temp, repo, owner)
}
pub(in crate::tests) fn check_pair_replay(make: fn() -> PairFixture) {
    let (_temp, repo, owner) = make();
    let db = open_connection(&repo.database_path).unwrap();
    let before = carrier(&db, &owner);
    let database_before = raw_state(&db);
    assert_eq!(
        repo.read_session_control_legacy_recovery(&owner).unwrap(),
        None
    );
    assert_eq!(raw_state(&db), database_before);
    let recovery = blocked("original/history ?#%é");
    let event = repo
        .record_session_control_legacy_recovery(&owner, &recovery, 110)
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, KIND);
    assert_eq!(event.scope, RuntimeEventScope::Session);
    assert_eq!(event.scope_id, owner.agent_id());
    assert_eq!(event.session_id.as_deref(), Some(owner.agent_id()));
    assert!(
        event.team_id.is_none()
            && event.turn_id.is_none()
            && event.provider.is_none()
            && event.provider_seq.is_none()
    );
    assert_eq!(event.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(event.created_at, 110);
    assert_eq!(
        event.payload,
        json!({"owner":&owner,"legacy_recovery":&recovery,"native_retry_authorized":false})
    );
    let after = carrier(&db, &owner);
    let mut old: Value = serde_json::from_str(&before.1).unwrap();
    let mut new: Value = serde_json::from_str(&after.1).unwrap();
    old.as_object_mut().unwrap().remove(SLOT);
    assert_eq!(
        new.as_object_mut().unwrap().remove(SLOT),
        Some(json!({"recovery":&recovery,"observed_at_ms":110}))
    );
    assert_eq!(new, old);
    assert_eq!((after.0, after.2, after.3), (before.0, before.2, before.3));
    let saved = raw_state(&db);
    for table in [
        "runtime_operations",
        "runtime_operation_effects",
        "runtime_operation_resource_claims",
        "sessions",
        "workspace_agents",
        "turns",
        "turn_admissions",
    ] {
        assert_eq!(
            saved[table], database_before[table],
            "diagnostic changed {table}"
        );
    }
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        reopened
            .read_session_control_legacy_recovery(&owner)
            .unwrap(),
        Some(recovery.clone())
    );
    assert!(reopened
        .record_session_control_legacy_recovery(&owner, &recovery, 900)
        .unwrap()
        .is_none());
    assert_eq!(raw_state(&db), saved);
    assert!(reopened
        .record_session_control_legacy_recovery(&owner, &blocked("different-cause"), 901)
        .is_err());
    assert_eq!(raw_state(&db), saved);
}
pub(in crate::tests) fn check_pair_rollbacks(make: fn() -> PairFixture) {
    for trigger in [
        "CREATE TRIGGER refuse_pair BEFORE UPDATE OF record_json ON {table} BEGIN SELECT RAISE(ABORT,'carrier fault'); END;",
        "CREATE TRIGGER refuse_pair BEFORE INSERT ON runtime_events WHEN NEW.kind='session_control.legacy_native_recovery_observed' BEGIN SELECT RAISE(ABORT,'Critical event fault'); END;",
        "CREATE TRIGGER refuse_pair AFTER INSERT ON runtime_events WHEN NEW.kind='session_control.legacy_native_recovery_observed' BEGIN UPDATE runtime_events SET critical=0 WHERE event_id=NEW.event_id; END;",
        "CREATE TRIGGER refuse_pair AFTER UPDATE OF record_json ON {table} WHEN json_type(NEW.record_json,'$.legacy_native_recovery_evidence') IS NOT NULL BEGIN UPDATE {table} SET record_json=json_remove(record_json,'$.legacy_native_recovery_evidence') WHERE {id}=NEW.{id}; END;",
        "CREATE TRIGGER refuse_pair AFTER INSERT ON runtime_events WHEN NEW.kind='session_control.legacy_native_recovery_observed' BEGIN UPDATE {table} SET record_json=json_remove(record_json,'$.legacy_native_recovery_evidence'); END;",
        "CREATE TRIGGER refuse_pair AFTER UPDATE OF record_json ON {table} WHEN NEW.phase='intended' AND json_type(NEW.record_json,'$.legacy_native_recovery_evidence') IS NOT NULL BEGIN UPDATE {table} SET phase='manual_review',record_json=json_set(record_json,'$.phase','manual_review') WHERE {id}=NEW.{id}; END;",
    ] {
        let (_temp, repo, owner) = make(); let db = open_connection(&repo.database_path).unwrap(); let before = raw_state(&db); let (table, id) = layout(&owner); let trigger = trigger.replace("{table}", table).replace("{id}", id);
        db.execute_batch(&trigger).unwrap();
        assert!(repo.record_session_control_legacy_recovery(&owner, &blocked("history"), 110).is_err(), "{trigger}"); assert_eq!(raw_state(&db), before, "{trigger}");
        assert_eq!(repo.read_session_control_legacy_recovery(&owner).unwrap(), None);
        db.execute_batch("DROP TRIGGER refuse_pair").unwrap(); assert!(repo.record_session_control_legacy_recovery(&owner, &blocked("history"), 111).unwrap().is_some());
    }
}
fn assert_unverified_without_repair(
    repo: &SqliteRuntimeRepository,
    db: &Connection,
    owner: &SessionControlCoordinate,
    recovery: &LegacyNativeRecovery,
    context: &str,
) {
    let damaged = raw_state(db);
    let record: Value = serde_json::from_str(&carrier(db, owner).1).unwrap();
    assert_eq!(
        raw_evidence(repo, owner),
        record.get(SLOT).filter(|v| !v.is_null()).cloned()
    );
    assert!(
        repo.read_session_control_legacy_recovery(owner).is_err(),
        "{context}"
    );
    assert!(
        repo.record_session_control_legacy_recovery(owner, recovery, 900)
            .is_err(),
        "{context}"
    );
    assert_eq!(raw_state(db), damaged, "{context}");
}
pub(in crate::tests) fn check_damaged_pairs(make: fn() -> PairFixture) {
    for damage in [
        "DELETE FROM runtime_events WHERE kind='session_control.legacy_native_recovery_observed'",
        "UPDATE {table} SET record_json=json_remove(record_json,'$.legacy_native_recovery_evidence')",
        "UPDATE runtime_events SET critical=0 WHERE kind='session_control.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET created_at=111 WHERE kind='session_control.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET scope_id='foreign-agent' WHERE kind='session_control.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET session_id='foreign-agent' WHERE kind='session_control.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET payload_json=json_set(payload_json,'$.owner.agent_id','foreign-agent') WHERE kind='session_control.legacy_native_recovery_observed'",
        "UPDATE runtime_events SET payload_json=json_set(payload_json,'$.legacy_recovery.coordinate.operation_id','other-history') WHERE kind='session_control.legacy_native_recovery_observed'",
        "UPDATE {table} SET record_json=json_set(record_json,'$.legacy_native_recovery_evidence.observed_at_ms',111)",
        "UPDATE {table} SET record_json=json_set(record_json,'$.agent_id','foreign-agent')",
        "UPDATE {table} SET record_json=json_set(record_json,'$.{id}','foreign-owner')",
        "UPDATE {table} SET record_json=json_set(record_json,'$.phase','completed')",
    ] {
        let (_temp, repo, owner) = make(); let recovery = blocked("history"); repo.record_session_control_legacy_recovery(&owner, &recovery, 110).unwrap();
        let db = open_connection(&repo.database_path).unwrap(); let (table,id) = layout(&owner); let sql = damage.replace("{table}", table).replace("{id}", id); db.execute_batch(&sql).unwrap();
        assert_unverified_without_repair(&repo, &db, &owner, &recovery, &sql);
    }
    for (recovery, path) in [
        (
            blocked("history"),
            "$.legacy_native_recovery_evidence.unverified",
        ),
        (
            blocked("history"),
            "$.legacy_native_recovery_evidence.recovery.unverified",
        ),
        (
            blocked("history"),
            "$.legacy_native_recovery_evidence.recovery.coordinate.unverified",
        ),
        (
            incomplete(),
            "$.legacy_native_recovery_evidence.recovery.incomplete.unverified",
        ),
        (
            incomplete(),
            "$.legacy_native_recovery_evidence.recovery.incomplete.stats.unverified",
        ),
    ] {
        let (_temp, repo, owner) = make();
        repo.record_session_control_legacy_recovery(&owner, &recovery, 110)
            .unwrap();
        let db = open_connection(&repo.database_path).unwrap();
        let (table, id) = layout(&owner);
        db.execute(&format!("UPDATE {table} SET record_json=json_set(record_json,?2,json('true')) WHERE {id}=?1"), params![owner.id(),path]).unwrap();
        assert_unverified_without_repair(&repo, &db, &owner, &recovery, path);
    }
}
pub(in crate::tests) fn check_owner_binding(make: fn() -> PairFixture) {
    let (_temp, repo, owner) = make();
    let db = open_connection(&repo.database_path).unwrap();
    let before = raw_state(&db);
    let (_, id) = layout(&owner);
    let recovery = blocked("history");
    for field in ["agent_id", id] {
        let mut wrong = json!(owner);
        wrong[field] = json!("foreign-owner");
        let wrong: SessionControlCoordinate = serde_json::from_value(wrong).unwrap();
        assert!(repo.read_session_control_legacy_recovery(&wrong).is_err());
        assert!(repo
            .record_session_control_legacy_recovery(&wrong, &recovery, 110)
            .is_err());
        assert_eq!(raw_state(&db), before);
    }
    let wrong = match &owner {
        SessionControlCoordinate::EditRerun {
            agent_id,
            operation_id,
        } => SessionControlCoordinate::Compaction {
            agent_id: agent_id.clone(),
            run_id: operation_id.clone(),
        },
        SessionControlCoordinate::Compaction { agent_id, run_id } => {
            SessionControlCoordinate::EditRerun {
                agent_id: agent_id.clone(),
                operation_id: run_id.clone(),
            }
        }
    };
    assert!(repo.read_session_control_legacy_recovery(&wrong).is_err());
    assert!(repo
        .record_session_control_legacy_recovery(&wrong, &recovery, 110)
        .is_err());
    assert!(repo
        .record_session_control_legacy_recovery(&owner, &recovery, -1)
        .is_err());
    assert!(repo
        .record_session_control_legacy_recovery(&owner, &blocked(&"x".repeat(40_000)), 110)
        .is_err());
    assert_eq!(raw_state(&db), before);
}
#[test]
fn edit_pair_is_immutable_and_survives_reopen() {
    check_pair_replay(fixture);
}
#[test]
fn edit_pair_rolls_back_carrier_and_critical_event_faults() {
    check_pair_rollbacks(fixture);
}
#[test]
fn edit_pair_rejects_orphans_mismatches_and_nested_unknown_fields() {
    check_damaged_pairs(fixture);
}
#[test]
fn edit_pair_requires_exact_family_agent_and_owner() {
    check_owner_binding(fixture);
}
#[test]
fn fresh_edit_cannot_supply_its_own_recovery_observation() {
    let temp = tempfile::tempdir().unwrap();
    let (repo, _, _, mut operation) = signed_edit_revision_fixture(&temp);
    let db = open_connection(&repo.database_path).unwrap();
    let before = raw_state(&db);
    for value in [
        json!({"recovery":blocked("forged"),"observed_at_ms":110}),
        json!({"unverified":true}),
        Value::Null,
    ] {
        operation.legacy_native_recovery_evidence = Some(value);
        assert!(matches!(
            repo.begin_session_edit_rerun(&operation),
            Err(RuntimeError::InvalidState(_))
        ));
        assert_eq!(raw_state(&db), before);
        assert!(repo
            .get_session_edit_rerun(&operation.operation_id)
            .unwrap()
            .is_none());
    }
}
