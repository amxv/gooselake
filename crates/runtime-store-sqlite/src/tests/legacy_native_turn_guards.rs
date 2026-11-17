//! Durable first-dispatch and admission oracles; all databases are disposable.
use super::*;
use runtime_core::{
    PersistedUserInputSnapshot, TurnAdmissionRecord, TurnCorrelationState,
    TurnDispatchPolicySnapshot, TurnDispatchState, TurnInputProjectionSource,
};
use std::collections::BTreeMap;

pub(super) const LEGACY_ID: &str = "arbitrary/original:legacy?%coordinate";

fn bundle() -> (
    SessionRecord,
    SessionRecord,
    TurnRecord,
    TurnAdmissionRecord,
) {
    let mut ready = sample_session();
    ready.status = "ready".into();
    ready.active_turn_id = None;
    ready.worktree_id = None;
    let mut turn = sample_turn();
    turn.status = "admitted".into();
    turn.provider_turn_ref = None;
    let mut admitted = ready.clone();
    admitted.status = "turn_admitted".into();
    admitted.active_turn_id = Some(turn.id.clone());
    admitted.updated_at = 102;
    let admission = TurnAdmissionRecord {
        turn_id: turn.id.clone(),
        session_id: ready.id.clone(),
        provider: ready.provider.clone(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: PersistedUserInputSnapshot::from_input(turn.input.as_array().unwrap()),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState::default(),
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 102,
        updated_at: 102,
    };
    (ready, admitted, turn, admission)
}

pub(super) fn legacy(db: &Connection, stage: &str, payload: &str, admission_only: bool) {
    if admission_only {
        db.execute(
            "INSERT INTO team_spawn_admissions
             (team_id,source_session_id,idempotency_key,request_hash,operation_id,
              completed_response_json,created_at,updated_at)
             VALUES('original-team','source','original-key','original-hash',?1,NULL,1,2)",
            [LEGACY_ID],
        )
        .unwrap();
    } else {
        db.execute(
            "INSERT INTO team_operation_journal
             (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
             VALUES(?1,'original-team','spawn_member_with_worktree',?2,?3,1,2)",
            params![LEGACY_ID, stage, payload],
        )
        .unwrap();
    }
}

pub(super) fn raw_state(db: &Connection) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
    let tables = db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            assert!(table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
            let mut query = db.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let columns = query.column_count();
            let mut rows = query
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| row.get(index))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            rows.sort_by_cached_key(|row| format!("{row:?}"));
            (table, rows)
        })
        .collect()
}

#[test]
fn late_legacy_history_blocks_fresh_turn_admission_without_any_authority_write() {
    for (stage, payload, admission_only) in [
        (
            "rolled_back",
            r#"{"spawned_session_id":"session_1"}"#,
            false,
        ),
        (
            "planned",
            r#"{"source_session_id":"unrelated-source"}"#,
            false,
        ),
        ("completed", "{broken", false),
        ("absent", "{}", true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let (ready, admitted, turn, admission) = bundle();
        repository.upsert_session(&ready).unwrap();
        // Independent handle appends after the caller's healthy snapshot.
        let independent = open_connection(&repository.database_path).unwrap();
        legacy(&independent, stage, payload, admission_only);
        let before = raw_state(&independent);
        let error = repository
            .admit_turn(&admission, &turn, &admitted, None)
            .expect_err("historical native uncertainty must fence a fresh turn");
        assert!(error.to_string().contains(LEGACY_ID), "{error}");
        assert_eq!(
            raw_state(&independent),
            before,
            "failed admission wrote authority"
        );
    }
}

#[test]
fn late_legacy_history_blocks_pending_first_dispatch_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let (ready, admitted, turn, pending) = bundle();
    repository.upsert_session(&ready).unwrap();
    repository
        .admit_turn(&pending, &turn, &admitted, None)
        .unwrap();
    let independent = open_connection(&repository.database_path).unwrap();
    legacy(
        &independent,
        "rolled_back",
        r#"{"spawned_session_id":"session_1"}"#,
        false,
    );
    let before = raw_state(&independent);
    let mut started = pending.clone();
    started.dispatch_state = TurnDispatchState::Dispatching;
    started.updated_at += 1;
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    let error = reopened
        .upsert_turn_admission(&started)
        .expect_err("a prior pending admission does not bypass the first native fence");
    assert!(error.to_string().contains(LEGACY_ID), "{error}");
    assert_eq!(reopened.list_turn_admissions().unwrap(), vec![pending]);
    assert_eq!(raw_state(&independent), before);
}

#[test]
fn two_independent_pending_dispatch_claims_have_exactly_one_winner() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let (ready, admitted, turn, pending) = bundle();
    repository.upsert_session(&ready).unwrap();
    repository
        .admit_turn(&pending, &turn, &admitted, None)
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = (0..2)
        .map(|index| {
            let mut started = pending.clone();
            started.dispatch_state = TurnDispatchState::Dispatching;
            started.updated_at += index + 1;
            let path = repository.database_path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let independent = SqliteRuntimeRepository::new(path);
                barrier.wait();
                independent.upsert_turn_admission(&started).is_ok()
            })
        })
        .collect::<Vec<_>>();
    let winners = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(|won| *won)
        .count();
    assert_eq!(
        winners, 1,
        "a repeated dispatching write cannot grant a second native attempt"
    );
    let actual = repository.list_turn_admissions().unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].dispatch_state, TurnDispatchState::Dispatching);
    assert_eq!(actual[0].session_id, ready.id);
}

#[test]
fn first_dispatch_cannot_substitute_the_persisted_target_or_input() {
    for change in [
        "session",
        "provider",
        "input",
        "policy",
        "correlation",
        "native",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let repository = repo(&temp);
        repository.initialize_schema().unwrap();
        let (ready, admitted, turn, pending) = bundle();
        repository.upsert_session(&ready).unwrap();
        repository
            .admit_turn(&pending, &turn, &admitted, None)
            .unwrap();
        let independent = open_connection(&repository.database_path).unwrap();
        let before = raw_state(&independent);
        let mut changed = pending.clone();
        changed.dispatch_state = TurnDispatchState::Dispatching;
        changed.updated_at += 1;
        match change {
            "session" => changed.session_id = "clear-other-session".into(),
            "provider" => changed.provider = "claude".into(),
            "input" => {
                changed.user_input_snapshot = PersistedUserInputSnapshot::from_input(&[
                    serde_json::json!({"type":"text","text":"substituted"}),
                ])
            }
            "policy" => changed.dispatch_policy.permission_mode = Some("full_auto".into()),
            "correlation" => changed.correlation.correlation_id = Some("other-correlation".into()),
            "native" => changed.provider_native_turn_id = Some("already-observed-native".into()),
            _ => unreachable!(),
        }
        assert!(
            repository.upsert_turn_admission(&changed).is_err(),
            "accepted {change} substitution"
        );
        assert_eq!(raw_state(&independent), before, "{change}");
    }
}

#[test]
fn positively_disjoint_session_remains_eligible_despite_weak_terminal_history() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let (ready, admitted, turn, mut admission) = bundle();
    repository.upsert_session(&ready).unwrap();
    let independent = open_connection(&repository.database_path).unwrap();
    legacy(
        &independent,
        "rolled_back",
        r#"{"spawned_session_id":"different-child"}"#,
        false,
    );
    repository
        .admit_turn(&admission, &turn, &admitted, None)
        .unwrap();
    admission.dispatch_state = TurnDispatchState::Dispatching;
    admission.updated_at += 1;
    repository.upsert_turn_admission(&admission).unwrap();
    let stage: String = independent
        .query_row(
            "SELECT stage FROM team_operation_journal WHERE operation_id=?1",
            [LEGACY_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stage, "rolled_back",
        "disjointness must not rewrite or certify historical outcome"
    );
}

#[test]
fn known_dispatch_outcome_settlement_remains_eligible_after_later_legacy_history() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let (ready, admitted, turn, mut admission) = bundle();
    repository.upsert_session(&ready).unwrap();
    repository
        .admit_turn(&admission, &turn, &admitted, None)
        .unwrap();
    admission.dispatch_state = TurnDispatchState::Dispatching;
    admission.updated_at += 1;
    repository.upsert_turn_admission(&admission).unwrap();
    let independent = open_connection(&repository.database_path).unwrap();
    legacy(
        &independent,
        "rolled_back",
        r#"{"spawned_session_id":"session_1"}"#,
        false,
    );
    admission.dispatch_state = TurnDispatchState::Dispatched;
    admission.provider_native_turn_id = Some("exact-observed-native-turn".into());
    admission.updated_at += 1;
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    reopened.upsert_turn_admission(&admission).unwrap();
    assert_eq!(reopened.list_turn_admissions().unwrap(), vec![admission]);
}
