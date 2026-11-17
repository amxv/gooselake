//! Real SQLite authority/event boundaries for edit-and-rerun.
use super::signed_edit_revision_fixture;
use crate::db::{open_connection, JournalConnection};
use crate::repository_turn_terminal::read_session;
use crate::tests::legacy_native_turn_guards::raw_state;
use crate::SqliteRuntimeRepository;
use runtime_core::{
    EditRerunTransitionReceipt, NewRuntimeEvent, ProviderSession, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionEditRerunOperation, SessionRecord,
    TurnAdmissionRecord, TurnCorrelationState, TurnDispatchPolicySnapshot, TurnDispatchState,
    TurnInputProjectionSource, TurnRecord,
};
use rusqlite::params;
use serde_json::json;

pub(super) struct Fixture {
    pub temp: tempfile::TempDir,
    pub repo: SqliteRuntimeRepository,
    pub operation: SessionEditRerunOperation,
}

pub(super) fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let (repo, mut session, _, operation) = signed_edit_revision_fixture(&temp);
    session.metadata = json!({
        "policy_marker":{"keep":"exact"},
        "codex_transcript":[{"role":"user","text":"superseded history"}]
    });
    repo.upsert_session(&session).unwrap();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "INSERT INTO session_context_limit_snapshots
         (session_id,provider,provider_session_ref,canonical_provider_session_ref,
          agent_revision,model_context_window,last_total_tokens,remaining_percentage,
          observed_at_ms,observed_turn_id) VALUES(?1,'codex',?2,NULL,0,1000,100,90,103,?3)",
        params![
            session.id,
            session.provider_session_ref,
            operation.target_turn_id
        ],
    )
    .unwrap();
    Fixture {
        temp,
        repo,
        operation,
    }
}

pub(super) fn database(f: &Fixture) -> JournalConnection {
    open_connection(&f.temp.path().join("runtime.sqlite3")).unwrap()
}

pub(super) fn child(f: &Fixture) -> ProviderSession {
    ProviderSession {
        runtime_session_id: f.operation.agent_id.clone(),
        provider_session_ref: "verified-n04-child".into(),
        canonical_provider_session_ref: Some("canonical-n04-child".into()),
    }
}

pub(super) fn begin(f: &Fixture) -> EditRerunTransitionReceipt {
    f.repo
        .begin_session_edit_rerun_with_receipt(&f.operation)
        .unwrap()
}

pub(super) fn native_started(f: &Fixture) -> EditRerunTransitionReceipt {
    begin(f);
    f.repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "native_started",
            None,
            None,
            None,
            110,
        )
        .unwrap()
}

pub(super) fn fork(f: &Fixture) -> EditRerunTransitionReceipt {
    native_started(f);
    f.repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "fork_observed",
            Some(&child(f)),
            None,
            None,
            120,
        )
        .unwrap()
}

/// Capture the actual child attachment before admission; the submitted record
/// is the new admitted projection, exactly as the real turn writer expects.
pub(super) fn pending_bundle(
    f: &Fixture,
    turn_id: &str,
    correlated: bool,
) -> (SessionRecord, TurnRecord, TurnAdmissionRecord) {
    let db = database(f);
    let mut session = read_session(&db, &f.operation.agent_id).unwrap();
    let at = session.updated_at.max(200) + 1;
    session.status = "turn_admitted".into();
    session.active_turn_id = Some(turn_id.into());
    session.updated_at = at;
    let turn = TurnRecord {
        id: turn_id.into(),
        session_id: session.id.clone(),
        provider_turn_ref: None,
        status: "admitted".into(),
        input: json!([{"type":"text","text":f.operation.edited_snapshot.prompt_text}]),
        source: Some("user".into()),
        started_at: Some(at),
        completed_at: None,
        usage: None,
        error: None,
    };
    let admission = TurnAdmissionRecord {
        turn_id: turn_id.into(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: TurnInputProjectionSource::UserVisible,
        user_input_snapshot: f.operation.edited_snapshot.clone(),
        dispatch_policy: TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: TurnCorrelationState {
            expected_turn_id: None,
            correlation_id: correlated.then(|| f.operation.operation_id.clone()),
        },
        dispatch_state: TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: at,
        updated_at: at,
    };
    (session, turn, admission)
}

pub(super) fn replacement(f: &Fixture, turn_id: &str) -> TurnAdmissionRecord {
    let (session, turn, admission) = pending_bundle(f, turn_id, true);
    f.repo
        .admit_turn(&admission, &turn, &session, None)
        .unwrap();
    admission
}

fn apply(
    f: &Fixture,
    boundary: &str,
    now: i64,
) -> Result<EditRerunTransitionReceipt, RuntimeError> {
    match boundary {
        "admitted" => f.repo.begin_session_edit_rerun_with_receipt(&f.operation),
        "reviewed" => f.repo.resolve_session_edit_rerun_review_with_receipt(
            &f.operation.operation_id,
            "verified known child has no replacement",
            now,
        ),
        "fork_observed" => f.repo.advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            boundary,
            Some(&child(f)),
            None,
            None,
            now,
        ),
        "completed" => f.repo.advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            boundary,
            None,
            Some("n04-replacement"),
            None,
            now,
        ),
        other => f.repo.advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            other,
            None,
            None,
            matches!(other, "manual_review" | "failed").then_some("fixture-held"),
            now,
        ),
    }
}

fn prepare_boundary(f: &Fixture, boundary: &str) {
    match boundary {
        "admitted" => {}
        "native_started" | "failed" => {
            begin(f);
        }
        "fork_observed" => {
            native_started(f);
        }
        "completed" => {
            fork(f);
            replacement(f, "n04-replacement");
        }
        "manual_review" => {
            fork(f);
        }
        "reviewed" => {
            fork(f);
            apply(f, "manual_review", 300).unwrap();
        }
        _ => panic!("unknown fixture boundary"),
    }
}

fn assert_changed(f: &Fixture, receipt: &EditRerunTransitionReceipt, boundary: &str) {
    assert!(receipt.changed);
    let event = receipt.journal_event.as_ref().expect("canonical receipt");
    assert_eq!(event.kind, format!("turn.edit_{boundary}"));
    assert_eq!(event.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(event.scope, RuntimeEventScope::Session);
    assert_eq!(event.scope_id, f.operation.agent_id);
    assert_eq!(
        event.session_id.as_deref(),
        Some(f.operation.agent_id.as_str())
    );
    assert_eq!(
        event.turn_id.as_deref(),
        Some(f.operation.target_turn_id.as_str())
    );
    assert_eq!(event.payload["operation_id"], f.operation.operation_id);
    assert_eq!(event.payload["phase"], receipt.operation.phase);
    assert_eq!(event.created_at, receipt.operation.updated_at_ms);
    assert_eq!(
        f.repo
            .get_session_edit_rerun(&f.operation.operation_id)
            .unwrap(),
        Some(receipt.operation.clone())
    );
    if boundary == "fork_observed" {
        assert_eq!(
            receipt.committed_session.as_ref(),
            Some(&read_session(&database(f), &f.operation.agent_id).unwrap())
        );
    } else {
        assert!(receipt.committed_session.is_none());
    }
}

#[test]
fn every_edit_boundary_rolls_back_its_critical_failure_and_replays_without_writing() {
    for boundary in [
        "admitted",
        "native_started",
        "fork_observed",
        "completed",
        "manual_review",
        "failed",
        "reviewed",
    ] {
        let f = fixture();
        prepare_boundary(&f, boundary);
        let db = database(&f);
        db.execute_batch(&format!(
            "CREATE TRIGGER n04_receipt_fault BEFORE INSERT ON runtime_events
             WHEN NEW.kind='turn.edit_{boundary}'
             BEGIN SELECT RAISE(ABORT,'injected exact edit receipt failure'); END;"
        ))
        .unwrap();
        let before = raw_state(&db);
        assert!(apply(&f, boundary, 500).is_err(), "{boundary}");
        assert_eq!(
            raw_state(&db),
            before,
            "authority/event rollback: {boundary}"
        );
        db.execute_batch("DROP TRIGGER n04_receipt_fault").unwrap();

        let prior_sequence: i64 = db
            .query_row(
                "SELECT COALESCE(MAX(seq),0) FROM runtime_events
                 WHERE scope='session' AND scope_id=?1",
                [&f.operation.agent_id],
                |row| row.get(0),
            )
            .unwrap();
        let committed = apply(&f, boundary, 500).unwrap();
        assert_changed(&f, &committed, boundary);
        assert_eq!(
            committed.journal_event.as_ref().unwrap().seq,
            prior_sequence.checked_add(1).unwrap(),
            "exact allocated sequence: {boundary}"
        );
        let saved = raw_state(&db);
        let replay = apply(&f, boundary, 900).unwrap();
        assert!(!replay.changed, "{boundary}");
        assert!(replay.journal_event.is_none());
        assert!(replay.committed_session.is_none());
        assert_eq!(replay.operation, committed.operation);
        assert_eq!(raw_state(&db), saved, "replay wrote: {boundary}");
    }
}

#[test]
fn two_handles_obtain_only_one_admission_and_native_start_execution_grant() {
    let f = fixture();
    let other = SqliteRuntimeRepository::new(f.temp.path().join("runtime.sqlite3"));
    let admitted = begin(&f);
    assert!(admitted.changed);
    let mut competing = f.operation.clone();
    competing.operation_id = "other-proposed-owner".into();
    let replay = other
        .begin_session_edit_rerun_with_receipt(&competing)
        .unwrap();
    assert!(!replay.changed);
    assert_eq!(replay.operation, admitted.operation);
    assert!(replay.journal_event.is_none());
    let first = f
        .repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "native_started",
            None,
            None,
            None,
            110,
        )
        .unwrap();
    assert!(first.changed);
    let saved = raw_state(&database(&f));
    let second = other
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "native_started",
            None,
            None,
            None,
            900,
        )
        .unwrap();
    assert!(!second.changed);
    assert_eq!(second.operation, first.operation);
    assert!(second.journal_event.is_none());
    assert_eq!(raw_state(&database(&f)), saved);
    assert!(other
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "native_started",
            None,
            None,
            Some("changed"),
            901,
        )
        .is_err());
    assert_eq!(raw_state(&database(&f)), saved);
}

#[test]
fn fork_receipt_rejects_ignored_damaged_or_colliding_events_and_changed_carriers() {
    for fault in [
        "ignore",
        "critical",
        "payload",
        "created_at",
        "sequence",
        "carrier",
        "session",
        "occupied",
    ] {
        let f = fixture();
        native_started(&f);
        let db = database(&f);
        if fault == "occupied" {
            let owner = runtime_core::SessionControlCoordinate::EditRerun {
                agent_id: f.operation.agent_id.clone(),
                operation_id: f.operation.operation_id.clone(),
            };
            let event_id = format!("evt_edit_state_{}", runtime_core::normalized_json_hash(
                &json!({"owner":owner,"phase":"fork_observed"}),
            ).unwrap());
            f.repo
                .append_runtime_event(&NewRuntimeEvent {
                    event_id,
                    scope: RuntimeEventScope::Session,
                    scope_id: f.operation.agent_id.clone(),
                    session_id: Some(f.operation.agent_id.clone()),
                    team_id: None,
                    turn_id: Some(f.operation.target_turn_id.clone()),
                    kind: "occupied".into(),
                    criticality: RuntimeEventCriticality::Critical,
                    payload: json!({}),
                    provider: None,
                    provider_seq: None,
                    created_at: 115,
                })
                .unwrap();
        } else {
            let (when, action) = match fault {
                "ignore" => ("BEFORE", "SELECT RAISE(IGNORE);"),
                "critical" => ("AFTER", "UPDATE runtime_events SET critical=0 WHERE event_id=NEW.event_id;"),
                "payload" => ("AFTER", "UPDATE runtime_events SET payload_json='{}' WHERE event_id=NEW.event_id;"),
                "created_at" => ("AFTER", "UPDATE runtime_events SET created_at=created_at+1 WHERE event_id=NEW.event_id;"),
                "sequence" => ("AFTER", "UPDATE runtime_events SET seq=seq+1 WHERE event_id=NEW.event_id;"),
                "carrier" => ("AFTER", "UPDATE session_edit_reruns SET phase='manual_review',record_json=json_set(record_json,'$.phase','manual_review') WHERE operation_id=json_extract(NEW.payload_json,'$.operation_id');"),
                "session" => ("AFTER", "UPDATE sessions SET metadata_json=json_set(metadata_json,'$.injected_drift',true) WHERE id=NEW.session_id;"),
                _ => unreachable!(),
            };
            db.execute_batch(&format!(
                "CREATE TRIGGER n04_damaged_receipt {when} INSERT ON runtime_events
                 WHEN NEW.kind='turn.edit_fork_observed' BEGIN {action} END;"
            ))
            .unwrap();
        }
        let before = raw_state(&db);
        assert!(apply(&f, "fork_observed", 120).is_err(), "{fault}");
        assert_eq!(raw_state(&db), before, "partial fork escaped: {fault}");
    }
}

#[test]
fn review_replay_retains_known_child_and_evidence_after_later_session_authority() {
    let f = fixture();
    fork(&f);
    apply(&f, "manual_review", 300).unwrap();
    let reviewed = apply(&f, "reviewed", 400).unwrap();
    assert_changed(&f, &reviewed, "reviewed");
    let db = database(&f);
    let mut later = read_session(&db, &f.operation.agent_id).unwrap();
    later.status = "closed".into();
    later.closed_at = Some(600);
    later.updated_at = 600;
    later.provider_session_ref = Some("later-attachment".into());
    f.repo.upsert_session(&later).unwrap();
    let before = raw_state(&db);
    let replay = apply(&f, "reviewed", 900).unwrap();
    assert!(!replay.changed);
    assert_eq!(replay.operation, reviewed.operation);
    assert_eq!(raw_state(&db), before);
    assert!(f
        .repo
        .resolve_session_edit_rerun_review_with_receipt(
            &f.operation.operation_id,
            "different independently written evidence",
            901,
        )
        .is_err());
    assert_eq!(raw_state(&db), before);
}

#[test]
fn unknown_child_review_and_historical_native_start_replay_never_invent_receipts() {
    let f = fixture();
    native_started(&f);
    let db = database(&f);
    db.execute(
        "DELETE FROM runtime_events WHERE kind='turn.edit_native_started'",
        [],
    )
    .unwrap();
    let before = raw_state(&db);
    let replay = f
        .repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "native_started",
            None,
            None,
            None,
            500,
        )
        .unwrap();
    assert!(!replay.changed);
    assert!(replay.journal_event.is_none());
    assert_eq!(raw_state(&db), before);
    let recovered = f
        .repo
        .recover_session_edit_reruns_with_receipts(600)
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].operation.phase, "manual_review");
    assert_eq!(
        recovered[0].journal_event.as_ref().unwrap().payload["origin"],
        "startup"
    );
    let held = raw_state(&db);
    assert!(f
        .repo
        .resolve_session_edit_rerun_review_with_receipt(
            &f.operation.operation_id,
            "a prose assertion cannot establish a native child",
            700,
        )
        .is_err());
    assert_eq!(raw_state(&db), held);
    assert!(f
        .repo
        .recover_session_edit_reruns_with_receipts(800)
        .unwrap()
        .is_empty());
    assert_eq!(raw_state(&db), held);
}

#[test]
fn same_phase_child_and_replacement_replays_reject_contradictory_evidence() {
    let f = fixture();
    let forked = fork(&f);
    let db = database(&f);
    let saved = raw_state(&db);
    let mut wrong = child(&f);
    wrong.canonical_provider_session_ref = Some("different-canonical".into());
    assert!(f
        .repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "fork_observed",
            Some(&wrong),
            None,
            None,
            500,
        )
        .is_err());
    assert_eq!(raw_state(&db), saved);
    let replay = apply(&f, "fork_observed", 501).unwrap();
    assert!(!replay.changed);
    assert_eq!(replay.operation, forked.operation);
    replacement(&f, "n04-replacement");
    let completed = apply(&f, "completed", 600).unwrap();
    let saved = raw_state(&db);
    assert!(f
        .repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "completed",
            None,
            Some("other-replacement"),
            None,
            700,
        )
        .is_err());
    assert!(f
        .repo
        .advance_session_edit_rerun_with_receipt(
            &f.operation.operation_id,
            "completed",
            Some(&child(&f)),
            Some("n04-replacement"),
            None,
            700,
        )
        .is_err());
    let replay = apply(&f, "completed", 701).unwrap();
    assert!(!replay.changed);
    assert_eq!(replay.operation, completed.operation);
    assert_eq!(raw_state(&db), saved);
}

#[test]
fn fork_receipt_invalid_or_exhausted_sequence_rolls_back_all_authority() {
    for prior_sequence in [-1_i64, i64::MAX] {
        let f = fixture();
        native_started(&f);
        let db = database(&f);
        // Deliberately damaged/exhausted session event history in a disposable
        // database. The native-start operation remains an existing known row;
        // this fixture does not infer or backfill any missing historical event.
        db.execute(
            "DELETE FROM runtime_events WHERE scope='session' AND scope_id=?1",
            [&f.operation.agent_id],
        )
        .unwrap();
        db.execute(
            "INSERT INTO runtime_events
             (event_id,scope,scope_id,session_id,seq,kind,critical,payload_json,created_at)
             VALUES('n04_sequence_sentinel','session',?1,?1,?2,
                    'fixture.edit_sequence',1,'{}',115)",
            params![f.operation.agent_id, prior_sequence],
        )
        .unwrap();
        let before = raw_state(&db);
        let error = apply(&f, "fork_observed", 120).unwrap_err();
        assert!(
            matches!(error, RuntimeError::Conflict(ref message)
                if message.contains("edit transition event sequence")),
            "{prior_sequence}: {error}"
        );
        assert_eq!(
            raw_state(&db),
            before,
            "sequence refusal must roll back fork session, revision and lineage: {prior_sequence}"
        );
    }
}
