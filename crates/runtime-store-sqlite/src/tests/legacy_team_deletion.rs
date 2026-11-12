//! SQLite fault, restart and multi-writer proofs for the legacy team tombstone.
use super::legacy_team_messages::{commit, fixture, proposed};
use super::*;
use runtime_core::RuntimeError;
use std::sync::{Arc, Barrier};

fn deletion_plan(
    team: &TeamRecord,
    deliveries: &[TeamDeliveryRecord],
    at: i64,
) -> (TeamRecord, Vec<TeamDeliveryRecord>, Vec<NewRuntimeEvent>) {
    let mut after = team.clone();
    after.updated_at = at;
    after.deleted_at = Some(at);
    let cancelled = deliveries
        .iter()
        .filter(|row| matches!(row.status.as_str(), "pending" | "deferred"))
        .map(|row| {
            let mut after = row.clone();
            after.status = "cancelled".into();
            after.updated_at = at;
            after.injection_strategy = None;
            after.injected_turn_id = None;
            after.last_error_code = None;
            after.last_error_message = None;
            after
        })
        .collect::<Vec<_>>();
    let mut events = deliveries
        .iter()
        .filter(|row| matches!(row.status.as_str(), "pending" | "deferred"))
        .zip(&cancelled)
        .map(|(old, new)| NewRuntimeEvent {
            event_id: format!(
                "evt_legacy_team_delete_{}_delivery_{}_cancelled_{}",
                team.id, old.id, old.updated_at
            ),
            scope: RuntimeEventScope::Team,
            scope_id: team.id.clone(),
            session_id: Some(old.recipient_agent_id.clone()),
            team_id: Some(team.id.clone()),
            turn_id: None,
            kind: "team_delivery.cancelled".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({
                "delivery":new,
                "before_status":old.status,
                "before_updated_at":old.updated_at,
            }),
            provider: None,
            provider_seq: None,
            created_at: at,
        })
        .collect::<Vec<_>>();
    events.push(NewRuntimeEvent {
        event_id: format!("evt_team_{}_deleted", team.id),
        scope: RuntimeEventScope::Team,
        scope_id: team.id.clone(),
        session_id: Some(team.created_by.clone()),
        team_id: Some(team.id.clone()),
        turn_id: None,
        kind: "team.deleted".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"team":after}),
        provider: None,
        provider_seq: None,
        created_at: at,
    });
    (after, cancelled, events)
}

fn deletion(
    repo: &SqliteRuntimeRepository,
    team: &TeamRecord,
    all: &[TeamDeliveryRecord],
    plan: &(TeamRecord, Vec<TeamDeliveryRecord>, Vec<NewRuntimeEvent>),
) -> Result<TeamRecord, RuntimeError> {
    repo.commit_legacy_team_deletion(team, &plan.0, all, &plan.1, &plan.2)
}

#[test]
fn last_critical_tombstone_failure_rolls_back_all_recipient_cancellations_and_cache_projection() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "delete_late_fault",
        &["session_a", "session_b"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let plan = deletion_plan(&team, &admission.1, 1400);
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER reject_last_deletion_event BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team.deleted'
         BEGIN SELECT RAISE(ABORT, 'forced last Critical tombstone fault'); END;",
    )
    .unwrap();
    assert!(deletion(&repo, &team, &admission.1, &plan).is_err());
    let hydrated = repo.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.team_deliveries, admission.1);
    assert_eq!(hydrated.teams, vec![team.clone()]);
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 3);
    conn.execute_batch("DROP TRIGGER reject_last_deletion_event")
        .unwrap();
    assert_eq!(deletion(&repo, &team, &admission.1, &plan).unwrap(), plan.0);
    let restarted = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        deletion(&restarted, &team, &admission.1, &plan).unwrap(),
        plan.0
    );
    assert_eq!(
        deletion(
            &restarted,
            &team,
            &admission.1,
            &deletion_plan(&team, &admission.1, 1600)
        )
        .unwrap(),
        plan.0,
        "lost-reply replay must reuse the persisted timestamp and receipts"
    );
    let saved = restarted.hydrate_runtime_state().unwrap();
    assert_eq!(saved.teams, vec![plan.0.clone()]);
    assert_eq!(saved.team_deliveries, plan.1);
    let events = restarted.list_runtime_events(None, None, 20).unwrap();
    assert_eq!(events.len(), 7);
    assert_eq!(events[3].kind, "team_delivery.cancelled");
    assert_eq!(events[4].kind, "team_delivery.cancelled");
    assert_eq!(events[5].kind, "team_message.completed");
    assert_eq!(events[6].kind, "team.deleted");
    assert_eq!(events[5].payload["deliveries"], serde_json::json!(plan.1));
    // An old upsert cannot resurrect a fully certified tombstone or rewrite
    // a recipient's immutable critical cancellation authority.
    assert!(matches!(
        restarted.upsert_team(&team),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(matches!(
        restarted.upsert_team_delivery(&admission.1[0]),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn early_critical_event_failure_rolls_back_tombstone_and_every_delivery() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(
        &team,
        "delete_early_fault",
        &["session_a", "session_b"],
        None,
    );
    commit(&repo, &admission).unwrap();
    let plan = deletion_plan(&team, &admission.1, 1400);
    let conn = open_connection(&repo.database_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER reject_recipient_deletion BEFORE INSERT ON runtime_events
         WHEN NEW.kind='team_delivery.cancelled'
         BEGIN SELECT RAISE(ABORT, 'forced early recipient Critical failure'); END;",
    )
    .unwrap();
    assert!(deletion(&repo, &team, &admission.1, &plan).is_err());
    assert_eq!(repo.hydrate_runtime_state().unwrap().teams, vec![team]);
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 3);
}

#[test]
fn forged_critical_envelope_and_incomplete_snapshot_fail_closed() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "delete_forged", &["session_a", "session_b"], None);
    commit(&repo, &admission).unwrap();
    let mut plan = deletion_plan(&team, &admission.1, 1400);
    plan.2[0].payload["delivery"]["recipient_agent_id"] = serde_json::json!("intruder");
    assert!(matches!(
        deletion(&repo, &team, &admission.1, &plan),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    plan = deletion_plan(&team, &admission.1, 1400);
    plan.2[2].criticality = RuntimeEventCriticality::Droppable;
    assert!(matches!(
        deletion(&repo, &team, &admission.1, &plan),
        Err(RuntimeError::ProtocolViolation(_))
    ));
    plan = deletion_plan(&team, &admission.1[..1], 1400);
    assert!(matches!(
        deletion(&repo, &team, &admission.1[..1], &plan),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        admission.1
    );
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 3);
}

#[test]
fn unjournaled_pre_native_forgery_or_injecting_generation_cannot_be_deleted() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "delete_stale_native", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let stale = &admission.1[0];
    let plan = deletion_plan(&team, &admission.1, 1400);
    let (injecting, event) = super::legacy_team_delivery_claim::claim_plan(stale, 1200, "native");
    assert!(repo
        .claim_legacy_team_delivery_injection(stale, &injecting, &event)
        .unwrap());
    assert!(matches!(
        deletion(&repo, &team, &admission.1, &plan),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        vec![injecting]
    );

    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "delete_raw_deferred", &["session_a"], None);
    commit(&repo, &admission).unwrap();
    let mut deferred = admission.1[0].clone();
    deferred.status = "deferred".into();
    deferred.updated_at = 1200;
    open_connection(&repo.database_path)
        .unwrap()
        .execute(
            "UPDATE team_deliveries SET status='deferred',updated_at=?1 WHERE id=?2",
            params![deferred.updated_at, deferred.id],
        )
        .unwrap();
    let plan = deletion_plan(&team, &[deferred.clone()], 1400);
    assert!(matches!(
        deletion(&repo, &team, &[deferred.clone()], &plan),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repo.hydrate_runtime_state().unwrap().team_deliveries,
        vec![deferred]
    );
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 2);
}

#[test]
fn competing_deletion_generations_converge_on_one_tombstone_and_one_critical_journal() {
    let (_temp, repo, team) = fixture();
    let admission = proposed(&team, "delete_race", &["session_a", "session_b"], None);
    commit(&repo, &admission).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let handles = [1400, 1500]
        .into_iter()
        .map(|at| {
            let barrier = Arc::clone(&barrier);
            let repo = repo.clone();
            let team = team.clone();
            let all = admission.1.clone();
            std::thread::spawn(move || {
                let plan = deletion_plan(&team, &all, at);
                barrier.wait();
                deletion(&repo, &team, &all, &plan)
            })
        })
        .collect::<Vec<_>>();
    let outcomes = handles
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 2);
    assert_eq!(outcomes[0].as_ref().unwrap(), outcomes[1].as_ref().unwrap());
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 7);
    assert!(repo.hydrate_runtime_state().unwrap().teams[0]
        .deleted_at
        .is_some());
    let mut forged_team = team;
    forged_team.name = "foreign stale writer".into();
    let forged_plan = deletion_plan(&forged_team, &admission.1, 1700);
    assert!(matches!(
        deletion(&repo, &forged_team, &admission.1, &forged_plan),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn many_messages_publish_ordered_complete_critical_receipts_with_exact_restart_replay() {
    let (_temp, repo, team) = fixture();
    let mut all = Vec::new();
    for i in 0..64 {
        let message_id = format!("scale_message_{i:03}");
        let admission = proposed(&team, &message_id, &["session_a", "session_b"], None);
        commit(&repo, &admission).unwrap();
        all.extend(admission.1);
    }
    all.sort_by(|a, b| (&a.message_id, &a.id).cmp(&(&b.message_id, &b.id)));
    let plan = deletion_plan(&team, &all, 1400);
    assert_eq!(plan.1.len(), 128);
    assert_eq!(deletion(&repo, &team, &all, &plan).unwrap(), plan.0);

    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert_eq!(
        deletion(&reopened, &team, &all, &deletion_plan(&team, &all, 1600)).unwrap(),
        plan.0,
        "fresh-clock replay may not fabricate a second terminal snapshot"
    );
    let events = reopened.list_runtime_events(None, None, 512).unwrap();
    assert_eq!(events.len(), 385); // 64*(created+2 pending) + 128 cancelled + 64 completed + deleted
    assert_eq!(
        events.iter().map(|row| row.seq).collect::<Vec<_>>(),
        (1..=385).collect::<Vec<_>>()
    );
    assert!(events[192..320]
        .iter()
        .all(|event| event.kind == "team_delivery.cancelled"));
    assert!(events[320..384]
        .iter()
        .all(|event| event.kind == "team_message.completed"));
    assert_eq!(events.last().unwrap().kind, "team.deleted");
    assert!(events[192..]
        .iter()
        .all(|event| event.criticality == RuntimeEventCriticality::Critical));
    let restarted = reopened.hydrate_runtime_state().unwrap();
    assert_eq!(restarted.team_deliveries.len(), 128);
    assert!(restarted
        .team_deliveries
        .iter()
        .all(|delivery| delivery.status == "cancelled"));
}

#[test]
fn competing_new_message_admission_invalidates_stale_team_deletion_snapshot() {
    let (_temp, repo, team) = fixture();
    let first = proposed(&team, "delete_snapshot_first", &["session_a"], None);
    commit(&repo, &first).unwrap();
    let stale_plan = deletion_plan(&team, &first.1, 1400);
    let second = proposed(&team, "delete_snapshot_new", &["session_b"], None);
    commit(&repo, &second).unwrap();
    assert!(matches!(
        deletion(&repo, &team, &first.1, &stale_plan),
        Err(RuntimeError::Conflict(_))
    ));
    let snapshot = repo.hydrate_runtime_state().unwrap();
    assert_eq!(snapshot.teams, vec![team]);
    assert_eq!(snapshot.team_deliveries, [first.1, second.1].concat());
    assert_eq!(repo.list_runtime_events(None, None, 20).unwrap().len(), 4);
}

#[test]
fn independent_message_admission_and_deletion_race_never_lose_a_recipient_or_event() {
    let (_temp, repo, team) = fixture();
    let first = proposed(&team, "delete_race_first", &["session_a"], None);
    commit(&repo, &first).unwrap();
    let second = proposed(&team, "delete_race_second", &["session_b"], None);
    let barrier = Arc::new(Barrier::new(2));
    let delete_thread = {
        let repo = repo.clone();
        let team = team.clone();
        let first_deliveries = first.1.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            let plan = deletion_plan(&team, &first_deliveries, 1400);
            barrier.wait();
            deletion(&repo, &team, &first_deliveries, &plan)
        })
    };
    let admit_thread = {
        let repo = repo.clone();
        std::thread::spawn(move || {
            barrier.wait();
            commit(&repo, &second)
        })
    };
    let deleted = delete_thread.join().unwrap().is_ok();
    let admitted = admit_thread.join().unwrap().is_ok();
    assert_ne!(deleted, admitted, "one exclusive SQLite writer must win");
    let snapshot = repo.hydrate_runtime_state().unwrap();
    let events = repo.list_runtime_events(None, None, 20).unwrap();
    if deleted {
        assert!(snapshot.teams[0].deleted_at.is_some());
        assert_eq!(snapshot.team_deliveries.len(), 1);
        assert_eq!(snapshot.team_deliveries[0].status, "cancelled");
        assert_eq!(events.len(), 5);
        assert_eq!(events.last().unwrap().kind, "team.deleted");
    } else {
        assert!(snapshot.teams[0].deleted_at.is_none());
        assert_eq!(snapshot.team_deliveries.len(), 2);
        assert!(snapshot
            .team_deliveries
            .iter()
            .all(|row| row.status == "pending"));
        assert_eq!(events.len(), 4);
        assert!(events.iter().all(|row| row.kind != "team.deleted"));
    }
}
