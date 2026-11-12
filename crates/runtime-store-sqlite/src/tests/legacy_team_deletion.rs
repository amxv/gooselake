//! SQLite fault, restart and multi-writer proofs for the legacy team tombstone.
use super::legacy_team_messages::{commit, fixture, proposed};
use super::*;
use runtime_core::RuntimeError;
use rusqlite::OptionalExtension;
use std::sync::{Arc, Barrier};

fn keyed_spawn(team: &TeamRecord, key: &str) -> runtime_core::TeamMemberSpawnRequest {
    runtime_core::TeamMemberSpawnRequest {
        team_id: team.id.clone(),
        source_session_id: "session_lead".into(),
        idempotency_key: Some(key.into()),
        provider: None,
        model: None,
        title: None,
        prompt: None,
        permission_mode: None,
        metadata: None,
        worktree: None,
        creator_agent_id: None,
        creator_compaction_subscription: None,
    }
}

fn admit_keyed_spawn(
    repo: &SqliteRuntimeRepository,
    team: &TeamRecord,
    operation_id: &str,
) -> Result<runtime_core::TeamMemberSpawnAdmission, RuntimeError> {
    let request = keyed_spawn(team, operation_id);
    let hash = runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap())?;
    repo.admit_team_member_spawn(operation_id, &request, &hash, 1100)
}

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

#[test]
fn admitted_keyed_spawn_blocks_deletion_until_certified_rollback_even_after_restart() {
    let (_temp, repo, team) = fixture();
    let operation_id = "op_spawn_deletion_in_flight";
    assert!(matches!(
        admit_keyed_spawn(&repo, &team, operation_id).unwrap(),
        runtime_core::TeamMemberSpawnAdmission::Execute { .. }
    ));
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    let plan = deletion_plan(&team, &[], 1400);
    for stage in [
        "planned",
        "worktree_created",
        "session_created",
        "team_joined",
        "rollback_pending",
        "rollback_review_required",
    ] {
        let mut row = reopened
            .get_team_operation_journal(operation_id)
            .unwrap()
            .unwrap();
        row.stage = stage.into();
        reopened.upsert_team_operation_journal(&row).unwrap();
        let error = deletion(&reopened, &team, &[], &plan).unwrap_err();
        assert!(
            matches!(error, RuntimeError::Conflict(ref msg) if msg.contains(operation_id)),
            "{stage}: {error:?}"
        );
        assert!(reopened.hydrate_runtime_state().unwrap().teams[0]
            .deleted_at
            .is_none());
        assert!(reopened
            .list_runtime_events(None, None, 10)
            .unwrap()
            .is_empty());
    }
    // A forged terminal stage without the atomic exact response cannot turn
    // an unknown native outcome into deletion authority.
    let mut row = reopened
        .get_team_operation_journal(operation_id)
        .unwrap()
        .unwrap();
    row.stage = "completed".into();
    reopened.upsert_team_operation_journal(&row).unwrap();
    assert!(matches!(
        deletion(&reopened, &team, &[], &plan),
        Err(RuntimeError::Conflict(_))
    ));

    // This fixture created no native resources. Mimic a confirmed full
    // compensation verdict; unknown or partial rollback above stayed fenced.
    row.stage = "rolled_back".into();
    reopened.upsert_team_operation_journal(&row).unwrap();
    assert_eq!(deletion(&reopened, &team, &[], &plan).unwrap(), plan.0);
    assert_eq!(
        reopened.list_runtime_events(None, None, 10).unwrap().len(),
        1
    );
    let new_request = keyed_spawn(&team, "new-after-delete");
    let new_hash =
        runtime_core::normalized_json_hash(&serde_json::to_value(&new_request).unwrap()).unwrap();
    assert!(matches!(
        reopened.admit_team_member_spawn("op_spawn_after_delete", &new_request, &new_hash, 1500),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn old_unkeyed_native_plan_blocks_delete_and_cannot_be_created_after_tombstone() {
    let (_temp, repo, team) = fixture();
    let mut journal = runtime_core::TeamOperationJournalRecord {
        operation_id: "op_unkeyed_original".into(),
        team_id: team.id.clone(),
        kind: "spawn_member_with_worktree".into(),
        stage: "planned".into(),
        payload: serde_json::json!({"source_session_id":"session_lead"}),
        created_at: 1100,
        updated_at: 1100,
    };
    repo.upsert_team_operation_journal(&journal).unwrap();
    let plan = deletion_plan(&team, &[], 1400);
    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    assert!(matches!(
        deletion(&reopened, &team, &[], &plan),
        Err(RuntimeError::Conflict(ref msg)) if msg.contains("op_unkeyed_original")
    ));
    journal.stage = "session_create_review_required".into();
    reopened.upsert_team_operation_journal(&journal).unwrap();
    assert!(matches!(
        deletion(&reopened, &team, &[], &plan),
        Err(RuntimeError::Conflict(_))
    ));
    journal.stage = "rolled_back".into();
    reopened.upsert_team_operation_journal(&journal).unwrap();
    assert_eq!(deletion(&reopened, &team, &[], &plan).unwrap(), plan.0);
    let mut newer = journal;
    newer.operation_id = "op_unkeyed_after_delete".into();
    newer.stage = "planned".into();
    assert!(matches!(
        reopened.upsert_team_operation_journal(&newer),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(reopened
        .get_team_operation_journal(&newer.operation_id)
        .unwrap()
        .is_none());
}

#[test]
fn independent_keyed_spawn_admission_and_team_deletion_have_exactly_one_sqlite_winner() {
    for round in 0..8 {
        let (_temp, repo, team) = fixture();
        let barrier = Arc::new(Barrier::new(2));
        let delete_thread = {
            let repo = repo.clone();
            let team = team.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let plan = deletion_plan(&team, &[], 1400);
                barrier.wait();
                deletion(&repo, &team, &[], &plan)
            })
        };
        let admit_thread = {
            let repo = repo.clone();
            let team = team.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                admit_keyed_spawn(&repo, &team, &format!("op_race_{round}"))
            })
        };
        let deleted = delete_thread.join().unwrap().is_ok();
        let admitted = admit_thread.join().unwrap().is_ok();
        assert_ne!(
            deleted, admitted,
            "round {round}: two native/deletion owners cannot win"
        );
        let state = repo.hydrate_runtime_state().unwrap();
        let original_id = format!("op_race_{round}");
        if admitted {
            assert!(state.teams[0].deleted_at.is_none());
            assert_eq!(
                repo.get_team_operation_journal(&original_id)
                    .unwrap()
                    .unwrap()
                    .stage,
                "planned"
            );
            assert!(repo.list_runtime_events(None, None, 10).unwrap().is_empty());
        } else {
            assert!(state.teams[0].deleted_at.is_some());
            assert!(repo
                .get_team_operation_journal(&original_id)
                .unwrap()
                .is_none());
            assert_eq!(repo.list_runtime_events(None, None, 10).unwrap().len(), 1);
        }
    }
}

#[test]
fn schema_30_upgrade_indexes_sparse_unsettled_spawn_deletion_fence_at_scale() {
    let (_temp, repo, team) = fixture();
    let mut connection = open_connection(&repo.database_path).unwrap();
    // A disposable v30 fixture with historical terminal rows. Migration 31
    // may only add the sparse index, without rewriting past native receipts.
    connection
        .execute_batch(
            "DROP INDEX idx_team_spawn_journal_unsettled;
             DELETE FROM schema_migrations WHERE version=31;",
        )
        .unwrap();
    let tx = connection.transaction().unwrap();
    for i in 0..2_000 {
        tx.execute(
            "INSERT INTO team_operation_journal
             (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
             VALUES (?1,?2,'spawn_member_with_worktree','completed','{}',?3,?3)",
            params![format!("old_spawn_{i}"), &team.id, i],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    drop(connection);
    let upgraded = SqliteRuntimeRepository::new(repo.database_path.clone());
    upgraded.initialize_schema().unwrap();
    let connection = open_connection(&repo.database_path).unwrap();
    let plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT j.operation_id
             FROM team_operation_journal j
             WHERE j.team_id=?1 AND j.kind='spawn_member_with_worktree'
               AND j.stage NOT IN ('completed','rolled_back')
               AND NOT EXISTS (SELECT 1 FROM team_spawn_admissions a
                               WHERE a.operation_id=j.operation_id) LIMIT 1",
            [&team.id],
            |row| row.get(3),
        )
        .unwrap();
    assert!(plan.contains("idx_team_spawn_journal_unsettled"), "{plan}");
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM team_operation_journal", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        2_000,
        "v31 index upgrade may not discard any older worktree operation"
    );
    drop(connection);
    let mut unsettled = runtime_core::TeamOperationJournalRecord {
        operation_id: "scale_unkeyed_pending".into(),
        team_id: team.id.clone(),
        kind: "spawn_member_with_worktree".into(),
        stage: "planned".into(),
        payload: serde_json::json!({"source_session_id":"session_lead"}),
        created_at: 1200,
        updated_at: 1200,
    };
    upgraded.upsert_team_operation_journal(&unsettled).unwrap();
    assert!(matches!(
        deletion(&upgraded, &team, &[], &deletion_plan(&team, &[], 1400)),
        Err(RuntimeError::Conflict(ref msg)) if msg.contains("scale_unkeyed_pending")
    ));
    unsettled.stage = "rolled_back".into();
    upgraded.upsert_team_operation_journal(&unsettled).unwrap();
    let plan = deletion_plan(&team, &[], 1400);
    assert_eq!(deletion(&upgraded, &team, &[], &plan).unwrap(), plan.0);
}

#[test]
fn sparse_spawn_deletion_fence_migrates_historical_rows_without_losing_corrupt_authority() {
    let (_temp, repo, team) = fixture();
    let mut connection = open_connection(&repo.database_path).unwrap();
    // Reconstruct a disposable v32 database with a large history before the
    // first v33 backfill. No original SQLite database or native effect is used.
    connection
        .execute_batch(
            "DROP INDEX idx_managed_worktrees_repo_name_id;
             DROP TRIGGER trg_spawn_deletion_fence_journal_insert;
             DROP TRIGGER trg_spawn_deletion_fence_journal_update;
             DROP TRIGGER trg_spawn_deletion_fence_journal_delete;
             DROP TRIGGER trg_spawn_deletion_fence_admission_insert;
             DROP TRIGGER trg_spawn_deletion_fence_admission_update;
             DROP TRIGGER trg_spawn_deletion_fence_admission_delete;
             DROP VIEW team_spawn_deletion_fence_candidates;
             DROP TABLE team_spawn_deletion_fence;
             DELETE FROM schema_migrations WHERE version IN (33,34);",
        )
        .unwrap();
    let tx = connection.transaction().unwrap();
    for number in 0..2_000_i64 {
        let operation_id = format!("terminal_{number:04}");
        tx.execute(
            "INSERT INTO team_operation_journal
             (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
             VALUES (?1,?2,'spawn_member_with_worktree','completed','{}',?3,?3)",
            params![operation_id, &team.id, number],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO team_spawn_admissions
             (team_id,source_session_id,idempotency_key,request_hash,operation_id,
              completed_response_json,created_at,updated_at)
             VALUES (?1,'session_lead',?2,'old_hash',?2,'{}',?3,?3)",
            params![&team.id, operation_id, number],
        )
        .unwrap();
    }
    for (operation_id, journal_kind, stage, response) in [
        ("missing_original_journal", None, None, None),
        (
            "completed_without_receipt",
            Some("spawn_member_with_worktree"),
            Some("completed"),
            None,
        ),
        (
            "foreign_kind_journal",
            Some("unrelated_kind"),
            Some("completed"),
            Some("{}"),
        ),
    ] {
        tx.execute(
            "INSERT INTO team_spawn_admissions
             (team_id,source_session_id,idempotency_key,request_hash,operation_id,
              completed_response_json,created_at,updated_at)
             VALUES (?1,'session_lead',?2,'hash',?2,?3,2200,2200)",
            params![&team.id, operation_id, response],
        )
        .unwrap();
        if let (Some(kind), Some(stage)) = (journal_kind, stage) {
            tx.execute(
                "INSERT INTO team_operation_journal
                 (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,'{}',2200,2200)",
                params![operation_id, &team.id, kind, stage],
            )
            .unwrap();
        }
    }
    tx.execute(
        "INSERT INTO team_operation_journal
         (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
         VALUES ('unfinished_unkeyed',?1,'spawn_member_with_worktree','planned','{}',2200,2200)",
        [&team.id],
    )
    .unwrap();
    tx.commit().unwrap();
    drop(connection);

    let reopened = SqliteRuntimeRepository::new(repo.database_path.clone());
    reopened.initialize_schema().unwrap();
    let connection = open_connection(&repo.database_path).unwrap();
    let indexed = connection
        .prepare(
            "SELECT operation_id,keyed FROM team_spawn_deletion_fence
             WHERE team_id=?1 ORDER BY keyed DESC,operation_id",
        )
        .unwrap()
        .query_map([&team.id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        indexed,
        vec![
            ("completed_without_receipt".into(), 1),
            ("foreign_kind_journal".into(), 1),
            ("missing_original_journal".into(), 1),
            ("unfinished_unkeyed".into(), 0),
        ]
    );
    let plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT operation_id FROM team_spawn_deletion_fence
             WHERE team_id=?1 AND keyed=1 LIMIT 1",
            [&team.id],
            |row| row.get(3),
        )
        .unwrap();
    assert!(
        plan.contains("idx_team_spawn_deletion_fence_team_keyed"),
        "{plan}"
    );
    assert!(matches!(
        deletion(&reopened, &team, &[], &deletion_plan(&team, &[], 3000)),
        Err(RuntimeError::Conflict(ref error)) if error.contains("completed_without_receipt")
    ));
    drop(connection);
    reopened.initialize_schema().unwrap();
    let restarted = open_connection(&repo.database_path).unwrap();
    let total: i64 = restarted
        .query_row(
            "SELECT COUNT(*) FROM team_spawn_deletion_fence",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        total, 4,
        "repeated startup must preserve exact sparse fence"
    );
}

#[test]
fn sparse_spawn_deletion_fence_tracks_journal_corruption_and_failed_updates_atomically() {
    let (_temp, repo, team) = fixture();
    let id = "keyed_sparse_fence";
    admit_keyed_spawn(&repo, &team, id).unwrap();
    let connection = open_connection(&repo.database_path).unwrap();
    let read_fence = || -> Option<i64> {
        connection
            .query_row(
                "SELECT keyed FROM team_spawn_deletion_fence WHERE operation_id=?1",
                [id],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
    };
    assert_eq!(read_fence(), Some(1));
    let current = deletion_plan(&team, &[], 3000);
    assert!(matches!(
        deletion(&repo, &team, &[], &current),
        Err(RuntimeError::Conflict(ref error)) if error.contains(id)
    ));
    // A forged completed stage without the matching original response stays
    // fenced. Injected journal defects must never silently clear admission.
    connection
        .execute(
            "UPDATE team_operation_journal SET stage='completed' WHERE operation_id=?1",
            [id],
        )
        .unwrap();
    assert_eq!(read_fence(), Some(1));
    connection
        .execute_batch(
            "CREATE TRIGGER reject_sparse_fence_update
             BEFORE DELETE ON team_spawn_deletion_fence
             BEGIN SELECT RAISE(ABORT,'injected index maintenance failure'); END;",
        )
        .unwrap();
    assert!(connection
        .execute(
            "UPDATE team_operation_journal SET stage='rolled_back' WHERE operation_id=?1",
            [id],
        )
        .is_err());
    let journal_stage: String = connection
        .query_row(
            "SELECT stage FROM team_operation_journal WHERE operation_id=?1",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(journal_stage, "completed");
    assert_eq!(read_fence(), Some(1));
    connection
        .execute_batch("DROP TRIGGER reject_sparse_fence_update")
        .unwrap();
    connection
        .execute(
            "DELETE FROM team_operation_journal WHERE operation_id=?1",
            [id],
        )
        .unwrap();
    assert_eq!(read_fence(), Some(1), "missing journal is not clearance");
    connection
        .execute(
            "INSERT INTO team_operation_journal
             (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
             VALUES (?1,?2,'wrong_kind','rolled_back','{}',1100,1100)",
            params![id, &team.id],
        )
        .unwrap();
    assert_eq!(read_fence(), Some(1), "foreign kind is not clearance");
    connection
        .execute(
            "UPDATE team_operation_journal SET kind='spawn_member_with_worktree'
             WHERE operation_id=?1",
            [id],
        )
        .unwrap();
    assert_eq!(
        read_fence(),
        None,
        "only matching rolled-back journal clears"
    );
    assert_eq!(deletion(&repo, &team, &[], &current).unwrap(), current.0);
}
