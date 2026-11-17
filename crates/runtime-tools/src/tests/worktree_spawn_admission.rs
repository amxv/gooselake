use super::worktree_spawn_recovery::fixture;
use super::*;
use runtime_core::{TeamCommsService, WorktreeService};

fn keyed_request(
    fixture: &super::worktree_spawn_recovery::SpawnFixture,
    key: &str,
) -> runtime_core::TeamMemberSpawnRequest {
    let mut request = fixture.request();
    request.metadata = None;
    request.idempotency_key = Some(key.into());
    request
}

fn onboarding_stage_payload(response: &runtime_core::TeamMemberSpawnResponse) -> serde_json::Value {
    serde_json::json!({
        "source_session_id": response.spawned_member.added_by,
        "spawned_session_id": response.spawned_session.id,
        "provider": response.spawned_session.provider,
        "provider_session_ref": response.spawned_session.provider_session_ref,
        "canonical_provider_session_ref": response.spawned_session.canonical_provider_session_ref,
        "worktree_id": response.worktree.as_ref().map(|row| &row.id),
        "message_id": response.onboarding["message_id"],
        "delivery_ids": response.onboarding["delivery_ids"],
    })
}

#[tokio::test]
async fn exact_keyed_completion_replays_original_receipt_without_native_work() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "caller-roundtrip-1");
    let native_before = fixture
        .provider
        .effect_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    let first = fixture.completed_history(&request, None);
    assert_eq!(first.journal_stage, "completed");
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "completed");
    assert_eq!(journal.operation_id, first.operation_id);
    let first_bytes = serde_json::to_vec(&first).unwrap();

    // An exact replay of independently seeded, validated history returns the
    // original team snapshot and session, never a newly hydrated projection.
    let same = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap();
    assert_eq!(serde_json::to_vec(&same).unwrap(), first_bytes);
    let new_service = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    let after_restart = new_service
        .spawn_team_member(request.clone())
        .await
        .unwrap();
    assert_eq!(serde_json::to_vec(&after_restart).unwrap(), first_bytes);
    let state = fixture.store.hydrate_runtime_state().unwrap();
    assert_eq!(state.sessions.len(), 2);
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .team_members
            .len(),
        2
    );
    assert_eq!(
        fixture
            .store
            .list_team_operation_journal(Some(&fixture.team_id))
            .unwrap()
            .len(),
        1
    );

    let mut altered = request;
    altered.prompt = Some("different role without a new key".into());
    let error = fixture
        .service
        .spawn_team_member(altered)
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Conflict(_)), "{error:?}");
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        2
    );
    assert_eq!(
        fixture
            .provider
            .effect_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        native_before
    );
}

#[tokio::test]
async fn completed_key_cannot_bind_a_foreign_member_message_or_unverified_native_stage() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "original-native-receipt-authority");
    let seed = fixture.onboarding_history(&request, None);
    let first = seed.response;
    let original_id = &first.operation_id;
    let message_id = first.onboarding["message_id"].as_str().unwrap();
    let delivery_id = first.onboarding["delivery_ids"][0].as_str().unwrap();
    let stage_payload = onboarding_stage_payload(&first);
    // The fixture has never held a completed response. Each fault is checked
    // before the first real DB-only completion transaction.
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: fixture.database_path.clone(),
    });

    let mut forgeries = Vec::new();
    let mut foreign_source = first.clone();
    foreign_source.spawned_member.added_by = "different_original_caller".into();
    forgeries.push(foreign_source);
    let mut foreign_session = first.clone();
    foreign_session.spawned_session.id = first.spawned_member.added_by.clone();
    foreign_session.spawned_member.agent_id = foreign_session.spawned_session.id.clone();
    forgeries.push(foreign_session);
    let mut foreign_member = first.clone();
    foreign_member
        .team
        .members
        .retain(|member| member.agent_id != first.spawned_session.id);
    forgeries.push(foreign_member);
    let mut foreign_message = first.clone();
    foreign_message.onboarding["message_id"] = serde_json::json!("other_message");
    forgeries.push(foreign_message);
    let mut foreign_delivery = first.clone();
    foreign_delivery.onboarding["delivery_ids"] = serde_json::json!(["other_delivery"]);
    forgeries.push(foreign_delivery);
    let mut forged_native_ref = first.clone();
    forged_native_ref.spawned_session.provider_session_ref = Some("foreign-native-session".into());
    forgeries.push(forged_native_ref);
    let mut forged_member_metadata = first.clone();
    forged_member_metadata.spawned_member.joined_at += 1;
    let stored_member = forged_member_metadata
        .team
        .members
        .iter_mut()
        .find(|member| member.agent_id == first.spawned_session.id)
        .unwrap();
    stored_member.joined_at += 1;
    forgeries.push(forged_member_metadata);
    let mut false_worktree = first.clone();
    false_worktree.spawned_member.worktree_id = Some("unclaimed_worktree".into());
    forgeries.push(false_worktree);
    for forged in &forgeries {
        assert!(matches!(
            reopened.complete_team_member_spawn(original_id, forged, 2000),
            Err(RuntimeError::Conflict(_) | RuntimeError::InvalidState(_))
        ));
        assert_eq!(fixture.fresh_journal().stage, "onboarding_sent");
        let terminal: Option<String> = connection
            .query_row(
                "SELECT completed_response_json FROM team_spawn_admissions WHERE operation_id=?1",
                [original_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(terminal.is_none(), "forgery must never certify replay");
    }

    let mut swapped_stage = stage_payload.clone();
    swapped_stage["source_session_id"] = serde_json::json!("other_source");
    connection
        .execute(
            "UPDATE team_operation_journal SET payload_json=?2 WHERE operation_id=?1",
            rusqlite::params![original_id, swapped_stage.to_string()],
        )
        .unwrap();
    assert!(matches!(
        reopened.complete_team_member_spawn(original_id, &first, 2000),
        Err(RuntimeError::Conflict(_))
    ));
    connection
        .execute(
            "UPDATE team_operation_journal SET payload_json=?2 WHERE operation_id=?1",
            rusqlite::params![original_id, stage_payload.to_string()],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE team_messages SET sender_agent_id='other_source' WHERE id=?1",
            [message_id],
        )
        .unwrap();
    assert!(matches!(
        reopened.complete_team_member_spawn(original_id, &first, 2000),
        Err(RuntimeError::Conflict(_))
    ));
    connection
        .execute(
            "UPDATE team_messages SET sender_agent_id=?2 WHERE id=?1",
            rusqlite::params![message_id, first.spawned_member.added_by],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE team_deliveries SET recipient_agent_id='other_recipient' WHERE id=?1",
            [delivery_id],
        )
        .unwrap();
    assert!(matches!(
        reopened.complete_team_member_spawn(original_id, &first, 2000),
        Err(RuntimeError::Conflict(_))
    ));
    connection
        .execute(
            "UPDATE team_deliveries SET recipient_agent_id=?2 WHERE id=?1",
            rusqlite::params![delivery_id, first.spawned_session.id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE sessions SET provider_session_ref='foreign-native-ref' WHERE id=?1",
            [&first.spawned_session.id],
        )
        .unwrap();
    assert!(matches!(
        reopened.complete_team_member_spawn(original_id, &first, 2000),
        Err(RuntimeError::Conflict(_))
    ));
    connection
        .execute(
            "UPDATE sessions SET provider_session_ref=?2 WHERE id=?1",
            rusqlite::params![
                first.spawned_session.id,
                first.spawned_session.provider_session_ref
            ],
        )
        .unwrap();

    reopened
        .complete_team_member_spawn(original_id, &first, 2000)
        .unwrap();
    let replay = reopened
        .replay_team_member_spawn(
            &request.team_id,
            &request.source_session_id,
            request.idempotency_key.as_deref().unwrap(),
            &runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap(),
        )
        .unwrap();
    let runtime_core::TeamMemberSpawnAdmission::Replay { response } = replay.unwrap() else {
        panic!("expected exact completed response")
    };
    assert_eq!(
        serde_json::to_value(response).unwrap(),
        serde_json::to_value(&first).unwrap()
    );
    assert_eq!(fixture.fresh_journal().stage, "completed");
    let native_sessions = fixture.provider.state.lock().await;
    assert_eq!(native_sessions.sessions.len(), 1); // only the real inert-provider caller
    assert!(
        native_sessions
            .sessions
            .get(&first.spawned_session.id)
            .is_none(),
        "historical database seeding never dispatches a native create"
    );
}

#[tokio::test]
async fn independent_workers_refuse_fresh_shared_calls_and_store_admission_has_one_winner() {
    let fixture = fixture().await;
    let other = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    let request = keyed_request(&fixture, "race-20-workers");
    let before = fixture.store.hydrate_runtime_state().unwrap();
    let mut workers = tokio::task::JoinSet::new();
    for i in 0..20 {
        let service = if i % 2 == 0 {
            fixture.service.clone()
        } else {
            other.clone()
        };
        let request = request.clone();
        workers.spawn(async move { service.spawn_team_member(request).await });
    }
    while let Some(result) = workers.join_next().await {
        assert!(matches!(result.unwrap(), Err(RuntimeError::Unsupported(_))));
    }
    assert_eq!(fixture.store.hydrate_runtime_state().unwrap(), before);
    // The retained DB-only import/settlement authority still admits one
    // original coordinate across genuinely independent SQLite connections.
    let hash =
        runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap();
    let mut threads = Vec::new();
    for i in 0..12 {
        let path = fixture.database_path.clone();
        let request = request.clone();
        let hash = hash.clone();
        threads.push(std::thread::spawn(move || {
            SqliteRuntimeStore::new(SqliteStoreConfig {
                database_path: path,
            })
            .admit_team_member_spawn(&format!("op_spawn_race_{i}"), &request, &hash, 1000)
            .unwrap()
        }));
    }
    let outcomes = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, runtime_core::TeamMemberSpawnAdmission::Execute { .. }))
            .count(),
        1
    );
    let original = fixture.fresh_journal().operation_id;
    for outcome in outcomes {
        match outcome {
            runtime_core::TeamMemberSpawnAdmission::Execute { operation_id }
            | runtime_core::TeamMemberSpawnAdmission::Unresolved { operation_id } => {
                assert_eq!(operation_id, original)
            }
            other => panic!("unexpected history admission {other:?}"),
        }
    }
    fixture
        .assert_unresolved_without_effects(request, &original)
        .await;
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        1
    );
}

#[tokio::test]
async fn failed_terminal_receipt_quarantines_original_key_across_restart() {
    let fixture = fixture().await;
    rusqlite::Connection::open(&fixture.database_path).unwrap().execute_batch(
        "CREATE TRIGGER deny_completed_spawn BEFORE UPDATE ON team_operation_journal
         WHEN NEW.stage='completed' BEGIN SELECT RAISE(ABORT,'simulated terminal receipt loss'); END;"
    ).unwrap();
    let request = keyed_request(&fixture, "lost-final-ack");
    let seed = fixture.onboarding_history(&request, None);
    assert!(seed.complete_seed(fixture.store.as_ref(), 2000).is_err());
    let first = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap_err();
    let first_id = match first {
        RuntimeError::OperationUnresolved { operation_id, .. } => operation_id,
        other => panic!("expected quarantined native success: {other:?}"),
    };
    assert_eq!(fixture.fresh_journal().stage, "onboarding_sent");
    let second_service = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    let retry = second_service.spawn_team_member(request).await.unwrap_err();
    match retry {
        RuntimeError::OperationUnresolved { operation_id, .. } => {
            assert_eq!(operation_id, first_id)
        }
        other => panic!("native outcome must remain quarantined: {other:?}"),
    }
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        2
    );
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .team_members
            .len(),
        2
    );
    assert_eq!(fixture.fresh_journal().stage, "onboarding_sent");
}

#[tokio::test]
async fn ignored_final_response_write_never_certifies_native_success() {
    let fixture = fixture().await;
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    // SQLite can report a successful UPDATE with no affected row if a
    // trigger suppresses it. The completed journal must roll back too.
    connection
        .execute_batch(
            "CREATE TRIGGER ignore_spawn_response BEFORE UPDATE OF completed_response_json
         ON team_spawn_admissions WHEN NEW.completed_response_json IS NOT NULL
         BEGIN SELECT RAISE(IGNORE); END;",
        )
        .unwrap();
    let request = keyed_request(&fixture, "ignore-receipt-write");
    let seed = fixture.onboarding_history(&request, None);
    assert!(seed.complete_seed(fixture.store.as_ref(), 2000).is_err());
    let first = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap_err();
    let operation_id = fixture.fresh_journal().operation_id;
    assert!(matches!(
        first,
        RuntimeError::OperationUnresolved { operation_id: id, .. } if id == operation_id
    ));
    assert_eq!(fixture.fresh_journal().stage, "onboarding_sent");
    assert!(matches!(
        fixture.service.spawn_team_member(request).await,
        Err(RuntimeError::OperationUnresolved { operation_id: id, .. }) if id == operation_id
    ));
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}

#[tokio::test]
async fn original_onboarding_completion_rejects_extra_deliveries_at_bounded_fanout() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "original-bounded-onboarding");
    let seed = fixture.onboarding_history(&request, None);
    let first = seed.response;
    let operation_id = &first.operation_id;
    let message_id = first.onboarding["message_id"].as_str().unwrap();
    let delivery_id = first.onboarding["delivery_ids"][0].as_str().unwrap();
    let mut connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    let transaction = connection.transaction().unwrap();
    // Synthetic corrupt historical fan-out: never contact the native
    // provider/Git again just because one terminal response was lost.
    for i in 0..2_048 {
        transaction
            .execute(
                "INSERT INTO team_deliveries (
                    id,message_id,team_id,recipient_agent_id,provider,status,created_at,updated_at
                 ) SELECT ?1,message_id,team_id,recipient_agent_id,provider,status,
                          created_at,updated_at FROM team_deliveries WHERE id=?2",
                rusqlite::params![format!("extra_spawn_delivery_{i}"), delivery_id],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT id,team_id,recipient_agent_id
             FROM team_deliveries WHERE message_id=?1 LIMIT 2",
            [message_id],
            |row| row.get(3),
        )
        .unwrap();
    assert!(plan.contains("idx_team_deliveries_by_message"), "{plan}");
    assert!(matches!(
        fixture
            .store
            .complete_team_member_spawn(operation_id, &first, 2000),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(fixture.fresh_journal().stage, "onboarding_sent");
    let native_count = fixture.provider.state.lock().await.sessions.len();
    assert_eq!(native_count, 1); // only the original inert-provider caller
    connection
        .execute(
            "DELETE FROM team_deliveries WHERE id LIKE 'extra_spawn_delivery_%'",
            [],
        )
        .unwrap();
    fixture
        .store
        .complete_team_member_spawn(operation_id, &first, 2000)
        .unwrap();
    assert_eq!(fixture.fresh_journal().stage, "completed");
    assert!(matches!(
        fixture
            .store
            .replay_team_member_spawn(
                &request.team_id,
                &request.source_session_id,
                request.idempotency_key.as_deref().unwrap(),
                &runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap())
                    .unwrap(),
            )
            .unwrap(),
        Some(runtime_core::TeamMemberSpawnAdmission::Replay { .. })
    ));
}

#[tokio::test]
async fn admission_sql_fault_rolls_back_planned_stage_without_native_session() {
    let fixture = fixture().await;
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER deny_spawn_key BEFORE INSERT ON team_spawn_admissions
        BEGIN SELECT RAISE(ABORT,'simulated keyed admission failure'); END;",
        )
        .unwrap();
    let request = keyed_request(&fixture, "fault-before-native");
    let hash =
        runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap();
    let before = fixture.store.hydrate_runtime_state().unwrap();
    assert!(fixture
        .store
        .admit_team_member_spawn("op_spawn_admission_sql_fault", &request, &hash, 1000)
        .is_err());
    assert_eq!(fixture.store.hydrate_runtime_state().unwrap(), before);
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM team_spawn_admissions", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    connection
        .execute_batch("DROP TRIGGER deny_spawn_key;")
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .admit_team_member_spawn("op_spawn_admission_sql_fault", &request, &hash, 1000)
            .unwrap(),
        runtime_core::TeamMemberSpawnAdmission::Execute { .. }
    ));
    assert_eq!(fixture.fresh_journal().stage, "planned");
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        1
    );
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}

#[tokio::test]
async fn weak_rolled_back_history_retains_original_key_and_recovery_coordinate() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "rolled-back-still-consumed");
    let seed = fixture.onboarding_history(&request, None);
    fixture.stage_history(
        &seed.response.operation_id,
        "rolled_back",
        json!({"spawned_session_id":seed.response.spawned_session.id}),
    );
    fixture
        .assert_unresolved_without_effects(request.clone(), &seed.response.operation_id)
        .await;
    let other = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    let before = fixture.store.hydrate_runtime_state().unwrap();
    let error = other.spawn_team_member(request).await.unwrap_err();
    assert!(
        matches!(error,RuntimeError::OperationUnresolved{operation_id,..} if operation_id==seed.response.operation_id)
    );
    assert_eq!(fixture.store.hydrate_runtime_state().unwrap(), before);
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}

#[tokio::test]
async fn forged_terminal_stage_without_original_success_response_cannot_replay() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "forged-completion-without-native");
    let hash =
        runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap();
    let operation_id = "op_spawn_disposable_forged";
    assert!(matches!(
        fixture
            .store
            .admit_team_member_spawn(operation_id, &request, &hash, 123)
            .unwrap(),
        runtime_core::TeamMemberSpawnAdmission::Execute { .. },
    ));
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    connection
        .execute(
            "UPDATE team_operation_journal SET stage='completed' WHERE operation_id=?1",
            [operation_id],
        )
        .unwrap();
    let retry = fixture
        .service
        .spawn_team_member(request)
        .await
        .unwrap_err();
    assert!(
        matches!(retry, RuntimeError::OperationUnresolved { operation_id: id, .. }
        if id == operation_id)
    );
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        1
    );
}

#[tokio::test]
async fn caller_scoped_key_never_replays_to_a_different_source_session() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "caller-scope-key");
    let first = fixture.completed_history(&request, None);
    let mut foreign = request;
    foreign.source_session_id = "foreign-source-session".into();
    let error = fixture
        .service
        .spawn_team_member(foreign)
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Unsupported(_)), "{error:?}");
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        2
    );
    assert_eq!(fixture.fresh_journal().operation_id, first.operation_id);
}

#[tokio::test]
async fn stale_team_cache_cannot_bypass_store_admission_after_sqlite_deletion() {
    let fixture = fixture().await;
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    connection
        .execute(
            "UPDATE teams SET deleted_at=999 WHERE id=?1",
            [&fixture.team_id],
        )
        .unwrap();
    let request = keyed_request(&fixture, "deleted-team-not-dispatched");
    let hash =
        runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap();
    assert!(matches!(
        fixture.service.spawn_team_member(request.clone()).await,
        Err(RuntimeError::Unsupported(_))
    ));
    let error = fixture
        .store
        .admit_team_member_spawn("op_spawn_deleted_team", &request, &hash, 1000)
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Conflict(_)), "{error:?}");
    assert!(fixture
        .store
        .list_team_operation_journal(Some(&fixture.team_id))
        .unwrap()
        .is_empty());
    connection
        .execute(
            "UPDATE teams SET deleted_at=NULL WHERE id=?1",
            [&fixture.team_id],
        )
        .unwrap();
    assert!(matches!(
        fixture.service.spawn_team_member(request.clone()).await,
        Err(RuntimeError::Unsupported(_))
    ));
    assert!(matches!(
        fixture
            .store
            .admit_team_member_spawn("op_spawn_deleted_team", &request, &hash, 1000)
            .unwrap(),
        runtime_core::TeamMemberSpawnAdmission::Execute { .. }
    ));
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        1
    );
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}

#[tokio::test]
async fn legacy_delete_service_keeps_cache_and_critical_tombstone_untouched_during_keyed_spawn() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "delete-after-admit-service");
    let hash =
        runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap();
    let operation_id = "op_service_deletion_interlock";
    assert!(matches!(
        fixture
            .store
            .admit_team_member_spawn(operation_id, &request, &hash, 1234)
            .unwrap(),
        runtime_core::TeamMemberSpawnAdmission::Execute { .. }
    ));
    let failure = fixture
        .comms
        .delete_team(&fixture.team_id)
        .await
        .unwrap_err();
    assert!(matches!(failure, RuntimeError::Conflict(ref msg) if msg.contains(operation_id)));
    assert_eq!(
        fixture
            .comms
            .get_team(&fixture.team_id)
            .await
            .unwrap()
            .members
            .len(),
        1
    );
    assert!(fixture.store.hydrate_runtime_state().unwrap().teams[0]
        .deleted_at
        .is_none());
    assert!(fixture
        .store
        .list_runtime_events(None, None, 10)
        .unwrap()
        .iter()
        .all(|event| event.kind != "team.deleted"));
    assert_eq!(fixture.fresh_journal().stage, "planned");

    // This is the existing logical deletion interlock, not native clearance.
    // Its historical terminal label may allow a Critical tombstone while the
    // original spawn key still remains unresolved and cannot execute again.
    let mut journal = fixture.fresh_journal();
    journal.stage = "rolled_back".into();
    fixture
        .store
        .upsert_team_operation_journal(&journal)
        .unwrap();
    fixture.comms.delete_team(&fixture.team_id).await.unwrap();
    assert!(matches!(
        fixture.comms.get_team(&fixture.team_id).await,
        Err(RuntimeError::NotFound(_))
    ));
    assert_eq!(
        fixture.store.hydrate_runtime_state().unwrap().teams[0]
            .deleted_at
            .is_some(),
        true
    );
    assert_eq!(
        fixture
            .store
            .list_runtime_events(None, None, 10)
            .unwrap()
            .iter()
            .filter(|event| event.kind == "team.deleted")
            .count(),
        1
    );
    fixture
        .assert_unresolved_without_effects(request, operation_id)
        .await;
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}
