use super::worktree_spawn_recovery::{fixture, SpawnFixture};
use super::*;
use runtime_core::{TeamMemberSpawnRequest, TeamMemberSpawnResponse, WorktreeService};

fn request(fixture: &SpawnFixture) -> TeamMemberSpawnRequest {
    let mut request = fixture.request();
    request.metadata = None;
    request.idempotency_key = Some("original-historical-inspection".into());
    request
}

async fn completed(
    fixture: &SpawnFixture,
    request: &TeamMemberSpawnRequest,
) -> TeamMemberSpawnResponse {
    // Explicit disposable history setup through the old internal service.
    // Public mutation paths never receive this execution authority.
    fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap()
}

fn inspect(
    fixture: &SpawnFixture,
    service: &dyn WorktreeService,
    request: &TeamMemberSpawnRequest,
) -> Result<TeamMemberSpawnResponse, RuntimeError> {
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    let counts = || {
        [
            "runtime_events",
            "team_messages",
            "team_deliveries",
            "sessions",
            "team_spawn_admissions",
            "team_operation_journal",
        ]
        .map(|table| {
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap()
        })
    };
    let before = counts();
    let calls = fixture
        .provider
        .effect_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    // This is a synchronous inspection interval: no test yield or background
    // onboarding can be mistaken for an effect initiated by the inspector.
    let result = service.inspect_legacy_team_member_spawn(request);
    assert_eq!(
        fixture
            .provider
            .effect_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        calls
    );
    assert_eq!(
        counts(),
        before,
        "historical inspection must not write authority or events"
    );
    result
}

fn assert_original_unresolved(error: RuntimeError, original: &str) {
    match error {
        RuntimeError::OperationUnresolved {
            operation_id,
            message,
        } => {
            assert_eq!(operation_id, original);
            assert!(
                message.contains("native retry is not authorized"),
                "{message}"
            );
        }
        error => panic!("expected original uncertain coordinate, got {error:?}"),
    }
}

#[tokio::test]
async fn legacy_spawn_inspection_replays_saved_snapshot_after_mutable_authority_disappears() {
    let fixture = fixture().await;
    let request = request(&fixture);
    let first = completed(&fixture, &request).await;
    let expected = serde_json::to_value(&first).unwrap();
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    connection
        .execute(
            "UPDATE sessions SET status='closed',cwd='/missing/historical/source' WHERE id=?1",
            [&request.source_session_id],
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM team_members WHERE team_id=?1",
            [&fixture.team_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE teams SET deleted_at=999,name='changed after completion' WHERE id=?1",
            [&fixture.team_id],
        )
        .unwrap();
    let before_events: i64 = connection
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |r| r.get(0))
        .unwrap();
    let before_sessions = fixture.provider.state.lock().await.sessions.clone();
    let reopened = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: fixture.database_path.clone(),
    }));
    let mut config = fixture.service.config.clone();
    config.enabled = false;
    config.root_dir = "/missing/historical/managed-root".into();
    let inspector = RuntimeWorktreeService::new(
        reopened,
        fixture.runtime.clone(),
        fixture.comms.clone(),
        config,
    )
    .unwrap();
    for _ in 0..2 {
        let replay = inspect(&fixture, inspector.as_ref(), &request).unwrap();
        assert_eq!(serde_json::to_value(replay).unwrap(), expected);
    }
    assert_eq!(
        fixture.provider.state.lock().await.sessions,
        before_sessions
    );
    let after_events: i64 = connection
        .query_row("SELECT COUNT(*) FROM runtime_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(after_events, before_events);
    assert_eq!(fixture.fresh_journal().operation_id, first.operation_id);
}

#[tokio::test]
async fn legacy_spawn_inspection_damage_retains_original_coordinate_and_never_repairs() {
    let fixture = fixture().await;
    let request = request(&fixture);
    let first = completed(&fixture, &request).await;
    let original = &first.operation_id;
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    let receipt = serde_json::to_string(&first).unwrap();
    let before_sessions = fixture.provider.state.lock().await.sessions.clone();
    let mut wrong_operation = serde_json::to_value(&first).unwrap();
    wrong_operation["operation_id"] = json!("foreign-operation");
    let mut wrong_team = serde_json::to_value(&first).unwrap();
    wrong_team["team"]["team"]["id"] = json!("foreign-team");
    let mut wrong_stage = serde_json::to_value(&first).unwrap();
    wrong_stage["journal_stage"] = json!("planned");
    for damaged in [
        None,
        Some("{".into()),
        Some("{}".into()),
        Some(wrong_operation.to_string()),
        Some(wrong_team.to_string()),
        Some(wrong_stage.to_string()),
    ] {
        connection
            .execute(
                "UPDATE team_spawn_admissions SET completed_response_json=?2 WHERE operation_id=?1",
                rusqlite::params![original, damaged],
            )
            .unwrap();
        assert_original_unresolved(
            inspect(&fixture, fixture.service.as_ref(), &request).unwrap_err(),
            original,
        );
        let after: Option<String> = connection
            .query_row(
                "SELECT completed_response_json FROM team_spawn_admissions WHERE operation_id=?1",
                [original],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            after, damaged,
            "inspection must not repair damaged evidence"
        );
    }
    connection
        .execute(
            "UPDATE team_spawn_admissions SET completed_response_json=?2 WHERE operation_id=?1",
            rusqlite::params![original, receipt],
        )
        .unwrap();
    for stage in [
        "planned",
        "session_create_review_required",
        "onboarding_sent",
        "rolled_back",
        "rollback_pending",
        "unknown-future-stage",
    ] {
        connection
            .execute(
                "UPDATE team_operation_journal SET stage=?2 WHERE operation_id=?1",
                rusqlite::params![original, stage],
            )
            .unwrap();
        assert_original_unresolved(
            inspect(&fixture, fixture.service.as_ref(), &request).unwrap_err(),
            original,
        );
        assert_eq!(fixture.fresh_journal().stage, stage);
    }
    connection.execute("UPDATE team_operation_journal SET stage='completed',kind='foreign-kind' WHERE operation_id=?1", [original]).unwrap();
    assert_original_unresolved(
        inspect(&fixture, fixture.service.as_ref(), &request).unwrap_err(),
        original,
    );
    connection.execute("UPDATE team_operation_journal SET kind='spawn_member_with_worktree',team_id='foreign-journal-team' WHERE operation_id=?1", [original]).unwrap();
    assert_original_unresolved(
        inspect(&fixture, fixture.service.as_ref(), &request).unwrap_err(),
        original,
    );
    connection
        .execute(
            "DELETE FROM team_operation_journal WHERE operation_id=?1",
            [original],
        )
        .unwrap();
    assert_original_unresolved(
        inspect(&fixture, fixture.service.as_ref(), &request).unwrap_err(),
        original,
    );
    assert_eq!(
        fixture.provider.state.lock().await.sessions,
        before_sessions
    );
    let admissions: i64 = connection
        .query_row("SELECT COUNT(*) FROM team_spawn_admissions", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(admissions, 1);
}

#[tokio::test]
async fn legacy_spawn_inspection_key_scope_and_hash_never_grant_execution() {
    let fixture = fixture().await;
    let request = request(&fixture);
    let first = completed(&fixture, &request).await;
    let mut changed = request.clone();
    changed.prompt = Some("different immutable request".into());
    assert!(matches!(
        inspect(&fixture, fixture.service.as_ref(), &changed),
        Err(RuntimeError::Conflict(_))
    ));
    let mut no_key = request.clone();
    no_key.idempotency_key = None;
    let mut new_key = request.clone();
    new_key.idempotency_key = Some("fresh-key".into());
    let mut other_source = request.clone();
    other_source.source_session_id = "foreign-source".into();
    let mut other_team = request.clone();
    other_team.team_id = "foreign-team".into();
    for absent in [no_key, new_key, other_source, other_team] {
        assert!(matches!(
            inspect(&fixture, fixture.service.as_ref(), &absent),
            Err(RuntimeError::Unsupported(_))
        ));
        if let Some(key) = absent.idempotency_key.as_deref() {
            assert!(fixture
                .store
                .find_team_member_spawn_operation(&absent.team_id, &absent.source_session_id, key)
                .unwrap()
                .is_none());
        }
    }
    assert_eq!(
        fixture
            .store
            .find_team_member_spawn_operation(
                &request.team_id,
                &request.source_session_id,
                request.idempotency_key.as_deref().unwrap()
            )
            .unwrap()
            .as_deref(),
        Some(first.operation_id.as_str())
    );
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 2);
    assert_eq!(
        fixture
            .store
            .list_team_operation_journal(Some(&fixture.team_id))
            .unwrap()
            .len(),
        1
    );
}
