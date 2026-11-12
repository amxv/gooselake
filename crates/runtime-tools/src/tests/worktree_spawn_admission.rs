use super::worktree_spawn_recovery::fixture;
use super::*;
use runtime_core::{TeamCommsService, WorktreeService};
use std::process::Command;

fn keyed_request(
    fixture: &super::worktree_spawn_recovery::SpawnFixture,
    key: &str,
) -> runtime_core::TeamMemberSpawnRequest {
    let mut request = fixture.request();
    request.metadata = None;
    request.idempotency_key = Some(key.into());
    request
}

#[tokio::test]
async fn exact_keyed_completion_replays_original_receipt_without_native_work() {
    let fixture = fixture().await;
    let request = keyed_request(&fixture, "caller-roundtrip-1");
    let first = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap();
    assert_eq!(first.journal_stage, "completed");
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "completed");
    assert_eq!(journal.operation_id, first.operation_id);
    let first_bytes = serde_json::to_vec(&first).unwrap();

    // An exact HTTP/tool replay after a successful native spawn returns the
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
            .comms
            .get_team(&fixture.team_id)
            .await
            .unwrap()
            .members
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
}

#[tokio::test]
async fn independent_workers_racing_one_key_never_launch_duplicate_provider_session() {
    let fixture = fixture().await;
    let other = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    let request = keyed_request(&fixture, "race-20-workers");
    let mut workers = tokio::task::JoinSet::new();
    for index in 0..20 {
        let service = if index % 2 == 0 {
            fixture.service.clone()
        } else {
            other.clone()
        };
        let input = request.clone();
        workers.spawn(async move { service.spawn_team_member(input).await });
    }
    let mut outcomes = Vec::new();
    while let Some(outcome) = workers.join_next().await {
        outcomes.push(outcome.unwrap());
    }
    let completed = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    assert!(completed >= 1, "at least the initial worker must settle");
    let original_id = fixture.fresh_journal().operation_id;
    for outcome in outcomes {
        match outcome {
            Ok(response) => assert_eq!(response.operation_id, original_id),
            Err(RuntimeError::OperationUnresolved { operation_id, .. }) => {
                assert_eq!(operation_id, original_id);
            }
            Err(error) => panic!("incorrect concurrent spawn authority: {error:?}"),
        }
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
            .comms
            .get_team(&fixture.team_id)
            .await
            .unwrap()
            .members
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
}

#[tokio::test]
async fn failed_terminal_receipt_quarantines_original_key_across_restart() {
    let fixture = fixture().await;
    rusqlite::Connection::open(&fixture.database_path).unwrap().execute_batch(
        "CREATE TRIGGER deny_completed_spawn BEFORE UPDATE ON team_operation_journal
         WHEN NEW.stage='completed' BEGIN SELECT RAISE(ABORT,'simulated terminal receipt loss'); END;"
    ).unwrap();
    let request = keyed_request(&fixture, "lost-final-ack");
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
            .comms
            .get_team(&fixture.team_id)
            .await
            .unwrap()
            .members
            .len(),
        2
    );
    assert_eq!(fixture.fresh_journal().stage, "onboarding_sent");
}

#[tokio::test]
async fn admission_sql_fault_rolls_back_planned_stage_before_native_session() {
    let fixture = fixture().await;
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER deny_spawn_key BEFORE INSERT ON team_spawn_admissions
         BEGIN SELECT RAISE(ABORT,'simulated keyed admission failure'); END;",
        )
        .unwrap();
    let request = keyed_request(&fixture, "fault-before-native");
    assert!(fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .is_err());
    assert!(fixture
        .store
        .list_team_operation_journal(Some(&fixture.team_id))
        .unwrap()
        .is_empty());
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        1
    );
    connection
        .execute_batch("DROP TRIGGER deny_spawn_key;")
        .unwrap();
    let first = fixture.service.spawn_team_member(request).await.unwrap();
    assert_eq!(first.journal_stage, "completed");
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        2
    );
}

#[tokio::test]
async fn confirmed_rollback_consumes_original_key_and_returns_recovery_coordinate() {
    let fixture = fixture().await;
    let mut request = keyed_request(&fixture, "rolled-back-still-consumed");
    request.metadata = Some(json!({"__test_force_claim_failure_after_join":true}));
    let first = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap_err();
    let original = fixture.fresh_journal();
    assert_eq!(original.stage, "rolled_back");
    assert!(
        matches!(first, RuntimeError::OperationUnresolved { operation_id, .. }
        if operation_id == original.operation_id)
    );
    let other = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    let second = other.spawn_team_member(request).await.unwrap_err();
    assert!(
        matches!(second, RuntimeError::OperationUnresolved { operation_id, .. }
        if operation_id == original.operation_id)
    );
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
            .comms
            .get_team(&fixture.team_id)
            .await
            .unwrap()
            .members
            .len(),
        1
    );
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
    let first = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap();
    let mut foreign = request;
    foreign.source_session_id = "foreign-source-session".into();
    let error = fixture
        .service
        .spawn_team_member(foreign)
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::NotFound(_)), "{error:?}");
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
async fn stale_team_cache_cannot_admit_new_native_work_after_sqlite_deletion() {
    let fixture = fixture().await;
    let connection = rusqlite::Connection::open(&fixture.database_path).unwrap();
    connection
        .execute(
            "UPDATE teams SET deleted_at=999 WHERE id=?1",
            [&fixture.team_id],
        )
        .unwrap();
    let request = keyed_request(&fixture, "deleted-team-not-dispatched");
    let error = fixture
        .service
        .spawn_team_member(request.clone())
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
        1
    );
    assert!(fixture
        .store
        .list_team_operation_journal(Some(&fixture.team_id))
        .unwrap()
        .is_empty());
    // Failed read-only preflight did not consume the key. A restored team
    // may admit its first actual operation, rather than replay a fake one.
    connection
        .execute(
            "UPDATE teams SET deleted_at=NULL WHERE id=?1",
            [&fixture.team_id],
        )
        .unwrap();
    assert_eq!(
        fixture
            .service
            .spawn_team_member(request)
            .await
            .unwrap()
            .journal_stage,
        "completed"
    );
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

    // The test never created a native provider session. A confirmed rollback
    // of the original reservation can subsequently allow the same service to
    // commit the real deletion and its Critical journal atomically.
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
}

#[tokio::test]
async fn real_disposable_git_checkout_and_fake_provider_replay_from_reopened_sqlite() {
    let fixture = fixture().await;
    let repo = fixture._temp.path();
    for args in [
        vec!["init", "--quiet"],
        vec![
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "initial",
        ],
    ] {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut request = keyed_request(&fixture, "native-git-once");
    request.worktree = Some(runtime_core::TeamMemberSpawnWorktreeInput {
        mode: Some("create".into()),
        name: Some("git-once".into()),
        branch_prefix: None,
        base_ref: None,
        run_init_script: Some(false),
    });
    let first = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap();
    let cwd = first.worktree.as_ref().unwrap().worktree_cwd.clone();
    assert!(std::path::Path::new(&cwd).is_dir());
    let first_git = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .unwrap();
    assert!(first_git.status.success());
    assert!(String::from_utf8_lossy(&first_git.stdout).contains(&cwd));

    let reopened = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: fixture.database_path.clone(),
    }));
    let mut registry = ProviderRegistry::new();
    registry.register(fixture.provider.clone()).unwrap();
    let restarted_runtime =
        Arc::new(RuntimeSessionManager::new(reopened.clone(), Arc::new(registry), 512).unwrap());
    let restarted_comms = RuntimeTeamCommsService::new(
        reopened.clone(),
        restarted_runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .unwrap();
    let worker = RuntimeWorktreeService::new(
        reopened,
        restarted_runtime,
        restarted_comms,
        fixture.service.config.clone(),
    )
    .unwrap();
    let replay = worker.spawn_team_member(request).await.unwrap();
    assert_eq!(
        serde_json::to_value(&replay).unwrap(),
        serde_json::to_value(&first).unwrap()
    );
    let replay_git = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .unwrap();
    assert_eq!(first_git.stdout, replay_git.stdout);
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktrees
            .len(),
        1
    );
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .sessions
            .len(),
        2
    );
}
