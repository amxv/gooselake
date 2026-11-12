use super::*;
use runtime_core::{TeamCommsService, TeamMemberSpawnRequest, WorktreeService};
use std::process::Command;

pub(super) struct SpawnFixture {
    pub(super) _temp: tempfile::TempDir,
    pub(super) store: Arc<SqliteRuntimeStore>,
    source_session_id: String,
    pub(super) team_id: String,
    pub(super) service: Arc<RuntimeWorktreeService>,
    pub(super) runtime: Arc<RuntimeSessionManager>,
    pub(super) comms: Arc<RuntimeTeamCommsService>,
    pub(super) provider: Arc<WorktreeTestProvider>,
    pub(super) database_path: std::path::PathBuf,
}

pub(super) async fn fixture() -> SpawnFixture {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("runtime.sqlite3");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: database_path.clone(),
    }));
    store.initialize().await.unwrap();
    let provider = Arc::new(WorktreeTestProvider::default());
    let mut registry = ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let runtime =
        Arc::new(RuntimeSessionManager::new(store.clone(), Arc::new(registry), 512).unwrap());
    let comms = RuntimeTeamCommsService::new(
        store.clone(),
        runtime.clone(),
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1_000,
        },
    )
    .unwrap();
    let source = create_test_session(&runtime, temp.path().to_str().unwrap()).await;
    let team_id = comms
        .create_team(TeamCreateRequest {
            name: "Disposable spawn rollback".into(),
            lead_agent_id: source.id.clone(),
            member_agent_ids: vec![],
            created_by: Some("spawn-recovery-fixture".into()),
        })
        .await
        .unwrap()
        .team
        .id;
    let service = RuntimeWorktreeService::new(
        store.clone(),
        runtime.clone(),
        comms.clone(),
        WorktreeServiceConfig {
            enabled: true,
            root_dir: temp.path().join("managed-worktrees").display().to_string(),
            init_script_path: ".agents/gg/worktree-init.sh".into(),
            deletion_policy_default: "delete_on_last_claim".into(),
        },
    )
    .unwrap();
    SpawnFixture {
        _temp: temp,
        store,
        source_session_id: source.id,
        team_id,
        service,
        runtime,
        comms,
        provider,
        database_path,
    }
}

impl SpawnFixture {
    pub(super) fn request(&self) -> TeamMemberSpawnRequest {
        TeamMemberSpawnRequest {
            team_id: self.team_id.clone(),
            source_session_id: self.source_session_id.clone(),
            idempotency_key: None,
            provider: None,
            model: None,
            title: None,
            prompt: None,
            permission_mode: None,
            metadata: Some(json!({"__test_force_claim_failure_after_join": true})),
            worktree: None,
            creator_agent_id: None,
            creator_compaction_subscription: None,
        }
    }

    pub(super) fn fresh_journal(&self) -> runtime_core::TeamOperationJournalRecord {
        // Read through an independent SQLite connection, not a cached runtime
        // projection; this is the state an operator sees after restart.
        let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
            database_path: self.database_path.clone(),
        });
        let rows = reopened
            .list_team_operation_journal(Some(&self.team_id))
            .unwrap();
        assert_eq!(rows.len(), 1);
        rows.into_iter().next().unwrap()
    }
}

#[tokio::test]
async fn invalid_requested_provider_never_creates_native_git_checkout() {
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
    let mut request = fixture.request();
    request.metadata = None;
    request.provider = Some("not-a-provider".into());
    request.worktree = Some(runtime_core::TeamMemberSpawnWorktreeInput {
        mode: Some("create".into()),
        name: Some("must-not-exist".into()),
        branch_prefix: None,
        base_ref: None,
        run_init_script: Some(true),
    });
    let error = fixture
        .service
        .spawn_team_member(request)
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)), "{error:?}");
    assert!(fixture
        .store
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktrees
        .is_empty());
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("must-not-exist"));
}

#[tokio::test]
async fn spawn_rollback_receipts_are_durable_only_after_confirmed_compensations() {
    let fixture = fixture().await;
    let error = fixture
        .service
        .spawn_team_member(fixture.request())
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)), "{error:?}");
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "rolled_back");
    assert_eq!(
        journal.payload["compensation_outcomes"]["team_remove"],
        "confirmed"
    );
    assert_eq!(
        journal.payload["compensation_outcomes"]["session_close"],
        "confirmed"
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
    let hydrated = fixture.store.hydrate_runtime_state().unwrap();
    assert_eq!(hydrated.sessions.len(), 2);
    assert!(hydrated
        .sessions
        .iter()
        .any(|row| row.id != fixture.source_session_id && row.status == "closed"));
}

#[tokio::test]
async fn rollback_intent_fault_leaves_joined_native_session_unmodified_for_review() {
    let fixture = fixture().await;
    rusqlite::Connection::open(&fixture.database_path).unwrap().execute_batch(
        "CREATE TRIGGER fail_rollback_intent BEFORE UPDATE ON team_operation_journal
         WHEN NEW.stage='rollback_pending' BEGIN SELECT RAISE(ABORT,'injected rollback intent failure'); END;"
    ).unwrap();
    let error = fixture
        .service
        .spawn_team_member(fixture.request())
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved { .. }),
        "{error:?}"
    );
    assert_eq!(fixture.fresh_journal().stage, "session_created");
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
}

#[tokio::test]
async fn rollback_final_receipt_failure_keeps_pending_review_intent() {
    let fixture = fixture().await;
    rusqlite::Connection::open(&fixture.database_path).unwrap().execute_batch(
        "CREATE TRIGGER fail_rollback_final BEFORE UPDATE ON team_operation_journal
         WHEN NEW.stage='rolled_back' BEGIN SELECT RAISE(ABORT,'injected rollback final failure'); END;"
    ).unwrap();
    let error = fixture
        .service
        .spawn_team_member(fixture.request())
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved { .. }),
        "{error:?}"
    );
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "rollback_pending");
    assert!(journal.payload["spawned_session_id"].is_string());
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
async fn diagnostic_failure_never_certifies_complete_rollback() {
    let fixture = fixture().await;
    rusqlite::Connection::open(&fixture.database_path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_rollback_diagnostic BEFORE INSERT ON team_operation_diagnostics
         BEGIN SELECT RAISE(ABORT,'injected diagnostic failure'); END;",
        )
        .unwrap();
    let error = fixture
        .service
        .spawn_team_member(fixture.request())
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved { .. }),
        "{error:?}"
    );
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "rollback_review_required");
    assert!(journal.payload["uncertainties"]
        .to_string()
        .contains("rollback_diagnostic_write"));
}

#[tokio::test]
async fn postnative_team_join_journal_fault_returns_operation_coordinate_not_retryable_error() {
    let fixture = fixture().await;
    rusqlite::Connection::open(&fixture.database_path).unwrap().execute_batch(
        "CREATE TRIGGER fail_join_receipt BEFORE UPDATE ON team_operation_journal
         WHEN NEW.stage='team_joined' BEGIN SELECT RAISE(ABORT,'injected join receipt failure'); END;"
    ).unwrap();
    let mut request = fixture.request();
    request.metadata = None;
    let error = fixture
        .service
        .spawn_team_member(request)
        .await
        .unwrap_err();
    let operation_id = match error {
        RuntimeError::OperationUnresolved { operation_id, .. } => operation_id,
        error => panic!("expected unresolved native join, got {error:?}"),
    };
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "session_created");
    assert_eq!(journal.operation_id, operation_id);
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
}

#[tokio::test]
async fn independent_spawn_services_allocate_distinct_durable_operation_ids() {
    let fixture = fixture().await;
    let other = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    assert!(matches!(
        fixture.service.spawn_team_member(fixture.request()).await,
        Err(RuntimeError::InvalidState(_))
    ));
    assert!(matches!(
        other.spawn_team_member(fixture.request()).await,
        Err(RuntimeError::InvalidState(_))
    ));
    let rows = fixture
        .store
        .list_team_operation_journal(Some(&fixture.team_id))
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0].operation_id, rows[1].operation_id);
    assert!(rows.iter().all(|row| row.stage == "rolled_back"));
}

#[tokio::test]
async fn native_git_checkout_survives_lost_spawn_stage_receipt_for_operator_review() {
    let fixture = fixture().await;
    let repo = fixture._temp.path();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "--quiet"]);
    git(&[
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--allow-empty",
        "-m",
        "init",
        "--quiet",
    ]);
    rusqlite::Connection::open(&fixture.database_path).unwrap().execute_batch(
        "CREATE TRIGGER fail_native_checkout_stage BEFORE UPDATE ON team_operation_journal
         WHEN NEW.stage='worktree_created' BEGIN SELECT RAISE(ABORT,'injected worktree stage failure'); END;"
    ).unwrap();
    let mut request = fixture.request();
    request.metadata = None;
    request.worktree = Some(runtime_core::TeamMemberSpawnWorktreeInput {
        mode: Some("create".into()),
        name: Some("pending-checkout".into()),
        branch_prefix: None,
        base_ref: None,
        run_init_script: Some(false),
    });
    request.idempotency_key = Some("git-stage-lost-reply".into());
    let error = fixture
        .service
        .spawn_team_member(request.clone())
        .await
        .unwrap_err();
    let operation_id = match error {
        RuntimeError::OperationUnresolved { operation_id, .. } => operation_id,
        error => panic!("native Git stage must report unresolved operation, got {error:?}"),
    };
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "planned");
    assert_eq!(journal.operation_id, operation_id);
    let worktrees = fixture
        .store
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktrees;
    assert_eq!(worktrees.len(), 1);
    assert_eq!(
        worktrees[0].created_by_operation_id.as_deref(),
        Some(operation_id.as_str())
    );
    assert!(std::path::Path::new(&worktrees[0].worktree_cwd).is_dir());
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["worktree", "list", "--porcelain"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&output.stdout).contains(&worktrees[0].worktree_cwd));
    // The original keyed request is still pending, even if Git has a real
    // checkout and a new worker cannot see the native stage acknowledgment.
    let retry = fixture
        .service
        .spawn_team_member(request)
        .await
        .unwrap_err();
    assert!(
        matches!(retry, RuntimeError::OperationUnresolved { operation_id: id, .. } if id == operation_id)
    );
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktrees
            .len(),
        1
    );
}

#[tokio::test]
async fn postjoin_rollback_preserves_dirty_checkout_and_requires_review() {
    let fixture = fixture().await;
    let repo = fixture._temp.path();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    run_git(&["init", "--quiet"]);
    run_git(&[
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--allow-empty",
        "--quiet",
        "-m",
        "initial",
    ]);
    let mut request = fixture.request();
    request.metadata = Some(json!({
        "__test_force_onboarding_failure_after_join": true,
        "__test_dirty_checkout_before_rollback": true,
    }));
    request.worktree = Some(runtime_core::TeamMemberSpawnWorktreeInput {
        mode: Some("create".into()),
        name: Some("dirty-retained".into()),
        branch_prefix: None,
        base_ref: None,
        run_init_script: Some(false),
    });
    let error = fixture
        .service
        .spawn_team_member(request)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved { .. }),
        "{error:?}"
    );
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "rollback_review_required");
    assert!(journal.payload["uncertainties"]
        .to_string()
        .contains("worktree_cleanup_not_deleted"));
    let worktrees = fixture
        .store
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktrees;
    assert_eq!(worktrees.len(), 1);
    let edited_file =
        std::path::Path::new(&worktrees[0].worktree_cwd).join("preserve-operator-edit.txt");
    assert_eq!(
        std::fs::read_to_string(edited_file).unwrap(),
        "uncommitted operator content\n"
    );
}

#[tokio::test]
async fn unconfirmed_provider_close_preserves_checkout_and_claim_for_review() {
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
    fixture
        .provider
        .fail_close
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let mut request = fixture.request();
    request.metadata = Some(json!({"__test_force_onboarding_failure_after_join": true}));
    request.worktree = Some(runtime_core::TeamMemberSpawnWorktreeInput {
        mode: Some("create".into()),
        name: Some("unconfirmed-close".into()),
        branch_prefix: None,
        base_ref: None,
        run_init_script: Some(false),
    });
    let error = fixture
        .service
        .spawn_team_member(request)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::OperationUnresolved { .. }),
        "{error:?}"
    );
    let journal = fixture.fresh_journal();
    assert_eq!(journal.stage, "rollback_review_required");
    assert_eq!(
        journal.payload["compensation_outcomes"]["worktree_release"],
        "deferred"
    );
    assert_eq!(
        journal.payload["compensation_outcomes"]["worktree_cleanup"],
        "deferred"
    );
    let state = fixture.store.hydrate_runtime_state().unwrap();
    assert_eq!(state.managed_worktrees.len(), 1);
    assert!(std::path::Path::new(&state.managed_worktrees[0].worktree_cwd).exists());
    assert!(state
        .managed_worktree_claims
        .iter()
        .any(|row| row.released_at.is_none()));
}
