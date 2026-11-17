use super::legacy_spawn_history::{HistoricalSpawnSeed, HistoricalWorktreeAssignment};
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
    let source = historical_test_session("sess_spawn_source", temp.path().to_str().unwrap());
    persist_test_history(store.as_ref(), provider.as_ref(), &source).await;
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
            metadata: None,
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

impl SpawnFixture {
    pub(super) fn history(
        &self,
        request: &TeamMemberSpawnRequest,
        assignment: Option<HistoricalWorktreeAssignment>,
    ) -> HistoricalSpawnSeed {
        let state = self.store.hydrate_runtime_state().unwrap();
        let source = state
            .sessions
            .iter()
            .find(|s| s.id == request.source_session_id)
            .unwrap()
            .clone();
        let team = runtime_core::TeamWithMembers {
            team: state
                .teams
                .iter()
                .find(|t| t.id == request.team_id)
                .unwrap()
                .clone(),
            members: state
                .team_members
                .into_iter()
                .filter(|m| m.team_id == request.team_id)
                .collect(),
        };
        let digest =
            runtime_core::normalized_json_hash(&serde_json::to_value(request).unwrap()).unwrap();
        HistoricalSpawnSeed::for_request(
            request.clone(),
            source,
            team,
            &format!("op_spawn_history_{digest}"),
            &format!("historical_child_{digest}"),
            assignment,
            1000,
        )
        .unwrap()
    }

    pub(super) fn onboarding_history(
        &self,
        request: &TeamMemberSpawnRequest,
        assignment: Option<HistoricalWorktreeAssignment>,
    ) -> HistoricalSpawnSeed {
        let seed = self.history(request, assignment);
        seed.seed_to_onboarding_sent(self.store.as_ref(), 1000, 1001)
            .unwrap();
        seed
    }

    pub(super) fn completed_history(
        &self,
        request: &TeamMemberSpawnRequest,
        assignment: Option<HistoricalWorktreeAssignment>,
    ) -> runtime_core::TeamMemberSpawnResponse {
        let seed = self.onboarding_history(request, assignment);
        seed.complete_seed(self.store.as_ref(), 1002).unwrap();
        seed.response
    }

    pub(super) fn keyed_request(&self, key: &str) -> TeamMemberSpawnRequest {
        let mut request = self.request();
        request.idempotency_key = Some(key.into());
        request
    }

    pub(super) fn init_git(&self) {
        self.git(&["init", "--quiet"]);
        self.git(&[
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
    }

    pub(super) fn git(&self, args: &[&str]) -> Vec<u8> {
        let output = Command::new("git")
            .arg("-C")
            .arg(self._temp.path())
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    pub(super) fn prepared_checkout(
        &self,
        request: &TeamMemberSpawnRequest,
        created: bool,
    ) -> ManagedWorktreeRecord {
        let digest =
            runtime_core::normalized_json_hash(&serde_json::to_value(request).unwrap()).unwrap();
        let name = request.worktree.as_ref().unwrap().name.as_deref().unwrap();
        let root = self._temp.path().join("historical-checkouts");
        std::fs::create_dir_all(&root).unwrap();
        let cwd = root.join(name);
        let branch = format!("historical/{name}");
        self.git(&[
            "worktree",
            "add",
            "--quiet",
            "-b",
            &branch,
            cwd.to_str().unwrap(),
            "HEAD",
        ]);
        ManagedWorktreeRecord {
            id: format!("wt_history_{digest}"),
            repo_root: self._temp.path().display().to_string(),
            worktree_root: root.display().to_string(),
            worktree_cwd: cwd.display().to_string(),
            branch_name: branch,
            worktree_name: name.into(),
            unified_workspace_path: "historical-fixture".into(),
            deletion_policy: "delete_on_last_claim".into(),
            created_by_session_id: Some(request.source_session_id.clone()),
            created_by_operation_id: Some(if created {
                format!("op_spawn_history_{digest}")
            } else {
                "op_prior_checkout_history".into()
            }),
            created_at: 900,
            updated_at: 900,
        }
    }

    pub(super) fn stage_history(
        &self,
        operation_id: &str,
        stage: &str,
        payload: serde_json::Value,
    ) {
        let mut journal = self
            .store
            .get_team_operation_journal(operation_id)
            .unwrap()
            .unwrap();
        journal.stage = stage.into();
        journal.payload = payload;
        journal.updated_at = 1002;
        self.store.upsert_team_operation_journal(&journal).unwrap();
    }

    pub(super) async fn assert_unresolved_without_effects(
        &self,
        request: TeamMemberSpawnRequest,
        operation_id: &str,
    ) {
        let before = self.store.hydrate_runtime_state().unwrap();
        let effects = self
            .provider
            .effect_calls
            .load(std::sync::atomic::Ordering::SeqCst);
        let error = self.service.spawn_team_member(request).await.unwrap_err();
        assert!(
            matches!(error,RuntimeError::OperationUnresolved{operation_id:ref id,..} if id==operation_id),
            "{error:?}"
        );
        assert_eq!(self.store.hydrate_runtime_state().unwrap(), before);
        assert_eq!(
            self.provider
                .effect_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            effects
        );
    }
}

#[tokio::test]
async fn invalid_requested_provider_never_creates_native_git_checkout() {
    let fixture = fixture().await;
    fixture.init_git();
    let mut request = fixture.request();
    request.provider = Some("not-a-provider".into());
    request.worktree = Some(worktree_input("create", "must-not-exist"));
    let before = fixture.store.hydrate_runtime_state().unwrap();
    let git_before = fixture.git(&["worktree", "list", "--porcelain"]);
    let error = fixture
        .service
        .spawn_team_member(request)
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Unsupported(_)), "{error:?}");
    assert_eq!(fixture.store.hydrate_runtime_state().unwrap(), before);
    assert_eq!(
        fixture.git(&["worktree", "list", "--porcelain"]),
        git_before
    );
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}

#[tokio::test]
async fn corrected_source_git_root_still_cannot_consume_a_fresh_legacy_spawn_key() {
    let fixture = fixture().await;
    let mut request = fixture.keyed_request("source-not-yet-a-git-repository");
    request.worktree = Some(worktree_input("create", "first-checkout"));
    let before = fixture.store.hydrate_runtime_state().unwrap();
    for initialized in [false, true] {
        if initialized {
            fixture.init_git();
        }
        assert!(matches!(
            fixture.service.spawn_team_member(request.clone()).await,
            Err(RuntimeError::Unsupported(_))
        ));
        assert_eq!(fixture.store.hydrate_runtime_state().unwrap(), before);
    }
    let conn = rusqlite::Connection::open(&fixture.database_path).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM team_spawn_admissions", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}

pub(super) fn worktree_input(mode: &str, name: &str) -> runtime_core::TeamMemberSpawnWorktreeInput {
    runtime_core::TeamMemberSpawnWorktreeInput {
        mode: Some(mode.into()),
        name: Some(name.into()),
        branch_prefix: None,
        base_ref: None,
        run_init_script: Some(false),
    }
}

#[tokio::test]
async fn historical_compensation_labels_never_authorize_legacy_reexecution() {
    let fixture = fixture().await;
    let request = fixture.keyed_request("weak-compensation-labels");
    let seed = fixture.onboarding_history(&request, None);
    fixture.stage_history(
        &seed.response.operation_id,
        "rolled_back",
        json!({
            "spawned_session_id":seed.response.spawned_session.id,
            "compensation_outcomes":{"team_remove":"confirmed","session_close":"confirmed"},
        }),
    );
    fixture
        .assert_unresolved_without_effects(request, &seed.response.operation_id)
        .await;
    assert_eq!(fixture.fresh_journal().stage, "rolled_back");
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
async fn historical_lost_rollback_intent_preserves_joined_session_for_review() {
    let fixture = fixture().await;
    let request = fixture.keyed_request("lost-rollback-intent");
    let seed = fixture.onboarding_history(&request, None);
    fixture.stage_history(
        &seed.response.operation_id,
        "session_created",
        json!({
            "spawned_session_id":seed.response.spawned_session.id,"provider":"codex",
            "provider_session_ref":seed.response.spawned_session.provider_session_ref,
        }),
    );
    fixture
        .assert_unresolved_without_effects(request, &seed.response.operation_id)
        .await;
    assert_eq!(fixture.fresh_journal().stage, "session_created");
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .team_members
            .len(),
        2
    );
}

#[tokio::test]
async fn historical_lost_rollback_receipt_preserves_pending_intent() {
    let fixture = fixture().await;
    let request = fixture.keyed_request("lost-rollback-receipt");
    let seed = fixture.onboarding_history(&request, None);
    fixture.stage_history(
        &seed.response.operation_id,
        "rollback_pending",
        json!({
            "spawned_session_id":seed.response.spawned_session.id,"worktree_id":null,
        }),
    );
    fixture
        .assert_unresolved_without_effects(request, &seed.response.operation_id)
        .await;
    assert_eq!(fixture.fresh_journal().stage, "rollback_pending");
}

#[tokio::test]
async fn historical_diagnostic_loss_keeps_original_uncertainty_without_writing_more_evidence() {
    let fixture = fixture().await;
    let request = fixture.keyed_request("lost-rollback-diagnostic");
    let seed = fixture.onboarding_history(&request, None);
    fixture.stage_history(&seed.response.operation_id,"rollback_review_required",json!({
        "spawned_session_id":seed.response.spawned_session.id,"uncertainties":["rollback_diagnostic_write"],
    }));
    rusqlite::Connection::open(&fixture.database_path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_rollback_diagnostic BEFORE INSERT ON team_operation_diagnostics
         BEGIN SELECT RAISE(ABORT,'fixture evidence sink unavailable'); END;",
        )
        .unwrap();
    fixture
        .assert_unresolved_without_effects(request, &seed.response.operation_id)
        .await;
    assert!(fixture.fresh_journal().payload["uncertainties"]
        .to_string()
        .contains("rollback_diagnostic_write"));
}

#[tokio::test]
async fn historical_join_receipt_gap_never_rejoins_or_creates_another_session() {
    let fixture = fixture().await;
    let request = fixture.keyed_request("lost-join-receipt");
    let seed = fixture.onboarding_history(&request, None);
    fixture.stage_history(
        &seed.response.operation_id,
        "session_created",
        json!({
            "spawned_session_id":seed.response.spawned_session.id,"worktree_id":null,
        }),
    );
    fixture
        .assert_unresolved_without_effects(request, &seed.response.operation_id)
        .await;
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .team_members
            .len(),
        2
    );
}

#[tokio::test]
async fn independent_shared_services_refuse_without_allocating_operation_ids() {
    let fixture = fixture().await;
    let other = RuntimeWorktreeService::new(
        fixture.store.clone(),
        fixture.runtime.clone(),
        fixture.comms.clone(),
        fixture.service.config.clone(),
    )
    .unwrap();
    let before = fixture.store.hydrate_runtime_state().unwrap();
    for service in [&fixture.service, &other] {
        for request in [
            fixture.request(),
            fixture.keyed_request("no-operation-allocation"),
        ] {
            assert!(matches!(
                service.spawn_team_member(request).await,
                Err(RuntimeError::Unsupported(_))
            ));
        }
    }
    assert_eq!(fixture.store.hydrate_runtime_state().unwrap(), before);
    assert!(fixture
        .store
        .list_team_operation_journal(Some(&fixture.team_id))
        .unwrap()
        .is_empty());
    assert_eq!(fixture.provider.state.lock().await.sessions.len(), 1);
}

#[tokio::test]
async fn prepared_checkout_survives_a_missing_historical_stage_receipt() {
    let fixture = fixture().await;
    fixture.init_git();
    let mut request = fixture.keyed_request("git-stage-lost-reply");
    request.worktree = Some(worktree_input("create", "pending-checkout"));
    let record = fixture.prepared_checkout(&request, true);
    let operation_id = record.created_by_operation_id.clone().unwrap();
    fixture.store.upsert_managed_worktree(&record).unwrap();
    let hash =
        runtime_core::normalized_json_hash(&serde_json::to_value(&request).unwrap()).unwrap();
    assert!(matches!(
        fixture
            .store
            .admit_team_member_spawn(&operation_id, &request, &hash, 1000)
            .unwrap(),
        runtime_core::TeamMemberSpawnAdmission::Execute { .. }
    ));
    let inventory = fixture.git(&["worktree", "list", "--porcelain"]);
    fixture
        .assert_unresolved_without_effects(request, &operation_id)
        .await;
    assert_eq!(fixture.fresh_journal().stage, "planned");
    assert_eq!(
        fixture
            .store
            .hydrate_runtime_state()
            .unwrap()
            .managed_worktrees,
        vec![record.clone()]
    );
    assert!(std::path::Path::new(&record.worktree_cwd).is_dir());
    assert_eq!(fixture.git(&["worktree", "list", "--porcelain"]), inventory);
}

#[tokio::test]
async fn prepared_dirty_checkout_history_never_runs_automatic_compensation() {
    let fixture = fixture().await;
    fixture.init_git();
    let mut request = fixture.keyed_request("dirty-retained-history");
    request.worktree = Some(worktree_input("create", "dirty-retained"));
    let record = fixture.prepared_checkout(&request, true);
    let seed = fixture.onboarding_history(
        &request,
        Some(HistoricalWorktreeAssignment::Created(record.clone())),
    );
    fixture.stage_history(
        &seed.response.operation_id,
        "rollback_review_required",
        json!({
            "spawned_session_id":seed.response.spawned_session.id,"worktree_id":record.id,
            "uncertainties":["worktree_cleanup_not_deleted"],
        }),
    );
    let edited = std::path::Path::new(&record.worktree_cwd).join("preserve-operator-edit.txt");
    std::fs::write(&edited, "uncommitted operator content\n").unwrap();
    let inventory = fixture.git(&["worktree", "list", "--porcelain"]);
    fixture
        .assert_unresolved_without_effects(request, &seed.response.operation_id)
        .await;
    assert_eq!(
        std::fs::read_to_string(edited).unwrap(),
        "uncommitted operator content\n"
    );
    assert_eq!(fixture.git(&["worktree", "list", "--porcelain"]), inventory);
    assert!(fixture
        .store
        .hydrate_runtime_state()
        .unwrap()
        .managed_worktree_claims
        .iter()
        .all(|c| c.released_at.is_none()));
}

#[tokio::test]
async fn local_closed_projection_never_proves_historical_native_close() {
    let fixture = fixture().await;
    fixture.init_git();
    let mut request = fixture.keyed_request("unconfirmed-close-history");
    request.worktree = Some(worktree_input("create", "unconfirmed-close"));
    let record = fixture.prepared_checkout(&request, true);
    let seed = fixture.onboarding_history(
        &request,
        Some(HistoricalWorktreeAssignment::Created(record.clone())),
    );
    fixture.stage_history(&seed.response.operation_id,"rollback_review_required",json!({
        "spawned_session_id":seed.response.spawned_session.id,"worktree_id":record.id,
        "compensation_outcomes":{"session_close":"unknown","worktree_release":"deferred","worktree_cleanup":"deferred"},
    }));
    rusqlite::Connection::open(&fixture.database_path)
        .unwrap()
        .execute(
            "UPDATE sessions SET status='closed',closed_at=1002 WHERE id=?1",
            [&seed.response.spawned_session.id],
        )
        .unwrap();
    fixture
        .assert_unresolved_without_effects(request, &seed.response.operation_id)
        .await;
    let state = fixture.store.hydrate_runtime_state().unwrap();
    assert!(state
        .sessions
        .iter()
        .any(|s| s.id == seed.response.spawned_session.id && s.status == "closed"));
    assert!(state
        .managed_worktree_claims
        .iter()
        .any(|c| c.session_id == seed.response.spawned_session.id && c.released_at.is_none()));
    assert!(std::path::Path::new(&record.worktree_cwd).is_dir());
}
