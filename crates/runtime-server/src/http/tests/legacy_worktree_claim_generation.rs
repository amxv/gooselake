use super::*;
use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeClaimTransition, ManagedWorktreeRecord,
    RuntimeError, SessionRecord,
};
use runtime_store_sqlite::SqliteRuntimeRepository;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};

const WORKTREE: &str = "wt_n06_http_lifetime";
const SESSION: &str = "session_n06_http_lifetime";
const TOKEN: &str = "n06-http-lifetime-operator";
const CHILD_DB: &str = "GOOSELAKE_N06_HTTP_LIFETIME_DB";
const CHILD_TERMINAL: &str = "GOOSELAKE_N06_HTTP_LIFETIME_TERMINAL";
const CHILD_PARENT: &str = "GOOSELAKE_N06_HTTP_LIFETIME_PARENT_PID";

#[derive(Default)]
struct NoNativeProvider {
    calls: AtomicUsize,
}
impl NoNativeProvider {
    fn unexpected<T>(&self, action: &str) -> Result<T, RuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(RuntimeError::Unsupported(format!(
            "N06 fixture forbids native {action}"
        )))
    }
}
#[async_trait::async_trait]
impl RuntimeProvider for NoNativeProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Codex
    }
    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata {
            kind: self.kind(),
            display_name: "N06 inert provider".into(),
            enabled: true,
        }
    }
    async fn healthcheck(&self) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn create_session(
        &self,
        _: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.unexpected("create")
    }
    async fn resume_session(
        &self,
        _: ProviderResumeSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.unexpected("resume")
    }
    async fn send_turn(&self, _: ProviderSendTurnRequest) -> Result<ProviderTurnAck, RuntimeError> {
        self.unexpected("send")
    }
    async fn wait_for_turn(
        &self,
        _: ProviderWaitTurnRequest,
    ) -> Result<ProviderTurnResult, RuntimeError> {
        self.unexpected("wait")
    }
    async fn interrupt_turn(&self, _: ProviderInterruptTurnRequest) -> Result<(), RuntimeError> {
        self.unexpected("interrupt")
    }
    async fn respond_approval(
        &self,
        _: ProviderApprovalResponseRequest,
    ) -> Result<(), RuntimeError> {
        self.unexpected("approval")
    }
    async fn close_session(
        &self,
        _: runtime_core::ProviderCloseSessionRequest,
    ) -> Result<(), RuntimeError> {
        self.unexpected("close")
    }
    async fn close_session_for_retirement(
        &self,
        _: runtime_core::ProviderCloseSessionRequest,
    ) -> Result<runtime_core::ProviderRetirementCloseEvidence, RuntimeError> {
        self.unexpected("retirement close")
    }
    async fn rebind_workspace(
        &self,
        _: runtime_core::ProviderWorkspaceRebindRequest,
    ) -> Result<runtime_core::ProviderWorkspaceRebindEvidence, RuntimeError> {
        self.unexpected("rebind")
    }
}
fn raw_claim(c: &ManagedWorktreeClaimRecord) -> (&str, &str, &str, i64, Option<i64>) {
    (
        &c.worktree_id,
        &c.session_id,
        &c.claim_role,
        c.created_at,
        c.released_at,
    )
}
fn release_body(generation: u64) -> String {
    json!({"session_id":SESSION,"expected_claim_generation":generation,"cleanup_if_last_claim":false}).to_string()
}
async fn request(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, Vec<u8>) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.unwrap_or("").to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap()
        .to_vec();
    (status, bytes)
}
async fn get_claim(router: &Router) -> ManagedWorktreeClaimRecord {
    let (status, bytes) = request(
        router,
        "GET",
        &format!("/v1/worktrees/{WORKTREE}/claims/{SESSION}"),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}
fn authority(repository: &SqliteRuntimeRepository) -> Value {
    json!({"worktree":repository.get_managed_worktree(WORKTREE).unwrap(),
        "worktree_revision":repository.managed_worktree_revision(WORKTREE).unwrap(),
        "claim":repository.get_managed_worktree_claim(WORKTREE,SESSION).unwrap(),
        "events":repository.list_runtime_events(None,None,32).unwrap()})
}
fn assert_no_native_effects(path: &Path, provider: &NoNativeProvider, marker: &Path) {
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fs::read(marker).unwrap(), b"retain this checkout");
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    for table in [
        "runtime_operations",
        "runtime_operation_effects",
        "runtime_operation_outbox",
        "runtime_operation_outbox_receipts",
        "team_operation_journal",
        "workspace_agent_rebinds",
        "managed_worktree_cleanup_receipts",
        "processes",
        "managed_processes",
    ] {
        let count: i64 = db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "unexpected effect authority in {table}");
    }
}

// Normal discovery is inert; only the parent's exact-filter disposable child writes.
#[test]
fn same_value_reclaim_child() {
    let Some(path) = std::env::var_os(CHILD_DB) else {
        return;
    };
    let parent: u32 = std::env::var(CHILD_PARENT).unwrap().parse().unwrap();
    assert_ne!(std::process::id(), parent);
    let terminal = std::env::var_os(CHILD_TERMINAL).is_some();
    let repository = SqliteRuntimeRepository::new(PathBuf::from(path));
    let old = repository
        .get_managed_worktree_claim(WORKTREE, SESSION)
        .unwrap()
        .unwrap();
    let g1 = old.claim_generation.unwrap();
    assert!(g1 > 0);
    assert_eq!(old.released_at.is_some(), terminal);
    if !terminal {
        repository
            .transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(old.created_at.checked_add(1).unwrap()),
                    ..old.clone()
                },
                ManagedWorktreeClaimTransition::Release,
            )
            .unwrap();
    }
    let newer = repository
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: None,
                ..old.clone()
            },
            ManagedWorktreeClaimTransition::Claim,
        )
        .unwrap();
    assert_eq!(newer.claim_generation, Some(g1.checked_add(1).unwrap()));
    let final_claim = if terminal {
        repository
            .transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: old.released_at,
                    ..newer
                },
                ManagedWorktreeClaimTransition::Release,
            )
            .unwrap()
    } else {
        newer
    };
    assert_eq!(raw_claim(&final_claim), raw_claim(&old));
}

async fn assert_http_lifetime(terminal: bool) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let repo_root = temp.path().join("repo");
    let checkout = temp.path().join("worktrees/checkout");
    fs::create_dir_all(&repo_root).unwrap();
    fs::create_dir_all(&checkout).unwrap();
    let marker = checkout.join("retain.txt");
    fs::write(&marker, b"retain this checkout").unwrap();
    let repository = SqliteRuntimeRepository::new(path.clone());
    repository.initialize_schema().unwrap();
    repository
        .upsert_session(&SessionRecord {
            id: SESSION.into(),
            provider: "codex".into(),
            status: "closed".into(),
            cwd: None,
            model: None,
            permission_mode: None,
            system_prompt: None,
            metadata: json!({}),
            provider_session_ref: None,
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: 1,
            updated_at: 2,
            closed_at: Some(2),
            failure_code: None,
            failure_message: None,
        })
        .unwrap();
    repository
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            id: WORKTREE.into(),
            repo_root: repo_root.display().to_string(),
            worktree_root: temp.path().join("worktrees").display().to_string(),
            worktree_cwd: checkout.display().to_string(),
            branch_name: "gg/n06-http-lifetime".into(),
            worktree_name: "n06-http-lifetime".into(),
            unified_workspace_path: "n06_http_lifetime".into(),
            deletion_policy: "retain_on_last_claim".into(),
            created_by_session_id: None,
            created_by_operation_id: None,
            created_at: 1,
            updated_at: 1,
        })
        .unwrap();
    let provider = Arc::new(NoNativeProvider::default());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider.clone()).unwrap();
    let registry = Arc::new(registry);
    let store: Arc<dyn RuntimeStore> = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    }));
    let (router, runtime, app) = super::support_router::assemble_test_router(
        temp.path(),
        store.clone(),
        registry.clone(),
        TOKEN,
        TeamMcpPolicy::default(),
    )
    .await;
    let body = json!({"session_id":SESSION,"claim_role":"owner"}).to_string();
    let (status, bytes) = request(
        &router,
        "POST",
        &format!("/v1/worktrees/{WORKTREE}/claims"),
        Some(&body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let claimed: runtime_core::WorktreeClaimResponse = serde_json::from_slice(&bytes).unwrap();
    let g1 = claimed.claim.claim_generation.unwrap();
    assert!(g1 > 0);
    let saved_g1_body = release_body(g1);
    let release_uri = format!("/v1/worktrees/{WORKTREE}/release");
    if terminal {
        let (status, bytes) = request(&router, "POST", &release_uri, Some(&saved_g1_body)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let before_get = authority(&repository);
    let observed = get_claim(&router).await;
    assert_eq!(observed.claim_generation, Some(g1));
    assert_eq!(observed.released_at.is_some(), terminal);
    assert_eq!(get_claim(&router).await, observed);
    assert_eq!(authority(&repository), before_get);
    for uri in [
        format!("/v1/worktrees/{WORKTREE}/claims/absent"),
        format!("/v1/worktrees/absent/claims/{SESSION}"),
    ] {
        assert_eq!(
            request(&router, "GET", &uri, None).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(authority(&repository), before_get);
    }
    assert_no_native_effects(&path, &provider, &marker);
    let child_path = path.clone();
    let parent = std::process::id();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "http::tests::legacy_worktree_claim_generation::same_value_reclaim_child",
                "--nocapture",
            ])
            .env(CHILD_DB, child_path)
            .env(CHILD_PARENT, parent.to_string());
        if terminal {
            command.env(CHILD_TERMINAL, "1");
        } else {
            command.env_remove(CHILD_TERMINAL);
        }
        command.output().expect("independent HTTP-lifetime writer")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    let newer = repository
        .get_managed_worktree_claim(WORKTREE, SESSION)
        .unwrap()
        .unwrap();
    let g2 = newer.claim_generation.unwrap();
    assert_eq!(g2, g1.checked_add(1).unwrap());
    assert_eq!(raw_claim(&newer), raw_claim(&observed));
    let events_before = repository.list_runtime_events(None, None, 32).unwrap();
    let mut kinds = vec!["worktree.claimed", "worktree.released", "worktree.claimed"];
    if terminal {
        kinds.push("worktree.released");
    }
    assert_eq!(
        events_before
            .iter()
            .map(|e| e.kind.as_str())
            .collect::<Vec<_>>(),
        kinds
    );
    let after_child = authority(&repository);
    assert_no_native_effects(&path, &provider, &marker);
    drop(router);
    drop(runtime);
    drop(app);
    drop(store);
    let reopened: Arc<dyn RuntimeStore> = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    }));
    let (router, _runtime, _app) = super::support_router::assemble_test_router(
        temp.path(),
        reopened,
        registry,
        TOKEN,
        TeamMcpPolicy::default(),
    )
    .await;
    assert_eq!(authority(&repository), after_child);
    assert_eq!(get_claim(&router).await, newer);
    assert_eq!(get_claim(&router).await, newer);
    assert_eq!(authority(&repository), after_child);
    assert_no_native_effects(&path, &provider, &marker);
    let (status, bytes) = request(&router, "POST", &release_uri, Some(&saved_g1_body)).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(authority(&repository), after_child);
    assert_no_native_effects(&path, &provider, &marker);
    for (body, status_expected) in [
        (
            json!({"session_id":SESSION,"cleanup_if_last_claim":false}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            json!({"session_id":SESSION,"expected_claim_generation":null,"cleanup_if_last_claim":false}),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            json!({"session_id":SESSION,"expected_claim_generation":0,"cleanup_if_last_claim":false}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"session_id":SESSION,"expected_claim_generation":(i64::MAX as u64)+1,"cleanup_if_last_claim":false}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let body = body.to_string();
        let (status, bytes) = request(&router, "POST", &release_uri, Some(&body)).await;
        assert_eq!(
            status,
            status_expected,
            "body={body} response={}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(authority(&repository), after_child);
        assert_no_native_effects(&path, &provider, &marker);
    }
    let fresh = get_claim(&router).await;
    assert_eq!(fresh, newer);
    assert_eq!(authority(&repository), after_child);
    let fresh_body = release_body(fresh.claim_generation.unwrap());
    let (status, bytes) = request(&router, "POST", &release_uri, Some(&fresh_body)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let first: Value = serde_json::from_slice(&bytes).unwrap();
    let released: ManagedWorktreeClaimRecord =
        serde_json::from_value(first["released_claim"].clone()).unwrap();
    assert_eq!(released.claim_generation, Some(g2));
    assert!(released.released_at.is_some());
    assert_eq!(released.worktree_id, newer.worktree_id);
    assert_eq!(released.session_id, newer.session_id);
    assert_eq!(released.claim_role, newer.claim_role);
    assert_eq!(released.created_at, newer.created_at);
    assert_eq!(first["active_claim_count"], 0);
    assert!(first["cleanup"].is_null());
    let events = repository.list_runtime_events(None, None, 32).unwrap();
    if terminal {
        assert_eq!(released, newer);
        assert_eq!(authority(&repository), after_child);
    } else {
        assert_eq!(events.len(), events_before.len() + 1);
        assert_eq!(&events[..events_before.len()], events_before.as_slice());
        assert_eq!(events.last().unwrap().kind, "worktree.released");
        assert_eq!(
            events.last().unwrap().payload["claim"],
            serde_json::to_value(&released).unwrap()
        );
    }
    let settled = authority(&repository);
    let (status, bytes) = request(&router, "POST", &release_uri, Some(&fresh_body)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), first);
    assert_eq!(authority(&repository), settled);
    let (status, bytes) = request(&router, "POST", &release_uri, Some(&saved_g1_body)).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(authority(&repository), settled);
    assert_eq!(get_claim(&router).await, released);
    assert_eq!(authority(&repository), settled);
    assert_no_native_effects(&path, &provider, &marker);
}
#[tokio::test]
async fn saved_http_release_cannot_revoke_same_value_active_lifetime_after_restart() {
    assert_http_lifetime(false).await;
}
#[tokio::test]
async fn saved_http_release_cannot_replay_same_value_terminal_lifetime_after_restart() {
    assert_http_lifetime(true).await;
}
