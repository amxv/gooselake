use super::*;

/// Test-owned history only: no provider request, operation receipt, event or
/// runtime cache mutation is manufactured by these helpers.
pub(crate) fn build_history_records(inputs: Vec<Value>) -> Vec<runtime_core::SessionRecord> {
    inputs
        .into_iter()
        .enumerate()
        .map(|(index, input)| {
            let input: CreateSessionInput =
                serde_json::from_value(input).expect("historical session input");
            let id = format!("sess_history_{}", index + 1);
            let (provider, native, canonical) = match input.provider {
                ProviderKind::Codex => ("codex", format!("test-thread-{id}"), None),
                ProviderKind::Claude => (
                    "claude",
                    format!("test-claude-thread-{id}"),
                    Some(format!("claude-canonical-{id}")),
                ),
                ProviderKind::Acp => (
                    "acp",
                    format!("test-acp-thread-{id}"),
                    Some(format!("test-acp-canonical-{id}")),
                ),
            };
            let at_ms = 100 + i64::try_from(index).expect("history fixture index");
            runtime_core::SessionRecord {
                id,
                provider: provider.to_string(),
                status: "ready".to_string(),
                cwd: input.cwd,
                model: input.model,
                permission_mode: input.permission_mode,
                system_prompt: None,
                metadata: input.metadata.unwrap_or_else(|| json!({})),
                provider_session_ref: Some(native),
                canonical_provider_session_ref: canonical,
                active_turn_id: None,
                worktree_id: None,
                created_at: at_ms,
                updated_at: at_ms,
                closed_at: None,
                failure_code: None,
                failure_message: None,
            }
        })
        .collect()
}

pub(crate) fn historical_session_json(session: &runtime_core::SessionRecord) -> Value {
    serde_json::to_value(session).expect("historical session json")
}

pub(crate) fn persist_historical_session(
    store: &dyn RuntimeStore,
    session: &runtime_core::SessionRecord,
) {
    assert!(store
        .get_persisted_session(&session.id)
        .expect("look up history fixture")
        .is_none());
    store
        .upsert_session(session)
        .expect("persist history fixture");
}

async fn seed_provider_state(
    state: &Mutex<TestProviderState>,
    expected_provider: &str,
    session: &runtime_core::SessionRecord,
) {
    assert_eq!(session.provider, expected_provider);
    let previous = state.lock().await.sessions.insert(
        session.id.clone(),
        TestProviderSession {
            provider_session_ref: session
                .provider_session_ref
                .clone()
                .expect("history native reference"),
            cwd: session.cwd.clone(),
            ..Default::default()
        },
    );
    assert!(previous.is_none(), "duplicate history fixture");
}

impl TestProvider {
    pub(crate) async fn seed_historical_session(&self, session: &runtime_core::SessionRecord) {
        seed_provider_state(&self.state, "codex", session).await;
    }
}

impl TestClaudeProvider {
    pub(crate) async fn seed_historical_session(&self, session: &runtime_core::SessionRecord) {
        seed_provider_state(&self.state, "claude", session).await;
    }
}

impl TestAcpProvider {
    pub(crate) async fn seed_historical_session(&self, session: &runtime_core::SessionRecord) {
        seed_provider_state(&self.state, "acp", session).await;
    }
}

/// Construct every consumer after the entire historical seed batch exists.
/// The closure can prepare fixture-owned directories under the temporary root.
pub(crate) async fn build_test_router_with_history(
    team_policy: TeamMcpPolicy,
    seed: impl FnOnce(&Path) -> Vec<Value>,
) -> (
    Router,
    String,
    tempfile::TempDir,
    Arc<TestProvider>,
    Arc<RuntimeSessionManager>,
    Arc<RuntimeApp>,
    Vec<runtime_core::SessionRecord>,
) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let test_provider = Arc::new(TestProvider::default());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry
        .register(test_provider.clone())
        .expect("register test provider");
    let history = build_history_records(seed(temp_dir.path()));
    for session in &history {
        test_provider.seed_historical_session(session).await;
        persist_historical_session(store.as_ref(), session);
    }
    let bearer_token = "test-token".to_string();
    let (router, runtime, app) = assemble_test_router(
        temp_dir.path(),
        store,
        Arc::new(registry),
        &bearer_token,
        team_policy,
    )
    .await;
    (
        router,
        bearer_token,
        temp_dir,
        test_provider,
        runtime,
        app,
        history,
    )
}
