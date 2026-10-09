use super::*;
use runtime_core::*;
use serde_json::json;

#[tokio::test]
async fn late_sdk_identity_is_durable_before_restart_and_reused_for_resume() {
    let harness = FakeClaudeBridgeHarness::new("late_identity");
    let provider = Arc::new(harness.provider(ClaudeGgMcpConfig::default()));
    let mut registry = ProviderRegistry::new();
    registry.register(provider).expect("register Claude");

    let temp = tempfile::tempdir().expect("temporary database and workspace");
    let root = temp.path().join("workspace");
    std::fs::create_dir(&root).expect("create workspace root");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("initialize store");
    let registered = store
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.to_string_lossy().into_owned(),
                    display_name: Some("Late SDK identity".into()),
                },
                OperationActor::operator("claude-identity-test"),
                None,
            )
            .expect("prepare workspace"),
        )
        .expect("register workspace");
    let manager = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(registry), 256).expect("start manager"),
    );
    let agent = manager
        .create_workspace_agent(
            registered.workspace.workspace_id.as_str(),
            WorkspaceAgentCreateRequest {
                provider: ProviderKind::Claude,
                model: Some("claude-sonnet-5-5".into()),
                permission_intent: ProviderPermissionIntent::InheritProviderConfiguration,
                setting_sources_intent: ProviderSettingSourcesIntent::Isolated,
                current_preferences: ProviderSessionPreferences::default(),
                system_prompt: None,
                allowed_tools: Vec::new(),
                disallowed_tools: Vec::new(),
                cwd: Some(root.to_string_lossy().into_owned()),
                worktree: None,
                harness_version_slot: Some(HARNESS_VERSION.into()),
                title: None,
                metadata: None,
            },
            "claude-identity-test",
        )
        .await
        .expect("create agent");
    assert!(agent.canonical_provider_session_ref.is_none());
    manager
        .send_turn(
            agent.agent_id.as_str(),
            SendTurnInput {
                input: vec![json!({"type":"text","text":"first SDK turn"})],
                expected_turn_id: None,
                permission_mode: None,
                projection_source: None,
                user_input_snapshot: None,
                correlation_id: None,
            },
        )
        .await
        .expect("send first turn");

    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let persisted = store
                .get_workspace_agent_by_id(agent.agent_id.as_str())
                .expect("read persisted agent")
                .expect("agent exists");
            let runtime = manager.get_session(agent.agent_id.as_str()).await.unwrap();
            if persisted.canonical_provider_session_ref.as_deref() == Some("canonical-late-1")
                && runtime.active_turn_id.is_none()
                && runtime.status == "ready"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("canonical SDK identity persisted before turn completion timed out");
    drop(manager);

    let resumed_harness = FakeClaudeBridgeHarness::new("normal");
    let resumed_provider = Arc::new(resumed_harness.provider(ClaudeGgMcpConfig::default()));
    let mut resumed_registry = ProviderRegistry::new();
    resumed_registry
        .register(resumed_provider)
        .expect("register restarted Claude");
    let restarted = Arc::new(
        RuntimeSessionManager::new(store.clone(), Arc::new(resumed_registry), 256)
            .expect("restart manager"),
    );
    let summary = restarted.recover_startup().await.expect("recover startup");
    assert_eq!(summary.resumed_sessions, 1, "{:#?}", summary.notes);
    let requests = resumed_harness.read_requests();
    let resumes = requests_for_method(&requests, "session.resume");
    assert_eq!(resumes.len(), 1);
    assert_eq!(
        resumes[0]["params"]["claudeCanonicalSessionRef"],
        "canonical-late-1"
    );
    let recovered = store
        .get_workspace_agent_by_id(agent.agent_id.as_str())
        .expect("read recovered agent")
        .expect("recovered agent exists");
    assert_eq!(
        recovered.canonical_provider_session_ref.as_deref(),
        Some("canonical-late-1")
    );
}
