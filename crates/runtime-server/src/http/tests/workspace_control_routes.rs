use super::*;

#[tokio::test]
async fn workspace_membership_mutations_follow_leadless_and_configured_non_lead_policy() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let root = temp_dir.path().join("membership-workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    }));
    store.initialize().await.expect("store initialize");
    let workspace = store
        .register_workspace(
            &runtime_core::prepare_workspace_registration(
                runtime_core::WorkspaceRegisterRequest {
                    canonical_root: root.to_string_lossy().into_owned(),
                    display_name: Some("Membership Workspace".to_string()),
                },
                runtime_core::OperationActor::operator("runtime_operator"),
                None,
            )
            .expect("workspace command"),
        )
        .expect("workspace registration")
        .workspace;

    let provider = Arc::new(TestProvider::default());
    let mut registry = runtime_core::ProviderRegistry::new();
    registry.register(provider).expect("provider register");
    let manager = Arc::new(
        runtime_core::RuntimeSessionManager::new(store.clone(), Arc::new(registry), 128)
            .expect("runtime manager"),
    );
    manager.recover_startup().await.expect("startup recovery");

    let caller = manager
        .create_workspace_agent(
            &workspace.workspace_id,
            membership_agent_request(&root, "Caller"),
            "runtime_operator",
        )
        .await
        .expect("caller create");
    let leadless_added = manager
        .create_workspace_agent_as_member(
            &workspace.workspace_id,
            membership_agent_request(&root, "Leadless child"),
            &caller.agent_id,
            runtime_core::WorkspaceMembershipPolicy::default(),
        )
        .await
        .expect("leadless member add");
    let leadless_removed = manager
        .archive_workspace_agent_as_member(
            &workspace.workspace_id,
            &leadless_added.agent_id,
            &caller.agent_id,
            Some("leadless member remove"),
            runtime_core::WorkspaceMembershipPolicy::default(),
        )
        .await
        .expect("leadless member remove");
    assert_eq!(
        leadless_removed.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );

    let peer = manager
        .create_workspace_agent(
            &workspace.workspace_id,
            membership_agent_request(&root, "Peer"),
            "runtime_operator",
        )
        .await
        .expect("peer create");
    store
        .transition_workspace_lead(
            &runtime_core::prepare_workspace_lead_transition(
                &workspace.workspace_id,
                runtime_core::WorkspaceLeadTransitionRequest {
                    lead_agent_id: Some(caller.agent_id.clone()),
                    expected_revision: 0,
                },
                runtime_core::OperationActor::operator("runtime_operator"),
                Some("membership-lead".to_string()),
            )
            .expect("lead command"),
        )
        .expect("set lead");

    let denied = manager
        .create_workspace_agent_as_member(
            &workspace.workspace_id,
            membership_agent_request(&root, "Denied child"),
            &peer.agent_id,
            runtime_core::WorkspaceMembershipPolicy::default(),
        )
        .await;
    assert!(matches!(
        denied,
        Err(runtime_core::RuntimeError::InvalidState(_))
    ));

    let allowed = manager
        .create_workspace_agent_as_member(
            &workspace.workspace_id,
            membership_agent_request(&root, "Allowed child"),
            &peer.agent_id,
            runtime_core::WorkspaceMembershipPolicy {
                non_lead_can_add_members: true,
                non_lead_can_remove_members: false,
            },
        )
        .await
        .expect("configured non-lead add");

    let denied_remove = manager
        .archive_workspace_agent_as_member(
            &workspace.workspace_id,
            &allowed.agent_id,
            &peer.agent_id,
            Some("denied remove"),
            runtime_core::WorkspaceMembershipPolicy::default(),
        )
        .await;
    assert!(matches!(
        denied_remove,
        Err(runtime_core::RuntimeError::InvalidState(_))
    ));
    let allowed_remove = manager
        .archive_workspace_agent_as_member(
            &workspace.workspace_id,
            &allowed.agent_id,
            &peer.agent_id,
            Some("configured remove"),
            runtime_core::WorkspaceMembershipPolicy {
                non_lead_can_add_members: false,
                non_lead_can_remove_members: true,
            },
        )
        .await
        .expect("configured non-lead remove");
    assert_eq!(
        allowed_remove.lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );
}

#[tokio::test]
async fn workspace_lead_routes_are_revisioned_operator_authority_and_archive_clears_lead() {
    let (router, token, temp_dir) = build_test_router().await;
    let root = temp_dir.path().join("lead-workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let workspace = register_control_workspace(&router, &token, &root, "Lead Workspace").await;
    let first =
        create_control_agent(&router, &token, &workspace.workspace_id, &root, "First").await;
    let second =
        create_control_agent(&router, &token, &workspace.workspace_id, &root, "Second").await;

    assert_eq!(workspace.lead_agent_id, None);
    let first_set = set_lead(
        &router,
        &token,
        &workspace.workspace_id,
        Some(first.agent_id.as_str()),
        0,
        "lead-first",
    )
    .await;
    assert_eq!(
        first_set.workspace.lead_agent_id.as_deref(),
        Some(first.agent_id.as_str())
    );
    assert_eq!(first_set.workspace.revision, 1);

    let replay = set_lead(
        &router,
        &token,
        &workspace.workspace_id,
        Some(first.agent_id.as_str()),
        0,
        "lead-first",
    )
    .await;
    assert_eq!(replay, first_set);

    let reassigned = set_lead(
        &router,
        &token,
        &workspace.workspace_id,
        Some(second.agent_id.as_str()),
        1,
        "lead-second",
    )
    .await;
    assert_eq!(
        reassigned.workspace.lead_agent_id.as_deref(),
        Some(second.agent_id.as_str())
    );
    assert_eq!(reassigned.workspace.revision, 2);

    let cleared = set_lead(
        &router,
        &token,
        &workspace.workspace_id,
        None,
        2,
        "lead-clear",
    )
    .await;
    assert_eq!(cleared.workspace.lead_agent_id, None);
    assert_eq!(cleared.workspace.revision, 3);

    let first_title =
        get_control_agent(&router, &token, &workspace.workspace_id, &first.agent_id).await;
    let second_title =
        get_control_agent(&router, &token, &workspace.workspace_id, &second.agent_id).await;
    assert_eq!(first_title.profile.title.as_deref(), Some("First"));
    assert_eq!(second_title.profile.title.as_deref(), Some("Second"));
    assert_eq!(first_title.profile.title_provenance, "v2_create_request");
    assert_eq!(second_title.profile.title_provenance, "v2_create_request");

    let set_before_archive = set_lead(
        &router,
        &token,
        &workspace.workspace_id,
        Some(first.agent_id.as_str()),
        3,
        "lead-before-archive",
    )
    .await;
    assert_eq!(set_before_archive.workspace.revision, 4);

    let archive = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/workspaces/{}/agents/{}/archive",
                    workspace.workspace_id, first.agent_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"reason":"workspace archive"}"#))
                .expect("archive request"),
        )
        .await
        .expect("archive response");
    assert_eq!(archive.status(), StatusCode::OK);

    let after_archive = get_control_workspace(&router, &token, &workspace.workspace_id).await;
    assert_eq!(after_archive.lead_agent_id, None);
    assert_eq!(after_archive.revision, 5);

    let restore = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/workspaces/{}/agents/{}/restore",
                    workspace.workspace_id, first.agent_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("restore request"),
        )
        .await
        .expect("restore response");
    assert_eq!(restore.status(), StatusCode::OK);
    let after_restore = get_control_workspace(&router, &token, &workspace.workspace_id).await;
    assert_eq!(after_restore.lead_agent_id, None);
    assert_eq!(after_restore.revision, 5);

    let stale = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{}/lead", workspace.workspace_id))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", "stale-lead")
                .body(Body::from(
                    serde_json::json!({
                        "lead_agent_id": second.agent_id,
                        "expected_revision": 4,
                    })
                    .to_string(),
                ))
                .expect("stale request"),
        )
        .await
        .expect("stale response");
    assert_eq!(stale.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn workspace_interrupt_targets_only_active_turns_in_authoritative_roster_and_replays_once() {
    let (router, token, temp_dir) = build_test_router().await;
    let root = temp_dir.path().join("interrupt-workspace");
    let outside_root = temp_dir.path().join("outside-workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    std::fs::create_dir_all(&outside_root).expect("outside root");
    let workspace = register_control_workspace(&router, &token, &root, "Interrupt Workspace").await;
    let outside =
        register_control_workspace(&router, &token, &outside_root, "Outside Workspace").await;

    let active = create_control_agent_with_permission(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Active",
        "require_approval",
    )
    .await;
    let idle = create_control_agent(&router, &token, &workspace.workspace_id, &root, "Idle").await;
    let outside_active = create_control_agent_with_permission(
        &router,
        &token,
        &outside.workspace_id,
        &outside_root,
        "Outside",
        "require_approval",
    )
    .await;

    let active_turn = send_waiting_turn(&router, &token, &active.agent_id).await;
    let outside_turn = send_waiting_turn(&router, &token, &outside_active.agent_id).await;
    assert_eq!(active_turn.status, "waiting_for_approval");
    assert_eq!(outside_turn.status, "waiting_for_approval");

    let first = interrupt_workspace(
        &router,
        &token,
        &workspace.workspace_id,
        "interrupt-regression",
    )
    .await;
    assert_eq!(first.interrupted_agent_ids, vec![active.agent_id.clone()]);
    assert_eq!(first.skipped_agent_ids, vec![idle.agent_id.clone()]);

    let replay = interrupt_workspace(
        &router,
        &token,
        &workspace.workspace_id,
        "interrupt-regression",
    )
    .await;
    assert_eq!(replay, first);

    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    });
    let active_events = store
        .list_runtime_events(
            Some((
                runtime_core::RuntimeEventScope::Session,
                active.agent_id.as_str(),
            )),
            None,
            100,
        )
        .expect("active events");
    assert_eq!(
        active_events
            .iter()
            .filter(|event| event.kind == "turn.interrupt_requested")
            .count(),
        1,
        "idempotent replay must not issue a second interrupt request"
    );
    let outside_events = store
        .list_runtime_events(
            Some((
                runtime_core::RuntimeEventScope::Session,
                outside_active.agent_id.as_str(),
            )),
            None,
            100,
        )
        .expect("outside events");
    assert_eq!(
        outside_events
            .iter()
            .filter(|event| event.kind == "turn.interrupt_requested")
            .count(),
        0,
        "workspace interrupt must never escape the workspace roster"
    );
    let hydrated = store.hydrate_runtime_state().expect("hydrate");
    let outside_session = hydrated
        .sessions
        .iter()
        .find(|session| session.id == outside_active.agent_id)
        .expect("outside session");
    assert_eq!(
        outside_session.active_turn_id.as_deref(),
        Some(outside_turn.turn_id.as_str())
    );

    let operation = store
        .get_operation(&first.operation_id)
        .expect("operation query")
        .expect("interrupt operation");
    assert_eq!(operation.transitions.len(), 2);
    assert!(operation.claims.is_empty());
}

async fn register_control_workspace(
    router: &Router,
    token: &str,
    root: &Path,
    name: &str,
) -> runtime_core::WorkspaceRecord {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/workspaces")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "canonical_root": root,
                        "display_name": name,
                    })
                    .to_string(),
                ))
                .expect("workspace request"),
        )
        .await
        .expect("workspace response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice::<runtime_core::WorkspaceRegisterResponse>(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("workspace body"),
    )
    .expect("workspace json")
    .workspace
}

async fn get_control_workspace(
    router: &Router,
    token: &str,
    workspace_id: &str,
) -> runtime_core::WorkspaceRecord {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("workspace get request"),
        )
        .await
        .expect("workspace get response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("workspace get body"),
    )
    .expect("workspace get json")
}

async fn create_control_agent(
    router: &Router,
    token: &str,
    workspace_id: &str,
    root: &Path,
    title: &str,
) -> runtime_core::WorkspaceAgentRecord {
    create_control_agent_with_permission(
        router,
        token,
        workspace_id,
        root,
        title,
        "workspace_write",
    )
    .await
}

async fn create_control_agent_with_permission(
    router: &Router,
    token: &str,
    workspace_id: &str,
    root: &Path,
    title: &str,
    permission_intent: &str,
) -> runtime_core::WorkspaceAgentRecord {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{workspace_id}/agents"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "provider": "codex",
                        "model": "test-model",
                        "permission_intent": permission_intent,
                        "cwd": root,
                        "title": title,
                    })
                    .to_string(),
                ))
                .expect("agent create request"),
        )
        .await
        .expect("agent create response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("agent create body"),
    )
    .expect("agent json")
}

async fn get_control_agent(
    router: &Router,
    token: &str,
    workspace_id: &str,
    agent_id: &str,
) -> runtime_core::WorkspaceAgentRecord {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v2/workspaces/{workspace_id}/agents/{agent_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("agent get request"),
        )
        .await
        .expect("agent get response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("agent get body"),
    )
    .expect("agent get json")
}

async fn set_lead(
    router: &Router,
    token: &str,
    workspace_id: &str,
    lead_agent_id: Option<&str>,
    expected_revision: u64,
    idempotency_key: &str,
) -> runtime_core::WorkspaceLeadTransitionResponse {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{workspace_id}/lead"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", idempotency_key)
                .body(Body::from(
                    serde_json::json!({
                        "lead_agent_id": lead_agent_id,
                        "expected_revision": expected_revision,
                    })
                    .to_string(),
                ))
                .expect("lead request"),
        )
        .await
        .expect("lead response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("lead body"),
    )
    .expect("lead json")
}

async fn send_waiting_turn(
    router: &Router,
    token: &str,
    agent_id: &str,
) -> runtime_core::SendTurnAccepted {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/sessions/{agent_id}/turns"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"input":[{"type":"text","text":"hold for approval"}]}"#,
                ))
                .expect("turn request"),
        )
        .await
        .expect("turn response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("turn body"),
    )
    .expect("turn json")
}

async fn interrupt_workspace(
    router: &Router,
    token: &str,
    workspace_id: &str,
    idempotency_key: &str,
) -> runtime_core::WorkspaceInterruptResponse {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/workspaces/{workspace_id}/interrupt"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("idempotency-key", idempotency_key)
                .body(Body::empty())
                .expect("interrupt request"),
        )
        .await
        .expect("interrupt response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("interrupt body"),
    )
    .expect("interrupt json")
}

fn membership_agent_request(root: &Path, title: &str) -> runtime_core::WorkspaceAgentCreateRequest {
    runtime_core::WorkspaceAgentCreateRequest {
        provider: runtime_core::ProviderKind::Codex,
        model: Some("test-model".to_string()),
        permission_intent: runtime_core::ProviderPermissionIntent::Explicit {
            mode: "workspace_write".to_string(),
        },
        setting_sources_intent: runtime_core::ProviderSettingSourcesIntent::Isolated,
        current_preferences: runtime_core::ProviderSessionPreferences::default(),
        system_prompt: None,
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        cwd: Some(root.to_string_lossy().into_owned()),
        harness_version_slot: None,
        title: Some(title.to_string()),
        metadata: Some(serde_json::json!({})),
    }
}
