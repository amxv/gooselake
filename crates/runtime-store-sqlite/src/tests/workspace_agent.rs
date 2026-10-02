use super::*;
use runtime_core::{
    prepare_workspace_registration, OperationActor, ProviderKind, RuntimeError,
    WorkspaceAgentLifecycleState, WorkspaceAgentProfile, WorkspaceAgentRecord,
    WorkspaceAgentRecreationPolicy, WorkspaceRegisterRequest,
};

fn registered_workspace(
    repository: &SqliteRuntimeRepository,
    root: &std::path::Path,
) -> runtime_core::WorkspaceRecord {
    let command = prepare_workspace_registration(
        WorkspaceRegisterRequest {
            canonical_root: root.to_string_lossy().into_owned(),
            display_name: Some("Agent Test".to_string()),
        },
        OperationActor::operator("workspace-agent-test"),
        None,
    )
    .expect("workspace command");
    repository
        .register_workspace(&command)
        .expect("register workspace")
        .workspace
}

fn workspace_agent_fixture(
    workspace: &runtime_core::WorkspaceRecord,
    session_id: &str,
    alias: &str,
) -> (SessionRecord, WorkspaceAgentRecord) {
    let session = SessionRecord {
        id: session_id.to_string(),
        provider: "codex".to_string(),
        status: "ready".to_string(),
        cwd: Some(workspace.canonical_root.clone()),
        model: Some("gpt-test".to_string()),
        permission_mode: Some("workspace_write".to_string()),
        system_prompt: Some("persist me".to_string()),
        metadata: serde_json::json!({"source":"workspace-agent-test"}),
        provider_session_ref: Some(format!("provider-{session_id}")),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 100,
        updated_at: 100,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    };
    let agent = WorkspaceAgentRecord {
        agent_id: session_id.to_string(),
        workspace_id: workspace.workspace_id.clone(),
        alias: alias.to_string(),
        lifecycle_state: WorkspaceAgentLifecycleState::Active,
        profile: WorkspaceAgentProfile {
            title: Some("Builder".to_string()),
            title_provenance: "v2_create_request".to_string(),
            added_by: "runtime_operator".to_string(),
            creator_session_id: None,
            creator_compaction_subscription: "auto".to_string(),
            joined_at: 100,
        },
        recreation_policy: WorkspaceAgentRecreationPolicy {
            provider: ProviderKind::Codex,
            model: Some("gpt-test".to_string()),
            permission_intent: Some("workspace_write".to_string()),
            setting_sources_intent: vec!["user".to_string(), "project".to_string()],
            system_prompt: Some("persist me".to_string()),
            allowed_tools: vec!["read".to_string()],
            disallowed_tools: vec!["danger".to_string()],
            authoritative_cwd: workspace.canonical_root.clone(),
            harness_version_slot: Some("v1".to_string()),
        },
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: None,
        metadata: session.metadata.clone(),
        archived_at: None,
        archive_reason: None,
        revision: 0,
        created_at: 100,
        updated_at: 100,
    };
    (session, agent)
}

#[test]
fn workspace_agent_create_is_atomic_and_round_trips_exact_policy() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "sess_atomic", "steady-otter-a1");

    let connection = open_connection(&repository.database_path).expect("connection");
    connection
        .execute_batch(
            "CREATE TRIGGER fail_workspace_agent_authority
             BEFORE INSERT ON workspace_agents
             BEGIN
               SELECT RAISE(ABORT, 'forced workspace agent failure');
             END;",
        )
        .expect("failure trigger");
    drop(connection);

    assert!(repository.create_workspace_agent(&session, &agent).is_err());
    let connection = open_connection(&repository.database_path).expect("after failure");
    for table in [
        "sessions",
        "workspace_session_ownership",
        "workspace_agent_profiles",
        "workspace_agents",
    ] {
        let count: i64 = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 0, "{table} must roll back with authority insert");
    }
    connection
        .execute_batch("DROP TRIGGER fail_workspace_agent_authority")
        .expect("drop trigger");
    drop(connection);

    repository
        .create_workspace_agent(&session, &agent)
        .expect("create workspace agent");
    assert_eq!(
        repository
            .get_workspace_agent(&workspace.workspace_id, &session.id)
            .expect("get")
            .expect("agent"),
        agent
    );
    assert_eq!(
        repository
            .list_workspace_agents(
                &workspace.workspace_id,
                Some(WorkspaceAgentLifecycleState::Active)
            )
            .expect("list active"),
        vec![agent]
    );
}

#[test]
fn workspace_agent_alias_is_global_immutable_and_concurrency_safe() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (session_one, agent_one) =
        workspace_agent_fixture(&workspace, "sess_alias_one", "shared-friendly-alias");
    let (session_two, agent_two) =
        workspace_agent_fixture(&workspace, "sess_alias_two", "shared-friendly-alias");

    let repo_one = repository.clone();
    let repo_two = repository.clone();
    let first =
        std::thread::spawn(move || repo_one.create_workspace_agent(&session_one, &agent_one));
    let second =
        std::thread::spawn(move || repo_two.create_workspace_agent(&session_two, &agent_two));
    let results = [
        first.join().expect("first thread"),
        second.join().expect("second thread"),
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert!(results
        .iter()
        .filter_map(|result| result.as_ref().err())
        .any(|error| {
            matches!(error, RuntimeError::Conflict(message) if message.contains("alias"))
        }));

    let agents = repository
        .list_workspace_agents(&workspace.workspace_id, None)
        .expect("agents");
    assert_eq!(agents.len(), 1);
    let connection = open_connection(&repository.database_path).expect("connection");
    let mutation = connection.execute(
        "UPDATE workspace_agents SET identity_alias = 'mutated-alias' WHERE session_id = ?1",
        params![agents[0].agent_id],
    );
    assert!(mutation.is_err(), "friendly alias must be immutable");
}

#[test]
fn archive_and_restore_preserve_agent_identity_and_policy() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "sess_lifecycle", "quiet-raven-b2");
    repository
        .create_workspace_agent(&session, &agent)
        .expect("create agent");

    session.status = "closed".to_string();
    session.closed_at = Some(200);
    session.updated_at = 200;
    let archived = repository
        .set_workspace_agent_lifecycle(
            &session,
            &session.id,
            WorkspaceAgentLifecycleState::Archived,
            Some("test archive"),
            200,
        )
        .expect("archive");
    assert_eq!(archived.agent_id, agent.agent_id);
    assert_eq!(archived.alias, agent.alias);
    assert_eq!(archived.recreation_policy, agent.recreation_policy);
    assert_eq!(
        archived.lifecycle_state,
        WorkspaceAgentLifecycleState::Archived
    );

    session.status = "ready".to_string();
    session.closed_at = None;
    session.updated_at = 300;
    session.provider_session_ref = Some("provider-restored".to_string());
    let restored = repository
        .set_workspace_agent_lifecycle(
            &session,
            &session.id,
            WorkspaceAgentLifecycleState::Active,
            None,
            300,
        )
        .expect("restore");
    assert_eq!(restored.agent_id, agent.agent_id);
    assert_eq!(restored.alias, agent.alias);
    assert_eq!(restored.recreation_policy, agent.recreation_policy);
    assert_eq!(
        restored.provider_session_ref.as_deref(),
        Some("provider-restored")
    );
    assert_eq!(
        restored.lifecycle_state,
        WorkspaceAgentLifecycleState::Active
    );
    assert!(restored.archived_at.is_none());
    assert!(restored.archive_reason.is_none());
    assert_eq!(restored.revision, 2);
}
