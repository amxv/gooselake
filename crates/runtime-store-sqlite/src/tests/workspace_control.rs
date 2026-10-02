use super::*;
use runtime_core::{
    prepare_workspace_interrupt, prepare_workspace_lead_transition, prepare_workspace_registration,
    OperationActor, ProviderKind, WorkspaceAgentLifecycleState, WorkspaceAgentProfile,
    WorkspaceAgentRecord, WorkspaceAgentRecreationPolicy, WorkspaceInterruptAdmission,
    WorkspaceLeadTransitionRequest, WorkspaceRegisterRequest,
};
use std::sync::{Arc, Barrier};

fn registered_workspace(
    repository: &SqliteRuntimeRepository,
    root: &std::path::Path,
    name: &str,
) -> runtime_core::WorkspaceRecord {
    repository
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: root.to_string_lossy().into_owned(),
                    display_name: Some(name.to_string()),
                },
                OperationActor::operator("workspace-control-test"),
                None,
            )
            .expect("workspace command"),
        )
        .expect("workspace registration")
        .workspace
}

fn create_agent(
    repository: &SqliteRuntimeRepository,
    workspace: &runtime_core::WorkspaceRecord,
    session_id: &str,
    alias: &str,
    title: &str,
    active_turn_id: Option<&str>,
) -> (SessionRecord, WorkspaceAgentRecord) {
    let session = SessionRecord {
        id: session_id.to_string(),
        provider: "codex".to_string(),
        status: if active_turn_id.is_some() {
            "turn_running".to_string()
        } else {
            "ready".to_string()
        },
        cwd: Some(workspace.canonical_root.clone()),
        model: Some("gpt-test".to_string()),
        permission_mode: Some("workspace_write".to_string()),
        system_prompt: None,
        metadata: serde_json::json!({}),
        provider_session_ref: Some(format!("provider-{session_id}")),
        canonical_provider_session_ref: None,
        active_turn_id: active_turn_id.map(str::to_string),
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
            title: Some(title.to_string()),
            title_provenance: "user_supplied".to_string(),
            added_by: "operator".to_string(),
            creator_session_id: None,
            creator_compaction_subscription: "auto".to_string(),
            joined_at: 100,
        },
        recreation_policy: WorkspaceAgentRecreationPolicy {
            provider: ProviderKind::Codex,
            model: Some("gpt-test".to_string()),
            permission_intent: Some("workspace_write".to_string()),
            setting_sources_intent: Vec::new(),
            system_prompt: None,
            allowed_tools: Vec::new(),
            disallowed_tools: Vec::new(),
            authoritative_cwd: workspace.canonical_root.clone(),
            harness_version_slot: None,
        },
        provider_session_ref: session.provider_session_ref.clone(),
        canonical_provider_session_ref: None,
        metadata: serde_json::json!({}),
        archived_at: None,
        archive_reason: None,
        revision: 0,
        created_at: 100,
        updated_at: 100,
    };
    repository
        .create_workspace_agent(&session, &agent)
        .expect("workspace agent");
    (session, agent)
}

fn lead_command(
    workspace_id: &str,
    lead_agent_id: Option<&str>,
    expected_revision: u64,
    key: &str,
) -> runtime_core::WorkspaceLeadTransitionCommand {
    prepare_workspace_lead_transition(
        workspace_id,
        WorkspaceLeadTransitionRequest {
            lead_agent_id: lead_agent_id.map(str::to_string),
            expected_revision,
        },
        OperationActor::operator("workspace-control-test"),
        Some(key.to_string()),
    )
    .expect("lead command")
}

#[test]
fn workspace_lead_set_reassign_clear_archive_restore_preserves_titles_and_never_auto_elects() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("root");
    let workspace = registered_workspace(&repository, &root, "Lead Test");
    let (mut first_session, first) = create_agent(
        &repository,
        &workspace,
        "sess_first",
        "first-agent",
        "Architect",
        None,
    );
    let (_second_session, second) = create_agent(
        &repository,
        &workspace,
        "sess_second",
        "second-agent",
        "Builder",
        None,
    );

    assert_eq!(workspace.lead_agent_id, None);
    let set = repository
        .transition_workspace_lead(&lead_command(
            &workspace.workspace_id,
            Some(&first.agent_id),
            0,
            "lead-set",
        ))
        .expect("set lead");
    assert_eq!(set.workspace.lead_agent_id.as_deref(), Some("sess_first"));
    assert_eq!(set.workspace.revision, 1);

    let reassigned = repository
        .transition_workspace_lead(&lead_command(
            &workspace.workspace_id,
            Some(&second.agent_id),
            1,
            "lead-reassign",
        ))
        .expect("reassign lead");
    assert_eq!(
        reassigned.workspace.lead_agent_id.as_deref(),
        Some("sess_second")
    );
    assert_eq!(reassigned.workspace.revision, 2);

    let cleared = repository
        .transition_workspace_lead(&lead_command(
            &workspace.workspace_id,
            None,
            2,
            "lead-clear",
        ))
        .expect("clear lead");
    assert_eq!(cleared.workspace.lead_agent_id, None);
    assert_eq!(cleared.workspace.revision, 3);

    let first_after_roles = repository
        .get_workspace_agent(&workspace.workspace_id, &first.agent_id)
        .expect("first query")
        .expect("first agent");
    let second_after_roles = repository
        .get_workspace_agent(&workspace.workspace_id, &second.agent_id)
        .expect("second query")
        .expect("second agent");
    assert_eq!(
        first_after_roles.profile.title.as_deref(),
        Some("Architect")
    );
    assert_eq!(first_after_roles.profile.title_provenance, "user_supplied");
    assert_eq!(second_after_roles.profile.title.as_deref(), Some("Builder"));

    let set_again = repository
        .transition_workspace_lead(&lead_command(
            &workspace.workspace_id,
            Some(&first.agent_id),
            3,
            "lead-set-again",
        ))
        .expect("set lead again");
    assert_eq!(set_again.workspace.revision, 4);

    first_session.status = "closed".to_string();
    first_session.closed_at = Some(200);
    first_session.updated_at = 200;
    repository
        .set_workspace_agent_lifecycle(
            &first_session,
            &first.agent_id,
            WorkspaceAgentLifecycleState::Archived,
            Some("archive lead"),
            200,
        )
        .expect("archive lead");
    let after_archive = repository
        .get_workspace(&workspace.workspace_id)
        .expect("workspace query")
        .expect("workspace");
    assert_eq!(after_archive.lead_agent_id, None);
    assert_eq!(after_archive.revision, 5);
    let archived_lead = repository.transition_workspace_lead(&lead_command(
        &workspace.workspace_id,
        Some(&first.agent_id),
        5,
        "archived-cannot-lead",
    ));
    assert!(matches!(
        archived_lead,
        Err(runtime_core::RuntimeError::InvalidState(_))
    ));

    first_session.status = "ready".to_string();
    first_session.closed_at = None;
    first_session.updated_at = 300;
    repository
        .set_workspace_agent_lifecycle(
            &first_session,
            &first.agent_id,
            WorkspaceAgentLifecycleState::Active,
            None,
            300,
        )
        .expect("restore former lead");
    let after_restore = repository
        .get_workspace(&workspace.workspace_id)
        .expect("workspace query")
        .expect("workspace");
    assert_eq!(after_restore.lead_agent_id, None);
    assert_eq!(after_restore.revision, 5);
}

#[test]
fn workspace_lead_db_guards_reject_cross_workspace_and_concurrent_cas_converges() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root_one = temp_dir.path().join("one");
    let root_two = temp_dir.path().join("two");
    std::fs::create_dir_all(&root_one).expect("root one");
    std::fs::create_dir_all(&root_two).expect("root two");
    let one = registered_workspace(&repository, &root_one, "One");
    let two = registered_workspace(&repository, &root_two, "Two");
    let (_, first) = create_agent(&repository, &one, "sess_one_a", "one-a", "A", None);
    let (_, second) = create_agent(&repository, &one, "sess_one_b", "one-b", "B", None);
    let (_, outsider) = create_agent(&repository, &two, "sess_two", "two-a", "C", None);

    let invalid = repository.transition_workspace_lead(&lead_command(
        &one.workspace_id,
        Some(&outsider.agent_id),
        0,
        "cross-workspace",
    ));
    assert!(matches!(
        invalid,
        Err(runtime_core::RuntimeError::InvalidState(_))
    ));

    let connection = open_connection(&repository.database_path).expect("connection");
    let raw_invalid = connection.execute(
        "UPDATE workspaces SET lead_agent_id = ?2 WHERE workspace_id = ?1",
        params![one.workspace_id, outsider.agent_id],
    );
    assert!(
        raw_invalid.is_err(),
        "database trigger must reject cross-workspace lead"
    );
    drop(connection);

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for (agent_id, key) in [
        (first.agent_id.clone(), "concurrent-first"),
        (second.agent_id.clone(), "concurrent-second"),
    ] {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let workspace_id = one.workspace_id.clone();
        handles.push(std::thread::spawn(move || {
            let command = lead_command(&workspace_id, Some(&agent_id), 0, key);
            barrier.wait();
            repository.transition_workspace_lead(&command)
        }));
    }
    let results = handles
        .into_iter()
        .map(|handle| handle.join().expect("lead thread"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(runtime_core::RuntimeError::Conflict(_))))
            .count(),
        1
    );
    let final_workspace = repository
        .get_workspace(&one.workspace_id)
        .expect("workspace query")
        .expect("workspace");
    assert_eq!(final_workspace.revision, 1);
    assert!(matches!(
        final_workspace.lead_agent_id.as_deref(),
        Some("sess_one_a") | Some("sess_one_b")
    ));
}

#[test]
fn workspace_interrupt_snapshots_roster_and_replays_exact_terminal_result() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("root");
    let workspace = registered_workspace(&repository, &root, "Interrupt Test");
    let (_, active) = create_agent(
        &repository,
        &workspace,
        "sess_active",
        "active-agent",
        "Active",
        Some("turn_live"),
    );
    let (_, idle) = create_agent(
        &repository,
        &workspace,
        "sess_idle",
        "idle-agent",
        "Idle",
        None,
    );
    let command = prepare_workspace_interrupt(
        &workspace.workspace_id,
        OperationActor::operator("workspace-control-test"),
        Some("interrupt-once".to_string()),
    )
    .expect("interrupt command");
    let plan = match repository
        .begin_workspace_interrupt(&command)
        .expect("begin interrupt")
    {
        WorkspaceInterruptAdmission::Execute(plan) => plan,
        WorkspaceInterruptAdmission::Replay(_) => panic!("first admission cannot replay"),
    };
    assert_eq!(
        plan.targets,
        vec![runtime_core::WorkspaceInterruptTarget {
            agent_id: active.agent_id.clone(),
            turn_id: "turn_live".to_string(),
        }]
    );
    repository
        .mark_workspace_interrupt_started(&plan.operation_id, &active.agent_id, "turn_live", 200)
        .expect("mark started");
    repository
        .finalize_workspace_interrupt_effect(
            &plan.operation_id,
            &active.agent_id,
            "turn_live",
            true,
            "test",
            201,
        )
        .expect("finalize active");
    let completed = repository
        .complete_workspace_interrupt(&plan.operation_id, 202)
        .expect("complete interrupt");
    assert_eq!(completed.interrupted_agent_ids, vec![active.agent_id]);
    assert_eq!(completed.skipped_agent_ids, vec![idle.agent_id]);

    let restarted = SqliteRuntimeRepository::new(repository.database_path.clone());
    let replay_command = prepare_workspace_interrupt(
        &workspace.workspace_id,
        OperationActor::operator("workspace-control-test"),
        Some("interrupt-once".to_string()),
    )
    .expect("replay command");
    let replay = restarted
        .begin_workspace_interrupt(&replay_command)
        .expect("replay admission");
    assert_eq!(
        replay,
        WorkspaceInterruptAdmission::Replay(completed.clone())
    );
    let details = restarted
        .get_operation(&completed.operation_id)
        .expect("operation query")
        .expect("operation");
    assert_eq!(details.transitions.len(), 2);
    assert_eq!(details.effects.len(), 2);
    assert!(details.claims.is_empty());
}
