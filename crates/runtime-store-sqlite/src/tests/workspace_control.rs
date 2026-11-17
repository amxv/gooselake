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
            permission_intent: runtime_core::ProviderPermissionIntent::Explicit {
                mode: "workspace_write".to_string(),
            },
            setting_sources_intent: runtime_core::ProviderSettingSourcesIntent::Isolated,
            current_preferences: runtime_core::ProviderSessionPreferences::default(),
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
fn signed_workspace_lead_ceiling_preserves_noop_and_exact_replay() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("synthetic-lead-ceiling");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root, "Signed Lead Ceiling");
    let (_session, agent) = create_agent(
        &repository,
        &workspace,
        "signed-lead-agent",
        "signed-lead-agent",
        "Signed",
        None,
    );
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspaces SET revision=?2 WHERE workspace_id=?1",
        params![workspace.workspace_id, i64::MAX],
    )
    .unwrap();

    let changed = lead_command(
        &workspace.workspace_id,
        Some(&agent.agent_id),
        i64::MAX as u64,
        "signed-lead-change-max",
    );
    assert!(repository.transition_workspace_lead(&changed).is_err());
    let (revision, kind, lead): (i64, String, Option<String>) = db
        .query_row(
            "SELECT revision,typeof(revision),lead_agent_id FROM workspaces WHERE workspace_id=?1",
            [&workspace.workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((revision, kind, lead), (i64::MAX, "integer".into(), None));
    assert!(repository
        .get_operation(&changed.operation_id)
        .unwrap()
        .is_none());

    // A legitimate no-op at MAX consumes no workspace revision and the
    // original durable key replays its exact response, not another mutation.
    let noop = lead_command(
        &workspace.workspace_id,
        None,
        i64::MAX as u64,
        "signed-lead-noop-max",
    );
    let accepted = repository.transition_workspace_lead(&noop).unwrap();
    assert_eq!(accepted.workspace.revision, i64::MAX as u64);
    assert_eq!(accepted.workspace.lead_agent_id, None);
    assert_eq!(
        repository.transition_workspace_lead(&noop).unwrap(),
        accepted
    );
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspaces WHERE workspace_id=?1",
            [&workspace.workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));

    db.execute(
        "UPDATE workspaces SET revision=?2 WHERE workspace_id=?1",
        params![workspace.workspace_id, i64::MAX - 1],
    )
    .unwrap();
    let final_change = lead_command(
        &workspace.workspace_id,
        Some(&agent.agent_id),
        (i64::MAX - 1) as u64,
        "signed-lead-change-last",
    );
    let accepted = repository.transition_workspace_lead(&final_change).unwrap();
    assert_eq!(accepted.workspace.revision, i64::MAX as u64);
    assert_eq!(
        accepted.workspace.lead_agent_id,
        Some(agent.agent_id.clone())
    );
    assert_eq!(
        repository.transition_workspace_lead(&final_change).unwrap(),
        accepted
    );
    let (revision, kind, lead): (i64, String, Option<String>) = db
        .query_row(
            "SELECT revision,typeof(revision),lead_agent_id FROM workspaces WHERE workspace_id=?1",
            [&workspace.workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (revision, kind, lead),
        (i64::MAX, "integer".into(), Some(agent.agent_id))
    );
}

#[test]
fn direct_sqlite_rebind_intent_rejects_signed_roster_revision_ceiling() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root, "Direct Rebind Revision Ceiling");
    let (_session, agent) = create_agent(
        &repository,
        &workspace,
        "signed-ceiling-agent",
        "signed-ceiling-agent",
        "Agent",
        None,
    );
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        params![i64::MAX, agent.agent_id],
    )
    .unwrap();
    let operation = runtime_core::WorkspaceAgentRebindOperation {
        operation_id: "direct-rebind-signed-max".into(),
        workspace_id: workspace.workspace_id.clone(),
        agent_id: agent.agent_id.clone(),
        idempotency_key: Some("direct-rebind-signed-max".into()),
        expected_revision: i64::MAX as u64,
        previous_worktree_id: None,
        destination_worktree_id: None,
        previous_cwd: workspace.canonical_root.clone(),
        destination_cwd: temp
            .path()
            .join("different-root")
            .to_string_lossy()
            .into_owned(),
        cleanup_previous_worktree: false,
        previous_cleanup_status: None,
        previous_cleanup_diagnostic: None,
        phase: "intended".into(),
        provider_evidence: None,
        error_code: None,
        created_at: 1,
        updated_at: 1,
    };
    let error = repository
        .begin_workspace_agent_rebind(&operation)
        .unwrap_err();
    assert!(
        matches!(error, runtime_core::RuntimeError::InvalidState(_)),
        "{error}"
    );
    assert!(repository
        .get_workspace_agent_rebind(&operation.operation_id)
        .unwrap()
        .is_none());
    assert!(repository
        .get_operation(&operation.operation_id)
        .unwrap()
        .is_none());
    assert!(!repository
        .unresolved_workspace_agent_rebind(&agent.agent_id)
        .unwrap());
}

#[test]
fn direct_sqlite_rebind_finalization_refuses_exhausted_signed_successor_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let root = temp.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let workspace = registered_workspace(&repository, &root, "Final Rebind Ceiling");
    let (session, agent) = create_agent(
        &repository,
        &workspace,
        "final-ceiling-agent",
        "final-ceiling-agent",
        "Agent",
        None,
    );
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        params![i64::MAX - 1, agent.agent_id],
    )
    .unwrap();
    let destination = temp
        .path()
        .join("different-root")
        .to_string_lossy()
        .into_owned();
    let operation = runtime_core::WorkspaceAgentRebindOperation {
        operation_id: "final-rebind-signed-max".into(),
        workspace_id: workspace.workspace_id.clone(),
        agent_id: agent.agent_id.clone(),
        idempotency_key: Some("final-rebind-signed-max".into()),
        expected_revision: (i64::MAX - 1) as u64,
        previous_worktree_id: None,
        destination_worktree_id: None,
        previous_cwd: workspace.canonical_root.clone(),
        destination_cwd: destination.clone(),
        cleanup_previous_worktree: false,
        previous_cleanup_status: None,
        previous_cleanup_diagnostic: None,
        phase: "intended".into(),
        provider_evidence: None,
        error_code: None,
        created_at: 1,
        updated_at: 1,
    };
    assert_eq!(
        repository
            .begin_workspace_agent_rebind(&operation)
            .unwrap()
            .phase,
        "intended"
    );
    // Model a pre-fix unresolved native intent that was admitted at the signed
    // maximum before the new admission fence existed. On upgrade it must not
    // be finalized, even with otherwise matching provider evidence.
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        params![i64::MAX, agent.agent_id],
    )
    .unwrap();
    let mut legacy_intent = operation.clone();
    legacy_intent.expected_revision = i64::MAX as u64;
    db.execute(
        "UPDATE workspace_agent_rebinds SET record_json=?1 WHERE operation_id=?2",
        params![
            serde_json::to_string(&legacy_intent).unwrap(),
            operation.operation_id
        ],
    )
    .unwrap();
    let mut updated_session = session.clone();
    updated_session.cwd = Some(destination.clone());
    let mut updated_policy = agent.recreation_policy.clone();
    updated_policy.authoritative_cwd = destination.clone();
    let evidence = runtime_core::ProviderWorkspaceRebindEvidence {
        binding_generation: None,
        canonical_provider_session_ref: None,
        runtime_session_id: agent.agent_id.clone(),
        cwd: destination,
        provider_session_ref: session.provider_session_ref.clone(),
    };
    let error = repository
        .finalize_workspace_agent_rebind(
            &operation.operation_id,
            &updated_session,
            &updated_policy,
            &evidence,
            2,
        )
        .unwrap_err();
    assert!(
        matches!(error, runtime_core::RuntimeError::InvalidState(_)),
        "{error}"
    );
    // The finalizer must roll everything back and retain the unresolved
    // legacy intent for external review, not forge a completed receipt.
    let (revision, cwd): (i64, String) = db
        .query_row(
            "SELECT a.revision,s.cwd FROM workspace_agents a JOIN sessions s ON s.id=a.session_id
             WHERE a.session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(revision, i64::MAX);
    assert_eq!(cwd, workspace.canonical_root);
    assert_eq!(
        repository
            .get_workspace_agent_rebind(&operation.operation_id)
            .unwrap()
            .unwrap()
            .phase,
        "intended"
    );
}

#[test]
fn workspace_lead_set_reassign_clear_archive_restore_preserves_titles_and_never_auto_elects() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("root");
    let workspace = registered_workspace(&repository, &root, "Lead Test");
    let instructions = root.join(".agents/gg/lead-readme.md");
    std::fs::create_dir_all(instructions.parent().unwrap()).unwrap();
    std::fs::write(&instructions, "Keep teammates informed about changes.").unwrap();
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
    let intro_id = format!(
        "workspace-lifecycle:{}:lead_promoted:sess_first",
        set.operation_id
    );
    let intro = repository
        .get_workspace_system_notice(&intro_id)
        .unwrap()
        .expect("lead onboarding committed with authority");
    assert_eq!(intro.state, "pending");
    assert!(intro.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("first-agent"));
    assert!(intro.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("Keep teammates informed about changes."));

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
    let demotion = repository
        .get_workspace_system_notice(&format!(
            "workspace-lifecycle:{}:lead_demoted:sess_first",
            reassigned.operation_id
        ))
        .unwrap()
        .expect("prior lead demotion is a durable notice");
    assert!(demotion.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("second-agent"));
    let promotion = repository
        .get_workspace_system_notice(&format!(
            "workspace-lifecycle:{}:lead_promoted:sess_second",
            reassigned.operation_id
        ))
        .unwrap()
        .expect("new lead promotion is durable");
    assert_eq!(promotion.recipient_agent_id, second.agent_id);

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
    assert!(repository
        .get_workspace_system_notice(&format!(
            "workspace-lifecycle:{}:lead_demoted:sess_second",
            cleared.operation_id
        ))
        .unwrap()
        .is_some());

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
    let (mut archived_session, archived) = create_agent(
        &repository,
        &one,
        "sess_archived",
        "one-archived",
        "Archived",
        None,
    );
    let (_, outsider) = create_agent(&repository, &two, "sess_two", "two-a", "C", None);

    archived_session.status = "closed".to_string();
    archived_session.closed_at = Some(150);
    archived_session.updated_at = 150;
    repository
        .set_workspace_agent_lifecycle(
            &archived_session,
            &archived.agent_id,
            WorkspaceAgentLifecycleState::Archived,
            Some("test archived lead rejection"),
            150,
        )
        .expect("archive candidate");

    for candidate in [Some("sess_missing"), Some(archived.agent_id.as_str())] {
        let invalid = repository.transition_workspace_lead(&lead_command(
            &one.workspace_id,
            candidate,
            0,
            candidate.expect("candidate key"),
        ));
        assert!(matches!(
            invalid,
            Err(runtime_core::RuntimeError::InvalidState(_))
        ));
    }

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
    let notice_id = format!(
        "workspace-lifecycle:{}:workspace_turn_interrupted:sess_active",
        plan.operation_id
    );
    assert_eq!(
        repository
            .get_workspace_system_notice(&notice_id)
            .unwrap()
            .expect("interrupt notice committed with completion")
            .state,
        "pending"
    );
    assert!(repository
        .get_workspace_system_notice(&format!(
            "workspace-lifecycle:{}:workspace_turn_interrupted:sess_idle",
            plan.operation_id
        ))
        .unwrap()
        .is_none());

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

#[test]
fn workspace_interrupt_recovery_uses_durable_interrupt_event_without_redispatch() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("root");
    let workspace = registered_workspace(&repository, &root, "Interrupt Recovery");
    let (_, active) = create_agent(
        &repository,
        &workspace,
        "sess_active_recovery",
        "active-recovery",
        "Active",
        Some("turn_recovery"),
    );
    let command = prepare_workspace_interrupt(
        &workspace.workspace_id,
        OperationActor::operator("workspace-control-test"),
        Some("interrupt-recovery".to_string()),
    )
    .expect("interrupt command");
    let plan = match repository
        .begin_workspace_interrupt(&command)
        .expect("begin interrupt")
    {
        WorkspaceInterruptAdmission::Execute(plan) => plan,
        WorkspaceInterruptAdmission::Replay(_) => panic!("first admission cannot replay"),
    };
    repository
        .mark_workspace_interrupt_started(
            &plan.operation_id,
            &active.agent_id,
            "turn_recovery",
            200,
        )
        .expect("mark started");
    repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_interrupt_recovery".to_string(),
            scope: RuntimeEventScope::Session,
            scope_id: active.agent_id.clone(),
            session_id: Some(active.agent_id.clone()),
            team_id: None,
            turn_id: Some("turn_recovery".to_string()),
            kind: "turn.interrupt_requested".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({}),
            provider: None,
            provider_seq: None,
            created_at: 201,
        })
        .expect("durable interrupt event");

    let retry = prepare_workspace_interrupt(
        &workspace.workspace_id,
        OperationActor::operator("workspace-control-test"),
        Some("interrupt-recovery".to_string()),
    )
    .expect("retry command");
    let recovered = repository
        .begin_workspace_interrupt(&retry)
        .expect("recover interrupt");
    let recovered_plan = match recovered {
        WorkspaceInterruptAdmission::Execute(plan) => plan,
        WorkspaceInterruptAdmission::Replay(_) => panic!("operation is not terminal yet"),
    };
    assert!(
        recovered_plan.targets.is_empty(),
        "durable provider-accepted evidence must prevent redispatch"
    );

    let completed = repository
        .complete_workspace_interrupt(&plan.operation_id, 202)
        .expect("complete recovered interrupt");
    assert_eq!(completed.interrupted_agent_ids, vec![active.agent_id]);
    assert!(completed.skipped_agent_ids.is_empty());
}
