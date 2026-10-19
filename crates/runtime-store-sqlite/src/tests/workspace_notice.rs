use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;

#[test]
fn notice_queue_pages_past_busy_older_entries_without_dropping_or_repeating_rows() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "sess_queue", "queue-heron");
    repository
        .create_workspace_agent_with_onboarding(&session, &agent, &Default::default())
        .unwrap();

    let mut connection = open_connection(repository.database_path.as_path()).unwrap();
    let tx = connection.transaction().unwrap();
    for index in 0..300 {
        let notice = runtime_core::WorkspaceSystemNoticeRecord::lifecycle(
            &workspace.workspace_id,
            &agent.agent_id,
            &format!("queue-event-{index:03}"),
            "member_joined",
            format!("Member {index} joined"),
            agent.created_at,
        );
        SqliteRuntimeRepository::insert_workspace_system_notice_tx(&tx, &notice).unwrap();
    }
    tx.commit().unwrap();

    let mut seen = std::collections::HashSet::new();
    let mut after = None;
    let mut page_lengths = Vec::new();
    let mut all = Vec::new();
    loop {
        let page = repository
            .list_workspace_system_notices_after(after.as_ref(), 256)
            .unwrap();
        if page.is_empty() {
            break;
        }
        page_lengths.push(page.len());
        for notice in &page {
            assert!(seen.insert(notice.notice_id.clone()));
            all.push(notice.notice_id.clone());
        }
        after = page.last().cloned();
    }
    assert_eq!(page_lengths, vec![256, 45]);
    assert_eq!(seen.len(), 301);
    assert_eq!(all[0], "workspace-onboarding:sess_queue");
    assert!(all.contains(&"workspace-lifecycle:queue-event-299:member_joined:sess_queue".into()));
    let later = repository
        .get_workspace_system_notice("workspace-lifecycle:queue-event-299:member_joined:sess_queue")
        .unwrap()
        .unwrap();
    assert!(!repository.is_workspace_system_notice_head(&later).unwrap());
    let connection = open_connection(repository.database_path.as_path()).unwrap();
    connection
        .execute(
            "UPDATE workspace_system_notices SET state = 'manual_review' WHERE notice_id != ?1",
            [later.notice_id.as_str()],
        )
        .unwrap();
    assert!(repository.is_workspace_system_notice_head(&later).unwrap());
}

#[test]
fn onboarding_is_atomic_snapshotted_ordered_and_compare_and_set() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (lead_session, lead) = workspace_agent_fixture(&workspace, "sess_lead", "steady-otter");
    repository
        .create_workspace_agent(&lead_session, &lead)
        .expect("lead");
    let connection = open_connection(repository.database_path.as_path()).expect("db");
    connection
        .execute(
            "UPDATE workspaces SET lead_agent_id = ?1 WHERE workspace_id = ?2",
            params![lead.agent_id, workspace.workspace_id],
        )
        .expect("set lead fixture");
    let (session, mut member) = workspace_agent_fixture(&workspace, "sess_new", "swift-heron");
    member.profile.added_by = lead.agent_id.clone();
    let task = runtime_core::WorkspaceAgentOnboardingRequest {
        prompt: Some("Make this useful".into()),
        image_paths: vec!["/example/a.png".into(), "/example/b.webp".into()],
        skill: None,
        creator_compaction_subscription: Some("auto".into()),
    };
    repository
        .create_workspace_agent_with_onboarding(&session, &member, &task)
        .expect("agent and notice admitted in one transaction");
    let notice_id = "workspace-onboarding:sess_new";
    let notice = repository
        .get_workspace_system_notice(notice_id)
        .unwrap()
        .unwrap();
    assert_eq!(notice.state, "pending");
    assert_eq!(notice.recipient_agent_id, "sess_new");
    assert_eq!(notice.input[1]["path"], "/example/a.png");
    assert_eq!(notice.input[2]["path"], "/example/b.webp");
    let text = notice.input[0]["text"].as_str().unwrap();
    assert!(text.contains("workspace team \"Agent Test\""));
    assert!(text.contains("team lead is steady-otter"));
    assert!(text.contains("Your name is swift-heron"));
    assert!(text.contains("Your title is Builder"));
    assert!(text.contains("Make this useful"));
    assert!(!text.contains("sess_new"));
    assert!(!text.contains("sess_lead"));

    // A failed duplicate agent create must not leave an orphan notice.
    let (other_session, mut duplicate) =
        workspace_agent_fixture(&workspace, "sess_duplicate", "swift-heron");
    duplicate.alias = member.alias.clone();
    assert!(repository
        .create_workspace_agent_with_onboarding(&other_session, &duplicate, &task)
        .is_err());
    assert!(repository
        .get_workspace_system_notice("workspace-onboarding:sess_duplicate")
        .unwrap()
        .is_none());
    assert!(repository
        .get_workspace_agent_by_id("sess_duplicate")
        .unwrap()
        .is_none());

    // Lead reassignment after creation cannot mutate the snapshotted intro.
    connection
        .execute(
            "UPDATE workspaces SET lead_agent_id = NULL WHERE workspace_id = ?1",
            [workspace.workspace_id.as_str()],
        )
        .unwrap();
    let recovered = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        recovered
            .get_workspace_system_notice(notice_id)
            .unwrap()
            .unwrap()
            .input,
        notice.input
    );
    assert!(recovered
        .transition_workspace_system_notice(notice_id, "pending", "injecting", None, None, 150)
        .unwrap());
    assert!(!recovered
        .transition_workspace_system_notice(notice_id, "pending", "injecting", None, None, 150)
        .unwrap());
    assert!(recovered
        .transition_workspace_system_notice(
            notice_id,
            "injecting",
            "manual_review",
            None,
            Some("native_response_unknown"),
            151
        )
        .unwrap());
    assert_eq!(
        recovered
            .get_workspace_system_notice(notice_id)
            .unwrap()
            .unwrap()
            .state,
        "manual_review"
    );

    let (no_lead_session, no_lead_agent) =
        workspace_agent_fixture(&workspace, "sess_leadless", "brisk-wolf");
    recovered
        .create_workspace_agent_with_onboarding(
            &no_lead_session,
            &no_lead_agent,
            &Default::default(),
        )
        .expect("leadless onboarding");
    let leadless = recovered
        .get_workspace_system_notice("workspace-onboarding:sess_leadless")
        .unwrap()
        .unwrap();
    assert!(leadless.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("no team lead"));
}

#[test]
fn notification_policy_is_revisioned_and_respected_by_authority_transactions() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let policy = repository
        .get_workspace_notice_policy(&workspace.workspace_id)
        .unwrap();
    assert_eq!(policy.revision, 0);
    assert!(policy.policy.lead_transitions);
    let mut silent = policy.policy;
    silent.lead_transitions = false;
    silent.membership_to_lead = false;
    silent.worktree_reassignment = false;
    silent.workspace_interrupt = false;
    let update = runtime_core::WorkspaceNoticePolicyUpdate {
        expected_revision: 0,
        policy: silent.clone(),
    };
    let saved = repository
        .update_workspace_notice_policy(&workspace.workspace_id, &update)
        .unwrap();
    assert_eq!(saved.revision, 1);
    assert_eq!(saved.policy, silent);
    assert!(matches!(
        repository.update_workspace_notice_policy(&workspace.workspace_id, &update),
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    let recovered = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        recovered
            .get_workspace_notice_policy(&workspace.workspace_id)
            .unwrap(),
        saved
    );

    let (session, agent) = workspace_agent_fixture(&workspace, "sess_silent", "silent-otter");
    recovered
        .create_workspace_agent_with_onboarding(&session, &agent, &Default::default())
        .unwrap();
    let command = runtime_core::prepare_workspace_lead_transition(
        &workspace.workspace_id,
        runtime_core::WorkspaceLeadTransitionRequest {
            lead_agent_id: Some(agent.agent_id.clone()),
            expected_revision: 0,
        },
        runtime_core::OperationActor::operator("runtime_operator"),
        Some("silent-lead".into()),
    )
    .unwrap();
    let response = recovered.transition_workspace_lead(&command).unwrap();
    assert!(recovered
        .get_workspace_system_notice(&format!(
            "workspace-lifecycle:{}:lead_promoted:{}",
            response.operation_id, agent.agent_id
        ))
        .unwrap()
        .is_none());
    assert!(
        recovered
            .get_workspace_system_notice("workspace-onboarding:sess_silent")
            .unwrap()
            .is_some(),
        "onboarding remains mandatory even when lifecycle notifications are disabled"
    );
}

#[test]
fn lead_receives_member_changes_except_own_actions_and_leadless_changes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (lead_session, lead) = workspace_agent_fixture(&workspace, "sess_teamlead", "gentle-heron");
    repository
        .create_workspace_agent_with_onboarding(&lead_session, &lead, &Default::default())
        .unwrap();
    let command = runtime_core::prepare_workspace_lead_transition(
        &workspace.workspace_id,
        runtime_core::WorkspaceLeadTransitionRequest {
            lead_agent_id: Some(lead.agent_id.clone()),
            expected_revision: 0,
        },
        runtime_core::OperationActor::operator("runtime_operator"),
        Some("teamlead".into()),
    )
    .unwrap();
    repository.transition_workspace_lead(&command).unwrap();

    let (mut member_session, member) =
        workspace_agent_fixture(&workspace, "sess_teammate", "purple-fox");
    repository
        .create_workspace_agent_with_onboarding(&member_session, &member, &Default::default())
        .unwrap();
    let joined = repository
        .get_workspace_system_notice(
            "workspace-lifecycle:workspace-agent-created:sess_teammate:joined:sess_teamlead",
        )
        .unwrap()
        .expect("lead member join notice");
    assert_eq!(joined.kind, "joined");
    assert!(joined.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("purple-fox"));
    let queued = repository.list_workspace_system_notices(20).unwrap();
    let onboarding_index = queued
        .iter()
        .position(|row| row.notice_id == "workspace-onboarding:sess_teammate")
        .unwrap();
    let lead_notice_index = queued
        .iter()
        .position(|row| row.notice_id == joined.notice_id)
        .unwrap();
    assert!(
        onboarding_index < lead_notice_index,
        "initial agent context precedes same-time lifecycle notices"
    );

    member_session.status = "closed".into();
    member_session.closed_at = Some(200);
    member_session.updated_at = 200;
    repository
        .set_workspace_agent_lifecycle_with_initiator(
            &member_session,
            &member.agent_id,
            runtime_core::WorkspaceAgentLifecycleState::Archived,
            Some("lead requested removal"),
            200,
            Some(&lead.agent_id),
        )
        .unwrap();
    assert!(repository.get_workspace_system_notice(
        "workspace-lifecycle:workspace-agent-lifecycle:sess_teammate:revision-1:removed:sess_teamlead"
    ).unwrap().is_none(), "lead never receives self-initiated removal notice");

    member_session.status = "ready".into();
    member_session.closed_at = None;
    member_session.updated_at = 300;
    repository
        .set_workspace_agent_lifecycle(
            &member_session,
            &member.agent_id,
            runtime_core::WorkspaceAgentLifecycleState::Active,
            None,
            300,
        )
        .unwrap();
    let restored = repository.get_workspace_system_notice(
        "workspace-lifecycle:workspace-agent-lifecycle:sess_teammate:revision-2:restored:sess_teamlead"
    ).unwrap().expect("operator-initiated restore notice");
    assert!(restored.input[0]["text"]
        .as_str()
        .unwrap()
        .contains("restored"));
    assert_eq!(restored.state, "pending");
}
