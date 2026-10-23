use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::{
    AgentDeliveryRecord, AgentMessageContextKind, AgentMessageRecord, TeamHookApprovalUpdate,
    TeamHookEffectPhase, TeamHookEffectReceipt, TeamHookEventName, TeamHookResolvedEntry,
    TeamHookScope, WorkspaceAgentLifecycleState,
};
use serde_json::json;

fn grant(repository: &SqliteRuntimeRepository, workspace_id: &str, events: &[TeamHookEventName]) {
    repository
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: workspace_id.into(),
            expected_revision: 0,
            approved_at_ms: 100,
            hooks: events
                .iter()
                .map(|event| TeamHookResolvedEntry {
                    event_name: *event,
                    name: "trusted_hook".into(),
                    command: "printf '{}'".into(),
                    timeout_ms: 500,
                    scope: TeamHookScope::User,
                })
                .collect(),
        })
        .unwrap();
}

fn finish(
    repository: &SqliteRuntimeRepository,
    event: TeamHookEventName,
    output: serde_json::Value,
) -> runtime_core::TeamHookEffectRecord {
    let effect = repository
        .list_dispatchable_team_hook_effects(100)
        .unwrap()
        .into_iter()
        .find(|effect| effect.hook.event_name == event)
        .unwrap();
    repository
        .claim_team_hook_effect(
            &effect.workspace_id,
            &effect.parent_event_id,
            event,
            &effect.hook.name,
            201,
        )
        .unwrap();
    repository
        .finish_team_hook_effect(
            &effect.workspace_id,
            &effect.parent_event_id,
            event,
            &effect.hook.name,
            TeamHookEffectPhase::Succeeded,
            TeamHookEffectReceipt {
                validated_output: Some(output),
                error_code: None,
            },
            202,
        )
        .unwrap()
}

#[test]
fn member_add_remove_hook_intents_commit_atomically_and_context_applies_once() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (lead_session, lead) = workspace_agent_fixture(&workspace, "sess_lead_hook", "lead-heron");
    repository
        .create_workspace_agent(&lead_session, &lead)
        .unwrap();
    grant(
        &repository,
        &workspace.workspace_id,
        &[
            TeamHookEventName::PostTeammateAdd,
            TeamHookEventName::PostTeammateRemove,
        ],
    );
    let (mut session, mut member) =
        workspace_agent_fixture(&workspace, "sess_member_hook", "member-otter");
    member.profile.added_by = lead.agent_id.clone();
    repository
        .create_workspace_agent_with_onboarding(&session, &member, &Default::default())
        .unwrap();

    let original = repository.list_dispatchable_team_hook_effects(20).unwrap();
    assert_eq!(original.len(), 1);
    assert_eq!(original[0].application_state, "not_ready");
    assert_eq!(original[0].input["actor_agent_id"], lead.agent_id);
    assert_eq!(
        original[0].input["subject"]["member_agent_id"],
        member.agent_id
    );
    let count: i64 = open_connection(&repository.database_path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM runtime_events WHERE kind='workspace.hook_parent'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "canonical parent is committed alongside member");

    // Rejected duplicate alias rolls back the entire member/notice/hook unit.
    let (bad_session, bad_member) =
        workspace_agent_fixture(&workspace, "sess_duplicate_hook", "member-otter");
    assert!(repository
        .create_workspace_agent(&bad_session, &bad_member)
        .is_err());
    assert_eq!(
        repository
            .list_dispatchable_team_hook_effects(20)
            .unwrap()
            .len(),
        1
    );

    let completed = finish(
        &repository,
        TeamHookEventName::PostTeammateAdd,
        json!({
            "inject":{"caller":{"text":"member admitted"},"targets":[
                {"agent_id":member.agent_id,"text":"new member context"}]}
        }),
    );
    assert_eq!(completed.application_state, "pending");
    let applied = repository
        .apply_team_hook_effect_output(&completed, 203)
        .unwrap();
    assert_eq!(applied.application_state, "applied");
    assert_eq!(
        repository
            .apply_team_hook_effect_output(&completed, 204)
            .unwrap(),
        applied
    );
    let notices = repository.list_workspace_system_notices(20).unwrap();
    let hook_notices: Vec<_> = notices
        .iter()
        .filter(|notice| notice.kind == "hook_context")
        .collect();
    assert_eq!(hook_notices.len(), 2);
    assert!(hook_notices
        .iter()
        .any(|notice| notice.recipient_agent_id == lead.agent_id));
    assert!(hook_notices
        .iter()
        .any(|notice| notice.recipient_agent_id == member.agent_id));

    session.status = "closed".into();
    session.closed_at = Some(300);
    session.updated_at = 300;
    repository
        .set_workspace_agent_lifecycle_with_initiator(
            &session,
            &member.agent_id,
            WorkspaceAgentLifecycleState::Archived,
            Some("removed"),
            300,
            Some(&lead.agent_id),
        )
        .unwrap();
    let removed = repository.list_dispatchable_team_hook_effects(10).unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(
        removed[0].hook.event_name,
        TeamHookEventName::PostTeammateRemove
    );
    assert_eq!(
        removed[0].input["eligible_target_agent_ids"],
        json!([lead.agent_id])
    );

    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    reopened.initialize_schema().unwrap();
    assert!(reopened
        .list_unapplied_team_hook_effects(10)
        .unwrap()
        .is_empty());
    assert_eq!(
        reopened.list_dispatchable_team_hook_effects(10).unwrap(),
        removed
    );
    assert_eq!(
        reopened.list_workspace_system_notices(20).unwrap(),
        notices
            .into_iter()
            .filter(|notice| notice.kind == "hook_context" || notice.kind == "onboarding")
            .collect::<Vec<_>>()
    );
}

#[test]
fn hook_output_denies_unapproved_injection_targets() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let ws = registered_workspace(&repository, &root);
    grant(
        &repository,
        &ws.workspace_id,
        &[TeamHookEventName::PostTeammateAdd],
    );
    let (session, agent) = workspace_agent_fixture(&ws, "sess_auth_test", "brisk-otter");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let finished = finish(
        &repository,
        TeamHookEventName::PostTeammateAdd,
        json!({
            "inject":{"targets":[{"agent_id":"sess_invented","text":"forged"}]}
        }),
    );
    let rejected = repository
        .apply_team_hook_effect_output(&finished, 204)
        .unwrap();
    assert_eq!(rejected.application_state, "rejected");
    assert_eq!(
        rejected.application_error_code.as_deref(),
        Some("unauthorized_hook_recipient")
    );
    assert!(repository
        .list_workspace_system_notices(20)
        .unwrap()
        .iter()
        .all(|notice| notice.kind != "hook_context"));
    assert_eq!(
        repository
            .apply_team_hook_effect_output(&finished, 205)
            .unwrap(),
        rejected
    );
}

#[test]
fn cross_workspace_direct_hook_keeps_immutable_recipient_scope() {
    let temp = tempfile::tempdir().unwrap();
    let root_a = temp.path().join("a");
    let root_b = temp.path().join("b");
    std::fs::create_dir_all(&root_a).unwrap();
    std::fs::create_dir_all(&root_b).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let a = registered_workspace(&repository, &root_a);
    let b = registered_workspace(&repository, &root_b);
    let (sender_session, sender) = workspace_agent_fixture(&a, "sess_sender_hook", "amber-fox");
    let (recipient_session, recipient) =
        workspace_agent_fixture(&b, "sess_recipient_hook", "calm-fox");
    repository
        .create_workspace_agent(&sender_session, &sender)
        .unwrap();
    repository
        .create_workspace_agent(&recipient_session, &recipient)
        .unwrap();
    grant(
        &repository,
        &a.workspace_id,
        &[TeamHookEventName::PostDirectMessageSend],
    );

    let message = AgentMessageRecord {
        id: "msg_hook_direct".into(),
        scope: "direct".into(),
        context_kind: AgentMessageContextKind::GlobalDirect,
        workspace_id: None,
        legacy_team_id: None,
        sender_agent_id: sender.agent_id.clone(),
        recipient_agent_ids: vec![recipient.agent_id.clone()],
        input: json!([{"type":"text","text":"hi"}]),
        image_paths: vec![],
        priority: "normal".into(),
        policy: "non_interrupting".into(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: None,
        created_at: 201,
    };
    let delivery = AgentDeliveryRecord {
        id: "delivery_hook_direct".into(),
        message_id: message.id.clone(),
        recipient_agent_id: recipient.agent_id.clone(),
        provider: "codex".into(),
        status: "pending".into(),
        effective_policy: None,
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 201,
        updated_at: 201,
    };
    repository
        .insert_agent_message_with_deliveries(&message, &[delivery.clone()])
        .unwrap();
    assert!(
        repository
            .list_dispatchable_team_hook_effects(10)
            .unwrap()
            .is_empty(),
        "post-send hook must not outrun deferred/pending message delivery"
    );
    let mut settled_delivery = delivery;
    settled_delivery.status = "injected".into();
    settled_delivery.updated_at = 205;
    repository.upsert_agent_delivery(&settled_delivery).unwrap();
    let effect = repository.list_dispatchable_team_hook_effects(10).unwrap();
    assert_eq!(effect.len(), 1);
    assert_eq!(effect[0].workspace_id, a.workspace_id);
    assert_eq!(
        effect[0].input["eligible_target_agent_ids"],
        json!([recipient.agent_id])
    );
    let finished = finish(
        &repository,
        TeamHookEventName::PostDirectMessageSend,
        json!({
            "inject":{"targets":[{"agent_id":recipient.agent_id,"text":"cross-workspace delivery"}]}
        }),
    );
    let applied = repository
        .apply_team_hook_effect_output(&finished, 210)
        .unwrap();
    assert_eq!(applied.application_state, "applied");
    let notices = repository.list_workspace_system_notices(10).unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].workspace_id, b.workspace_id);
    assert_eq!(notices[0].recipient_agent_id, recipient.agent_id);
    assert_eq!(notices[0].kind, "hook_context");
    assert!(!notices[0].input[0]["text"]
        .as_str()
        .unwrap()
        .contains("sess_sender_hook"));
}
