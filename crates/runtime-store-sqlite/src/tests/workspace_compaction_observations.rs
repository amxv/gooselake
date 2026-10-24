use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::{
    ProviderCompactionObservation, ProviderKind, RuntimeError, TeamHookApprovalUpdate,
    TeamHookEventName, TeamHookResolvedEntry, TeamHookScope,
};

fn compaction_event(
    agent_id: &str,
    phase: &str,
    proposed_run_id: &str,
    observed_at_ms: i64,
) -> ProviderCompactionObservation {
    ProviderCompactionObservation {
        proposed_run_id: proposed_run_id.into(),
        agent_id: agent_id.into(),
        provider: ProviderKind::Codex,
        provider_session_ref: format!("provider-{agent_id}"),
        canonical_provider_session_ref: None,
        turn_id: None,
        phase: phase.into(),
        trigger: Some("auto".into()),
        pre_tokens: Some(125),
        post_tokens: (phase == "completed").then_some(60),
        context_window_size: Some(200),
        observed_at_ms,
    }
}

#[test]
fn provider_compaction_commits_one_durable_lifecycle_notice_and_approved_hook() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (lead_session, lead) = workspace_agent_fixture(&workspace, "sess_lead", "steady-lead");
    repository
        .create_workspace_agent(&lead_session, &lead)
        .unwrap();
    let (session, mut agent) =
        workspace_agent_fixture(&workspace, "sess_native_compact", "steady-compact");
    agent.profile.creator_session_id = Some(lead.agent_id.clone());
    repository.create_workspace_agent(&session, &agent).unwrap();
    open_connection(&temp.path().join("runtime.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE workspaces SET lead_agent_id=?1 WHERE workspace_id=?2",
            params![lead.agent_id, workspace.workspace_id],
        )
        .unwrap();
    repository
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: workspace.workspace_id.clone(),
            expected_revision: 0,
            hooks: vec![TeamHookResolvedEntry {
                event_name: TeamHookEventName::PostCompaction,
                name: "compaction-observer".into(),
                command: "printf '{}'".into(),
                timeout_ms: 1000,
                scope: TeamHookScope::User,
            }],
            approved_at_ms: 100,
        })
        .unwrap();

    let started = repository
        .observe_provider_compaction(&compaction_event(
            &agent.agent_id,
            "started",
            "cmp_auto_1",
            101,
        ))
        .unwrap();
    assert_eq!(started.run.phase, "started");
    assert_eq!(started.run.source, "provider");
    assert!(repository
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap()
        .is_empty());
    assert!(repository
        .list_workspace_system_notices(10)
        .unwrap()
        .is_empty());

    let completed_signal = compaction_event(&agent.agent_id, "completed", "cmp_auto_2", 102);
    let completed = repository
        .observe_provider_compaction(&completed_signal)
        .unwrap();
    assert!(completed.changed);
    assert_eq!(completed.run.phase, "completed");
    assert_eq!(completed.run.run_id, started.run.run_id);
    assert_eq!(completed.run.post_tokens, Some(60));
    assert!(!repository
        .unresolved_session_compaction(&agent.agent_id)
        .unwrap());
    let notices = repository.list_workspace_system_notices(10).unwrap();
    assert_eq!(notices.len(), 1, "creator and lead are deduplicated");
    assert_eq!(notices[0].recipient_agent_id, lead.agent_id);
    assert_eq!(notices[0].kind, "compaction");
    let effects = repository
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(
        effects[0].hook.event_name,
        TeamHookEventName::PostCompaction
    );

    let replay = repository
        .observe_provider_compaction(&completed_signal)
        .unwrap();
    assert!(!replay.changed);
    assert_eq!(replay.run.run_id, completed.run.run_id);
    assert!(
        !repository
            .observe_provider_compaction(&compaction_event(
                &agent.agent_id,
                "started",
                "cmp_delayed",
                103
            ))
            .unwrap()
            .changed,
        "late started is not a new run"
    );
    assert!(matches!(
        repository.observe_provider_compaction(&compaction_event(
            &agent.agent_id,
            "failed",
            "cmp_wrong_terminal",
            104
        )),
        Err(RuntimeError::Conflict(_))
    ));

    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        restarted
            .get_session_compaction(&completed.run.run_id)
            .unwrap(),
        Some(completed.run)
    );
    assert_eq!(
        restarted.list_workspace_system_notices(10).unwrap().len(),
        1
    );
    assert_eq!(
        restarted
            .list_team_hook_effects(&workspace.workspace_id, 10)
            .unwrap()
            .len(),
        1
    );
    let mut replacement = session.clone();
    replacement.provider_session_ref = Some("replacement-native".into());
    replacement.updated_at += 1;
    restarted.upsert_session(&replacement).unwrap();
    assert!(matches!(
        restarted.observe_provider_compaction(&compaction_event(
            &agent.agent_id,
            "completed",
            "cmp_stale_attachment",
            400
        )),
        Err(RuntimeError::Conflict(_))
    ));
}

#[test]
fn provider_completion_before_manual_acceptance_reuses_the_admitted_run() {
    use runtime_core::{ProviderCompactSessionOutcome, SessionCompactionRun};

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) = workspace_agent_fixture(&workspace, "sess_manual_race", "steady-race");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let run = SessionCompactionRun {
        run_id: "cmp_manual_race".into(),
        agent_id: agent.agent_id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.unwrap(),
        canonical_provider_session_ref: None,
        agent_revision: 0,
        expected_session_updated_at: session.updated_at,
        idempotency_key: Some("same-request".into()),
        phase: "intended".into(),
        outcome: None,
        error_code: None,
        review_evidence: None,
        source: "manual".into(),
        confidence: None,
        observed_turn_id: None,
        trigger: Some("manual".into()),
        pre_tokens: None,
        post_tokens: None,
        context_window_size: None,
        created_at_ms: 101,
        updated_at_ms: 101,
    };
    repository.begin_session_compaction(&run).unwrap();
    let completed = repository
        .observe_provider_compaction(&compaction_event(
            &agent.agent_id,
            "completed",
            "cmp_unused",
            102,
        ))
        .unwrap()
        .run;
    assert_eq!(completed.run_id, run.run_id);
    assert_eq!(completed.phase, "completed");
    assert_eq!(completed.source, "manual");
    let accepted = repository
        .transition_session_compaction(
            &run.run_id,
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            103,
        )
        .unwrap();
    assert_eq!(
        accepted, completed,
        "native completion is stronger than late acceptance"
    );
    assert!(!repository
        .unresolved_session_compaction(&agent.agent_id)
        .unwrap());
}

#[test]
fn unresolved_automatic_compaction_fences_recovery_until_review() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) =
        workspace_agent_fixture(&workspace, "sess_auto_restart", "steady-restart");
    repository.create_workspace_agent(&session, &agent).unwrap();
    let started = repository
        .observe_provider_compaction(&compaction_event(
            &agent.agent_id,
            "started",
            "cmp_auto_restart",
            101,
        ))
        .unwrap()
        .run;
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let unresolved = restarted.recover_session_compactions(200).unwrap();
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0].phase, "manual_review");
    assert!(
        matches!(
            restarted.observe_provider_compaction(&compaction_event(
                &agent.agent_id,
                "completed",
                "cmp_ambiguous",
                201
            )),
            Err(RuntimeError::Conflict(_))
        ),
        "a weak post-restart alias cannot claim a reviewed run"
    );
    let reviewed = restarted
        .resolve_session_compaction_review(
            &started.run_id,
            "completed",
            "Operator verified current provider compaction completion",
            202,
        )
        .unwrap();
    assert_eq!(reviewed.phase, "completed");
    assert!(!restarted
        .unresolved_session_compaction(&agent.agent_id)
        .unwrap());
    assert!(restarted
        .recover_session_compactions(203)
        .unwrap()
        .is_empty());
}

#[test]
fn compaction_notifications_respect_leadless_creator_opt_out_and_team_policy() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (creator_session, creator) =
        workspace_agent_fixture(&workspace, "sess_creator", "steady-creator");
    repository
        .create_workspace_agent(&creator_session, &creator)
        .unwrap();
    let (other_session, other) = workspace_agent_fixture(&workspace, "sess_other", "steady-other");
    repository
        .create_workspace_agent(&other_session, &other)
        .unwrap();
    let (session, mut subject) =
        workspace_agent_fixture(&workspace, "sess_subject", "steady-subject");
    subject.profile.creator_session_id = Some(creator.agent_id.clone());
    repository
        .create_workspace_agent(&session, &subject)
        .unwrap();
    assert!(repository
        .get_workspace(&workspace.workspace_id)
        .unwrap()
        .unwrap()
        .lead_agent_id
        .is_none());
    let first = repository
        .observe_provider_compaction(&compaction_event(
            &subject.agent_id,
            "completed",
            "cmp_leadless_1",
            101,
        ))
        .unwrap();
    assert!(first.changed);
    let notices = repository.list_workspace_system_notices(10).unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].recipient_agent_id, creator.agent_id);

    open_connection(&temp.path().join("runtime.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE workspace_agent_profiles SET creator_compaction_subscription='unsubscribed'
             WHERE session_id=?1",
            [&subject.agent_id],
        )
        .unwrap();
    let mut notice_policy = repository
        .get_workspace_notice_policy(&workspace.workspace_id)
        .unwrap()
        .policy;
    notice_policy.compaction_to_lead = false;
    notice_policy.compaction_to_team = false;
    repository
        .update_workspace_notice_policy(
            &workspace.workspace_id,
            &runtime_core::WorkspaceNoticePolicyUpdate {
                expected_revision: 0,
                policy: notice_policy.clone(),
            },
        )
        .unwrap();
    repository
        .observe_provider_compaction(&compaction_event(
            &subject.agent_id,
            "completed",
            "cmp_leadless_2",
            500,
        ))
        .unwrap();
    assert_eq!(
        repository.list_workspace_system_notices(10).unwrap().len(),
        1
    );

    notice_policy.compaction_to_team = true;
    repository
        .update_workspace_notice_policy(
            &workspace.workspace_id,
            &runtime_core::WorkspaceNoticePolicyUpdate {
                expected_revision: 1,
                policy: notice_policy,
            },
        )
        .unwrap();
    repository
        .observe_provider_compaction(&compaction_event(
            &subject.agent_id,
            "completed",
            "cmp_leadless_3",
            900,
        ))
        .unwrap();
    let notices = repository.list_workspace_system_notices(10).unwrap();
    assert_eq!(notices.len(), 3);
    assert!(notices.iter().any(|notice| {
        notice.recipient_agent_id == creator.agent_id && notice.notice_id.contains("cmp_leadless_3")
    }));
    assert!(notices.iter().any(|notice| {
        notice.recipient_agent_id == other.agent_id && notice.notice_id.contains("cmp_leadless_3")
    }));
    assert!(!notices
        .iter()
        .any(|notice| notice.recipient_agent_id == subject.agent_id));
}

#[test]
fn correlated_turn_compaction_replay_cannot_duplicate_after_weak_window() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    let (session, subject) =
        workspace_agent_fixture(&workspace, "sess_turn_compact", "steady-turn-compact");
    repository
        .create_workspace_agent(&session, &subject)
        .unwrap();
    repository
        .upsert_turn(&TurnRecord {
            id: "turn_native_compaction".into(),
            session_id: subject.agent_id.clone(),
            provider_turn_ref: Some("native-turn".into()),
            status: "completed".into(),
            input: serde_json::json!([{"type":"text","text":"small"}]),
            source: Some("user".into()),
            started_at: Some(100),
            completed_at: Some(101),
            usage: None,
            error: None,
        })
        .unwrap();
    let mut signal = compaction_event(&subject.agent_id, "completed", "cmp_turn_1", 101);
    signal.turn_id = Some("turn_native_compaction".into());
    let first = repository.observe_provider_compaction(&signal).unwrap();
    assert_eq!(first.run.confidence.as_deref(), Some("native_turn"));
    signal.proposed_run_id = "cmp_turn_2".into();
    signal.observed_at_ms = 3000;
    let replay = repository.observe_provider_compaction(&signal).unwrap();
    assert!(!replay.changed);
    assert_eq!(replay.run.run_id, first.run.run_id);
    signal.turn_id = Some("unknown_turn".into());
    assert!(matches!(
        repository.observe_provider_compaction(&signal),
        Err(RuntimeError::Conflict(_))
    ));
}
