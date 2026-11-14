use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::{
    ProviderCompactSessionOutcome, ProviderKind, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, SessionCompactionRun, TeamHookApprovalUpdate, TeamHookEventName,
    TeamHookResolvedEntry, TeamHookScope,
};

fn run_for(
    repository: &SqliteRuntimeRepository,
    workspace: &runtime_core::WorkspaceRecord,
    agent_id: &str,
    run_id: &str,
    key: &str,
) -> SessionCompactionRun {
    let (session, agent) =
        workspace_agent_fixture(workspace, agent_id, &format!("agent-{agent_id}"));
    repository.create_workspace_agent(&session, &agent).unwrap();
    SessionCompactionRun {
        run_id: run_id.into(),
        agent_id: session.id,
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.unwrap(),
        canonical_provider_session_ref: None,
        agent_revision: agent.revision,
        expected_session_updated_at: session.updated_at,
        idempotency_key: Some(key.into()),
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
    }
}

fn setup() -> (
    tempfile::TempDir,
    SqliteRuntimeRepository,
    runtime_core::WorkspaceRecord,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);
    (temp, repository, workspace)
}

fn events(
    repository: &SqliteRuntimeRepository,
    agent_id: &str,
    kind: &str,
) -> Vec<runtime_core::RuntimeEventRecord> {
    repository
        .list_runtime_events(Some((RuntimeEventScope::Session, agent_id)), None, 1000)
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == kind)
        .collect()
}

#[test]
fn admission_critical_failure_rolls_back_and_same_key_replay_is_inspection_only() {
    let (temp, repository, workspace) = setup();
    let run = run_for(
        &repository,
        &workspace,
        "session_admit",
        "cmp_admit",
        "same-key",
    );
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_admission_critical BEFORE INSERT ON runtime_events
         WHEN NEW.kind='compaction.admitted'
         BEGIN SELECT RAISE(ABORT, 'injected admission event failure'); END;",
        )
        .unwrap();
    assert!(repository
        .begin_session_compaction_with_receipt(&run)
        .is_err());
    assert_eq!(
        repository.get_session_compaction(&run.run_id).unwrap(),
        None
    );
    assert!(events(&repository, &run.agent_id, "compaction.admitted").is_empty());
    connection
        .execute_batch("DROP TRIGGER reject_admission_critical")
        .unwrap();

    let admitted = repository
        .begin_session_compaction_with_receipt(&run)
        .unwrap();
    assert!(admitted.changed);
    assert_eq!(admitted.run, run);
    let event = admitted.journal_event.unwrap();
    assert_eq!(event.kind, "compaction.admitted");
    assert_eq!(event.criticality, RuntimeEventCriticality::Critical);
    assert_eq!(event.payload["run_id"], run.run_id);
    assert_eq!(event.payload["phase"], "intended");

    let mut competing = run.clone();
    competing.run_id = "cmp_competing".into();
    let replay = repository
        .begin_session_compaction_with_receipt(&competing)
        .unwrap();
    assert!(
        !replay.changed,
        "race-lost same-key admission is not native dispatch permission"
    );
    assert_eq!(replay.run, run);
    assert!(replay.journal_event.is_none());
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        events(&reopened, &run.agent_id, "compaction.admitted").len(),
        1
    );
}

#[test]
fn racing_admissions_return_one_changed_receipt_and_one_durable_event() {
    let (temp, repository, workspace) = setup();
    let run = run_for(
        &repository,
        &workspace,
        "session_race",
        "cmp_race_a",
        "racing-key",
    );
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let path = temp.path().join("runtime.sqlite3");
    let mut workers = Vec::new();
    for suffix in ["a", "b"] {
        let barrier = barrier.clone();
        let path = path.clone();
        let mut candidate = run.clone();
        candidate.run_id = format!("cmp_race_{suffix}");
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            SqliteRuntimeRepository::new(path)
                .begin_session_compaction_with_receipt(&candidate)
                .unwrap()
        }));
    }
    let receipts = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(receipts.iter().filter(|receipt| receipt.changed).count(), 1);
    assert_eq!(receipts[0].run.run_id, receipts[1].run.run_id);
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.journal_event.is_some())
            .count(),
        1
    );
    assert_eq!(
        events(&repository, &run.agent_id, "compaction.admitted").len(),
        1
    );
}

#[test]
fn startup_quarantine_batch_journals_every_changed_run_or_none() {
    let (temp, repository, workspace) = setup();
    let first = run_for(&repository, &workspace, "session_q1", "cmp_q_1", "q1");
    let second = run_for(&repository, &workspace, "session_q2", "cmp_q_2", "q2");
    repository.begin_session_compaction(&first).unwrap();
    repository.begin_session_compaction(&second).unwrap();
    repository
        .transition_session_compaction(
            &second.run_id,
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            102,
        )
        .unwrap();

    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_second_quarantine BEFORE INSERT ON runtime_events
         WHEN NEW.kind='compaction.restart_quarantined'
           AND json_extract(NEW.payload_json,'$.run_id')='cmp_q_2'
         BEGIN SELECT RAISE(ABORT,'injected second quarantine event failure'); END;",
        )
        .unwrap();
    assert!(repository
        .recover_session_compactions_with_receipts(200)
        .is_err());
    assert_eq!(
        repository
            .get_session_compaction(&first.run_id)
            .unwrap()
            .unwrap()
            .phase,
        "intended"
    );
    assert_eq!(
        repository
            .get_session_compaction(&second.run_id)
            .unwrap()
            .unwrap()
            .phase,
        "accepted"
    );
    assert!(events(
        &repository,
        &first.agent_id,
        "compaction.restart_quarantined"
    )
    .is_empty());
    assert!(events(
        &repository,
        &second.agent_id,
        "compaction.restart_quarantined"
    )
    .is_empty());
    connection
        .execute_batch("DROP TRIGGER reject_second_quarantine")
        .unwrap();

    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let receipts = reopened
        .recover_session_compactions_with_receipts(200)
        .unwrap();
    assert_eq!(receipts.len(), 2);
    assert!(receipts.iter().all(|receipt| receipt.changed
        && receipt.run.phase == "manual_review"
        && receipt
            .journal_event
            .as_ref()
            .is_some_and(|event| event.criticality == RuntimeEventCriticality::Critical)));
    for (run, before) in [(&first, "intended"), (&second, "accepted")] {
        let event = events(&reopened, &run.agent_id, "compaction.restart_quarantined");
        assert_eq!(event.len(), 1);
        assert_eq!(event[0].payload["run_id"], run.run_id);
        assert_eq!(event[0].payload["phase"], "manual_review");
        assert_eq!(event[0].payload["previous_phase"], before);
    }
    assert_eq!(
        reopened
            .recover_session_compactions_with_receipts(201)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn operator_review_critical_failure_rolls_back_and_never_replays_native() {
    let (temp, repository, workspace) = setup();
    let run = run_for(
        &repository,
        &workspace,
        "session_review",
        "cmp_review",
        "review-key",
    );
    repository.begin_session_compaction(&run).unwrap();
    repository.recover_session_compactions(200).unwrap();
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_review_critical BEFORE INSERT ON runtime_events
         WHEN NEW.kind='compaction.review_resolved'
         BEGIN SELECT RAISE(ABORT,'injected review event failure'); END;",
        )
        .unwrap();
    let evidence = "Operator independently verified no native compaction took place";
    assert!(repository
        .resolve_session_compaction_review_with_receipt(&run.run_id, "failed", evidence, 201)
        .is_err());
    let unresolved = repository
        .get_session_compaction(&run.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(unresolved.phase, "manual_review");
    assert!(unresolved.review_evidence.is_none());
    assert!(events(&repository, &run.agent_id, "compaction.review_resolved").is_empty());
    connection
        .execute_batch("DROP TRIGGER reject_review_critical")
        .unwrap();

    let receipt = repository
        .resolve_session_compaction_review_with_receipt(&run.run_id, "failed", evidence, 202)
        .unwrap();
    assert!(receipt.changed);
    assert_eq!(receipt.run.phase, "failed");
    assert_eq!(receipt.run.review_evidence.as_deref(), Some(evidence));
    assert_eq!(
        receipt.journal_event.as_ref().unwrap().criticality,
        RuntimeEventCriticality::Critical
    );
    let observed = events(&repository, &run.agent_id, "compaction.review_resolved");
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].payload["phase"], "failed");
    assert!(
        observed[0].payload.get("review_evidence").is_none(),
        "evidence remains in protected operation"
    );
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert!(matches!(
        restarted.resolve_session_compaction_review_with_receipt(
            &run.run_id,
            "failed",
            evidence,
            203
        ),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        events(&restarted, &run.agent_id, "compaction.review_resolved").len(),
        1
    );
}

#[test]
fn completed_operator_review_critical_failure_rolls_back_notices_and_hooks() {
    let (temp, repository, workspace) = setup();
    let (lead_session, lead) =
        workspace_agent_fixture(&workspace, "session_review_lead", "review-lead");
    repository
        .create_workspace_agent(&lead_session, &lead)
        .unwrap();
    let run = run_for(
        &repository,
        &workspace,
        "session_review_completed",
        "cmp_review_completed",
        "review-completed-key",
    );
    let connection = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE workspaces SET lead_agent_id = ?1 WHERE workspace_id = ?2",
            params![lead.agent_id, workspace.workspace_id],
        )
        .unwrap();
    repository
        .update_team_hook_approval(&TeamHookApprovalUpdate {
            workspace_id: workspace.workspace_id.clone(),
            expected_revision: 0,
            hooks: vec![TeamHookResolvedEntry {
                event_name: TeamHookEventName::PostCompaction,
                name: "review-finished".into(),
                command: "printf approved".into(),
                timeout_ms: 1000,
                scope: TeamHookScope::User,
            }],
            approved_at_ms: 100,
        })
        .unwrap();
    repository.begin_session_compaction(&run).unwrap();
    repository.recover_session_compactions(200).unwrap();
    assert!(repository
        .list_workspace_system_notices(10)
        .unwrap()
        .is_empty());
    assert!(repository
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap()
        .is_empty());

    connection
        .execute_batch(
            "CREATE TRIGGER reject_review_completion BEFORE INSERT ON runtime_events
             WHEN NEW.kind='compaction.review_resolved'
             BEGIN SELECT RAISE(ABORT,'injected final review Critical failure'); END;",
        )
        .unwrap();
    let evidence = "Provider record independently verified completed compaction";
    assert!(repository
        .resolve_session_compaction_review_with_receipt(&run.run_id, "completed", evidence, 201)
        .is_err());
    assert_eq!(
        repository
            .get_session_compaction(&run.run_id)
            .unwrap()
            .unwrap()
            .phase,
        "manual_review"
    );
    assert!(repository
        .list_workspace_system_notices(10)
        .unwrap()
        .is_empty());
    assert!(repository
        .list_team_hook_effects(&workspace.workspace_id, 10)
        .unwrap()
        .is_empty());
    connection
        .execute_batch("DROP TRIGGER reject_review_completion")
        .unwrap();

    let receipt = repository
        .resolve_session_compaction_review_with_receipt(&run.run_id, "completed", evidence, 202)
        .unwrap();
    assert!(receipt.changed);
    assert_eq!(receipt.run.phase, "completed");
    assert_eq!(
        receipt.journal_event.unwrap().kind,
        "compaction.review_resolved"
    );
    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        events(&reopened, &run.agent_id, "compaction.review_resolved").len(),
        1
    );
    assert_eq!(reopened.list_workspace_system_notices(10).unwrap().len(), 1);
    assert_eq!(
        reopened
            .list_team_hook_effects(&workspace.workspace_id, 10)
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        reopened.resolve_session_compaction_review_with_receipt(
            &run.run_id,
            "completed",
            evidence,
            203
        ),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(reopened.list_workspace_system_notices(10).unwrap().len(), 1);
    assert_eq!(
        reopened
            .list_team_hook_effects(&workspace.workspace_id, 10)
            .unwrap()
            .len(),
        1
    );
}
