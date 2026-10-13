use super::*;
use runtime_core::{
    prepare_workspace_registration, OperationActor, ProviderKind, RuntimeError,
    WorkspaceAgentLifecycleState, WorkspaceAgentProfile, WorkspaceAgentRecord,
    WorkspaceAgentRecreationPolicy, WorkspaceRegisterRequest,
};
use rusqlite::TransactionBehavior;

fn apply_schema_through(database_path: &std::path::Path, version: i64) {
    let mut connection = open_connection(database_path).expect("connection");
    for migration in crate::schema::MIGRATIONS
        .iter()
        .filter(|migration| migration.version <= version)
    {
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("migration transaction");
        tx.execute_batch(migration.sql).expect("migration sql");
        tx.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
            params![migration.version, migration.version],
        )
        .expect("migration marker");
        tx.commit().expect("migration commit");
    }
}

pub(super) fn registered_workspace(
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

pub(super) fn workspace_agent_fixture(
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
            permission_intent: runtime_core::ProviderPermissionIntent::Explicit {
                mode: "workspace_write".to_string(),
            },
            setting_sources_intent: runtime_core::ProviderSettingSourcesIntent::Explicit {
                sources: vec![
                    runtime_core::ProviderSettingSource::User,
                    runtime_core::ProviderSettingSource::Project,
                ],
            },
            current_preferences: runtime_core::ProviderSessionPreferences {
                thinking_effort: Some(runtime_core::ProviderThinkingEffort::High),
            },
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
fn manual_compaction_admission_replay_and_native_recovery_are_durable() {
    use runtime_core::{ProviderCompactSessionOutcome, SessionCompactionRun};

    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).expect("root");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "sess_compact", "steady-heron-compaction");
    repository
        .create_workspace_agent(&session, &agent)
        .expect("agent");

    let run = SessionCompactionRun {
        run_id: "cmp_1".into(),
        agent_id: session.id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: None,
        agent_revision: 0,
        expected_session_updated_at: session.updated_at,
        idempotency_key: Some("first".into()),
        phase: "intended".into(),
        outcome: None,
        error_code: None,
        review_evidence: None,
        created_at_ms: 101,
        updated_at_ms: 101,
    };
    repository
        .begin_session_compaction(&run)
        .expect("admit before provider");
    // A second runtime instance using the same database must not be able to
    // admit a turn while a native compact request is unresolved, even when
    // it bypasses the manager's in-memory session mutation lock.
    let pending_turn = runtime_core::TurnRecord {
        id: "turn_racing_compaction".into(),
        session_id: session.id.clone(),
        provider_turn_ref: None,
        status: "admitted".into(),
        input: serde_json::json!([{"type":"text","text":"race"}]),
        source: Some("user".into()),
        started_at: Some(102),
        completed_at: None,
        usage: None,
        error: None,
    };
    let pending_admission = runtime_core::TurnAdmissionRecord {
        turn_id: pending_turn.id.clone(),
        session_id: session.id.clone(),
        provider: "codex".into(),
        projection_source: runtime_core::TurnInputProjectionSource::UserVisible,
        user_input_snapshot: runtime_core::PersistedUserInputSnapshot {
            prompt_text: "race".into(),
            ..Default::default()
        },
        dispatch_policy: runtime_core::TurnDispatchPolicySnapshot {
            permission_mode: None,
            pre_dispatch_approval_required: false,
            turn_settings: Default::default(),
        },
        correlation: Default::default(),
        dispatch_state: runtime_core::TurnDispatchState::Pending,
        provider_native_turn_id: None,
        dispatch_error: None,
        admitted_at: 102,
        updated_at: 102,
    };
    let mut admitted_session = session.clone();
    admitted_session.status = "turn_admitted".into();
    admitted_session.active_turn_id = Some(pending_turn.id.clone());
    admitted_session.updated_at = 102;
    assert!(matches!(
        repository.admit_turn(&pending_admission, &pending_turn, &admitted_session, None),
        Err(RuntimeError::Conflict(_))
    ));
    assert!(repository
        .unresolved_session_compaction(&session.id)
        .unwrap());
    assert_eq!(
        repository
            .get_session_compaction_by_key(&session.id, "first")
            .unwrap(),
        Some(run.clone())
    );
    let mut other = run.clone();
    other.run_id = "cmp_2".into();
    other.idempotency_key = Some("different".into());
    assert!(matches!(
        repository.begin_session_compaction(&other),
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(repository.begin_session_compaction(&run).unwrap(), run);

    let accepted = repository
        .transition_session_compaction(
            "cmp_1",
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            102,
        )
        .expect("native admission accepted");
    assert_eq!(accepted.phase, "accepted");
    assert!(repository
        .unresolved_session_compaction(&session.id)
        .unwrap());
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    assert_eq!(
        restarted.get_session_compaction("cmp_1").unwrap(),
        Some(accepted)
    );
    assert!(restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
    let completed = restarted
        .transition_session_compaction(
            "cmp_1",
            "completed",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            103,
        )
        .expect("provider completion evidence");
    assert_eq!(completed.phase, "completed");
    assert!(!restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
    assert!(matches!(
        restarted.transition_session_compaction("cmp_1", "accepted", None, None, 104),
        Err(RuntimeError::Conflict(_))
    ));

    restarted
        .begin_session_compaction(&other)
        .expect("next fresh request");
    session.provider_session_ref = Some("different-native-thread".into());
    session.updated_at += 1;
    restarted
        .upsert_session(&session)
        .expect("native session replacement");
    assert!(matches!(
        restarted.transition_session_compaction("cmp_2", "completed", None, None, 105),
        Err(RuntimeError::Conflict(_))
    ));
    let unresolved = restarted
        .transition_session_compaction(
            "cmp_2",
            "manual_review",
            None,
            Some("attachment_changed"),
            106,
        )
        .expect("fail closed");
    assert_eq!(unresolved.phase, "manual_review");
    assert!(restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
    assert!(
        restarted
            .resolve_session_compaction_review(
                "cmp_2",
                "completed",
                "Provider reports compaction completed",
                107
            )
            .is_err(),
        "old attachment cannot be falsely confirmed complete"
    );
    let resolved = restarted
        .resolve_session_compaction_review(
            "cmp_2",
            "failed",
            "Operator confirmed no native compact completion",
            108,
        )
        .expect("explicit native outcome review");
    assert_eq!(resolved.phase, "failed");
    assert!(resolved.review_evidence.is_some());
    assert!(!restarted
        .unresolved_session_compaction(&session.id)
        .unwrap());
}

#[test]
fn restart_classifies_pre_dispatch_and_accepted_compaction_without_replaying_provider() {
    use runtime_core::{ProviderCompactSessionOutcome, SessionCompactionRun};

    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (session, agent) =
        workspace_agent_fixture(&workspace, "sess_compact_restart", "nimble-heron-restart");
    repository
        .create_workspace_agent(&session, &agent)
        .expect("create agent");

    let run = SessionCompactionRun {
        run_id: "cmp_pending_restart".into(),
        agent_id: session.id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: session.canonical_provider_session_ref,
        agent_revision: agent.revision,
        expected_session_updated_at: session.updated_at,
        idempotency_key: Some("retry-after-restart".into()),
        phase: "intended".into(),
        outcome: None,
        error_code: None,
        review_evidence: None,
        created_at_ms: 101,
        updated_at_ms: 101,
    };
    repository
        .begin_session_compaction(&run)
        .expect("admit run");
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let recovered = restarted
        .recover_session_compactions(200)
        .expect("restart recovery");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].phase, "manual_review");
    assert_eq!(
        recovered[0].error_code.as_deref(),
        Some("restart_native_compaction_outcome_unverified")
    );
    assert_eq!(restarted.recover_session_compactions(201).unwrap().len(), 0);
    assert_eq!(
        restarted.begin_session_compaction(&run).unwrap(),
        recovered[0]
    );
    assert!(restarted
        .unresolved_session_compaction(&run.agent_id)
        .unwrap());
    let settled = restarted
        .resolve_session_compaction_review(
            &run.run_id,
            "failed",
            "Operator confirmed no provider work was dispatched",
            202,
        )
        .expect("explicit review");
    assert_eq!(settled.phase, "failed");
    assert!(!restarted
        .unresolved_session_compaction(&run.agent_id)
        .unwrap());

    let mut next_run = run.clone();
    next_run.run_id = "cmp_accepted_restart".into();
    next_run.idempotency_key = Some("second-after-restart".into());
    restarted
        .begin_session_compaction(&next_run)
        .expect("second run");
    restarted
        .transition_session_compaction(
            &next_run.run_id,
            "accepted",
            Some(ProviderCompactSessionOutcome::Accepted),
            None,
            205,
        )
        .expect("native admitted");
    let recovered = restarted
        .recover_session_compactions(210)
        .expect("accepted after restart");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].phase, "manual_review");
    assert_eq!(
        recovered[0].outcome,
        Some(ProviderCompactSessionOutcome::Accepted)
    );
    assert_eq!(
        restarted.begin_session_compaction(&next_run).unwrap(),
        recovered[0]
    );
}

#[test]
fn provider_context_snapshot_rejects_late_identity_revision_and_observation_replays() {
    use runtime_core::{ProviderContextLimitObservation, SessionContextLimitSnapshot};

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "sess_context", "steady-heron-context");
    repository
        .create_workspace_agent(&session, &agent)
        .expect("create workspace agent");

    let mut snapshot = SessionContextLimitSnapshot {
        agent_id: session.id.clone(),
        provider: ProviderKind::Codex,
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: None,
        agent_revision: 0,
        observation: ProviderContextLimitObservation {
            model_context_window: 200_000,
            last_total_tokens: 20_000,
            remaining_percentage: 90,
        },
        observed_at_ms: 200,
        observed_turn_id: Some("turn_1".into()),
    };
    assert!(repository
        .record_session_context_limit(&snapshot, session.updated_at)
        .expect("initial observation"));
    assert_eq!(
        repository.get_session_context_limit(&session.id).unwrap(),
        Some(snapshot.clone())
    );

    let mut equal_timestamp = snapshot.clone();
    equal_timestamp.observation.last_total_tokens += 1;
    assert!(!repository
        .record_session_context_limit(&equal_timestamp, session.updated_at)
        .expect("same-timestamp conflicting observation is ignored"));
    assert_eq!(
        repository.get_session_context_limit(&session.id).unwrap(),
        Some(snapshot.clone())
    );

    let mut out_of_order = snapshot.clone();
    out_of_order.observed_at_ms = 199;
    out_of_order.observation.last_total_tokens = 10_000;
    assert!(!repository
        .record_session_context_limit(&out_of_order, session.updated_at)
        .expect("stale observation is ignored"));
    assert_eq!(
        repository.get_session_context_limit(&session.id).unwrap(),
        Some(snapshot.clone())
    );
    assert!(!repository
        .record_session_context_limit(&snapshot, session.updated_at + 1)
        .expect("stale session update rejected"));

    snapshot.observation.remaining_percentage = 101;
    assert!(repository
        .record_session_context_limit(&snapshot, session.updated_at)
        .is_err());
    snapshot.observation.remaining_percentage = 90;

    // Re-attaching the same logical agent with a different native identity
    // invalidates the projection even if the old observation remains on disk.
    session.provider_session_ref = Some("fresh-native-provider-thread".into());
    session.canonical_provider_session_ref = Some("fresh-native-provider-thread".into());
    session.updated_at += 1;
    repository
        .upsert_session(&session)
        .expect("native reattach");
    assert!(repository
        .get_session_context_limit(&session.id)
        .unwrap()
        .is_none());
    assert!(!repository
        .record_session_context_limit(&snapshot, 100)
        .unwrap());

    let mut fresh = snapshot;
    fresh.provider_session_ref = session.provider_session_ref.clone().unwrap();
    fresh.canonical_provider_session_ref = session.canonical_provider_session_ref.clone();
    fresh.observed_at_ms += 1;
    fresh.observation.last_total_tokens = 30_000;
    fresh.observation.remaining_percentage = 85;
    assert!(repository
        .record_session_context_limit(&fresh, session.updated_at)
        .unwrap());

    // Recreating the repository proves the evidence, not an in-memory cache,
    // survives restarts while retaining the exact current provider attachment.
    let restarted = SqliteRuntimeRepository::new(temp_dir.path().join("runtime.sqlite3"));
    assert_eq!(
        restarted.get_session_context_limit(&session.id).unwrap(),
        Some(fresh.clone())
    );

    // A revisioned policy mutation invalidates the old projection without
    // changing the native provider reference.
    session.updated_at += 1;
    restarted
        .compare_and_set_workspace_agent_recreation_policy(
            &session,
            &session.id,
            0,
            &agent.recreation_policy,
            session.updated_at,
        )
        .expect("policy revision mutation");
    assert!(restarted
        .get_session_context_limit(&session.id)
        .unwrap()
        .is_none());
    assert!(!restarted
        .record_session_context_limit(&fresh, session.updated_at)
        .unwrap());
    fresh.agent_revision = 1;
    fresh.observed_at_ms += 1;
    assert!(restarted
        .record_session_context_limit(&fresh, session.updated_at)
        .unwrap());
    assert_eq!(
        restarted.get_session_context_limit(&session.id).unwrap(),
        Some(fresh)
    );
}

#[test]
fn workspace_agent_recreation_policy_cas_is_durable_and_revision_protected() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let workspace = registered_workspace(&repository, &root);
    let (mut session, agent) =
        workspace_agent_fixture(&workspace, "sess_policy_cas", "steady-otter-cas");
    repository
        .create_workspace_agent(&session, &agent)
        .expect("create agent");

    let mut updated_policy = agent.recreation_policy.clone();
    updated_policy.permission_intent =
        runtime_core::ProviderPermissionIntent::InheritProviderConfiguration;
    updated_policy.current_preferences = runtime_core::ProviderSessionPreferences {
        thinking_effort: Some(runtime_core::ProviderThinkingEffort::Max),
    };
    session.permission_mode = None;
    session.updated_at = 200;

    let updated = repository
        .compare_and_set_workspace_agent_recreation_policy(
            &session,
            session.id.as_str(),
            0,
            &updated_policy,
            200,
        )
        .expect("cas recreation policy");
    assert_eq!(updated.revision, 1);
    assert_eq!(updated.recreation_policy, updated_policy);
    assert_eq!(updated.updated_at, 200);

    let stale = repository.compare_and_set_workspace_agent_recreation_policy(
        &session,
        session.id.as_str(),
        0,
        &agent.recreation_policy,
        201,
    );
    assert!(matches!(stale, Err(RuntimeError::Conflict(_))));

    let hydrated = repository
        .get_workspace_agent_by_id(session.id.as_str())
        .expect("read persisted agent")
        .expect("agent exists");
    assert_eq!(hydrated.revision, 1);
    assert_eq!(hydrated.recreation_policy, updated_policy);
    let hydrated_session = repository
        .hydrate_runtime_state()
        .expect("hydrate state")
        .sessions
        .into_iter()
        .find(|candidate| candidate.id == session.id)
        .expect("hydrated session");
    assert_eq!(hydrated_session.permission_mode, None);
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

#[test]
fn model_refresh_migration_updates_builtin_provider_state_without_crossing_acp() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("runtime.sqlite3");
    apply_schema_through(&db_path, 8);
    let repository = SqliteRuntimeRepository::new(db_path.clone());
    let root = temp_dir.path().join("model-refresh-workspace");
    std::fs::create_dir_all(&root).expect("workspace root");
    let workspace = registered_workspace(&repository, &root);

    let (mut codex_session, mut codex_agent) =
        workspace_agent_fixture(&workspace, "sess_retired_codex", "steady-otter-m1");
    codex_session.model = Some("gpt-5.4".to_string());
    codex_agent.recreation_policy.model = Some("gpt-5.4".to_string());
    repository
        .create_workspace_agent(&codex_session, &codex_agent)
        .expect("seed retired codex agent");
    let legacy_policy = serde_json::json!({
        "provider": "codex",
        "model": "gpt-5.4",
        "permission_intent": null,
        "setting_sources_intent": [],
        "system_prompt": "persist me",
        "allowed_tools": ["read"],
        "disallowed_tools": ["danger"],
        "authoritative_cwd": codex_agent.recreation_policy.authoritative_cwd,
        "harness_version_slot": null
    });
    open_connection(&db_path)
        .expect("legacy policy connection")
        .execute(
            "UPDATE workspace_agents SET recreation_policy_json = ?2 WHERE session_id = ?1",
            params![
                codex_session.id,
                serde_json::to_string(&legacy_policy).expect("legacy policy json")
            ],
        )
        .expect("replace recreation policy with legacy wire shape");

    let mut claude_session = sample_session();
    claude_session.id = "sess_retired_claude".to_string();
    claude_session.provider = "claude".to_string();
    claude_session.model = Some("claude-opus-4-9-20260901".to_string());
    claude_session.active_turn_id = None;
    claude_session.provider_session_ref = None;
    claude_session.worktree_id = None;
    repository
        .upsert_session(&claude_session)
        .expect("seed retired claude session");

    let mut acp_session = sample_session();
    acp_session.id = "sess_agent_managed".to_string();
    acp_session.provider = "acp".to_string();
    acp_session.model = Some("gpt-5.4".to_string());
    acp_session.active_turn_id = None;
    acp_session.provider_session_ref = None;
    acp_session.worktree_id = None;
    repository
        .upsert_session(&acp_session)
        .expect("seed ACP session");

    repository
        .initialize_schema()
        .expect("apply model refresh migration");

    let hydrated = repository
        .hydrate_runtime_state()
        .expect("hydrate migrated state");
    let session_model = |id: &str| {
        hydrated
            .sessions
            .iter()
            .find(|session| session.id == id)
            .and_then(|session| session.model.as_deref())
            .map(str::to_string)
    };
    assert_eq!(
        session_model("sess_retired_codex").as_deref(),
        Some("gpt-6.1-sol")
    );
    assert_eq!(
        session_model("sess_retired_claude").as_deref(),
        Some("claude-opus-5-5")
    );
    assert_eq!(
        session_model("sess_agent_managed").as_deref(),
        Some("gpt-5.4")
    );

    let migrated_agent = repository
        .get_workspace_agent_by_id("sess_retired_codex")
        .expect("read migrated agent")
        .expect("migrated agent exists");
    assert_eq!(
        migrated_agent.recreation_policy.model.as_deref(),
        Some("gpt-6.1-sol")
    );
    assert_eq!(
        migrated_agent.recreation_policy.permission_intent,
        runtime_core::ProviderPermissionIntent::ProviderDefault
    );
    assert_eq!(
        migrated_agent.recreation_policy.setting_sources_intent,
        runtime_core::ProviderSettingSourcesIntent::Isolated
    );
    assert_eq!(
        migrated_agent.recreation_policy.current_preferences,
        runtime_core::ProviderSessionPreferences::default()
    );
    assert_eq!(migrated_agent.revision, codex_agent.revision + 1);

    repository
        .initialize_schema()
        .expect("model migration is restart-idempotent");
    let replayed_agent = repository
        .get_workspace_agent_by_id("sess_retired_codex")
        .expect("read replayed agent")
        .expect("replayed agent exists");
    assert_eq!(replayed_agent.revision, migrated_agent.revision);
}
