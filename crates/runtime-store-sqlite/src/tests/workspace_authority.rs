use super::*;
use runtime_core::{prepare_workspace_registration, OperationActor, WorkspaceRegisterRequest};

#[test]
fn generation_one_database_upgrades_without_changing_legacy_state() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("runtime.sqlite3");
    {
        let connection = Connection::open(&db_path).expect("open raw");
        connection
            .execute_batch(crate::schema::MIGRATION_1_SQL)
            .expect("apply generation one schema");
        connection
            .execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (1, 1)",
                [],
            )
            .expect("record generation one migration");
    }

    let repository = SqliteRuntimeRepository::new(db_path.clone());
    let session = sample_session();
    repository.upsert_session(&session).expect("session");
    repository.upsert_turn(&sample_turn()).expect("turn");
    repository
        .upsert_approval(&ApprovalRecord {
            id: "approval_1".to_string(),
            session_id: session.id.clone(),
            turn_id: "turn_1".to_string(),
            origin: "legacy".to_string(),
            tool_call_id: Some("tool_1".to_string()),
            provider_approval_ref: Some("provider_approval_1".to_string()),
            status: "pending".to_string(),
            request: serde_json::json!({"command":"git status"}),
            response: None,
            created_at: 102,
            resolved_at: None,
        })
        .expect("approval");
    repository
        .upsert_team(&TeamRecord {
            id: "team_legacy".to_string(),
            name: "Legacy Team".to_string(),
            lead_agent_id: session.id.clone(),
            created_by: "user".to_string(),
            created_at: 103,
            updated_at: 103,
            deleted_at: None,
        })
        .expect("team");
    repository
        .upsert_team_member(&TeamMemberRecord {
            team_id: "team_legacy".to_string(),
            agent_id: session.id.clone(),
            title: Some("lead".to_string()),
            joined_at: 103,
            added_by: "user".to_string(),
            creator_agent_id: None,
            creator_compaction_subscription: "auto".to_string(),
            worktree_id: Some("wt_legacy".to_string()),
        })
        .expect("member");
    repository
        .upsert_team_message(&TeamMessageRecord {
            id: "msg_legacy".to_string(),
            team_id: "team_legacy".to_string(),
            scope: "direct".to_string(),
            sender_agent_id: session.id.clone(),
            recipient_agent_ids: serde_json::json!([session.id.clone()]),
            input: serde_json::json!([{"type":"text","text":"preserve me"}]),
            image_paths: serde_json::json!(["/tmp/legacy.png"]),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: Some("legacy-correlation".to_string()),
            reply_to_message_id: None,
            idempotency_key: Some("legacy-message-key".to_string()),
            created_at: 104,
        })
        .expect("message");
    repository
        .upsert_team_delivery(&TeamDeliveryRecord {
            id: "delivery_legacy".to_string(),
            message_id: "msg_legacy".to_string(),
            team_id: "team_legacy".to_string(),
            recipient_agent_id: session.id.clone(),
            provider: "codex".to_string(),
            status: "pending".to_string(),
            effective_policy: Some("non_interrupting".to_string()),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: None,
            last_error_message: None,
            created_at: 104,
            updated_at: 104,
        })
        .expect("delivery");
    repository
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            id: "wt_legacy".to_string(),
            repo_root: "/tmp/legacy-repo".to_string(),
            worktree_root: "/tmp/legacy-worktrees".to_string(),
            worktree_cwd: "/tmp/legacy-worktrees/feature".to_string(),
            branch_name: "feature/legacy".to_string(),
            worktree_name: "legacy".to_string(),
            unified_workspace_path: "legacy_repo".to_string(),
            deletion_policy: "retain_on_last_claim".to_string(),
            created_by_session_id: Some(session.id.clone()),
            created_by_operation_id: Some("legacy_op".to_string()),
            created_at: 105,
            updated_at: 105,
        })
        .expect("worktree");
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_legacy".to_string(),
            session_id: session.id.clone(),
            claim_role: "primary".to_string(),
            created_at: 105,
            released_at: None,
        })
        .expect("worktree claim");
    repository
        .upsert_process(&ProcessRecord {
            id: "process_legacy".to_string(),
            session_id: Some(session.id.clone()),
            tool_call_id: Some("tool_legacy".to_string()),
            pid: Some(1234),
            command: serde_json::json!(["echo", "legacy"]),
            cwd: Some("/tmp/legacy-repo".to_string()),
            status: "completed".to_string(),
            exit_code: Some(0),
            signal: None,
            stdout_path: Some("/tmp/legacy.out".to_string()),
            stderr_path: Some("/tmp/legacy.err".to_string()),
            started_at: 106,
            ended_at: Some(107),
            timeout_ms: Some(60_000),
        })
        .expect("process");
    repository
        .upsert_credential(&CredentialRecord {
            id: "credential_legacy".to_string(),
            provider: "codex".to_string(),
            profile: "default".to_string(),
            kind: "api_key".to_string(),
            encrypted_secret: "encrypted".to_string(),
            metadata: serde_json::json!({"source":"legacy"}),
            created_at: 108,
            updated_at: 108,
        })
        .expect("credential");
    repository
        .upsert_team_operation_journal(&runtime_core::TeamOperationJournalRecord {
            operation_id: "teamop_legacy".to_string(),
            team_id: "team_legacy".to_string(),
            kind: "spawn".to_string(),
            stage: "completed".to_string(),
            payload: serde_json::json!({"preserved":true}),
            created_at: 109,
            updated_at: 109,
        })
        .expect("team operation");
    repository
        .append_team_operation_diagnostic(
            Some("teamop_legacy"),
            Some("team_legacy"),
            "legacy_diagnostic",
            "preserve diagnostic",
            &serde_json::json!({"detail":"legacy"}),
            110,
        )
        .expect("diagnostic");
    repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_legacy".to_string(),
            scope: RuntimeEventScope::Session,
            scope_id: session.id.clone(),
            session_id: Some(session.id.clone()),
            team_id: Some("team_legacy".to_string()),
            turn_id: Some("turn_1".to_string()),
            kind: "legacy.event".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"preserved":true}),
            provider: Some("codex".to_string()),
            provider_seq: Some(42),
            created_at: 111,
        })
        .expect("event");

    let before = repository.hydrate_runtime_state().expect("hydrate before");
    let before_events = repository
        .list_runtime_events(None, None, 100)
        .expect("events before");

    repository.initialize_schema().expect("upgrade schema");

    let mut after = repository.hydrate_runtime_state().expect("hydrate after");
    let agent_messages = std::mem::take(&mut after.agent_messages);
    let agent_deliveries = std::mem::take(&mut after.agent_deliveries);
    assert_eq!(
        after, before,
        "all generation-one authority must remain unchanged"
    );
    assert_eq!(agent_messages.len(), 1, "legacy message is backfilled once");
    assert_eq!(agent_messages[0].id, "msg_legacy");
    assert_eq!(
        agent_messages[0].context_kind,
        runtime_core::AgentMessageContextKind::LegacyTeam
    );
    assert_eq!(
        agent_messages[0].legacy_team_id.as_deref(),
        Some("team_legacy")
    );
    assert_eq!(
        agent_deliveries.len(),
        1,
        "legacy delivery is backfilled once"
    );
    assert_eq!(agent_deliveries[0].id, "delivery_legacy");
    assert_eq!(agent_deliveries[0].status, "pending");
    assert_eq!(
        repository
            .list_runtime_events(None, None, 100)
            .expect("events after"),
        before_events
    );
    assert_eq!(
        repository
            .list_workspaces()
            .expect("new workspace table remains empty"),
        Vec::new()
    );
}

#[test]
fn failed_migration_rolls_back_and_retry_resumes_cleanly() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("runtime.sqlite3");
    {
        let connection = Connection::open(&db_path).expect("open raw");
        connection
            .execute_batch(crate::schema::MIGRATION_1_SQL)
            .expect("generation one schema");
        connection
            .execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (1, 1)",
                [],
            )
            .expect("record generation one");
        connection
            .execute_batch(
                "CREATE TRIGGER fail_next_migration
                 BEFORE INSERT ON schema_migrations
                 WHEN NEW.version = 2
                 BEGIN
                   SELECT RAISE(ABORT, 'forced migration failure');
                 END;",
            )
            .expect("failure trigger");
    }

    let repository = SqliteRuntimeRepository::new(db_path.clone());
    assert!(repository.initialize_schema().is_err());
    let connection = Connection::open(&db_path).expect("open after failure");
    let workspace_table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='workspaces'",
            [],
            |row| row.get(0),
        )
        .expect("workspace table count");
    assert_eq!(workspace_table_count, 0, "migration DDL must roll back");
    let versions = connection
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .expect("versions")
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("query versions")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect versions");
    assert_eq!(versions, vec![1]);
    connection
        .execute_batch("DROP TRIGGER fail_next_migration")
        .expect("remove failure trigger");
    drop(connection);

    repository.initialize_schema().expect("retry migration");
    assert!(repository.list_workspaces().expect("workspaces").is_empty());
    let connection = Connection::open(&db_path).expect("reopen after retry");
    let versions = connection
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .expect("versions")
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("query versions")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect versions");
    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

fn registration_command(
    root: &std::path::Path,
    display_name: &str,
    idempotency_key: Option<&str>,
) -> runtime_core::WorkspaceRegisterCommand {
    prepare_workspace_registration(
        WorkspaceRegisterRequest {
            canonical_root: root.display().to_string(),
            display_name: Some(display_name.to_string()),
        },
        OperationActor::operator("test_operator"),
        idempotency_key.map(str::to_string),
    )
    .expect("prepare registration")
}

#[test]
fn workspace_registration_replays_exact_result_and_rejects_key_reuse_with_different_input() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir(&root).expect("workspace root");

    let first = repository
        .register_workspace(&registration_command(
            &root,
            "Alpha",
            Some("register-alpha"),
        ))
        .expect("first registration");
    let replay = repository
        .register_workspace(&registration_command(
            &root,
            "Alpha",
            Some("register-alpha"),
        ))
        .expect("idempotent replay");
    assert_eq!(replay, first);
    assert_eq!(repository.list_workspaces().expect("list").len(), 1);

    let conflict = repository.register_workspace(&registration_command(
        &root,
        "Different display name",
        Some("register-alpha"),
    ));
    assert!(matches!(
        conflict,
        Err(runtime_core::RuntimeError::Conflict(_))
    ));
    assert_eq!(
        repository
            .list_workspaces()
            .expect("list after conflict")
            .len(),
        1
    );
    let details = repository
        .get_operation(&first.operation_id)
        .expect("operation query")
        .expect("operation");
    assert_eq!(
        details.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(details.transitions.len(), 2);
    assert!(details.claims.is_empty());
    assert!(details.effects.is_empty());
    assert!(details.outbox.is_empty());
    assert!(details.receipts.is_empty());
}

#[test]
fn failed_workspace_commit_leaves_no_half_operation_and_retry_succeeds() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir(&root).expect("workspace root");
    let command = registration_command(&root, "Atomic", Some("atomic-register"));

    {
        let connection = open_connection(&temp_dir.path().join("runtime.sqlite3")).expect("open");
        connection
            .execute_batch(
                "CREATE TRIGGER fail_workspace_insert
                 BEFORE INSERT ON workspaces
                 BEGIN
                   SELECT RAISE(ABORT, 'forced workspace insert failure');
                 END;",
            )
            .expect("failure trigger");
    }
    assert!(repository.register_workspace(&command).is_err());
    assert!(repository
        .list_workspaces()
        .expect("list failed")
        .is_empty());
    assert!(repository
        .get_operation(&command.operation_id)
        .expect("operation query")
        .is_none());

    {
        let connection = open_connection(&temp_dir.path().join("runtime.sqlite3")).expect("open");
        let claim_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM runtime_operation_resource_claims",
                [],
                |row| row.get(0),
            )
            .expect("claims");
        assert_eq!(claim_count, 0);
        connection
            .execute_batch("DROP TRIGGER fail_workspace_insert")
            .expect("drop trigger");
    }

    let retried = repository
        .register_workspace(&command)
        .expect("retry succeeds");
    assert_eq!(retried.operation_id, command.operation_id);
    assert_eq!(repository.list_workspaces().expect("list").len(), 1);
}

#[test]
fn concurrent_same_root_registration_converges_on_one_workspace_identity() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir(&root).expect("workspace root");
    let writer_count = 12usize;
    let barrier = Arc::new(Barrier::new(writer_count));
    let mut handles = Vec::new();
    for index in 0..writer_count {
        let repository = repository.clone();
        let barrier = barrier.clone();
        let root = root.clone();
        handles.push(thread::spawn(move || {
            let command = registration_command(&root, &format!("Workspace {index}"), None);
            barrier.wait();
            repository.register_workspace(&command)
        }));
    }

    let mut workspace_ids = std::collections::BTreeSet::new();
    for handle in handles {
        let response = handle
            .join()
            .expect("registration thread")
            .expect("registration result");
        workspace_ids.insert(response.workspace.workspace_id);
    }
    assert_eq!(workspace_ids.len(), 1);
    assert_eq!(repository.list_workspaces().expect("list").len(), 1);
}
