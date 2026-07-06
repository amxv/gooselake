use super::*;
use crate::db::open_connection;
use runtime_core::{
    ApprovalRecord, CredentialRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
    NewRuntimeEvent, ProcessRecord, RuntimeEventCriticality, RuntimeEventScope, RuntimeStore,
    SessionRecord, TeamDeliveryRecord, TeamMemberRecord, TeamMessageRecord, TeamRecord, TurnRecord,
};
use rusqlite::{params, Connection};

fn repo(temp_dir: &tempfile::TempDir) -> SqliteRuntimeRepository {
    SqliteRuntimeRepository::new(temp_dir.path().join("runtime.sqlite3"))
}

fn sample_session() -> SessionRecord {
    SessionRecord {
        id: "session_1".to_string(),
        provider: "codex".to_string(),
        status: "active".to_string(),
        cwd: Some("/tmp/repo".to_string()),
        model: Some("gpt-5.4".to_string()),
        permission_mode: Some("workspace_write".to_string()),
        system_prompt: Some("you are helpful".to_string()),
        metadata: serde_json::json!({"source": "test"}),
        provider_session_ref: Some("prov_sess_1".to_string()),
        canonical_provider_session_ref: None,
        active_turn_id: Some("turn_1".to_string()),
        worktree_id: Some("wt_1".to_string()),
        created_at: 100,
        updated_at: 101,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    }
}

fn sample_turn() -> TurnRecord {
    TurnRecord {
        id: "turn_1".to_string(),
        session_id: "session_1".to_string(),
        provider_turn_ref: Some("prov_turn_1".to_string()),
        status: "running".to_string(),
        input: serde_json::json!([{"type": "text", "text": "hello"}]),
        source: Some("user".to_string()),
        started_at: Some(101),
        completed_at: None,
        usage: None,
        error: None,
    }
}

#[test]
fn initialize_schema_creates_all_phase_two_tables() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);

    repository.initialize_schema().expect("initialize schema");

    let connection = open_connection(&temp_dir.path().join("runtime.sqlite3")).expect("open");
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type='table'")
        .expect("prepare");
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");

    let table_names = rows.into_iter().collect::<std::collections::BTreeSet<_>>();
    for expected in [
        "schema_migrations",
        "sessions",
        "turns",
        "approvals",
        "runtime_events",
        "teams",
        "team_members",
        "team_messages",
        "team_deliveries",
        "managed_worktrees",
        "managed_worktree_claims",
        "processes",
        "credentials",
        "team_operation_journal",
        "team_operation_diagnostics",
        "diagnostics_journal",
    ] {
        assert!(table_names.contains(expected), "missing table {expected}");
    }
}

#[test]
fn initialize_schema_migrates_partially_populated_database_without_reset() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("runtime.sqlite3");

    {
        let connection = Connection::open(&db_path).expect("open raw");
        connection
            .execute_batch(
                "CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    status TEXT NOT NULL,
                    cwd TEXT,
                    model TEXT,
                    permission_mode TEXT,
                    system_prompt TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    provider_session_ref TEXT,
                    canonical_provider_session_ref TEXT,
                    active_turn_id TEXT,
                    worktree_id TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    closed_at INTEGER,
                    failure_code TEXT,
                    failure_message TEXT
                );",
            )
            .expect("create partial schema");
        connection
            .execute(
                "INSERT INTO sessions (id, provider, status, metadata_json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params!["existing_session", "codex", "created", "{}", 1_i64, 1_i64],
            )
            .expect("insert session");
    }

    let repository = SqliteRuntimeRepository::new(db_path.clone());
    repository.initialize_schema().expect("schema migration");

    let hydrated = repository.hydrate_runtime_state().expect("hydrate");
    assert_eq!(hydrated.sessions.len(), 1);
    assert_eq!(hydrated.sessions[0].id, "existing_session");

    let connection = open_connection(&db_path).expect("open");
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='runtime_events'",
            [],
            |row| row.get(0),
        )
        .expect("query");
    assert_eq!(count, 1);
}

#[test]
fn append_runtime_event_assigns_monotonic_scope_sequence() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    let first = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_1".to_string(),
            scope: RuntimeEventScope::Session,
            scope_id: "session_1".to_string(),
            session_id: Some("session_1".to_string()),
            team_id: None,
            turn_id: Some("turn_1".to_string()),
            kind: "turn.started".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"status": "started"}),
            provider: Some("codex".to_string()),
            provider_seq: Some(11),
            created_at: 100,
        })
        .expect("append first event");
    let second = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_2".to_string(),
            scope: RuntimeEventScope::Session,
            scope_id: "session_1".to_string(),
            session_id: Some("session_1".to_string()),
            team_id: None,
            turn_id: Some("turn_1".to_string()),
            kind: "turn.completed".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"status": "completed"}),
            provider: Some("codex".to_string()),
            provider_seq: Some(12),
            created_at: 101,
        })
        .expect("append second event");
    let third = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_3".to_string(),
            scope: RuntimeEventScope::Team,
            scope_id: "team_1".to_string(),
            session_id: None,
            team_id: Some("team_1".to_string()),
            turn_id: None,
            kind: "team.created".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"name": "alpha"}),
            provider: None,
            provider_seq: None,
            created_at: 102,
        })
        .expect("append third event");

    assert_eq!(first.seq, 1);
    assert_eq!(second.seq, 2);
    assert_eq!(third.seq, 1);
}

#[test]
fn append_runtime_event_is_idempotent_for_existing_event_id() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    let first = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_same".to_string(),
            scope: RuntimeEventScope::Session,
            scope_id: "session_1".to_string(),
            session_id: Some("session_1".to_string()),
            team_id: None,
            turn_id: None,
            kind: "session.created".to_string(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"a": 1}),
            provider: Some("codex".to_string()),
            provider_seq: Some(1),
            created_at: 1,
        })
        .expect("append first event");

    let second = repository
        .append_runtime_event(&NewRuntimeEvent {
            event_id: "evt_same".to_string(),
            scope: RuntimeEventScope::Session,
            scope_id: "session_ignored".to_string(),
            session_id: Some("session_ignored".to_string()),
            team_id: None,
            turn_id: None,
            kind: "session.changed".to_string(),
            criticality: RuntimeEventCriticality::Droppable,
            payload: serde_json::json!({"a": 2}),
            provider: Some("codex".to_string()),
            provider_seq: Some(2),
            created_at: 2,
        })
        .expect("append second event");

    assert_eq!(first.row_id, second.row_id);
    assert_eq!(first.scope_id, second.scope_id);
    assert_eq!(first.kind, second.kind);

    let events = repository
        .list_runtime_events(None, None, 10)
        .expect("list events");
    assert_eq!(events.len(), 1);
}

#[test]
fn concurrent_appends_same_scope_allocate_distinct_monotonic_sequences() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    let scope_id = "session_concurrent";
    let writer_count = 20usize;
    let barrier = Arc::new(Barrier::new(writer_count));

    let mut handles = Vec::with_capacity(writer_count);
    for writer_idx in 0..writer_count {
        let barrier = barrier.clone();
        let repository = repository.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            repository.append_runtime_event(&NewRuntimeEvent {
                event_id: format!("evt_concurrent_{writer_idx}"),
                scope: RuntimeEventScope::Session,
                scope_id: scope_id.to_string(),
                session_id: Some(scope_id.to_string()),
                team_id: None,
                turn_id: None,
                kind: "turn.delta".to_string(),
                criticality: RuntimeEventCriticality::Droppable,
                payload: serde_json::json!({"writer_idx": writer_idx}),
                provider: Some("codex".to_string()),
                provider_seq: Some(writer_idx as i64),
                created_at: 1_000 + writer_idx as i64,
            })
        }));
    }

    let mut appended = Vec::with_capacity(writer_count);
    for handle in handles {
        let event = handle
            .join()
            .expect("writer thread panicked")
            .expect("append should succeed");
        appended.push(event);
    }
    assert_eq!(appended.len(), writer_count);

    let events = repository
        .list_runtime_events(
            Some((RuntimeEventScope::Session, scope_id)),
            None,
            writer_count + 5,
        )
        .expect("list scoped events");
    assert_eq!(events.len(), writer_count);

    let sequences = events.iter().map(|event| event.seq).collect::<Vec<_>>();
    assert_eq!(
        sequences,
        (1_i64..=(writer_count as i64)).collect::<Vec<_>>()
    );
}

#[test]
fn team_message_idempotency_key_retry_converges_with_different_id() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    let session = sample_session();
    repository.upsert_session(&session).expect("insert session");
    repository
        .upsert_team(&TeamRecord {
            id: "team_1".to_string(),
            name: "Team Alpha".to_string(),
            lead_agent_id: session.id.clone(),
            created_by: "user".to_string(),
            created_at: 100,
            updated_at: 100,
            deleted_at: None,
        })
        .expect("insert team");

    repository
        .upsert_team_message(&TeamMessageRecord {
            id: "msg_1".to_string(),
            team_id: "team_1".to_string(),
            scope: "direct".to_string(),
            sender_agent_id: session.id.clone(),
            recipient_agent_ids: serde_json::json!([session.id.clone()]),
            input: serde_json::json!([{"type":"text","text":"first"}]),
            image_paths: serde_json::json!([]),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("idem_same".to_string()),
            created_at: 100,
        })
        .expect("insert message");
    repository
        .upsert_team_message(&TeamMessageRecord {
            id: "msg_2".to_string(),
            team_id: "team_1".to_string(),
            scope: "direct".to_string(),
            sender_agent_id: session.id.clone(),
            recipient_agent_ids: serde_json::json!([session.id.clone()]),
            input: serde_json::json!([{"type":"text","text":"retry"}]),
            image_paths: serde_json::json!(["/tmp/a.png"]),
            priority: "high".to_string(),
            policy: "immediate_interrupt".to_string(),
            correlation_id: Some("corr_2".to_string()),
            reply_to_message_id: None,
            idempotency_key: Some("idem_same".to_string()),
            created_at: 101,
        })
        .expect("logical retry upsert");

    let hydrated = repository.hydrate_runtime_state().expect("hydrate");
    assert_eq!(hydrated.team_messages.len(), 1);
    let message = &hydrated.team_messages[0];
    assert_eq!(message.id, "msg_1");
    assert_eq!(message.priority, "high");
    assert_eq!(message.policy, "immediate_interrupt");
    assert_eq!(
        message.input,
        serde_json::json!([{"type":"text","text":"retry"}])
    );
}

#[test]
fn managed_worktree_natural_key_retry_converges_with_different_id() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    repository
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            id: "wt_1".to_string(),
            repo_root: "/tmp/repo".to_string(),
            worktree_root: "/tmp/worktrees".to_string(),
            worktree_cwd: "/tmp/worktrees/repo-feature".to_string(),
            branch_name: "gg/feature".to_string(),
            worktree_name: "repo-feature".to_string(),
            unified_workspace_path: "repo_feature".to_string(),
            deletion_policy: "retain_on_last_claim".to_string(),
            created_by_session_id: Some("session_1".to_string()),
            created_by_operation_id: Some("op_1".to_string()),
            created_at: 100,
            updated_at: 100,
        })
        .expect("insert worktree");
    repository
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            id: "wt_2".to_string(),
            repo_root: "/tmp/repo".to_string(),
            worktree_root: "/tmp/worktrees".to_string(),
            worktree_cwd: "/tmp/worktrees/repo-feature".to_string(),
            branch_name: "gg/feature".to_string(),
            worktree_name: "repo-feature-updated".to_string(),
            unified_workspace_path: "repo_feature_v2".to_string(),
            deletion_policy: "delete_on_last_claim".to_string(),
            created_by_session_id: Some("session_2".to_string()),
            created_by_operation_id: Some("op_2".to_string()),
            created_at: 100,
            updated_at: 101,
        })
        .expect("logical retry upsert");

    let hydrated = repository.hydrate_runtime_state().expect("hydrate");
    assert_eq!(hydrated.managed_worktrees.len(), 1);
    let worktree = &hydrated.managed_worktrees[0];
    assert_eq!(worktree.id, "wt_1");
    assert_eq!(worktree.worktree_name, "repo-feature-updated");
    assert_eq!(worktree.unified_workspace_path, "repo_feature_v2");
    assert_eq!(worktree.deletion_policy, "delete_on_last_claim");
    assert_eq!(worktree.created_by_session_id.as_deref(), Some("session_2"));
}

#[test]
fn credential_logical_key_retry_converges_with_different_id() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    repository
        .upsert_credential(&CredentialRecord {
            id: "cred_1".to_string(),
            provider: "codex".to_string(),
            profile: "default".to_string(),
            kind: "api_key".to_string(),
            encrypted_secret: "enc:first".to_string(),
            metadata: serde_json::json!({"source":"first"}),
            created_at: 100,
            updated_at: 100,
        })
        .expect("insert credential");
    repository
        .upsert_credential(&CredentialRecord {
            id: "cred_2".to_string(),
            provider: "codex".to_string(),
            profile: "default".to_string(),
            kind: "api_key".to_string(),
            encrypted_secret: "enc:second".to_string(),
            metadata: serde_json::json!({"source":"retry"}),
            created_at: 100,
            updated_at: 101,
        })
        .expect("logical retry upsert");

    let hydrated = repository.hydrate_runtime_state().expect("hydrate");
    assert_eq!(hydrated.credentials.len(), 1);
    let credential = &hydrated.credentials[0];
    assert_eq!(credential.id, "cred_1");
    assert_eq!(credential.encrypted_secret, "enc:second");
    assert_eq!(credential.metadata, serde_json::json!({"source":"retry"}));
    assert_eq!(credential.updated_at, 101);
}

#[test]
fn hydrate_runtime_state_round_trips_core_entities() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    let session = sample_session();
    let turn = sample_turn();

    repository.upsert_session(&session).expect("insert session");
    repository.upsert_turn(&turn).expect("insert turn");
    repository
        .upsert_approval(&ApprovalRecord {
            id: "apr_1".to_string(),
            session_id: session.id.clone(),
            turn_id: turn.id.clone(),
            tool_call_id: Some("tool_1".to_string()),
            provider_approval_ref: Some("prov_apr_1".to_string()),
            status: "pending".to_string(),
            request: serde_json::json!({"question": "allow?"}),
            response: None,
            created_at: 102,
            resolved_at: None,
        })
        .expect("insert approval");
    repository
        .upsert_team(&TeamRecord {
            id: "team_1".to_string(),
            name: "Team Alpha".to_string(),
            lead_agent_id: session.id.clone(),
            created_by: "user".to_string(),
            created_at: 103,
            updated_at: 103,
            deleted_at: None,
        })
        .expect("insert team");
    repository
        .upsert_team_member(&TeamMemberRecord {
            team_id: "team_1".to_string(),
            agent_id: session.id.clone(),
            title: Some("Lead".to_string()),
            joined_at: 103,
            added_by: "user".to_string(),
            creator_agent_id: None,
            creator_compaction_subscription: "auto".to_string(),
            worktree_id: Some("wt_1".to_string()),
        })
        .expect("insert team member");
    repository
        .upsert_team_message(&TeamMessageRecord {
            id: "msg_1".to_string(),
            team_id: "team_1".to_string(),
            scope: "direct".to_string(),
            sender_agent_id: session.id.clone(),
            recipient_agent_ids: serde_json::json!([session.id.clone()]),
            input: serde_json::json!([{"type": "text", "text": "hello"}]),
            image_paths: serde_json::json!([]),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("idem_1".to_string()),
            created_at: 104,
        })
        .expect("insert team message");
    repository
        .upsert_team_delivery(&TeamDeliveryRecord {
            id: "dlv_1".to_string(),
            message_id: "msg_1".to_string(),
            team_id: "team_1".to_string(),
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
        .expect("insert team delivery");
    repository
        .upsert_managed_worktree(&ManagedWorktreeRecord {
            id: "wt_1".to_string(),
            repo_root: "/tmp/repo".to_string(),
            worktree_root: "/tmp/worktrees".to_string(),
            worktree_cwd: "/tmp/worktrees/repo-feature".to_string(),
            branch_name: "gg/feature".to_string(),
            worktree_name: "repo-feature".to_string(),
            unified_workspace_path: "repo_feature".to_string(),
            deletion_policy: "retain_on_last_claim".to_string(),
            created_by_session_id: Some(session.id.clone()),
            created_by_operation_id: Some("op_1".to_string()),
            created_at: 105,
            updated_at: 105,
        })
        .expect("insert worktree");
    repository
        .upsert_managed_worktree_claim(&ManagedWorktreeClaimRecord {
            worktree_id: "wt_1".to_string(),
            session_id: session.id.clone(),
            claim_role: "owner".to_string(),
            created_at: 106,
            released_at: None,
        })
        .expect("insert worktree claim");
    repository
        .upsert_process(&ProcessRecord {
            id: "proc_1".to_string(),
            session_id: Some(session.id.clone()),
            tool_call_id: Some("tool_1".to_string()),
            pid: Some(4242),
            command: serde_json::json!(["bash", "-lc", "echo hello"]),
            cwd: Some("/tmp/repo".to_string()),
            status: "running".to_string(),
            exit_code: None,
            signal: None,
            stdout_path: Some("/tmp/stdout.log".to_string()),
            stderr_path: Some("/tmp/stderr.log".to_string()),
            started_at: 107,
            ended_at: None,
            timeout_ms: Some(30_000),
        })
        .expect("insert process");
    repository
        .upsert_credential(&CredentialRecord {
            id: "cred_1".to_string(),
            provider: "codex".to_string(),
            profile: "default".to_string(),
            kind: "api_key".to_string(),
            encrypted_secret: "enc:abc".to_string(),
            metadata: serde_json::json!({"origin": "test"}),
            created_at: 108,
            updated_at: 108,
        })
        .expect("insert credential");

    let hydrated = repository.hydrate_runtime_state().expect("hydrate state");
    assert_eq!(hydrated.sessions.len(), 1);
    assert_eq!(hydrated.turns.len(), 1);
    assert_eq!(hydrated.approvals.len(), 1);
    assert_eq!(hydrated.teams.len(), 1);
    assert_eq!(hydrated.team_members.len(), 1);
    assert_eq!(hydrated.team_messages.len(), 1);
    assert_eq!(hydrated.team_deliveries.len(), 1);
    assert_eq!(hydrated.managed_worktrees.len(), 1);
    assert_eq!(hydrated.managed_worktree_claims.len(), 1);
    assert_eq!(hydrated.processes.len(), 1);
    assert_eq!(hydrated.credentials.len(), 1);

    assert_eq!(hydrated.sessions[0].id, "session_1");
    assert_eq!(hydrated.turns[0].id, "turn_1");
    assert_eq!(hydrated.teams[0].id, "team_1");
}

#[test]
fn delete_team_member_persists_membership_removal() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("initialize schema");

    let lead = sample_session();
    let member = SessionRecord {
        id: "session_2".to_string(),
        provider: "codex".to_string(),
        status: "idle".to_string(),
        cwd: None,
        model: Some("gpt-5.4-mini".to_string()),
        permission_mode: Some("default".to_string()),
        system_prompt: None,
        metadata: serde_json::json!({}),
        provider_session_ref: Some("thread_2".to_string()),
        canonical_provider_session_ref: None,
        active_turn_id: None,
        worktree_id: None,
        created_at: 200,
        updated_at: 200,
        closed_at: None,
        failure_code: None,
        failure_message: None,
    };
    repository
        .upsert_session(&lead)
        .expect("insert lead session");
    repository
        .upsert_session(&member)
        .expect("insert member session");

    repository
        .upsert_team(&TeamRecord {
            id: "team_1".to_string(),
            name: "Team Alpha".to_string(),
            lead_agent_id: lead.id.clone(),
            created_by: "user".to_string(),
            created_at: 201,
            updated_at: 201,
            deleted_at: None,
        })
        .expect("insert team");

    for agent_id in [&lead.id, &member.id] {
        repository
            .upsert_team_member(&TeamMemberRecord {
                team_id: "team_1".to_string(),
                agent_id: agent_id.to_string(),
                title: None,
                joined_at: 202,
                added_by: "user".to_string(),
                creator_agent_id: None,
                creator_compaction_subscription: "auto".to_string(),
                worktree_id: None,
            })
            .expect("insert team member");
    }

    repository
        .delete_team_member("team_1", member.id.as_str())
        .expect("delete team member");

    let hydrated = repository.hydrate_runtime_state().expect("hydrate");
    assert_eq!(hydrated.team_members.len(), 1);
    assert_eq!(hydrated.team_members[0].agent_id, lead.id);
}

#[tokio::test]
async fn store_initialize_and_healthcheck_use_schema() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp_dir.path().join("runtime.sqlite3"),
    });

    store.initialize().await.expect("initialize store");
    store.healthcheck().await.expect("healthcheck");

    let hydrated = store.hydrate_runtime_state().expect("hydrate");
    assert!(hydrated.sessions.is_empty());
}
