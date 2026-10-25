use super::*;
use runtime_core::{AgentDeliveryRecord, AgentMessageContextKind, AgentMessageRecord};
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

#[test]
fn mapped_legacy_messages_backfill_with_workspace_context_and_pending_state() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("runtime.sqlite3");
    apply_schema_through(&db_path, 6);
    let repository = SqliteRuntimeRepository::new(db_path.clone());
    let session = sample_session();
    repository.upsert_session(&session).expect("session");
    repository
        .upsert_team(&TeamRecord {
            id: "team_mapped".to_string(),
            name: "Mapped".to_string(),
            lead_agent_id: session.id.clone(),
            created_by: "user".to_string(),
            created_at: 100,
            updated_at: 100,
            deleted_at: None,
        })
        .expect("team");
    repository
        .upsert_team_message(&TeamMessageRecord {
            id: "msg_mapped".to_string(),
            team_id: "team_mapped".to_string(),
            scope: "broadcast".to_string(),
            sender_agent_id: session.id.clone(),
            recipient_agent_ids: serde_json::json!([session.id.clone()]),
            input: serde_json::json!([{"type":"text","text":"legacy"}]),
            image_paths: serde_json::json!(["/tmp/legacy-one.png", "/tmp/legacy-two.jpg"]),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: Some("legacy-correlation".to_string()),
            reply_to_message_id: Some("msg_legacy_parent".to_string()),
            idempotency_key: Some("legacy-key".to_string()),
            created_at: 101,
        })
        .expect("message");
    repository
        .upsert_team_delivery(&TeamDeliveryRecord {
            id: "delivery_mapped".to_string(),
            message_id: "msg_mapped".to_string(),
            team_id: "team_mapped".to_string(),
            recipient_agent_id: session.id.clone(),
            provider: "codex".to_string(),
            status: "pending".to_string(),
            effective_policy: Some("non_interrupting".to_string()),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: None,
            last_error_message: None,
            created_at: 101,
            updated_at: 101,
        })
        .expect("delivery");
    let connection = open_connection(&db_path).expect("connection");
    connection
        .execute(
            "INSERT INTO workspaces (
                workspace_id, canonical_root, display_name, lifecycle_state,
                revision, created_at, updated_at, lead_agent_id
             ) VALUES ('workspace_mapped', '/tmp/mapped', 'Mapped', 'active', 0, 1, 1, NULL)",
            [],
        )
        .expect("workspace");
    connection
        .execute(
            "INSERT INTO legacy_workspace_migration_subjects (
                subject_kind, subject_id, classification, workspace_id, canonical_root,
                git_common_dir, repository_fingerprint, reason_code, evidence_json,
                resolution_source, applied_at, updated_at
             ) VALUES (
                'team', 'team_mapped', 'mapped', 'workspace_mapped', '/tmp/mapped',
                '/tmp/mapped/.git', 'repo_v2_mapped', 'deterministic_fixture', '{}',
                'deterministic', 1, 1
             )",
            [],
        )
        .expect("migration subject");
    drop(connection);

    repository
        .initialize_schema()
        .expect("apply agent message migration");
    let hydrated = repository.hydrate_runtime_state().expect("hydrate");
    assert_eq!(hydrated.agent_messages.len(), 1);
    let message = &hydrated.agent_messages[0];
    assert_eq!(message.id, "msg_mapped");
    assert_eq!(message.context_kind, AgentMessageContextKind::WorkspaceTeam);
    assert_eq!(message.workspace_id.as_deref(), Some("workspace_mapped"));
    assert_eq!(message.legacy_team_id.as_deref(), Some("team_mapped"));
    assert_eq!(
        message.correlation_id.as_deref(),
        Some("legacy-correlation")
    );
    assert_eq!(
        message.reply_to_message_id.as_deref(),
        Some("msg_legacy_parent")
    );
    assert_eq!(
        message.image_paths,
        vec![
            "/tmp/legacy-one.png".to_string(),
            "/tmp/legacy-two.jpg".to_string()
        ]
    );
    assert_eq!(hydrated.agent_deliveries.len(), 1);
    assert_eq!(hydrated.agent_deliveries[0].id, "delivery_mapped");
    assert_eq!(hydrated.agent_deliveries[0].status, "pending");

    repository.initialize_schema().expect("idempotent reopen");
    let reopened = repository.hydrate_runtime_state().expect("reopen hydrate");
    assert_eq!(reopened.agent_messages, hydrated.agent_messages);
    assert_eq!(reopened.agent_deliveries, hydrated.agent_deliveries);
}

#[test]
fn new_agent_message_and_deliveries_commit_atomically() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let message = AgentMessageRecord {
        id: "msg_atomic_agent".to_string(),
        scope: "direct".to_string(),
        context_kind: AgentMessageContextKind::GlobalDirect,
        workspace_id: None,
        legacy_team_id: None,
        sender_agent_id: "sender".to_string(),
        recipient_agent_ids: vec!["recipient".to_string()],
        input: serde_json::json!([{"type":"text","text":"hello"}]),
        image_paths: Vec::new(),
        priority: "normal".to_string(),
        policy: "non_interrupting".to_string(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: Some("atomic-agent-key".to_string()),
        created_at: 10,
    };
    let first = AgentDeliveryRecord {
        id: "delivery_atomic_one".to_string(),
        message_id: message.id.clone(),
        recipient_agent_id: "recipient".to_string(),
        provider: "codex".to_string(),
        status: "pending".to_string(),
        effective_policy: Some("non_interrupting".to_string()),
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 10,
        updated_at: 10,
    };
    let mut invalid = first.clone();
    invalid.id = "delivery_atomic_invalid".to_string();
    invalid.message_id = "different_message".to_string();

    assert!(repository
        .insert_agent_message_with_deliveries(&message, &[first, invalid])
        .is_err());
    let hydrated = repository.hydrate_runtime_state().expect("hydrate");
    assert!(hydrated.agent_messages.is_empty());
    assert!(hydrated.agent_deliveries.is_empty());
}

#[test]
fn indexed_delivery_pages_bound_fanout_preserve_equal_timestamp_order_and_filter_recipient() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let repository = repo(&temp_dir);
    repository.initialize_schema().expect("schema");
    let connection = open_connection(&temp_dir.path().join("runtime.sqlite3")).expect("db");
    connection
        .execute(
            "INSERT INTO workspaces (
                workspace_id, canonical_root, display_name, lifecycle_state,
                revision, created_at, updated_at, lead_agent_id
             ) VALUES ('workspace_paged', '/tmp/paged', 'Paged', 'active', 0, 1, 1, NULL)",
            [],
        )
        .expect("workspace");
    drop(connection);

    let make_message = |id: &str| AgentMessageRecord {
        id: id.to_owned(),
        scope: "broadcast".to_string(),
        context_kind: AgentMessageContextKind::WorkspaceTeam,
        workspace_id: Some("workspace_paged".to_string()),
        legacy_team_id: None,
        sender_agent_id: "sender".to_string(),
        recipient_agent_ids: (0..180).map(|index| format!("agent_{index:03}")).collect(),
        input: serde_json::json!([{"type":"text","text":"fanout"}]),
        image_paths: Vec::new(),
        priority: "normal".to_string(),
        policy: "non_interrupting".to_string(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: None,
        created_at: 100,
    };
    let make_delivery = |id: String, message_id: &str, index: usize| AgentDeliveryRecord {
        id,
        message_id: message_id.to_string(),
        recipient_agent_id: format!("agent_{index:03}"),
        provider: "codex".to_string(),
        status: "pending".to_string(),
        effective_policy: Some("non_interrupting".to_string()),
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 100 + (index / 3) as i64,
        updated_at: 100 + (index / 3) as i64,
    };
    let message = make_message("msg_paged");
    let deliveries = (0..180)
        .map(|index| make_delivery(format!("delivery_{index:03}"), &message.id, index))
        .collect::<Vec<_>>();
    repository
        .insert_agent_message_with_deliveries(&message, &deliveries)
        .expect("message and fanout");
    let connection = open_connection(&temp_dir.path().join("runtime.sqlite3")).expect("db");
    let query_plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT id FROM agent_deliveries \
             WHERE message_id=?1 AND (created_at,id) > (?2,?3) \
             ORDER BY created_at,id LIMIT ?4",
            rusqlite::params![message.id, 100_i64, "delivery_005", 17_i64],
            |row| row.get(3),
        )
        .expect("delivery query plan");
    assert!(
        query_plan.contains("idx_agent_deliveries_message"),
        "delivery paging should use the message/time/id index: {query_plan}"
    );
    let other = make_message("msg_other");
    repository
        .insert_agent_message_with_deliveries(
            &other,
            &[make_delivery("delivery_other".to_string(), &other.id, 7)],
        )
        .expect("other message");

    let request = runtime_core::AgentDeliveryListRequest {
        message_id: Some(message.id.clone()),
        recipient_agent_id: None,
    };
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = repository
            .list_agent_delivery_records_page(&request, cursor.clone(), 17)
            .expect("page");
        assert!(page.len() <= 17);
        if page.is_empty() {
            break;
        }
        for row in &page {
            assert_eq!(row.message_id, message.id);
            if let Some((at, id)) = cursor.as_ref() {
                assert!((row.created_at, row.id.as_str()) > (*at, id.as_str()));
            }
            seen.push(row.id.clone());
        }
        let last = page.last().unwrap();
        cursor = Some((last.created_at, last.id.clone()));
    }
    assert_eq!(seen.len(), 180);
    assert_eq!(
        seen,
        deliveries
            .iter()
            .map(|row| row.id.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        repository
            .list_agent_delivery_records(&request)
            .unwrap()
            .len(),
        180,
        "existing unbounded recovery inspection still works"
    );
    let filtered = repository
        .list_agent_delivery_records_page(
            &runtime_core::AgentDeliveryListRequest {
                message_id: Some(message.id.clone()),
                recipient_agent_id: Some("agent_050".to_string()),
            },
            None,
            101,
        )
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id, "delivery_050");
    assert!(repository
        .list_agent_delivery_records_page(&request, None, 102)
        .is_err());
    assert!(repository
        .list_agent_delivery_records_page(
            &runtime_core::AgentDeliveryListRequest {
                message_id: None,
                recipient_agent_id: None,
            },
            None,
            1,
        )
        .is_err());
}
