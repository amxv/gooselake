use super::workspace_retirement::{register, retirement_command, setup};
use super::*;
use runtime_core::{
    AgentDeliveryListRequest, AgentDeliveryRecord, AgentMessageContextKind, AgentMessageRecord,
};

fn legacy_delivery(
    repository: &SqliteRuntimeRepository,
    workspace: &runtime_core::WorkspaceRecord,
    db: &Connection,
    status: &str,
) -> TeamDeliveryRecord {
    let mut owner = sample_session();
    owner.cwd = Some(workspace.canonical_root.clone());
    owner.status = "ready".into();
    owner.active_turn_id = None;
    owner.worktree_id = None;
    repository.upsert_session(&owner).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
         VALUES (?1,?2,'migration',1)",
        params![owner.id, workspace.workspace_id],
    )
    .unwrap();
    repository
        .upsert_team(&TeamRecord {
            id: "legacy-team".into(),
            name: "Legacy delivery retirement".into(),
            lead_agent_id: owner.id.clone(),
            created_by: "test".into(),
            created_at: 1,
            updated_at: 1,
            deleted_at: None,
        })
        .unwrap();
    repository
        .upsert_team_message(&TeamMessageRecord {
            id: "legacy-message".into(),
            team_id: "legacy-team".into(),
            scope: "direct".into(),
            sender_agent_id: owner.id.clone(),
            recipient_agent_ids: serde_json::json!([owner.id]),
            input: serde_json::json!([{"type":"text","text":"Retirement delivery fixture"}]),
            image_paths: serde_json::json!([]),
            priority: "normal".into(),
            policy: "non_interrupting".into(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
            created_at: 1,
        })
        .unwrap();
    let delivery = TeamDeliveryRecord {
        id: "legacy-delivery".into(),
        message_id: "legacy-message".into(),
        team_id: "legacy-team".into(),
        recipient_agent_id: owner.id,
        provider: "codex".into(),
        status: status.into(),
        effective_policy: Some("non_interrupting".into()),
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 1,
        updated_at: 1,
    };
    repository.upsert_team_delivery(&delivery).unwrap();
    delivery
}

fn canonical_delivery(
    repository: &SqliteRuntimeRepository,
    workspace: &runtime_core::WorkspaceRecord,
    legacy: &TeamDeliveryRecord,
    status: &str,
) -> AgentDeliveryRecord {
    let delivery = AgentDeliveryRecord {
        id: legacy.id.clone(),
        message_id: legacy.message_id.clone(),
        recipient_agent_id: legacy.recipient_agent_id.clone(),
        provider: legacy.provider.clone(),
        status: status.into(),
        effective_policy: legacy.effective_policy.clone(),
        injection_strategy: None,
        injected_turn_id: None,
        last_error_code: None,
        last_error_message: None,
        created_at: 1,
        updated_at: 1,
    };
    repository
        .insert_agent_message_with_deliveries(
            &AgentMessageRecord {
                id: legacy.message_id.clone(),
                scope: "direct".into(),
                context_kind: AgentMessageContextKind::LegacyTeam,
                workspace_id: Some(workspace.workspace_id.clone()),
                legacy_team_id: Some(legacy.team_id.clone()),
                sender_agent_id: legacy.recipient_agent_id.clone(),
                recipient_agent_ids: vec![legacy.recipient_agent_id.clone()],
                input: serde_json::json!([{"type":"text","text":"Retirement delivery fixture"}]),
                image_paths: vec![],
                priority: "normal".into(),
                policy: "non_interrupting".into(),
                correlation_id: None,
                reply_to_message_id: None,
                idempotency_key: None,
                created_at: 1,
            },
            std::slice::from_ref(&delivery),
        )
        .unwrap();
    delivery
}

fn legacy_state(db: &Connection) -> (String, Option<String>, Option<String>) {
    db.query_row(
        "SELECT status,last_error_code,injected_turn_id FROM team_deliveries
         WHERE id='legacy-delivery'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

#[test]
fn retirement_blocks_legacy_only_and_inconsistent_unfinished_delivery_without_freeze() {
    for (legacy_status, canonical_status) in [
        ("pending", None),
        ("injecting", None),
        ("injecting", Some("deferred")),
        ("injecting", Some("injected")),
        ("pending", Some("injected")),
    ] {
        let (temp, repository, workspace) = setup();
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        let legacy = legacy_delivery(&repository, &workspace, &db, legacy_status);
        if let Some(status) = canonical_status {
            canonical_delivery(&repository, &workspace, &legacy, status);
        }
        let preflight = repository
            .workspace_retirement_preflight(&workspace.workspace_id)
            .unwrap();
        assert_eq!(
            preflight
                .blockers
                .iter()
                .find(|blocker| blocker.code == "legacy_deliveries_unfinished")
                .unwrap()
                .count,
            1
        );
        let command = retirement_command(&workspace, "legacy-unresolved");
        let error = repository.admit_workspace_retirement(&command).unwrap_err();
        assert!(error.to_string().contains("unfinished legacy deliveries"));
        assert!(repository
            .get_operation(&command.operation_id)
            .unwrap()
            .is_none());
        assert!(repository
            .retiring_operation_for_session(&legacy.recipient_agent_id)
            .unwrap()
            .is_none());
        assert_eq!(
            repository.get_workspace(&workspace.workspace_id).unwrap(),
            Some(workspace)
        );
        assert_eq!(legacy_state(&db).0, legacy_status);
    }
}

#[test]
fn retirement_cancellation_updates_both_delivery_authorities_and_rejects_stale_writers() {
    for (legacy_status, canonical_status) in [("pending", "deferred"), ("deferred", "pending")] {
        let (temp, repository, workspace) = setup();
        let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
        let legacy = legacy_delivery(&repository, &workspace, &db, legacy_status);
        let canonical = canonical_delivery(&repository, &workspace, &legacy, canonical_status);
        let command = retirement_command(&workspace, "legacy-reconciled");
        repository.admit_workspace_retirement(&command).unwrap();
        let plan = repository
            .settle_passive_workspace_retirement(&command.operation_id, command.requested_at + 1)
            .unwrap();
        assert_eq!(plan.delivery_ids, vec![canonical.id.clone()]);
        let durable = repository
            .get_agent_delivery(&canonical.id)
            .unwrap()
            .unwrap();
        assert_eq!(durable.status, "cancelled");
        assert_eq!(
            durable.last_error_code.as_deref(),
            Some("workspace_retirement")
        );
        assert_eq!(
            legacy_state(&db),
            (
                "cancelled".into(),
                Some("workspace_retirement".into()),
                None
            )
        );
        assert_eq!(
            repository
                .list_agent_delivery_records(&AgentDeliveryListRequest {
                    message_id: Some(canonical.message_id.clone()),
                    recipient_agent_id: Some(canonical.recipient_agent_id.clone()),
                })
                .unwrap(),
            vec![durable.clone()]
        );
        assert!(repository.upsert_agent_delivery(&canonical).is_err());
        assert!(repository.upsert_team_delivery(&legacy).is_err());
        assert_eq!(
            repository.get_agent_delivery(&canonical.id).unwrap(),
            Some(durable)
        );
        assert_eq!(legacy_state(&db).0, "cancelled");
    }
}

#[test]
fn retirement_mirroring_preserves_existing_terminal_legacy_evidence() {
    let (temp, repository, workspace) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut legacy = legacy_delivery(&repository, &workspace, &db, "injected");
    legacy.injected_turn_id = Some("historical-native-turn".into());
    repository.upsert_team_delivery(&legacy).unwrap();
    canonical_delivery(&repository, &workspace, &legacy, "pending");
    let command = retirement_command(&workspace, "legacy-evidence");
    repository.admit_workspace_retirement(&command).unwrap();
    repository
        .settle_passive_workspace_retirement(&command.operation_id, command.requested_at + 1)
        .unwrap();
    assert_eq!(
        legacy_state(&db),
        (
            "injected".into(),
            None,
            Some("historical-native-turn".into())
        )
    );
}

#[test]
fn retirement_freezes_outbound_delivery_injection_before_passive_cancellation() {
    let (temp, repository, workspace) = setup();
    let db = open_connection(&temp.path().join("runtime.sqlite3")).unwrap();
    let mut legacy = legacy_delivery(&repository, &workspace, &db, "pending");
    let mut canonical = canonical_delivery(&repository, &workspace, &legacy, "pending");
    let other_root = temp.path().join("other-workspace");
    std::fs::create_dir(&other_root).unwrap();
    let other_workspace = register(&repository, &other_root, "other-workspace");
    let mut outside = sample_session();
    outside.id = "outside-recipient".into();
    outside.cwd = Some(other_workspace.canonical_root.clone());
    outside.status = "ready".into();
    outside.active_turn_id = None;
    outside.worktree_id = None;
    repository.upsert_session(&outside).unwrap();
    db.execute(
        "INSERT INTO workspace_session_ownership(session_id,workspace_id,source,created_at)
         VALUES (?1,?2,'migration',1)",
        params![outside.id, other_workspace.workspace_id],
    )
    .unwrap();
    legacy.recipient_agent_id = outside.id.clone();
    canonical.recipient_agent_id = outside.id.clone();
    repository.upsert_team_delivery(&legacy).unwrap();
    repository.upsert_agent_delivery(&canonical).unwrap();
    db.execute(
        "UPDATE team_messages SET recipient_agent_ids_json=json_array(?1)
         WHERE id=?2",
        params![outside.id, canonical.message_id],
    )
    .unwrap();
    db.execute(
        "UPDATE agent_messages SET recipient_agent_ids_json=json_array(?1),
         workspace_id=NULL,context_kind='global_direct' WHERE id=?2",
        params![outside.id, canonical.message_id],
    )
    .unwrap();

    let command = retirement_command(&workspace, "outbound-freeze");
    repository.admit_workspace_retirement(&command).unwrap();
    assert!(repository
        .retiring_operation_for_session(&outside.id)
        .unwrap()
        .is_none());
    assert_eq!(
        repository
            .get_agent_delivery(&canonical.id)
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    canonical.status = "injecting".into();
    legacy.status = "injecting".into();
    assert!(
        repository.upsert_agent_delivery(&canonical).is_err(),
        "the frozen sender must block dispatch even when its recipient remains active"
    );
    assert!(
        repository.upsert_team_delivery(&legacy).is_err(),
        "the legacy mirror must enforce the same sender fence"
    );
    assert_eq!(
        repository
            .get_agent_delivery(&canonical.id)
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    assert_eq!(legacy_state(&db).0, "pending");
}
