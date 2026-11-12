use super::*;
use runtime_core::{
    prepare_workspace_registration, AgentDeliveryRecord, AgentMessageContextKind,
    AgentMessageRecord, OperationActor, WorkspaceRegisterRequest,
};

#[test]
fn independent_sqlite_workers_commit_128_exact_native_claims_with_complete_scoped_events() {
    let temp = tempfile::tempdir().unwrap();
    let repository = repo(&temp);
    repository.initialize_schema().unwrap();
    let workspace_root = temp.path().join("scale-workspace");
    std::fs::create_dir(&workspace_root).unwrap();
    let workspace = repository
        .register_workspace(
            &prepare_workspace_registration(
                WorkspaceRegisterRequest {
                    canonical_root: workspace_root.display().to_string(),
                    display_name: Some("Canonical scale fixture".into()),
                },
                OperationActor::operator("canonical-scale-test"),
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .workspace;
    let mut deliveries = Vec::new();
    for index in 0..64 {
        let message_id = format!("scale_msg_{index:03}");
        let recipients = ["scale_first", "scale_second"];
        let message = AgentMessageRecord {
            id: message_id.clone(),
            scope: "broadcast".into(),
            context_kind: AgentMessageContextKind::WorkspaceTeam,
            workspace_id: Some(workspace.workspace_id.clone()),
            legacy_team_id: None,
            sender_agent_id: "scale_sender".into(),
            recipient_agent_ids: recipients.iter().map(|s| s.to_string()).collect(),
            input: serde_json::json!([{"type":"text","text":"isolated canonical fanout"}]),
            image_paths: vec![],
            priority: "normal".into(),
            policy: "non_interrupting".into(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: None,
            created_at: 1_000 + index,
        };
        let children = recipients
            .iter()
            .enumerate()
            .map(|(recipient_index, recipient)| AgentDeliveryRecord {
                id: format!("{message_id}_{recipient_index}"),
                message_id: message_id.clone(),
                recipient_agent_id: (*recipient).into(),
                provider: "codex".into(),
                status: "pending".into(),
                effective_policy: Some("non_interrupting".into()),
                injection_strategy: None,
                injected_turn_id: None,
                last_error_code: None,
                last_error_message: None,
                created_at: 1_000 + index,
                updated_at: 1_000 + index,
            })
            .collect::<Vec<_>>();
        repository
            .insert_agent_message_with_deliveries(&message, &children)
            .unwrap();
        deliveries.extend(children);
    }
    assert_eq!(deliveries.len(), 128);
    let db_path = temp.path().join("runtime.sqlite3");
    std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|worker| {
                let shard = deliveries
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| index % 8 == worker)
                    .map(|(_, row)| row.clone())
                    .collect::<Vec<_>>();
                let db_path = db_path.clone();
                scope.spawn(move || {
                    let independent = SqliteRuntimeRepository::new(db_path);
                    for before in shard {
                        let mut injecting = before.clone();
                        injecting.status = "injecting".into();
                        injecting.updated_at += 1;
                        assert!(
                            independent
                                .compare_and_swap_agent_delivery_status(&before, &injecting)
                                .unwrap(),
                            "each independent worker must own exactly its original generation: {}",
                            before.id
                        );
                        assert!(
                            !independent
                                .compare_and_swap_agent_delivery_status(&before, &injecting)
                                .unwrap(),
                            "stale second claim cannot repeat the native effect"
                        );
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let reopened = SqliteRuntimeRepository::new(db_path.clone());
    for before in &deliveries {
        let row = reopened.get_agent_delivery(&before.id).unwrap().unwrap();
        assert_eq!(row.status, "injecting");
        assert_eq!(row.injected_turn_id, None);
        assert_eq!(row.updated_at, before.updated_at + 1);
        assert!(
            reopened.upsert_agent_delivery(before).is_err(),
            "certified claims cannot be downgraded by raw legacy upsert"
        );
    }
    let connection = open_connection(&db_path).unwrap();
    for (kind, count) in [("message.sent", 64), ("agent_delivery.injecting", 128)] {
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM runtime_events WHERE kind=?1 AND critical=1",
                    [kind],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            count
        );
    }
    assert_eq!(connection.query_row(
        "SELECT count(DISTINCT event_id) FROM runtime_events WHERE kind='agent_delivery.injecting'",
        [],|row|row.get::<_,i64>(0)
    ).unwrap(),128);
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM runtime_events WHERE kind='agent_delivery.injecting'
         AND (turn_id IS NOT NULL OR critical!=1
              OR json_extract(payload_json,'$.delivery.status')!='injecting')",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    for index in 0..64 {
        let scope = format!("scale_msg_{index:03}");
        let (count, first, last, distinct): (i64, i64, i64, i64) = connection
            .query_row(
                "SELECT count(*),min(seq),max(seq),count(DISTINCT seq)
             FROM runtime_events WHERE scope='message' AND scope_id=?1",
                [&scope],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            (count, first, last, distinct),
            (1, 1, 1, 1),
            "the standalone message event retains its own subject scope"
        );
    }
    let (claimed, first, last, distinct): (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT count(*),min(seq),max(seq),count(DISTINCT seq)
             FROM runtime_events WHERE scope='workspace' AND scope_id=?1
               AND kind='agent_delivery.injecting'",
            [&workspace.workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        (claimed, last - first + 1, distinct),
        (128, 128, 128),
        "independent writers must persist all 128 ordered workspace Critical generations"
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT count(*) FROM turns WHERE source='agent_message_delivery_transport'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0,
        "status claims alone never imply a native provider turn"
    );
}
