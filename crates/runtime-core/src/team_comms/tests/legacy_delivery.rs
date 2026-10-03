use super::*;

#[tokio::test]
async fn restart_appends_new_team_event_rows_without_event_id_collision() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 1);
    let lead = create_test_session(&runtime).await;
    let member = create_test_session(&runtime).await;

    let created = service
        .create_team(TeamCreateRequest {
            name: "Restart Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![member.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team");
    let team_id = created.team.id.clone();

    let before = service
        .replay_team_events(team_id.as_str(), None, 128)
        .expect("replay before");
    assert!(
        before.iter().any(|event| event.kind == "team.created"),
        "expected team.created before restart"
    );

    drop(service);
    drop(runtime);

    let (_runtime_after_restart, service_after_restart) =
        build_runtime_and_service(store.clone(), 1);
    service_after_restart
        .set_team_lead(TeamSetLeadRequest {
            team_id: team_id.clone(),
            lead_agent_id: member.clone(),
        })
        .await
        .expect("set team lead after restart");

    let after = service_after_restart
        .replay_team_events(team_id.as_str(), None, 256)
        .expect("replay after");
    assert!(
        after.len() > before.len(),
        "expected event stream to append after restart mutation"
    );
    assert!(
        after.iter().any(|event| event.kind == "team.lead_changed"),
        "expected team.lead_changed event to append after restart"
    );
}

#[tokio::test]
async fn legacy_team_lead_removal_requires_explicit_reassignment_instead_of_auto_election() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store, 0);
    let lead = create_test_session(&runtime).await;
    let member = create_test_session(&runtime).await;

    let created = service
        .create_team(TeamCreateRequest {
            name: "No Auto Election".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![member.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team");
    let team_id = created.team.id.clone();

    let error = service
        .remove_team_member(TeamRemoveMemberRequest {
            team_id: team_id.clone(),
            agent_id: lead.clone(),
        })
        .await
        .expect_err("current lead removal must be rejected");
    assert!(matches!(error, RuntimeError::InvalidState(_)));

    let unchanged = service
        .get_team(&team_id)
        .await
        .expect("team after rejection");
    assert_eq!(unchanged.team.lead_agent_id, lead);
    assert!(unchanged
        .members
        .iter()
        .any(|candidate| candidate.agent_id == member));
}

#[tokio::test]
async fn startup_recovery_retries_deferred_delivery_for_ready_recipient() {
    let store = Arc::new(TestStore::default());
    let now = now_ms();

    store
        .upsert_session(&SessionRecord {
            id: "sess_lead_seed".to_string(),
            provider: "codex".to_string(),
            status: "ready".to_string(),
            cwd: None,
            model: Some("test-model".to_string()),
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("provider-lead-seed".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        })
        .expect("seed lead session");
    store
        .upsert_session(&SessionRecord {
            id: "sess_ready_seed".to_string(),
            provider: "codex".to_string(),
            status: "ready".to_string(),
            cwd: None,
            model: Some("test-model".to_string()),
            permission_mode: None,
            system_prompt: None,
            metadata: serde_json::json!({}),
            provider_session_ref: Some("provider-ready-seed".to_string()),
            canonical_provider_session_ref: None,
            active_turn_id: None,
            worktree_id: None,
            created_at: now,
            updated_at: now,
            closed_at: None,
            failure_code: None,
            failure_message: None,
        })
        .expect("seed recipient session");
    store
        .upsert_team(&TeamRecord {
            id: "team_seed".to_string(),
            name: "Seed Team".to_string(),
            lead_agent_id: "sess_lead_seed".to_string(),
            created_by: "test".to_string(),
            created_at: now,
            updated_at: now,
            deleted_at: None,
        })
        .expect("seed team");
    store
        .upsert_team_member(&TeamMemberRecord {
            team_id: "team_seed".to_string(),
            agent_id: "sess_ready_seed".to_string(),
            title: None,
            joined_at: now,
            added_by: "test".to_string(),
            creator_agent_id: None,
            creator_compaction_subscription: "auto".to_string(),
            worktree_id: None,
        })
        .expect("seed member");
    store
        .upsert_team_message(&TeamMessageRecord {
            id: "msg_seed".to_string(),
            team_id: "team_seed".to_string(),
            scope: "direct".to_string(),
            sender_agent_id: "sess_lead_seed".to_string(),
            recipient_agent_ids: serde_json::json!(["sess_ready_seed"]),
            input: serde_json::json!([{ "type": "text", "text": "seed deferred" }]),
            image_paths: serde_json::json!([]),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("seed-idempotency".to_string()),
            created_at: now,
        })
        .expect("seed message");
    store
        .upsert_team_delivery(&TeamDeliveryRecord {
            id: "dlv_seed".to_string(),
            message_id: "msg_seed".to_string(),
            team_id: "team_seed".to_string(),
            recipient_agent_id: "sess_ready_seed".to_string(),
            provider: "codex".to_string(),
            status: DELIVERY_STATUS_DEFERRED.to_string(),
            effective_policy: Some("non_interrupting".to_string()),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: Some("seed_restart_gap".to_string()),
            last_error_message: Some("seed deferred before restart".to_string()),
            created_at: now,
            updated_at: now,
        })
        .expect("seed delivery");

    let (_runtime, service) = build_runtime_and_service(store.clone(), 0);
    let before = service
        .get_deliveries(TeamGetDeliveriesRequest {
            team_id: "team_seed".to_string(),
            message_id: Some("msg_seed".to_string()),
            recipient_agent_id: Some("sess_ready_seed".to_string()),
        })
        .await
        .expect("delivery before startup replay");
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].status, DELIVERY_STATUS_DEFERRED);

    let retried = service
        .recover_startup_deferred_deliveries()
        .await
        .expect("startup deferred recovery");
    assert!(
        retried >= 1,
        "expected startup replay to retry at least one deferred delivery"
    );

    let mut recovered_status = None;
    for _ in 0..30 {
        let rows = service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: "team_seed".to_string(),
                message_id: Some("msg_seed".to_string()),
                recipient_agent_id: Some("sess_ready_seed".to_string()),
            })
            .await
            .expect("delivery rows");
        if let Some(row) = rows.first() {
            recovered_status = Some(row.status.clone());
            if row.status != DELIVERY_STATUS_DEFERRED {
                break;
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert_ne!(
        recovered_status.as_deref(),
        Some(DELIVERY_STATUS_DEFERRED),
        "deferred delivery should not remain permanently deferred after startup recovery"
    );
}

#[tokio::test]
async fn delete_team_cancels_outstanding_delivery_and_clears_recipient_queue_blockers() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 300);
    let lead = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;

    let created = service
        .create_team(TeamCreateRequest {
            name: "Delete Queue Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create team");
    let deleted_team_id = created.team.id.clone();

    let first_ack = service
        .send_direct(TeamSendDirectRequest {
            team_id: deleted_team_id.clone(),
            sender_agent_id: lead.clone(),
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "first" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("delete-q-1".to_string()),
        })
        .await
        .expect("first direct");
    assert_eq!(first_ack.deliveries.len(), 1);

    let second_ack = service
        .send_direct(TeamSendDirectRequest {
            team_id: deleted_team_id.clone(),
            sender_agent_id: lead.clone(),
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "second" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("delete-q-2".to_string()),
        })
        .await
        .expect("second direct");
    assert_eq!(second_ack.deliveries.len(), 1);
    let second_delivery_id = second_ack.deliveries[0].id.clone();

    let mut second_is_outstanding = false;
    for _ in 0..40 {
        let rows = service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: deleted_team_id.clone(),
                message_id: Some(second_ack.message.id.clone()),
                recipient_agent_id: Some(recipient.clone()),
            })
            .await
            .expect("list deliveries");
        if let Some(delivery) = rows.first() {
            if matches!(
                delivery.status.as_str(),
                DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED
            ) {
                second_is_outstanding = true;
                break;
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        second_is_outstanding,
        "expected second delivery to be pending/deferred before team deletion"
    );

    service
        .delete_team(deleted_team_id.as_str())
        .await
        .expect("delete team");
    sleep(Duration::from_millis(450)).await;

    let hydrated_after_delete = store.hydrate_runtime_state().expect("hydrate");
    let deleted_delivery = hydrated_after_delete
        .team_deliveries
        .iter()
        .find(|delivery| delivery.id == second_delivery_id)
        .cloned()
        .expect("deleted team delivery row");
    assert_eq!(
        deleted_delivery.status, DELIVERY_STATUS_CANCELLED,
        "deleted team's outstanding delivery must be cancelled and must not resume/inject"
    );

    let created_second_team = service
        .create_team(TeamCreateRequest {
            name: "Live Team".to_string(),
            lead_agent_id: lead.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: Some("test".to_string()),
        })
        .await
        .expect("create second team");
    let live_team_id = created_second_team.team.id;

    let third_ack = service
        .send_direct(TeamSendDirectRequest {
            team_id: live_team_id.clone(),
            sender_agent_id: lead.clone(),
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{ "type": "text", "text": "third" }]),
            image_paths: Vec::new(),
            priority: "normal".to_string(),
            policy: "non_interrupting".to_string(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("delete-q-3".to_string()),
        })
        .await
        .expect("third direct");
    let third_delivery_id = third_ack.deliveries[0].id.clone();

    let mut third_terminal_status = None;
    for _ in 0..80 {
        let rows = service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: live_team_id.clone(),
                message_id: Some(third_ack.message.id.clone()),
                recipient_agent_id: Some(recipient.clone()),
            })
            .await
            .expect("list third delivery");
        if let Some(delivery) = rows
            .iter()
            .find(|delivery| delivery.id == third_delivery_id)
        {
            if is_terminal_status(delivery.status.as_str()) {
                third_terminal_status = Some(delivery.status.clone());
                break;
            }
        }
        sleep(Duration::from_millis(15)).await;
    }

    assert_eq!(
        third_terminal_status.as_deref(),
        Some(DELIVERY_STATUS_INJECTED),
        "later delivery must not be blocked/deferred by stale deleted-team queue state"
    );
}
