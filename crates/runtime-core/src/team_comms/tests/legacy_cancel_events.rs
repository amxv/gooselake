use super::*;
use crate::{TeamCancelMessageRequest, TeamGetDeliveriesRequest};

impl TestStore {
    pub(super) fn read_legacy_cancelled_test(
        &self,
        message: &TeamMessageRecord,
    ) -> Result<Option<Vec<TeamDeliveryRecord>>, RuntimeError> {
        let hydrated = self.hydrated.lock().expect("hydrated lock");
        if hydrated
            .team_messages
            .iter()
            .find(|row| row.id == message.id)
            != Some(message)
        {
            return Err(RuntimeError::Conflict(
                "mock cancellation message changed".into(),
            ));
        }
        let mut rows = hydrated
            .team_deliveries
            .iter()
            .filter(|row| row.message_id == message.id)
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.id.cmp(&right.id));
        if message
            .recipient_agent_ids
            .as_array()
            .is_none_or(|recipients| recipients.len() != rows.len())
        {
            return Err(RuntimeError::Conflict(
                "mock cancellation receipt lost one or more delivery rows".into(),
            ));
        }
        let events = self.events.lock().expect("events lock");
        let Some(completed) = events.iter().find(|event| {
            event.event_id == format!("evt_legacy_message_{}_completed_cancel", message.id)
        }) else {
            return Ok(None);
        };
        if completed.kind != "team_message.completed"
            || completed.criticality != crate::RuntimeEventCriticality::Critical
            || completed.scope != RuntimeEventScope::Team
            || completed.scope_id != message.team_id
            || completed.payload != serde_json::json!({"message_id":message.id})
            || rows
                .iter()
                .any(|row| row.status != "cancelled" || row.updated_at != completed.created_at)
        {
            return Err(RuntimeError::Conflict(
                "mock cancellation completion receipt drifted".into(),
            ));
        }
        for row in &rows {
            let event = events
                .iter()
                .find(|event| event.event_id == format!("evt_legacy_delivery_{}_cancelled", row.id))
                .ok_or_else(|| {
                    RuntimeError::Conflict(
                        "mock missing Critical delivery cancellation receipt".into(),
                    )
                })?;
            if event.kind != "team_delivery.cancelled"
                || event.criticality != crate::RuntimeEventCriticality::Critical
                || event.scope != RuntimeEventScope::Team
                || event.scope_id != message.team_id
                || event.session_id.as_deref() != Some(row.recipient_agent_id.as_str())
                || event.payload["delivery"] != serde_json::json!(row)
                || !matches!(
                    event.payload["before_status"].as_str(),
                    Some("pending" | "deferred")
                )
                || event.payload["before_updated_at"]
                    .as_i64()
                    .is_none_or(|before| before >= row.updated_at)
            {
                return Err(RuntimeError::Conflict(
                    "mock cancellation delivery receipt drifted".into(),
                ));
            }
        }
        Ok(Some(rows))
    }

    pub(super) fn commit_legacy_cancelled_test(
        &self,
        message: &TeamMessageRecord,
        before: &[TeamDeliveryRecord],
        after: &[TeamDeliveryRecord],
        events: &[NewRuntimeEvent],
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("hydrated lock");
        if hydrated
            .team_messages
            .iter()
            .find(|row| row.id == message.id)
            != Some(message)
        {
            return Err(RuntimeError::Conflict(
                "mock legacy cancellation missing message authority".into(),
            ));
        }
        if before.len() != after.len() || events.len() != after.len() + 1 {
            return Err(RuntimeError::ProtocolViolation(
                "mock legacy cancellation event snapshot incomplete".into(),
            ));
        }
        if message
            .recipient_agent_ids
            .as_array()
            .is_none_or(|recipients| recipients.len() != before.len())
        {
            return Err(RuntimeError::Conflict(
                "mock cancellation did not capture every recipient".into(),
            ));
        }
        for (previous, cancelled) in before.iter().zip(after) {
            if hydrated
                .team_deliveries
                .iter()
                .find(|row| row.id == previous.id)
                != Some(previous)
                || cancelled.id != previous.id
                || cancelled.status != "cancelled"
            {
                return Err(RuntimeError::Conflict(
                    "mock legacy cancellation lost pending generation".into(),
                ));
            }
        }
        // The in-memory fake must emulate SQLite's late-critical-event fault
        // rollback rather than appending earlier events before the failure.
        {
            let mut fail = self.fail_next_team_event_kind.lock().expect("failure lock");
            if fail
                .as_ref()
                .is_some_and(|kind| events.iter().any(|event| &event.kind == kind))
            {
                fail.take();
                return Err(RuntimeError::Io(
                    "forced legacy cancellation Critical journal failure".into(),
                ));
            }
        }
        for event in events {
            self.append_runtime_event(event)?;
        }
        for cancelled in after {
            Self::upsert_with_key(&mut hydrated.team_deliveries, cancelled.clone(), |row| {
                row.id.clone()
            });
        }
        Ok(after.to_vec())
    }
}

async fn seeded(
    store: Arc<TestStore>,
    status: &str,
) -> (
    Arc<RuntimeSessionManager>,
    Arc<RuntimeTeamCommsService>,
    TeamMessageRecord,
    Vec<TeamDeliveryRecord>,
) {
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient_a = create_test_session(&runtime).await;
    let recipient_b = create_test_session(&runtime).await;
    let team = service
        .create_team(TeamCreateRequest {
            name: "Pre-native cancellation fixture".into(),
            lead_agent_id: sender.clone(),
            member_agent_ids: vec![recipient_a.clone(), recipient_b.clone()],
            created_by: Some("operator".into()),
        })
        .await
        .unwrap();
    let at = crate::team_comms::now_ms();
    let message = TeamMessageRecord {
        id: "msg_pre_native_cancel_seed".into(),
        team_id: team.team.id,
        scope: "broadcast".into(),
        sender_agent_id: sender,
        recipient_agent_ids: serde_json::json!([recipient_a, recipient_b]),
        input: serde_json::json!([{"type":"text","text":"cancel before send"}]),
        image_paths: serde_json::json!([]),
        priority: "normal".into(),
        policy: "non_interrupting".into(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: None,
        created_at: at,
    };
    let deliveries = message
        .recipient_agent_ids
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, recipient)| TeamDeliveryRecord {
            id: format!("dlv_seed_cancel_{i}"),
            message_id: message.id.clone(),
            team_id: message.team_id.clone(),
            recipient_agent_id: recipient.as_str().unwrap().into(),
            provider: "codex".into(),
            status: status.into(),
            effective_policy: Some(message.policy.clone()),
            injection_strategy: None,
            injected_turn_id: None,
            last_error_code: None,
            last_error_message: None,
            created_at: at,
            updated_at: at,
        })
        .collect::<Vec<_>>();

    store.upsert_team_message(&message).unwrap();
    let mut state = service.state.write().await;
    state
        .team_message_ids
        .entry(message.team_id.clone())
        .or_default()
        .push(message.id.clone());
    state.messages.insert(message.id.clone(), message.clone());
    for delivery in &deliveries {
        store.upsert_team_delivery(delivery).unwrap();
        state
            .team_delivery_ids
            .entry(message.team_id.clone())
            .or_default()
            .push(delivery.id.clone());
        state
            .message_delivery_ids
            .entry(message.id.clone())
            .or_default()
            .push(delivery.id.clone());
        state
            .recipient_delivery_ids
            .entry(delivery.recipient_agent_id.clone())
            .or_default()
            .push(delivery.id.clone());
        state
            .deliveries
            .insert(delivery.id.clone(), delivery.clone());
    }
    drop(state);
    (runtime, service, message, deliveries)
}

fn cancellation_events(store: &TestStore) -> Vec<RuntimeEventRecord> {
    store
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            event.kind == "team_delivery.cancelled" || event.kind == "team_message.completed"
        })
        .cloned()
        .collect()
}

#[tokio::test]
async fn late_completion_event_failure_does_not_change_cache_or_durable_delivery() {
    let store = Arc::new(TestStore::default());
    let (runtime, service, message, before) = seeded(store.clone(), "pending").await;
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team_message.completed".into());
    let req = TeamCancelMessageRequest {
        team_id: message.team_id.clone(),
        message_id: message.id.clone(),
    };
    assert!(matches!(
        service.cancel_message(req.clone()).await,
        Err(RuntimeError::Io(_))
    ));
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        before
    );
    assert!(cancellation_events(&store).is_empty());
    assert_eq!(
        service
            .get_deliveries(TeamGetDeliveriesRequest {
                team_id: message.team_id.clone(),
                message_id: Some(message.id.clone()),
                recipient_agent_id: None,
            })
            .await
            .unwrap(),
        before
    );

    let committed = service.cancel_message(req).await.unwrap();
    assert_eq!(committed.len(), 2);
    assert!(committed.iter().all(|row| row.status == "cancelled"));
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        committed
    );
    let events = cancellation_events(&store);
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        [
            "team_delivery.cancelled",
            "team_delivery.cancelled",
            "team_message.completed",
        ]
    );
    assert!(events
        .iter()
        .all(|row| row.criticality == crate::RuntimeEventCriticality::Critical));
    for row in &committed {
        assert!(
            events.iter().any(|event| {
                event.kind == "team_delivery.cancelled"
                    && event.payload["delivery"]["id"].as_str() == Some(row.id.as_str())
            }),
            "each cancelled delivery must have a canonical critical event"
        );
    }

    // A completed cancellation is terminal: a delayed queue worker must not
    // issue a fresh native send even if it still holds the original ID.
    let blocked = service
        .inject_delivery(
            &before[0].id,
            crate::team_comms::state::DeliveryAttemptTrigger::Queue,
        )
        .await
        .unwrap();
    assert_eq!(blocked.status, "cancelled");
    assert_eq!(cancellation_events(&store).len(), 3);

    // A lost HTTP response can be retried in the same service, and in a
    // fresh instance hydrated from the same durable message/critical events.
    let req = TeamCancelMessageRequest {
        team_id: message.team_id.clone(),
        message_id: message.id.clone(),
    };
    assert_eq!(
        service.cancel_message(req.clone()).await.unwrap(),
        committed
    );
    let new_service = RuntimeTeamCommsService::new(
        store.clone(),
        runtime,
        RuntimeTeamCommsConfig {
            enabled: true,
            max_pending_deliveries: 1000,
        },
    )
    .unwrap();
    assert_eq!(new_service.cancel_message(req).await.unwrap(), committed);
    assert_eq!(cancellation_events(&store).len(), 3);
}

#[tokio::test]
async fn injecting_delivery_rejects_pre_native_cancel_without_partial_mutation() {
    let store = Arc::new(TestStore::default());
    let (_runtime, service, message, mut before) = seeded(store.clone(), "pending").await;
    before[0].status = "injecting".into();
    before[0].updated_at += 1;
    store.upsert_team_delivery(&before[0]).unwrap();
    service
        .state
        .write()
        .await
        .deliveries
        .insert(before[0].id.clone(), before[0].clone());
    let error = service
        .cancel_message(TeamCancelMessageRequest {
            team_id: message.team_id.clone(),
            message_id: message.id.clone(),
        })
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::InvalidState(_)));
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        before
    );
    assert!(cancellation_events(&store).is_empty());
}

#[tokio::test]
async fn old_unjournaled_cancelled_status_is_not_certified_as_a_new_receipt() {
    let store = Arc::new(TestStore::default());
    let (_runtime, service, message, before) = seeded(store.clone(), "cancelled").await;
    assert!(matches!(
        service
            .cancel_message(TeamCancelMessageRequest {
                team_id: message.team_id,
                message_id: message.id,
            })
            .await,
        Err(RuntimeError::Conflict(_))
    ));
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        before
    );
    assert!(cancellation_events(&store).is_empty());
}
