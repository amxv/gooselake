//! The test store mirrors the all-or-nothing Critical boundary for pre-native
//! legacy status transitions and last-recipient terminal completion.

use super::*;

impl TestStore {
    pub(super) fn commit_legacy_pre_native_test(
        &self,
        before: &TeamDeliveryRecord,
        after: &TeamDeliveryRecord,
        event: &NewRuntimeEvent,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        let mut hydrated = self.hydrated.lock().unwrap();
        let current = hydrated
            .team_deliveries
            .iter()
            .find(|row| row.id == before.id)
            .ok_or_else(|| RuntimeError::Conflict("mock pre-native delivery absent".into()))?
            .clone();
        let trigger = event.payload["trigger"].as_str().unwrap_or_default();
        let allowed = match (before.status.as_str(), after.status.as_str()) {
            ("pending", "deferred" | "failed") | ("deferred", "failed") => true,
            ("deferred" | "failed", "pending") => trigger == "retry",
            _ => false,
        };
        let mut expected = before.clone();
        expected.status = after.status.clone();
        expected.updated_at = after.updated_at;
        expected.injection_strategy = None;
        expected.injected_turn_id = None;
        expected.last_error_code = after.last_error_code.clone();
        expected.last_error_message = after.last_error_message.clone();
        if !allowed
            || *after != expected
            || after.updated_at <= before.updated_at
            || event.event_id
                != format!(
                    "evt_legacy_delivery_{}_{}_{}",
                    after.id, after.status, after.updated_at
                )
            || event.created_at != after.updated_at
            || event.kind != format!("team_delivery.{}", after.status)
            || event.criticality != crate::RuntimeEventCriticality::Critical
            || event.payload
                != serde_json::json!({
                    "delivery":after,
                    "before_status":before.status,
                    "before_updated_at":before.updated_at,
                    "trigger":trigger,
                })
        {
            return Err(RuntimeError::ProtocolViolation(
                "mock legacy pre-native transition mismatch".into(),
            ));
        }
        let message = hydrated
            .team_messages
            .iter()
            .find(|row| row.id == before.message_id)
            .ok_or_else(|| RuntimeError::Conflict("mock legacy message missing".into()))?;
        let mut rows = hydrated
            .team_deliveries
            .iter()
            .filter(|row| row.message_id == message.id)
            .cloned()
            .collect::<Vec<_>>();
        let recipients = message
            .recipient_agent_ids
            .as_array()
            .ok_or_else(|| RuntimeError::Conflict("mock recipients malformed".into()))?;
        if rows.len() != recipients.len() {
            return Err(RuntimeError::Conflict(
                "mock legacy transition missing a recipient".into(),
            ));
        }
        for row in &mut rows {
            if row.id == before.id {
                *row = after.clone();
            }
        }
        rows.sort_by(|a, b| (a.created_at, a.id.as_str()).cmp(&(b.created_at, b.id.as_str())));
        let terminal = rows
            .iter()
            .all(|row| matches!(row.status.as_str(), "injected" | "failed" | "cancelled"));
        let completion = NewRuntimeEvent {
            event_id: format!("evt_legacy_message_{}_completed_delivery", message.id),
            scope: RuntimeEventScope::Team,
            scope_id: message.team_id.clone(),
            session_id: None,
            team_id: Some(message.team_id.clone()),
            turn_id: None,
            kind: "team_message.completed".into(),
            criticality: crate::RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"message_id":message.id,"deliveries":rows}),
            provider: None,
            provider_seq: None,
            created_at: rows
                .iter()
                .map(|row| row.updated_at)
                .max()
                .unwrap_or(message.created_at),
        };
        {
            let events = self.events.lock().unwrap();
            if current == *after {
                if !events.iter().any(|row| {
                    row.event_id == event.event_id
                        && row.kind == event.kind
                        && row.payload == event.payload
                }) || (terminal
                    && !events.iter().any(|row| {
                        row.event_id == completion.event_id && row.payload == completion.payload
                    }))
                {
                    return Err(RuntimeError::Conflict(
                        "mock pre-native replay receipt missing".into(),
                    ));
                }
                return Ok(after.clone());
            }
            if current != *before
                || events.iter().any(|row| row.event_id == event.event_id)
                || events.iter().any(|row| row.event_id == completion.event_id)
            {
                return Err(RuntimeError::Conflict(
                    "mock pre-native authority or completion was already consumed".into(),
                ));
            }
        }
        {
            let mut fault = self.fail_next_team_event_kind.lock().unwrap();
            if fault.as_deref() == Some(event.kind.as_str())
                || (terminal && fault.as_deref() == Some(completion.kind.as_str()))
            {
                fault.take();
                return Err(RuntimeError::Io(
                    "forced mock pre-native Critical event fault".into(),
                ));
            }
        }
        self.append_runtime_event(event)?;
        if terminal {
            self.append_runtime_event(&completion)?;
        }
        Self::upsert_with_key(&mut hydrated.team_deliveries, after.clone(), |row| {
            row.id.clone()
        });
        Ok(after.clone())
    }
}

async fn seeded_pending() -> (
    Arc<TestStore>,
    Arc<RuntimeSessionManager>,
    Arc<RuntimeTeamCommsService>,
    TeamDeliveryRecord,
    String,
) {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let team = service
        .create_team(TeamCreateRequest {
            name: "Critical pre-native transition".into(),
            lead_agent_id: sender.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: None,
        })
        .await
        .unwrap();
    // Fail the dispatch claim before any native effect. The pending receipt
    // was committed and is now eligible for a separately authorized local
    // deferred/failed transition.
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team_delivery.injecting".into());
    let ack = service
        .send_direct(TeamSendDirectRequest {
            team_id: team.team.id,
            sender_agent_id: sender,
            recipient_agent_id: recipient.clone(),
            input: serde_json::json!([{"type":"text","text":"pre-native receipt"}]),
            image_paths: Vec::new(),
            priority: "normal".into(),
            policy: "non_interrupting".into(),
            correlation_id: None,
            reply_to_message_id: None,
            idempotency_key: Some("pre-native-fault".into()),
        })
        .await
        .unwrap();
    assert_eq!(ack.deliveries[0].status, "pending");
    assert!(runtime
        .list_session_turns(&recipient)
        .await
        .unwrap()
        .is_empty());
    (
        store,
        runtime,
        service,
        ack.deliveries[0].clone(),
        recipient,
    )
}

#[tokio::test]
async fn deferred_critical_fault_preserves_durable_and_cached_pending_generation() {
    let (store, runtime, service, pending, recipient) = seeded_pending().await;
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team_delivery.deferred".into());
    let failed_event_fault = service
        .transition_delivery_status(
            &pending,
            DELIVERY_STATUS_DEFERRED,
            None,
            None,
            None,
            DeliveryAttemptTrigger::Queue,
        )
        .await;
    assert!(
        matches!(failed_event_fault, Err(RuntimeError::Io(_))),
        "expected late Critical fault, received {failed_event_fault:?}"
    );
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        vec![pending.clone()]
    );
    assert_eq!(service.state.read().await.deliveries[&pending.id], pending);
    assert!(runtime
        .list_session_turns(&recipient)
        .await
        .unwrap()
        .is_empty());
    assert!(!store
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|row| row.kind == "team_delivery.deferred"));

    let deferred = service
        .transition_delivery_status(
            &pending,
            DELIVERY_STATUS_DEFERRED,
            None,
            None,
            None,
            DeliveryAttemptTrigger::Queue,
        )
        .await
        .unwrap();
    assert_eq!(deferred.status, "deferred");
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        vec![deferred.clone()]
    );
    assert_eq!(service.state.read().await.deliveries[&pending.id], deferred);
    let events = store.events.lock().unwrap();
    let critical = events
        .iter()
        .filter(|row| row.kind == "team_delivery.deferred")
        .collect::<Vec<_>>();
    assert_eq!(critical.len(), 1);
    assert_eq!(critical[0].criticality, RuntimeEventCriticality::Critical);
}

#[tokio::test]
async fn terminal_failure_completion_fault_rolls_back_both_critical_events_and_cache() {
    let (store, runtime, service, pending, recipient) = seeded_pending().await;
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team_message.completed".into());
    let late_failure = service
        .transition_delivery_status(
            &pending,
            DELIVERY_STATUS_FAILED,
            None,
            Some("recipient_closed".into()),
            Some("no native send attempted".into()),
            DeliveryAttemptTrigger::Queue,
        )
        .await;
    assert!(
        matches!(late_failure, Err(RuntimeError::Io(_))),
        "expected terminal Critical fault, got {late_failure:?}"
    );
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        vec![pending.clone()]
    );
    assert_eq!(service.state.read().await.deliveries[&pending.id], pending);
    let events = store.events.lock().unwrap();
    assert!(!events.iter().any(|row| matches!(
        row.kind.as_str(),
        "team_delivery.failed" | "team_message.completed"
    )));
    drop(events);

    let failed = service
        .transition_delivery_status(
            &pending,
            DELIVERY_STATUS_FAILED,
            None,
            Some("recipient_closed".into()),
            Some("no native send attempted".into()),
            DeliveryAttemptTrigger::Queue,
        )
        .await
        .unwrap();
    assert_eq!(failed.status, "failed");
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries,
        vec![failed.clone()]
    );
    let events = store.events.lock().unwrap();
    let receipt_kinds = events
        .iter()
        .filter(|row| {
            matches!(
                row.kind.as_str(),
                "team_delivery.failed" | "team_message.completed"
            )
        })
        .map(|row| row.kind.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        receipt_kinds,
        ["team_delivery.failed", "team_message.completed"]
    );
    drop(events);
    assert!(runtime
        .list_session_turns(&recipient)
        .await
        .unwrap()
        .is_empty());
}
