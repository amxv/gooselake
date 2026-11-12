use super::*;
use crate::TeamSendDirectRequest;

impl TestStore {
    pub(super) fn claim_legacy_injecting_test(
        &self,
        previous: &TeamDeliveryRecord,
        injecting: &TeamDeliveryRecord,
        event: &NewRuntimeEvent,
    ) -> Result<bool, RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("legacy claim lock");
        let Some(stored) = hydrated
            .team_deliveries
            .iter()
            .find(|row| row.id == previous.id)
        else {
            return Err(RuntimeError::Conflict(
                "mock legacy delivery disappeared".into(),
            ));
        };
        if stored != previous {
            return Ok(false);
        }
        let mut expected = previous.clone();
        expected.status = "injecting".into();
        expected.updated_at = injecting.updated_at;
        let trigger = event.payload["trigger"].as_str().unwrap_or_default();
        if !matches!(previous.status.as_str(), "pending" | "deferred")
            || injecting.updated_at <= previous.updated_at
            || *injecting != expected
            || event.kind != "team_delivery.injecting"
            || event.criticality != crate::RuntimeEventCriticality::Critical
            || event.scope != RuntimeEventScope::Team
            || event.scope_id != previous.team_id
            || event.team_id.as_deref() != Some(previous.team_id.as_str())
            || event.session_id.as_deref() != Some(previous.recipient_agent_id.as_str())
            || event.payload != serde_json::json!({"delivery":injecting,"trigger":trigger})
        {
            return Err(RuntimeError::ProtocolViolation(
                "mock legacy injection claim drifted".into(),
            ));
        }
        let pending_id = format!("evt_legacy_delivery_{}_pending", previous.id);
        {
            let events = self.events.lock().expect("legacy event lock");
            if !events.iter().any(|row| {
                row.event_id == pending_id
                    && row.kind == "team_delivery.pending"
                    && row.criticality == crate::RuntimeEventCriticality::Critical
            }) || events.iter().any(|row| row.event_id == event.event_id)
                || hydrated
                    .agent_deliveries
                    .iter()
                    .any(|row| row.id == previous.id)
            {
                return Err(RuntimeError::Conflict(
                    "mock legacy claim missing or consumed receipt".into(),
                ));
            }
            let initial = events
                .iter()
                .find(|row| row.event_id == pending_id)
                .and_then(|row| {
                    serde_json::from_value::<TeamDeliveryRecord>(row.payload["delivery"].clone())
                        .ok()
                });
            if initial.as_ref() != Some(previous) {
                let generation_id = format!(
                    "evt_legacy_delivery_{}_{}_{}",
                    previous.id, previous.status, previous.updated_at
                );
                if !events.iter().any(|row| {
                    row.event_id == generation_id
                        && row.kind == format!("team_delivery.{}", previous.status)
                        && row.criticality == crate::RuntimeEventCriticality::Critical
                        && row.payload["delivery"] == serde_json::json!(previous)
                }) {
                    return Err(RuntimeError::Conflict(
                        "mock legacy delivery has no generation-specific Critical event".into(),
                    ));
                }
            }
        }
        // The fake may append only once validation and the injected failure
        // are resolved, with no status publication on an event error.
        self.append_runtime_event(event)?;
        Self::upsert_with_key(&mut hydrated.team_deliveries, injecting.clone(), |row| {
            row.id.clone()
        });
        Ok(true)
    }

    pub(super) fn commit_legacy_injected_test(
        &self,
        before: &TeamDeliveryRecord,
        injected: &TeamDeliveryRecord,
        event: &NewRuntimeEvent,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        let mut hydrated = self.hydrated.lock().expect("mock native result lock");
        let Some(current) = hydrated
            .team_deliveries
            .iter()
            .find(|row| row.id == before.id)
        else {
            return Err(RuntimeError::Conflict(
                "mock native delivery vanished".into(),
            ));
        };
        let mut expected = before.clone();
        expected.status = DELIVERY_STATUS_INJECTED.into();
        expected.injection_strategy = Some("runtime_send_turn".into());
        expected.injected_turn_id = injected.injected_turn_id.clone();
        expected.last_error_code = None;
        expected.last_error_message = None;
        expected.updated_at = injected.updated_at;
        let trigger = event.payload["trigger"].as_str().unwrap_or_default();
        if *injected != expected
            || injected.updated_at <= before.updated_at
            || injected
                .injected_turn_id
                .as_deref()
                .is_none_or(str::is_empty)
            || event.kind != "team_delivery.injected"
            || event.criticality != RuntimeEventCriticality::Critical
            || event.scope != RuntimeEventScope::Team
            || event.scope_id != before.team_id
            || event.team_id.as_deref() != Some(before.team_id.as_str())
            || event.session_id.as_deref() != Some(before.recipient_agent_id.as_str())
            || event.turn_id != injected.injected_turn_id
            || event.event_id
                != format!(
                    "evt_legacy_delivery_{}_injected_{}",
                    before.id, before.updated_at
                )
            || event.created_at != injected.updated_at
            || event.payload
                != serde_json::json!({"delivery":injected,"before_updated_at":before.updated_at,"trigger":trigger})
            || !matches!(
                trigger,
                "queue" | "retry" | "turncompletedboundary" | "startuprecovery"
            )
        {
            return Err(RuntimeError::ProtocolViolation(
                "mock native result Critical receipt drifted".into(),
            ));
        }
        let events = self.events.lock().expect("mock native result event lookup");
        if current == injected {
            if events.iter().any(|row| {
                row.event_id == event.event_id
                    && row.payload == event.payload
                    && row.kind == event.kind
            }) {
                return Ok(injected.clone());
            }
            return Err(RuntimeError::Conflict(
                "mock injected row has no matching event".into(),
            ));
        }
        if current != before
            || hydrated
                .agent_deliveries
                .iter()
                .any(|row| row.id == before.id)
        {
            return Err(RuntimeError::Conflict(
                "mock injecting authority lost".into(),
            ));
        }
        let claim_id = format!(
            "evt_legacy_delivery_{}_injecting_{}",
            before.id, before.updated_at
        );
        if !events.iter().any(|row| {
            row.event_id == claim_id
                && row.kind == "team_delivery.injecting"
                && row.payload["delivery"] == serde_json::json!(before)
        }) || events.iter().any(|row| row.event_id == event.event_id)
        {
            return Err(RuntimeError::Conflict(
                "mock injected result missing original claim or event id consumed".into(),
            ));
        }
        let turn_id = injected.injected_turn_id.as_deref().unwrap_or_default();
        if !hydrated.turns.iter().any(|row| {
            row.id == turn_id
                && row.session_id == before.recipient_agent_id
                && row.source.as_deref() == Some("agent_message_delivery_transport")
        }) || !self.turn_admissions.lock().unwrap().iter().any(|row| {
            row.turn_id == turn_id
                && row.session_id == before.recipient_agent_id
                && row.correlation.correlation_id.as_deref() == Some(before.message_id.as_str())
                && row.projection_source
                    == crate::TurnInputProjectionSource::AgentMessageDeliveryTransport
        }) {
            return Err(RuntimeError::Conflict(
                "mock injected result has no admitted native turn".into(),
            ));
        }
        let Some(message) = hydrated
            .team_messages
            .iter()
            .find(|row| row.id == before.message_id)
        else {
            return Err(RuntimeError::Conflict(
                "mock message no longer exists".into(),
            ));
        };
        let mut all = hydrated
            .team_deliveries
            .iter()
            .filter(|row| row.message_id == before.message_id)
            .cloned()
            .collect::<Vec<_>>();
        for row in &mut all {
            if row.id == before.id {
                *row = injected.clone();
            }
        }
        all.sort_by(|a, b| (a.created_at, a.id.as_str()).cmp(&(b.created_at, b.id.as_str())));
        let terminal = message
            .recipient_agent_ids
            .as_array()
            .is_some_and(|recipients| {
                recipients.len() == all.len()
                    && all.iter().all(|row| {
                        matches!(row.status.as_str(), "injected" | "failed" | "cancelled")
                            && recipients
                                .iter()
                                .any(|id| id.as_str() == Some(row.recipient_agent_id.as_str()))
                    })
            });
        let completion = NewRuntimeEvent {
            event_id: format!("evt_legacy_message_{}_completed_delivery", message.id),
            scope: RuntimeEventScope::Team,
            scope_id: message.team_id.clone(),
            session_id: None,
            team_id: Some(message.team_id.clone()),
            turn_id: None,
            kind: "team_message.completed".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({"message_id":message.id,"deliveries":all}),
            provider: None,
            provider_seq: None,
            created_at: all
                .iter()
                .map(|row| row.updated_at)
                .max()
                .unwrap_or(message.created_at),
        };
        if terminal && events.iter().any(|row| row.event_id == completion.event_id) {
            return Err(RuntimeError::Conflict(
                "mock completion ID already consumed".into(),
            ));
        }
        drop(events);
        // The fake stages both event inserts before either cache/DB row is
        // touched, including fault injection at the LAST completion event.
        {
            let mut fault = self.fail_next_team_event_kind.lock().unwrap();
            if fault.as_deref() == Some(event.kind.as_str())
                || (terminal && fault.as_deref() == Some(completion.kind.as_str()))
            {
                fault.take();
                return Err(RuntimeError::Io(
                    "forced critical native result transaction fault".into(),
                ));
            }
        }
        self.append_runtime_event(event)?;
        if terminal {
            self.append_runtime_event(&completion)?;
        }
        Self::upsert_with_key(&mut hydrated.team_deliveries, injected.clone(), |row| {
            row.id.clone()
        });
        Ok(injected.clone())
    }
}

fn request(team_id: &str, sender: &str, recipient: &str) -> TeamSendDirectRequest {
    TeamSendDirectRequest {
        team_id: team_id.into(),
        sender_agent_id: sender.into(),
        recipient_agent_id: recipient.into(),
        input: serde_json::json!([{"type":"text","text":"atomic claim not native before commit"}]),
        image_paths: Vec::new(),
        priority: "normal".into(),
        policy: "non_interrupting".into(),
        correlation_id: None,
        reply_to_message_id: None,
        idempotency_key: Some("claim-fault".into()),
    }
}

#[tokio::test]
async fn critical_claim_failure_does_not_publish_injecting_or_send_native_turn() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let team = service
        .create_team(TeamCreateRequest {
            name: "Atomic pre-native claim".into(),
            lead_agent_id: sender.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: None,
        })
        .await
        .unwrap();
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team_delivery.injecting".into());
    let ack = service
        .send_direct(request(&team.team.id, &sender, &recipient))
        .await
        .unwrap();
    assert_eq!(ack.disposition, "created");
    let initial = &ack.deliveries[0];
    assert_eq!(
        store.hydrate_runtime_state().unwrap().team_deliveries[0],
        *initial
    );
    assert_eq!(service.state.read().await.deliveries[&initial.id], *initial);
    assert!(!store
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|row| row.kind == "team_delivery.injecting"));
    assert!(runtime
        .list_session_turns(&recipient)
        .await
        .unwrap()
        .is_empty());

    // A new, deliberate attempt after the test proved no SQLite commit can
    // safely gain a first claim. It must still be exactly one native turn.
    let confirmed = service
        .inject_delivery(&initial.id, DeliveryAttemptTrigger::Retry)
        .await
        .unwrap();
    assert_eq!(confirmed.status, "injected");
    let first_turns = runtime.list_session_turns(&recipient).await.unwrap();
    assert_eq!(first_turns.len(), 1);
    let repeated = service
        .inject_delivery(&initial.id, DeliveryAttemptTrigger::Retry)
        .await
        .unwrap();
    assert_eq!(repeated.status, "injected");
    assert_eq!(
        runtime.list_session_turns(&recipient).await.unwrap().len(),
        1
    );
    let events = store.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|row| row.kind == "team_delivery.injecting")
            .count(),
        1
    );
}

#[tokio::test]
async fn late_critical_completion_fault_quarantines_acknowledged_turn_without_redelivery() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let sender = create_test_session(&runtime).await;
    let recipient = create_test_session(&runtime).await;
    let team = service
        .create_team(TeamCreateRequest {
            name: "Atomic accepted-turn completion".into(),
            lead_agent_id: sender.clone(),
            member_agent_ids: vec![recipient.clone()],
            created_by: None,
        })
        .await
        .unwrap();
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team_message.completed".into());
    let ack = service
        .send_direct(request(&team.team.id, &sender, &recipient))
        .await
        .unwrap();
    let current = &store.hydrate_runtime_state().unwrap().team_deliveries[0];
    assert_eq!(current.status, "injecting");
    assert_eq!(service.state.read().await.deliveries[&current.id], *current);
    let turns = runtime.list_session_turns(&recipient).await.unwrap();
    assert_eq!(
        turns.len(),
        1,
        "native acknowledgement happened before late event failure"
    );
    assert!(store.events.lock().unwrap().iter().all(|event| {
        !matches!(
            event.kind.as_str(),
            "team_delivery.injected" | "team_message.completed"
        )
    }));
    let no_retry = service
        .inject_delivery(&current.id, DeliveryAttemptTrigger::Retry)
        .await
        .unwrap();
    assert_eq!(no_retry.status, "injecting");
    assert_eq!(
        runtime.list_session_turns(&recipient).await.unwrap().len(),
        1
    );
    // Reconcile the *same positively acknowledged runtime turn*, rather
    // than calling runtime.send_turn a second time to repair the journal.
    let repaired = service
        .commit_legacy_delivery_injected(
            current,
            turns[0].id.clone(),
            DeliveryAttemptTrigger::Queue,
        )
        .await
        .unwrap();
    assert_eq!(repaired.status, "injected");
    assert_eq!(
        repaired.injected_turn_id.as_deref(),
        Some(turns[0].id.as_str())
    );
    assert_eq!(
        runtime.list_session_turns(&recipient).await.unwrap().len(),
        1
    );
    let events = store.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "team_delivery.injected")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "team_message.completed")
            .count(),
        1
    );
    assert_eq!(ack.deliveries.len(), 1);
}
