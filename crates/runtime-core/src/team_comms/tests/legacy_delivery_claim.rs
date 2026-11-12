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
        }
        // The fake may append only once validation and the injected failure
        // are resolved, with no status publication on an event error.
        self.append_runtime_event(event)?;
        Self::upsert_with_key(&mut hydrated.team_deliveries, injecting.clone(), |row| {
            row.id.clone()
        });
        Ok(true)
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
