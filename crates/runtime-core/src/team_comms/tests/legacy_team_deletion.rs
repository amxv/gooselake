//! Mock the same fail-closed Critical boundary as SQLite team deletion.

use super::*;
use std::collections::HashSet;

impl TestStore {
    pub(super) fn commit_legacy_team_deletion_test(
        &self,
        before: &TeamRecord,
        after: &TeamRecord,
        all: &[TeamDeliveryRecord],
        cancelled: &[TeamDeliveryRecord],
        events: &[NewRuntimeEvent],
    ) -> Result<TeamRecord, RuntimeError> {
        let mut hydrated = self.hydrated.lock().unwrap();
        if hydrated.teams.iter().find(|row| row.id == before.id) != Some(before) {
            return Err(RuntimeError::Conflict(
                "mock legacy team deletion lost team revision".into(),
            ));
        }
        let mut actual = hydrated
            .team_deliveries
            .iter()
            .filter(|row| row.team_id == before.id)
            .cloned()
            .collect::<Vec<_>>();
        actual.sort_by(|a, b| (&a.message_id, &a.id).cmp(&(&b.message_id, &b.id)));
        if actual != all
            || all.iter().any(|row| row.status == "injecting")
            || cancelled.len()
                != all
                    .iter()
                    .filter(|row| matches!(row.status.as_str(), "pending" | "deferred"))
                    .count()
            || events.len() != cancelled.len() + 1
        {
            return Err(RuntimeError::Conflict(
                "mock legacy team deletion has changed/incomplete recipient authority".into(),
            ));
        }
        for (index, (previous, result)) in all
            .iter()
            .filter(|row| matches!(row.status.as_str(), "pending" | "deferred"))
            .zip(cancelled.iter())
            .enumerate()
        {
            let mut expected = previous.clone();
            expected.status = "cancelled".into();
            expected.updated_at = result.updated_at;
            if *result != expected
                || events[index].kind != "team_delivery.cancelled"
                || events[index].criticality != RuntimeEventCriticality::Critical
                || events[index].payload["delivery"] != serde_json::json!(result)
                || events[index].payload["before_status"] != before_status(previous)
            {
                return Err(RuntimeError::ProtocolViolation(
                    "mock team deletion Critical recipient mismatched".into(),
                ));
            }
        }
        let deletion = events.last().expect("validated event count");
        if deletion.kind != "team.deleted"
            || deletion.event_id != format!("evt_team_{}_deleted", before.id)
            || deletion.criticality != RuntimeEventCriticality::Critical
            || deletion.payload != serde_json::json!({"team":after})
        {
            return Err(RuntimeError::ProtocolViolation(
                "mock team deletion Critical tombstone mismatched".into(),
            ));
        }
        let mut receipts = events[..cancelled.len()].to_vec();
        let mut affected = HashSet::new();
        let replacement = cancelled
            .iter()
            .map(|row| (row.id.clone(), row.clone()))
            .collect::<HashMap<_, _>>();
        for row in &mut actual {
            if let Some(after) = replacement.get(&row.id) {
                affected.insert(row.message_id.clone());
                *row = after.clone();
            }
        }
        for message_id in affected {
            let message = hydrated
                .team_messages
                .iter()
                .find(|row| row.id == message_id)
                .ok_or_else(|| {
                    RuntimeError::Conflict("mock deleted team message missing".into())
                })?;
            let mut rows = actual
                .iter()
                .filter(|row| row.message_id == message_id)
                .cloned()
                .collect::<Vec<_>>();
            rows.sort_by(|a, b| (a.created_at, a.id.as_str()).cmp(&(b.created_at, b.id.as_str())));
            if rows
                .iter()
                .all(|row| matches!(row.status.as_str(), "injected" | "failed" | "cancelled"))
            {
                receipts.push(NewRuntimeEvent {
                    event_id: format!("evt_legacy_message_{}_completed_delivery", message.id),
                    scope: RuntimeEventScope::Team,
                    scope_id: message.team_id.clone(),
                    session_id: None,
                    team_id: Some(message.team_id.clone()),
                    turn_id: None,
                    kind: "team_message.completed".into(),
                    criticality: RuntimeEventCriticality::Critical,
                    payload: serde_json::json!({"message_id":message.id,"deliveries":rows}),
                    provider: None,
                    provider_seq: None,
                    created_at: rows
                        .iter()
                        .map(|row| row.updated_at)
                        .max()
                        .unwrap_or(message.created_at),
                });
            }
        }
        receipts.push(deletion.clone());
        {
            let mut fault = self.fail_next_team_event_kind.lock().unwrap();
            if fault
                .as_ref()
                .is_some_and(|kind| receipts.iter().any(|row| &row.kind == kind))
            {
                fault.take();
                return Err(RuntimeError::Io(
                    "forced mock team deletion late Critical fault".into(),
                ));
            }
        }
        {
            let existing = self.events.lock().unwrap();
            if receipts.iter().any(|row| {
                existing
                    .iter()
                    .any(|committed| committed.event_id == row.event_id)
            }) {
                return Err(RuntimeError::Conflict(
                    "mock team deletion receipt already consumed".into(),
                ));
            }
        }
        for event in &receipts {
            self.append_runtime_event(event)?;
        }
        Self::upsert_with_key(&mut hydrated.teams, after.clone(), |row| row.id.clone());
        for row in &actual {
            Self::upsert_with_key(&mut hydrated.team_deliveries, row.clone(), |row| {
                row.id.clone()
            });
        }
        Ok(after.clone())
    }
}

fn before_status(row: &TeamDeliveryRecord) -> serde_json::Value {
    serde_json::json!(row.status)
}

#[tokio::test]
async fn deletion_tombstone_critical_failure_preserves_live_team_and_next_attempt_commits() {
    let store = Arc::new(TestStore::default());
    let (runtime, service) = build_runtime_and_service(store.clone(), 0);
    let lead = create_test_session(&runtime).await;
    let created = service
        .create_team(TeamCreateRequest {
            name: "Deletion fault team".into(),
            lead_agent_id: lead,
            member_agent_ids: vec![],
            created_by: Some("operator".into()),
        })
        .await
        .unwrap();
    let team_id = created.team.id;
    *store.fail_next_team_event_kind.lock().unwrap() = Some("team.deleted".into());
    assert!(matches!(
        service.delete_team(&team_id).await,
        Err(RuntimeError::Io(_))
    ));
    assert_eq!(
        service.get_team(&team_id).await.unwrap().team.deleted_at,
        None
    );
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|event| event.kind != "team.deleted"));
    assert_eq!(store.hydrate_runtime_state().unwrap().teams.len(), 1);

    service.delete_team(&team_id).await.unwrap();
    assert!(matches!(
        service.get_team(&team_id).await,
        Err(RuntimeError::NotFound(_))
    ));
    let events = store.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "team.deleted")
            .count(),
        1
    );
    assert!(events.iter().any(|event| event.kind == "team.deleted"
        && event.criticality == RuntimeEventCriticality::Critical));
    assert!(store.hydrate_runtime_state().unwrap().teams[0]
        .deleted_at
        .is_some());
}
