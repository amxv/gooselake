//! Legacy /v1 pre-native cancellation of a message and all its still-pending
//! deliveries is one durable event/state commit. It cannot race a native
//! injection past an already-cancelled delivery.

use crate::{RuntimeError, TeamDeliveryRecord};

use super::{
    now_ms, RuntimeTeamCommsService, DELIVERY_STATUS_CANCELLED, DELIVERY_STATUS_DEFERRED,
    DELIVERY_STATUS_PENDING,
};

impl RuntimeTeamCommsService {
    pub(super) async fn legacy_cancel_message(
        &self,
        team_id: &str,
        message_id: &str,
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        let mut state = self.state.write().await;
        let message = state
            .messages
            .get(message_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("message {message_id}")))?;
        if message.team_id != team_id {
            return Err(RuntimeError::InvalidState(format!(
                "message {message_id} does not belong to team {team_id}"
            )));
        }

        let delivery_ids = state
            .message_delivery_ids
            .get(message_id)
            .cloned()
            .unwrap_or_default();
        let mut before = Vec::with_capacity(delivery_ids.len());
        let mut saw_cancelled = false;
        let mut saw_pending = false;
        for id in delivery_ids {
            let previous = state.deliveries.get(&id).cloned().ok_or_else(|| {
                RuntimeError::Conflict(format!(
                    "legacy cancellation missing delivery {id}; no native retry authorized"
                ))
            })?;
            if previous.message_id != message_id || previous.team_id != team_id {
                return Err(RuntimeError::Conflict(
                    "legacy cancellation delivery belonged to another message/team".into(),
                ));
            }
            match previous.status.as_str() {
                DELIVERY_STATUS_PENDING | DELIVERY_STATUS_DEFERRED => saw_pending = true,
                DELIVERY_STATUS_CANCELLED => saw_cancelled = true,
                _ => {
                    return Err(RuntimeError::InvalidState(format!(
                        "message {message_id} cannot be cancelled because delivery {} is in {}",
                        previous.id, previous.status
                    )));
                }
            }
            before.push(previous);
        }
        before.sort_by(|left, right| left.id.cmp(&right.id));

        if saw_cancelled || before.is_empty() {
            if saw_pending {
                return Err(RuntimeError::Conflict(
                    "partially cancelled legacy message cannot authorize a native retry".into(),
                ));
            }
            if let Some(mut receipt) = self
                .store
                .get_legacy_team_message_cancellation_receipt(&message)?
            {
                receipt.sort_by(|left, right| left.id.cmp(&right.id));
                if receipt != before {
                    return Err(RuntimeError::Conflict(
                        "replayed legacy cancellation disagrees with cached durable state".into(),
                    ));
                }
                return Ok(receipt);
            }
            if saw_cancelled {
                return Err(RuntimeError::Conflict(
                    "legacy delivery was cancelled without a verified terminal Critical receipt"
                        .into(),
                ));
            }
        }

        // One generation strictly beyond every old pending row, even if all
        // these updates occur within a single clock tick.
        let mut now = now_ms().max(message.created_at);
        for previous in &before {
            let minimum = previous.updated_at.checked_add(1).ok_or_else(|| {
                RuntimeError::InvalidState("legacy delivery revision timestamp exhausted".into())
            })?;
            now = now.max(minimum);
        }
        let after = before
            .iter()
            .map(|row| {
                let mut cancelled = row.clone();
                cancelled.status = DELIVERY_STATUS_CANCELLED.into();
                cancelled.updated_at = now;
                cancelled
            })
            .collect::<Vec<_>>();

        let mut events = Vec::with_capacity(after.len() + 1);
        for (previous, cancelled) in before.iter().zip(&after) {
            let mut event = self.new_team_event(
                team_id,
                "team_delivery.cancelled",
                serde_json::json!({
                    "delivery": cancelled,
                    "before_updated_at": previous.updated_at,
                    "before_status": previous.status,
                }),
                Some(cancelled.recipient_agent_id.clone()),
            );
            event.event_id = format!("evt_legacy_delivery_{}_cancelled", cancelled.id);
            event.created_at = now;
            events.push(event);
        }
        let mut completed = self.new_team_event(
            team_id,
            "team_message.completed",
            serde_json::json!({"message_id":message.id}),
            None,
        );
        completed.event_id = format!("evt_legacy_message_{}_completed_cancel", message.id);
        completed.created_at = now;
        events.push(completed);

        let persisted = self
            .store
            .commit_legacy_team_message_cancellation(&message, &before, &after, &events)?;

        // A prior committed receipt can be returned by another process after
        // losing a race. Publish only those exact durable records, not our
        // speculative new values.
        for delivery in &persisted {
            state
                .deliveries
                .insert(delivery.id.clone(), delivery.clone());
        }
        Ok(persisted)
    }
}
