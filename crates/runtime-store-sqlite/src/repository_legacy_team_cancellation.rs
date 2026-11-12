//! Pre-native cancellation is a state/event transaction, not a cache-first
//! mutation followed by best-effort critical messages. The cancelled state
//! and every delivery plus message event are a single durable authority.

use std::collections::{HashMap, HashSet};

use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    TeamDeliveryRecord, TeamMessageRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_legacy_team_messages::{deliveries_by_message, event_matches, message_by_id};
use crate::SqliteRuntimeRepository;

fn is_cancelled_receipt(
    record: &RuntimeEventRecord,
    previous: &TeamDeliveryRecord,
    current: &TeamDeliveryRecord,
) -> bool {
    record.kind == "team_delivery.cancelled"
        && record.criticality == RuntimeEventCriticality::Critical
        && record.scope == RuntimeEventScope::Team
        && record.scope_id == current.team_id
        && record.team_id.as_deref() == Some(current.team_id.as_str())
        && record.session_id.as_deref() == Some(current.recipient_agent_id.as_str())
        && record.turn_id.is_none()
        && record.provider.is_none()
        && record.provider_seq.is_none()
        && record.payload
            == serde_json::json!({
                "delivery":current,
                "before_updated_at":previous.updated_at,
                "before_status":previous.status,
            })
        && record.created_at == current.updated_at
}

fn is_completed_receipt(
    record: &RuntimeEventRecord,
    message: &TeamMessageRecord,
    after: &[TeamDeliveryRecord],
) -> bool {
    record.kind == "team_message.completed"
        && record.criticality == RuntimeEventCriticality::Critical
        && record.scope == RuntimeEventScope::Team
        && record.scope_id == message.team_id
        && record.team_id.as_deref() == Some(message.team_id.as_str())
        && record.session_id.is_none()
        && record.turn_id.is_none()
        && record.provider.is_none()
        && record.provider_seq.is_none()
        && record.payload == serde_json::json!({"message_id":message.id})
        && record.created_at >= message.created_at
        && (after.is_empty() || after.iter().all(|row| row.updated_at == record.created_at))
}

fn validate(
    message: &TeamMessageRecord,
    before: &[TeamDeliveryRecord],
    after: &[TeamDeliveryRecord],
    events: &[NewRuntimeEvent],
) -> Result<(), RuntimeError> {
    if message.id.is_empty()
        || message.team_id.is_empty()
        || before.len() != after.len()
        || events.len() != after.len() + 1
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy message cancellation snapshot or journal length mismatch".into(),
        ));
    }
    let completed_at = events
        .last()
        .ok_or_else(|| RuntimeError::ProtocolViolation("missing completion event".into()))?
        .created_at;
    if completed_at < message.created_at {
        return Err(RuntimeError::ProtocolViolation(
            "legacy cancellation completion time precedes its message".into(),
        ));
    }
    let mut seen_delivery_ids = HashSet::new();
    let mut seen_event_ids = HashSet::new();
    for (index, (old, new)) in before.iter().zip(after).enumerate() {
        let mut expected = old.clone();
        expected.status = "cancelled".into();
        expected.updated_at = new.updated_at;
        if old.message_id != message.id
            || old.team_id != message.team_id
            || old.id.is_empty()
            || !seen_delivery_ids.insert(old.id.as_str())
            || !matches!(old.status.as_str(), "pending" | "deferred")
            || new.updated_at <= old.updated_at
            || new.updated_at != completed_at
            || *new != expected
        {
            return Err(RuntimeError::ProtocolViolation(
                "legacy cancellation contains changed identity or non-pending delivery".into(),
            ));
        }
        let event = &events[index];
        if event.event_id != format!("evt_legacy_delivery_{}_cancelled", new.id)
            || !seen_event_ids.insert(event.event_id.as_str())
            || event.kind != "team_delivery.cancelled"
            || event.payload
                != serde_json::json!({
                    "delivery":new,
                    "before_updated_at":old.updated_at,
                    "before_status":old.status,
                })
            || event.session_id.as_deref() != Some(new.recipient_agent_id.as_str())
            || event.created_at != new.updated_at
            || !valid_envelope(event, &message.team_id)
        {
            return Err(RuntimeError::ProtocolViolation(
                "legacy delivery cancellation Critical receipt differs from state".into(),
            ));
        }
    }
    let completed = events.last().expect("validated event count");
    if completed.event_id != format!("evt_legacy_message_{}_completed_cancel", message.id)
        || !seen_event_ids.insert(completed.event_id.as_str())
        || completed.kind != "team_message.completed"
        || completed.payload != serde_json::json!({"message_id":message.id})
        || completed.session_id.is_some()
        || !valid_envelope(completed, &message.team_id)
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy message completion Critical receipt differs from cancellation".into(),
        ));
    }
    Ok(())
}

fn valid_envelope(event: &NewRuntimeEvent, team_id: &str) -> bool {
    event.scope == RuntimeEventScope::Team
        && event.scope_id == team_id
        && event.team_id.as_deref() == Some(team_id)
        && event.criticality == RuntimeEventCriticality::Critical
        && event.turn_id.is_none()
        && event.provider.is_none()
        && event.provider_seq.is_none()
}

fn completed_event_id(message_id: &str) -> String {
    format!("evt_legacy_message_{message_id}_completed_cancel")
}

fn complete_recipient_snapshot(message: &TeamMessageRecord, rows: &[TeamDeliveryRecord]) -> bool {
    let Some(recipients) = message.recipient_agent_ids.as_array() else {
        return false;
    };
    if recipients.len() != rows.len() {
        return false;
    }
    let mut seen = HashSet::new();
    rows.iter().all(|row| {
        row.message_id == message.id
            && row.team_id == message.team_id
            && seen.insert(row.recipient_agent_id.as_str())
            && recipients
                .iter()
                .any(|recipient| recipient.as_str() == Some(row.recipient_agent_id.as_str()))
    })
}

fn replay_previous_cancellation(
    tx: &Transaction<'_>,
    message: &TeamMessageRecord,
    before: &[TeamDeliveryRecord],
    prior_completed: &RuntimeEventRecord,
) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
    let current = deliveries_by_message(tx, &message.id)?;
    let current_by_id: HashMap<_, _> = current.iter().map(|row| (row.id.as_str(), row)).collect();
    if !complete_recipient_snapshot(message, &current)
        || current.len() != before.len()
        || current.iter().any(|row| row.status != "cancelled")
        || !is_completed_receipt(prior_completed, message, &current)
    {
        return Err(RuntimeError::Conflict(
            "legacy cancellation receipt no longer matches durable delivery state".into(),
        ));
    }
    for previous in before {
        let current = current_by_id.get(previous.id.as_str()).ok_or_else(|| {
            RuntimeError::Conflict("legacy cancellation delivery was removed or replaced".into())
        })?;
        let mut expected = previous.clone();
        expected.status = "cancelled".into();
        expected.updated_at = current.updated_at;
        if **current != expected {
            return Err(RuntimeError::Conflict(
                "legacy cancellation replay refers to different original delivery".into(),
            ));
        }
        let prior = fetch_runtime_event_by_event_id(
            tx,
            &format!("evt_legacy_delivery_{}_cancelled", previous.id),
        )?
        .ok_or_else(|| RuntimeError::Conflict("missing original cancellation event".into()))?;
        if !is_cancelled_receipt(&prior, previous, current) {
            return Err(RuntimeError::Conflict(
                "legacy cancellation event does not match original pending generation".into(),
            ));
        }
    }
    Ok(current)
}

impl SqliteRuntimeRepository {
    pub fn get_legacy_team_message_cancellation_receipt(
        &self,
        message: &TeamMessageRecord,
    ) -> Result<Option<Vec<TeamDeliveryRecord>>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| db_error("failed starting legacy cancellation receipt read", error))?;
        if message_by_id(&tx, &message.id)?.as_ref() != Some(message) {
            return Err(RuntimeError::Conflict(
                "legacy cancellation receipt belongs to another or missing message".into(),
            ));
        }
        let Some(completed) =
            fetch_runtime_event_by_event_id(&tx, &completed_event_id(&message.id))?
        else {
            return Ok(None);
        };
        let current = deliveries_by_message(&tx, &message.id)?;
        if !complete_recipient_snapshot(message, &current)
            || current.iter().any(|row| row.status != "cancelled")
            || !is_completed_receipt(&completed, message, &current)
        {
            return Err(RuntimeError::Conflict(
                "legacy cancellation completion has missing or changed durable state".into(),
            ));
        }
        for row in &current {
            let event = fetch_runtime_event_by_event_id(
                &tx,
                &format!("evt_legacy_delivery_{}_cancelled", row.id),
            )?
            .ok_or_else(|| {
                RuntimeError::Conflict("legacy cancellation missing delivery Critical event".into())
            })?;
            let before_status = event.payload["before_status"].as_str().ok_or_else(|| {
                RuntimeError::Conflict(
                    "legacy cancellation original status receipt malformed".into(),
                )
            })?;
            let before_updated_at =
                event.payload["before_updated_at"].as_i64().ok_or_else(|| {
                    RuntimeError::Conflict(
                        "legacy cancellation prior revision receipt malformed".into(),
                    )
                })?;
            let mut previous = row.clone();
            previous.status = before_status.into();
            previous.updated_at = before_updated_at;
            if !matches!(before_status, "pending" | "deferred")
                || before_updated_at >= row.updated_at
                || !is_cancelled_receipt(&event, &previous, row)
            {
                return Err(RuntimeError::Conflict(
                    "legacy cancellation delivery receipt is not an exact pending-to-cancelled transition".into(),
                ));
            }
        }
        Ok(Some(current))
    }

    pub fn commit_legacy_team_message_cancellation(
        &self,
        message: &TeamMessageRecord,
        before: &[TeamDeliveryRecord],
        after: &[TeamDeliveryRecord],
        events: &[NewRuntimeEvent],
    ) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
        validate(message, before, after, events)?;
        if !complete_recipient_snapshot(message, before) {
            return Err(RuntimeError::ProtocolViolation(
                "legacy cancellation did not include every original message recipient".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error("failed starting atomic legacy message cancellation", error)
            })?;

        let stored_message = message_by_id(&tx, &message.id)?.ok_or_else(|| {
            RuntimeError::Conflict("legacy cancellation message no longer exists".into())
        })?;
        if stored_message != *message {
            return Err(RuntimeError::Conflict(
                "legacy cancellation message authority differs from cached intent".into(),
            ));
        }
        if let Some(prior_completed) =
            fetch_runtime_event_by_event_id(&tx, &completed_event_id(&message.id))?
        {
            return replay_previous_cancellation(&tx, message, before, &prior_completed);
        }

        let team_active: bool = tx
            .query_row(
                "SELECT deleted_at IS NULL FROM teams WHERE id=?1",
                [&message.team_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking cancellation team authority", error))?
            .unwrap_or(false);
        if !team_active {
            return Err(RuntimeError::Conflict(
                "legacy cancellation team is missing or deleted".into(),
            ));
        }
        let mut current = deliveries_by_message(&tx, &message.id)?;
        let mut original = before.to_vec();
        current.sort_by(|left, right| left.id.cmp(&right.id));
        original.sort_by(|left, right| left.id.cmp(&right.id));
        if current != original {
            return Err(RuntimeError::Conflict(
                "legacy cancellation cannot overwrite changed or missing deliveries".into(),
            ));
        }
        for (previous, next) in before.iter().zip(after) {
            let canonical: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM agent_deliveries WHERE id=?1)",
                    [&previous.id],
                    |row| row.get(0),
                )
                .map_err(|error| db_error("failed checking canonical delivery authority", error))?;
            if canonical {
                return Err(RuntimeError::Conflict(
                    "canonical agent delivery must be cancelled by its owner".into(),
                ));
            }
            if fetch_runtime_event_by_event_id(
                &tx,
                &format!("evt_legacy_delivery_{}_cancelled", previous.id),
            )?
            .is_some()
            {
                return Err(RuntimeError::Conflict(
                    "legacy cancellation event ID is already consumed".into(),
                ));
            }
            let changed = tx
                .execute(
                    "UPDATE team_deliveries SET status='cancelled',updated_at=?1
                     WHERE id=?2 AND message_id=?3 AND status=?4 AND updated_at=?5",
                    params![
                        next.updated_at,
                        previous.id,
                        previous.message_id,
                        previous.status,
                        previous.updated_at,
                    ],
                )
                .map_err(|error| db_error("failed cancelling legacy delivery", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "legacy delivery was concurrently changed before cancellation".into(),
                ));
            }
        }

        for event in events {
            let inserted = insert_runtime_event_in_transaction(&tx, event)?;
            if !event_matches(&inserted, event) {
                return Err(RuntimeError::Conflict(
                    "legacy cancellation critical event ID collision".into(),
                ));
            }
        }
        tx.commit().map_err(|error| {
            db_error(
                "failed committing legacy cancellation and Critical events",
                error,
            )
        })?;
        Ok(after.to_vec())
    }
}
