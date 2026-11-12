//! Every pre-native legacy delivery deferral, failure and retry changes one
//! generation and its Critical receipt in one SQLite transaction.
//! Unknown native injecting outcomes are not eligible for this operation.

use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, TeamDeliveryRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_legacy_team_delivery_result::{complete_recipient_snapshot, completed_event};
use crate::repository_legacy_team_messages::{deliveries_by_message, event_matches, message_by_id};
use crate::SqliteRuntimeRepository;

fn allowed_transition(before: &str, after: &str, trigger: &str) -> bool {
    match (before, after) {
        ("pending", "deferred" | "failed") | ("deferred", "failed") => true,
        ("deferred" | "failed", "pending") => trigger == "retry",
        _ => false,
    }
}

fn valid_trigger(trigger: &str) -> bool {
    matches!(
        trigger,
        "queue" | "retry" | "turncompletedboundary" | "startuprecovery"
    )
}

/// Verify original immutable pending admission and the latest Critical
/// generation. Unjournaled historical direct upserts fail closed instead of
/// granting an implicit native retry.
pub(crate) fn require_legacy_pre_native_lineage(
    tx: &Transaction<'_>,
    row: &TeamDeliveryRecord,
) -> Result<(), RuntimeError> {
    if !matches!(row.status.as_str(), "pending" | "deferred" | "failed") {
        return Err(RuntimeError::Conflict(
            "legacy delivery is not a pre-native retryable generation".into(),
        ));
    }
    let initial_event_id = format!("evt_legacy_delivery_{}_pending", row.id);
    let initial_event =
        fetch_runtime_event_by_event_id(tx, &initial_event_id)?.ok_or_else(|| {
            RuntimeError::Conflict("legacy delivery lacks Critical pending admission".into())
        })?;
    let initial: TeamDeliveryRecord =
        serde_json::from_value(initial_event.payload["delivery"].clone()).map_err(|_| {
            RuntimeError::Conflict("legacy pending admission payload is not a delivery".into())
        })?;
    if initial_event.kind != "team_delivery.pending"
        || initial_event.scope != RuntimeEventScope::Team
        || initial_event.scope_id != row.team_id
        || initial_event.team_id.as_deref() != Some(row.team_id.as_str())
        || initial_event.session_id.as_deref() != Some(row.recipient_agent_id.as_str())
        || initial_event.turn_id.is_some()
        || initial_event.provider.is_some()
        || initial_event.provider_seq.is_some()
        || initial_event.criticality != RuntimeEventCriticality::Critical
        || initial_event.payload != serde_json::json!({"delivery":initial})
        || initial_event.created_at != initial.created_at
        || initial.status != "pending"
        || initial.updated_at != initial.created_at
        || initial.id != row.id
        || initial.message_id != row.message_id
        || initial.team_id != row.team_id
        || initial.recipient_agent_id != row.recipient_agent_id
        || initial.provider != row.provider
        || initial.effective_policy != row.effective_policy
        || initial.created_at != row.created_at
    {
        return Err(RuntimeError::Conflict(
            "legacy pre-native status lost its immutable Critical admission".into(),
        ));
    }
    if *row == initial {
        return Ok(());
    }
    let event_id = format!(
        "evt_legacy_delivery_{}_{}_{}",
        row.id, row.status, row.updated_at
    );
    let last = fetch_runtime_event_by_event_id(tx, &event_id)?.ok_or_else(|| {
        RuntimeError::Conflict(
            "legacy delivery changed generation without a Critical pre-native transition".into(),
        )
    })?;
    let before_status = last.payload["before_status"].as_str().unwrap_or_default();
    let trigger = last.payload["trigger"].as_str().unwrap_or_default();
    if last.kind != format!("team_delivery.{}", row.status)
        || last.scope != RuntimeEventScope::Team
        || last.scope_id != row.team_id
        || last.team_id.as_deref() != Some(row.team_id.as_str())
        || last.session_id.as_deref() != Some(row.recipient_agent_id.as_str())
        || last.turn_id.is_some()
        || last.provider.is_some()
        || last.provider_seq.is_some()
        || last.criticality != RuntimeEventCriticality::Critical
        || last.created_at != row.updated_at
        || last.payload["delivery"] != serde_json::json!(row)
        || last.payload["before_updated_at"]
            .as_i64()
            .is_none_or(|time| time < row.created_at || time >= row.updated_at)
        || !valid_trigger(trigger)
        || !allowed_transition(before_status, &row.status, trigger)
    {
        return Err(RuntimeError::Conflict(
            "legacy delivery latest Critical transition receipt is forged or incomplete".into(),
        ));
    }
    Ok(())
}

fn validate(
    before: &TeamDeliveryRecord,
    after: &TeamDeliveryRecord,
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let trigger = event.payload["trigger"].as_str().unwrap_or_default();
    let mut expected = before.clone();
    expected.status = after.status.clone();
    expected.updated_at = after.updated_at;
    expected.injection_strategy = None;
    expected.injected_turn_id = None;
    expected.last_error_code = after.last_error_code.clone();
    expected.last_error_message = after.last_error_message.clone();
    if before.id.is_empty()
        || before.message_id.is_empty()
        || before.team_id.is_empty()
        || before.recipient_agent_id.is_empty()
        || before.injected_turn_id.is_some()
        || before.injection_strategy.is_some()
        || !valid_trigger(trigger)
        || !allowed_transition(&before.status, &after.status, trigger)
        || (after.status == "failed" && after.last_error_code.as_deref().is_none_or(str::is_empty))
        || (after.status != "failed"
            && (after.last_error_code.is_some() || after.last_error_message.is_some()))
        || after.updated_at <= before.updated_at
        || *after != expected
        || event.event_id
            != format!(
                "evt_legacy_delivery_{}_{}_{}",
                before.id, after.status, after.updated_at
            )
        || event.kind != format!("team_delivery.{}", after.status)
        || event.scope != RuntimeEventScope::Team
        || event.scope_id != before.team_id
        || event.team_id.as_deref() != Some(before.team_id.as_str())
        || event.session_id.as_deref() != Some(before.recipient_agent_id.as_str())
        || event.turn_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || event.created_at != after.updated_at
        || event.payload
            != serde_json::json!({
                "delivery":after,
                "before_updated_at":before.updated_at,
                "before_status":before.status,
                "trigger":trigger,
            })
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy pre-native delivery transition lacks one exact Critical generation".into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn commit_legacy_team_delivery_pre_native_transition(
        &self,
        before: &TeamDeliveryRecord,
        after: &TeamDeliveryRecord,
        event: &NewRuntimeEvent,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        validate(before, after, event)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed beginning atomic legacy pre-native transition",
                    error,
                )
            })?;
        let message = message_by_id(&tx, &before.message_id)?.ok_or_else(|| {
            RuntimeError::Conflict("pre-native legacy delivery message missing".into())
        })?;
        let team_active: bool = tx
            .query_row(
                "SELECT deleted_at IS NULL FROM teams WHERE id=?1",
                [&before.team_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading legacy team authority", error))?
            .unwrap_or(false);
        if !team_active
            || message.team_id != before.team_id
            || !message.recipient_agent_ids.as_array().is_some_and(|ids| {
                ids.iter()
                    .any(|id| id.as_str() == Some(before.recipient_agent_id.as_str()))
            })
        {
            return Err(RuntimeError::Conflict(
                "pre-native legacy delivery lost active team/recipient authority".into(),
            ));
        }
        let canonical: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agent_deliveries WHERE id=?1)",
                [&before.id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed reading legacy canonical delivery owner", error))?;
        if canonical {
            return Err(RuntimeError::Conflict(
                "pre-native legacy transition belongs to canonical agent delivery".into(),
            ));
        }

        let mut rows = deliveries_by_message(&tx, &before.message_id)?;
        let current = rows
            .iter()
            .find(|row| row.id == before.id)
            .cloned()
            .ok_or_else(|| RuntimeError::Conflict("legacy pre-native delivery vanished".into()))?;
        if !complete_recipient_snapshot(&message, &rows) {
            return Err(RuntimeError::Conflict(
                "pre-native legacy delivery recipient snapshot incomplete".into(),
            ));
        }
        // A completed message may not silently reopen via retry.
        let completion_id = format!("evt_legacy_message_{}_completed_delivery", message.id);
        if current == *after {
            require_legacy_pre_native_lineage(&tx, after)?;
            let receipt = fetch_runtime_event_by_event_id(&tx, &event.event_id)?;
            if receipt
                .as_ref()
                .is_none_or(|row| !event_matches(row, event))
            {
                return Err(RuntimeError::Conflict(
                    "legacy delivery transition replay lacks its Critical event".into(),
                ));
            }
            if rows
                .iter()
                .all(|row| matches!(row.status.as_str(), "injected" | "failed" | "cancelled"))
            {
                let completed = completed_event(&message, &rows);
                let existing = fetch_runtime_event_by_event_id(&tx, &completion_id)?;
                if existing
                    .as_ref()
                    .is_none_or(|row| !event_matches(row, &completed))
                {
                    return Err(RuntimeError::Conflict(
                        "terminal legacy delivery replay lacks Critical completion".into(),
                    ));
                }
            }
            return Ok(current);
        }
        if current != *before {
            return Err(RuntimeError::Conflict(
                "another legacy delivery generation already advanced".into(),
            ));
        }
        require_legacy_pre_native_lineage(&tx, before)?;
        if fetch_runtime_event_by_event_id(&tx, &completion_id)?.is_some() {
            return Err(RuntimeError::Conflict(
                "completed legacy message cannot reopen a pre-native delivery".into(),
            ));
        }
        if fetch_runtime_event_by_event_id(&tx, &event.event_id)?.is_some() {
            return Err(RuntimeError::Conflict(
                "legacy pre-native event ID was consumed by another generation".into(),
            ));
        }
        let changed = tx
            .execute(
                "UPDATE team_deliveries
             SET status=?1,injection_strategy=NULL,injected_turn_id=NULL,
                 last_error_code=?2,last_error_message=?3,updated_at=?4
             WHERE id=?5 AND message_id=?6 AND status=?7 AND updated_at=?8",
                params![
                    after.status,
                    after.last_error_code,
                    after.last_error_message,
                    after.updated_at,
                    before.id,
                    before.message_id,
                    before.status,
                    before.updated_at,
                ],
            )
            .map_err(|error| db_error("failed updating legacy pre-native delivery", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "pre-native legacy delivery CAS was not the unique winning generation".into(),
            ));
        }
        let slot = rows
            .iter_mut()
            .find(|row| row.id == after.id)
            .expect("verified delivery slot");
        *slot = after.clone();
        if !complete_recipient_snapshot(&message, &rows) {
            return Err(RuntimeError::Conflict(
                "legacy pre-native completion snapshot lost a recipient".into(),
            ));
        }
        let saved = insert_runtime_event_in_transaction(&tx, event)?;
        if !event_matches(&saved, event) {
            return Err(RuntimeError::Conflict(
                "legacy pre-native Critical delivery receipt changed at commit".into(),
            ));
        }
        if rows
            .iter()
            .all(|row| matches!(row.status.as_str(), "injected" | "failed" | "cancelled"))
        {
            let completed = completed_event(&message, &rows);
            let saved = insert_runtime_event_in_transaction(&tx, &completed)?;
            if !event_matches(&saved, &completed) {
                return Err(RuntimeError::Conflict(
                    "legacy pre-native Critical completion receipt changed at commit".into(),
                ));
            }
        }
        tx.commit().map_err(|error| {
            db_error(
                "failed committing pre-native Critical delivery transition",
                error,
            )
        })?;
        Ok(after.clone())
    }
}
