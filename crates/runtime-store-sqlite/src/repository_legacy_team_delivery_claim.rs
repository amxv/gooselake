//! An older `/v1` delivery cannot enter native injection based on the
//! process-local recipient mutex or a blind status upsert. The precise
//! pending/deferred generation and its Critical injecting event are claimed
//! together, before any provider interruption or turn dispatch.

use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, TeamDeliveryRecord,
    TeamMessageRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_legacy_team_delivery_transition::require_legacy_pre_native_lineage;
use crate::repository_legacy_team_messages::{deliveries_by_message, message_by_id};
use crate::SqliteRuntimeRepository;

fn validate(
    previous: &TeamDeliveryRecord,
    injecting: &TeamDeliveryRecord,
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let mut expected = previous.clone();
    expected.status = "injecting".into();
    expected.updated_at = injecting.updated_at;
    let trigger = event.payload["trigger"].as_str().unwrap_or_default();
    if previous.id.is_empty()
        || previous.message_id.is_empty()
        || previous.team_id.is_empty()
        || previous.recipient_agent_id.is_empty()
        || !matches!(previous.status.as_str(), "pending" | "deferred")
        || injecting.updated_at <= previous.updated_at
        || *injecting != expected
        || !matches!(
            trigger,
            "queue" | "retry" | "turncompletedboundary" | "startuprecovery"
        )
        || event.event_id.is_empty()
        || event.event_id
            != format!(
                "evt_legacy_delivery_{}_injecting_{}",
                previous.id, injecting.updated_at
            )
        || event.kind != "team_delivery.injecting"
        || event.scope != RuntimeEventScope::Team
        || event.scope_id != previous.team_id
        || event.team_id.as_deref() != Some(previous.team_id.as_str())
        || event.session_id.as_deref() != Some(previous.recipient_agent_id.as_str())
        || event.turn_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || event.created_at != injecting.updated_at
        || event.payload != serde_json::json!({"delivery":injecting,"trigger":trigger})
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy native injection claim does not certify one exact pending delivery".into(),
        ));
    }
    Ok(())
}

/// Keep the ownership/freeze checks under the same IMMEDIATE transaction as
/// the native claim. An archived member, closed session or retirement fence
/// cannot race in between a standalone preflight and a successful claim.
fn require_active_legacy_owners(
    tx: &Transaction<'_>,
    message: &TeamMessageRecord,
    recipient_id: &str,
) -> Result<(), RuntimeError> {
    for (role, session_id) in [
        ("sender", message.sender_agent_id.as_str()),
        ("recipient", recipient_id),
    ] {
        let active: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions
             WHERE id=?1 AND status NOT IN ('closed','failed') AND closed_at IS NULL)",
                [session_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error("failed inspecting legacy delivery session authority", error)
            })?;
        let is_member: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM team_members WHERE team_id=?1 AND agent_id=?2)",
                params![message.team_id, session_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed inspecting legacy team delivery recipient membership",
                    error,
                )
            })?;
        if !active || !is_member {
            return Err(RuntimeError::Conflict(format!(
                "legacy delivery {role} {session_id} is closed or no longer an active member",
            )));
        }
        let owner: Option<(String, String)> = tx
            .query_row(
                "SELECT workspace_id,lifecycle_state FROM workspace_agents WHERE session_id=?1",
                [session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| {
                db_error("failed inspecting legacy recipient workspace owner", error)
            })?;
        if let Some((workspace_id, lifecycle)) = owner {
            let workspace: Option<(String, bool)> = tx
                .query_row(
                    "SELECT lifecycle_state,EXISTS(
                  SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)
                 FROM workspaces WHERE workspace_id=?1",
                    [&workspace_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| {
                    db_error("failed inspecting legacy native retirement fence", error)
                })?;
            if lifecycle != "active"
                || !workspace.is_some_and(|(state, frozen)| state == "active" && !frozen)
            {
                return Err(RuntimeError::Conflict(format!(
                    "legacy delivery {role} owner {workspace_id} is archived, retired or frozen",
                )));
            }
        }
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// `true` alone authorizes the caller to enter its first native effect.
    /// A stale writer gets `false`, and even a lost commit reply must not be
    /// converted into a new claim or a second provider dispatch.
    pub fn claim_legacy_team_delivery_injection(
        &self,
        previous: &TeamDeliveryRecord,
        injecting: &TeamDeliveryRecord,
        event: &NewRuntimeEvent,
    ) -> Result<bool, RuntimeError> {
        validate(previous, injecting, event)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting legacy native delivery claim", error))?;
        let message = message_by_id(&tx, &previous.message_id)?.ok_or_else(|| {
            RuntimeError::Conflict("legacy delivery claim has no durable message".into())
        })?;
        let team_active: bool = tx
            .query_row(
                "SELECT deleted_at IS NULL FROM teams WHERE id=?1",
                [&previous.team_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking legacy delivery team", error))?
            .unwrap_or(false);
        if !team_active
            || message.team_id != previous.team_id
            || !message
                .recipient_agent_ids
                .as_array()
                .is_some_and(|recipients| {
                    recipients
                        .iter()
                        .any(|id| id.as_str() == Some(previous.recipient_agent_id.as_str()))
                })
        {
            return Err(RuntimeError::Conflict(
                "legacy delivery native claim has no active team/recipient authority".into(),
            ));
        }
        require_active_legacy_owners(&tx, &message, &previous.recipient_agent_id)?;
        let pending_id = format!("evt_legacy_delivery_{}_pending", previous.id);
        let pending = fetch_runtime_event_by_event_id(&tx, &pending_id)?.ok_or_else(|| {
            RuntimeError::Conflict(
                "pre-journal legacy delivery cannot gain a new native injection claim".into(),
            )
        })?;
        let initial: TeamDeliveryRecord =
            serde_json::from_value(pending.payload["delivery"].clone()).map_err(|_| {
                RuntimeError::Conflict(
                    "legacy delivery admission Critical receipt is malformed".into(),
                )
            })?;
        if pending.kind != "team_delivery.pending"
            || pending.criticality != RuntimeEventCriticality::Critical
            || pending.scope != RuntimeEventScope::Team
            || pending.scope_id != previous.team_id
            || pending.team_id.as_deref() != Some(previous.team_id.as_str())
            || pending.session_id.as_deref() != Some(previous.recipient_agent_id.as_str())
            || pending.turn_id.is_some()
            || pending.provider.is_some()
            || pending.provider_seq.is_some()
            || pending.payload != serde_json::json!({"delivery":initial})
            || initial.status != "pending"
            || initial.updated_at != initial.created_at
            || pending.created_at != initial.created_at
            || initial.id != previous.id
            || initial.message_id != previous.message_id
            || initial.team_id != previous.team_id
            || initial.recipient_agent_id != previous.recipient_agent_id
            || initial.provider != previous.provider
            || initial.effective_policy != previous.effective_policy
            || initial.created_at != previous.created_at
        {
            return Err(RuntimeError::Conflict(
                "legacy delivery native claim admission receipt does not match identity".into(),
            ));
        }
        // The immutable admission also pins every later pre-native status
        // generation to an exact Critical transition. A direct-upserted old
        // deferred row is not an automatic retry token.
        require_legacy_pre_native_lineage(&tx, previous)?;
        let canonical: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agent_deliveries WHERE id=?1)",
                [&previous.id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking canonical delivery ownership", error))?;
        if canonical {
            return Err(RuntimeError::Conflict(
                "canonical agent delivery owns this native claim".into(),
            ));
        }
        if fetch_runtime_event_by_event_id(&tx, &event.event_id)?.is_some()
            || fetch_runtime_event_by_event_id(
                &tx,
                &format!("evt_legacy_delivery_{}_cancelled", previous.id),
            )?
            .is_some()
        {
            return Err(RuntimeError::Conflict(
                "legacy native claim event identifier or cancellation receipt consumed".into(),
            ));
        }
        let stored = deliveries_by_message(&tx, &previous.message_id)?
            .into_iter()
            .find(|row| row.id == previous.id)
            .ok_or_else(|| {
                RuntimeError::Conflict("legacy native claim delivery was deleted".into())
            })?;
        if stored != *previous {
            return Ok(false);
        }
        let changed = tx
            .execute(
                "UPDATE team_deliveries SET status='injecting', updated_at=?1
             WHERE id=?2 AND message_id=?3 AND status=?4 AND updated_at=?5",
                params![
                    injecting.updated_at,
                    previous.id,
                    previous.message_id,
                    previous.status,
                    previous.updated_at
                ],
            )
            .map_err(|error| db_error("failed claiming legacy delivery injection", error))?;
        if changed != 1 {
            return Ok(false);
        }
        let saved_event = insert_runtime_event_in_transaction(&tx, event)?;
        if saved_event.event_id != event.event_id || saved_event.kind != event.kind {
            return Err(RuntimeError::Conflict(
                "legacy injection critical event changed during commit".into(),
            ));
        }
        tx.commit().map_err(|error| {
            db_error(
                "failed committing legacy native dispatch claim and event",
                error,
            )
        })?;
        Ok(true)
    }
}
