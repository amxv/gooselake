use runtime_core::{
    AgentDeliveryRecord, NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};
use crate::repository_agent_delivery_guard::require_active_delivery_owners;
use crate::repository_legacy_team_messages::event_matches;
use crate::SqliteRuntimeRepository;

fn valid_transition(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (
            "pending",
            "pending" | "deferred" | "failed" | "injecting" | "cancelled"
        ) | (
            "deferred",
            "pending" | "deferred" | "failed" | "injecting" | "cancelled"
        ) | ("failed", "pending")
    )
}

/// The exact previous status generation determines the Critical event ID.
/// Native acknowledgement uses this same receipt shape, but only after
/// correlated turn admission has been proved in the SQLite transaction.
pub(crate) fn agent_delivery_transition_event(
    tx: &rusqlite::Transaction<'_>,
    before: &AgentDeliveryRecord,
    after: &AgentDeliveryRecord,
) -> Result<NewRuntimeEvent, RuntimeError> {
    let workspace: Option<String> = tx
        .query_row(
            "SELECT COALESCE(message.workspace_id, sender.workspace_id)
             FROM agent_messages message
             LEFT JOIN workspace_agents sender ON sender.session_id=message.sender_agent_id
             WHERE message.id=?1",
            [&before.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed resolving delivery event workspace", error))?
        .flatten();
    let (scope, scope_id) = match workspace {
        Some(workspace_id) => (RuntimeEventScope::Workspace, workspace_id),
        None => (RuntimeEventScope::Message, before.message_id.clone()),
    };
    Ok(NewRuntimeEvent {
        event_id: format!(
            "evt_agent_delivery_{}_{}_{}",
            before.id, after.status, before.updated_at
        ),
        scope,
        scope_id,
        session_id: Some(before.recipient_agent_id.clone()),
        team_id: None,
        turn_id: after.injected_turn_id.clone(),
        kind: format!("agent_delivery.{}", after.status),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "delivery": after,
            "before_updated_at": before.updated_at,
        }),
        provider: None,
        provider_seq: None,
        created_at: after.updated_at,
    })
}

pub(crate) fn insert_agent_delivery_receipt(
    tx: &rusqlite::Transaction<'_>,
    before: &AgentDeliveryRecord,
    after: &AgentDeliveryRecord,
) -> Result<(), RuntimeError> {
    let event = agent_delivery_transition_event(tx, before, after)?;
    let receipt = insert_runtime_event_in_transaction(tx, &event)?;
    if !event_matches(&receipt, &event) {
        return Err(RuntimeError::Conflict(format!(
            "agent delivery {} Critical event identity collision",
            before.id
        )));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// A durable, Critical-journaled status claim; two separate RuntimeTeamCommsService
    /// instances sharing the DB cannot both enter native delivery dispatch
    /// from the same pending/deferred row. The legacy mirror is updated in
    /// the same transaction, so a failed second write cannot leave a claim.
    pub fn compare_and_swap_agent_delivery_status(
        &self,
        expected: &AgentDeliveryRecord,
        updated: &AgentDeliveryRecord,
    ) -> Result<bool, RuntimeError> {
        if updated.id != expected.id
            || updated.message_id != expected.message_id
            || updated.recipient_agent_id != expected.recipient_agent_id
            || updated.provider != expected.provider
            || updated.created_at != expected.created_at
        {
            return Err(RuntimeError::ProtocolViolation(
                "delivery status CAS attempted to change immutable recipient identity".into(),
            ));
        }
        if !valid_transition(&expected.status, &updated.status)
            || updated.updated_at <= expected.updated_at
            || expected.injected_turn_id.is_some()
            || updated.injected_turn_id.is_some()
        {
            return Err(RuntimeError::InvalidState(format!(
                "invalid non-monotonic delivery transition {} -> {}",
                expected.status, updated.status
            )));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting delivery status CAS", error))?;
        let observed = tx
            .query_row(
                &format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE id=?1"),
                [&expected.id],
                decode_delivery,
            )
            .optional()
            .map_err(|error| db_error("failed reading delivery status CAS target", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", expected.id)))?;
        if observed.status != expected.status
            || observed.updated_at != expected.updated_at
            || observed.message_id != expected.message_id
            || observed.recipient_agent_id != expected.recipient_agent_id
        {
            tx.commit()
                .map_err(|error| db_error("failed finishing lost delivery CAS", error))?;
            return Ok(false);
        }

        if updated.status == "injecting" {
            // Claim and retirement check must use the same database lock;
            // an independent process cannot freeze retirement between them.
            require_active_delivery_owners(&tx, &observed)?;
        }
        let legacy: Option<(String, i64)> = tx
            .query_row(
                "SELECT status,updated_at FROM team_deliveries WHERE id=?1",
                [&expected.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed inspecting legacy delivery CAS mirror", error))?;
        if let Some((ref status, updated_at)) = legacy {
            if status != &expected.status || updated.updated_at <= updated_at {
                return Err(RuntimeError::Conflict(format!(
                    "legacy delivery {} is not at the same transition authority",
                    expected.id
                )));
            }
        }

        let changed = tx
            .execute(
                "UPDATE agent_deliveries
                 SET status=?2,effective_policy=?3,injection_strategy=?4,
                     injected_turn_id=?5,last_error_code=?6,last_error_message=?7,
                     updated_at=?8
                 WHERE id=?1 AND status=?9 AND updated_at=?10",
                params![
                    expected.id,
                    updated.status,
                    updated.effective_policy,
                    updated.injection_strategy,
                    updated.injected_turn_id,
                    updated.last_error_code,
                    updated.last_error_message,
                    updated.updated_at,
                    expected.status,
                    expected.updated_at,
                ],
            )
            .map_err(|error| db_error("failed claiming authoritative delivery status", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "delivery {} status CAS was not unique",
                expected.id
            )));
        }
        if legacy.is_some() {
            let mirrored = tx
                .execute(
                    "UPDATE team_deliveries
                     SET status=?2,effective_policy=?3,injection_strategy=?4,
                         injected_turn_id=?5,last_error_code=?6,last_error_message=?7,
                         updated_at=?8
                     WHERE id=?1 AND status=?9",
                    params![
                        expected.id,
                        updated.status,
                        updated.effective_policy,
                        updated.injection_strategy,
                        updated.injected_turn_id,
                        updated.last_error_code,
                        updated.last_error_message,
                        updated.updated_at,
                        expected.status,
                    ],
                )
                .map_err(|error| {
                    db_error("failed committing legacy delivery status mirror", error)
                })?;
            if mirrored != 1 {
                return Err(RuntimeError::Conflict(format!(
                    "legacy delivery {} lost transition authority",
                    expected.id
                )));
            }
        }
        insert_agent_delivery_receipt(&tx, expected, updated)?;
        tx.commit()
            .map_err(|error| db_error("failed committing delivery status claim", error))?;
        Ok(true)
    }
}
