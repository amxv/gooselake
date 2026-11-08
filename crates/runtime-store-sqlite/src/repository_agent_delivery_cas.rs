use runtime_core::{AgentDeliveryRecord, RuntimeError};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};
use crate::repository_agent_delivery_guard::require_active_delivery_owners;
use crate::SqliteRuntimeRepository;

fn valid_transition(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (
            "pending",
            "pending" | "deferred" | "injecting" | "cancelled"
        ) | (
            "deferred",
            "pending" | "deferred" | "injecting" | "cancelled"
        ) | ("injecting", "injected" | "failed" | "deferred")
    )
}

impl SqliteRuntimeRepository {
    /// A durable, atomic status claim; two separate RuntimeTeamCommsService
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
        tx.commit()
            .map_err(|error| db_error("failed committing delivery status claim", error))?;
        Ok(true)
    }
}
