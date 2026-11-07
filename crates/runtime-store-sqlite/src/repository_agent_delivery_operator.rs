use runtime_core::{
    AgentCancelMessageMutationCommand, AgentCancelMessageMutationResponse, OperationPhase,
    OperationRecord, OperationTransitionRecord, RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};
use crate::repository_workspace::operation_by_idempotency;
use crate::SqliteRuntimeRepository;

const CANCELLATION_KIND: &str = "agent_message_cancel";

impl SqliteRuntimeRepository {
    /// All delivery rows, legacy mirrors, operation history, and the immutable
    /// receipt are committed together. A lost HTTP response never requires
    /// another cancellation attempt or a mutable delivery-state lookup.
    pub fn cancel_agent_message_operator(
        &self,
        command: &AgentCancelMessageMutationCommand,
    ) -> Result<AgentCancelMessageMutationResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting message cancellation operation", error))?;

        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            command.idempotency_key.as_deref(),
        )? {
            if existing.kind != CANCELLATION_KIND
                || existing.normalized_request_hash != command.normalized_request_hash
            {
                return Err(RuntimeError::Conflict(format!(
                    "Idempotency-Key already belongs to operation {} with different input",
                    existing.operation_id
                )));
            }
            let exact = existing.exact_terminal_result.ok_or_else(|| {
                RuntimeError::OperationUnresolved {
                    operation_id: existing.operation_id,
                    message: "message cancellation has no terminal receipt".into(),
                }
            })?;
            let result = serde_json::from_value(exact).map_err(|error| {
                RuntimeError::Bootstrap(format!(
                    "invalid durable message cancellation receipt: {error}"
                ))
            })?;
            tx.commit()
                .map_err(|error| db_error("failed finishing cancellation replay", error))?;
            return Ok(result);
        }

        let workspace_id: Option<Option<String>> = tx
            .query_row(
                "SELECT COALESCE(message.workspace_id, sender.workspace_id)
                 FROM agent_messages message
                 LEFT JOIN workspace_agents sender ON sender.session_id=message.sender_agent_id
                 WHERE message.id=?1",
                [&command.message_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed finding message for cancellation", error))?;
        let workspace_id = workspace_id
            .ok_or_else(|| RuntimeError::NotFound(format!("message {}", command.message_id)))?;
        let mut deliveries = {
            let mut statement = tx
                .prepare(&format!(
                    "SELECT {DELIVERY_COLUMNS} FROM agent_deliveries \
                     WHERE message_id=?1 ORDER BY created_at,id"
                ))
                .map_err(|error| db_error("failed preparing message cancellation fanout", error))?;
            let rows = statement
                .query_map([&command.message_id], decode_delivery)
                .map_err(|error| db_error("failed reading message cancellation fanout", error))?;
            collect_rows(rows)?
        };
        // Never undo a provider dispatch, nor partially cancel a broadcast:
        // every delivery must still be positively known to be undispatched.
        for delivery in &deliveries {
            if !matches!(delivery.status.as_str(), "pending" | "deferred") {
                return Err(RuntimeError::Conflict(format!(
                    "message {} cannot be cancelled because delivery {} is in {}",
                    command.message_id, delivery.id, delivery.status
                )));
            }
            // A migrated legacy row may have advanced independently. Never
            // misclassify already-injected provider work as cancellable just
            // because the canonical projection is momentarily behind.
            let legacy_status: Option<String> = tx
                .query_row(
                    "SELECT status FROM team_deliveries WHERE id=?1",
                    [&delivery.id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| db_error("failed checking legacy delivery state", error))?;
            if let Some(status) = legacy_status {
                if !matches!(status.as_str(), "pending" | "deferred") {
                    return Err(RuntimeError::Conflict(format!(
                        "legacy delivery {} in {} is not safely cancellable",
                        delivery.id, status
                    )));
                }
            }
        }

        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: workspace_id.clone(),
            kind: CANCELLATION_KIND.into(),
            actor: command.actor.clone(),
            idempotency_key: command.idempotency_key.clone(),
            normalized_request_hash: command.normalized_request_hash.clone(),
            normalized_request: command.normalized_request.clone(),
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: command.requested_at,
            updated_at: command.requested_at,
        })?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: None,
            created_at: command.requested_at,
        })?;
        let claim = authority.acquire_claim(
            &command.operation_id,
            "agent_message_cancellation",
            &command.message_id,
            command.requested_at,
        )?;

        for delivery in &mut deliveries {
            let updated = tx
                .execute(
                    "UPDATE agent_deliveries
                     SET status='cancelled', updated_at=?2,
                         last_error_code=NULL, last_error_message=NULL
                     WHERE id=?1 AND status IN ('pending','deferred')",
                    params![delivery.id, command.requested_at],
                )
                .map_err(|error| db_error("failed atomically cancelling delivery", error))?;
            if updated != 1 {
                return Err(RuntimeError::Conflict(format!(
                    "delivery {} changed during cancellation",
                    delivery.id
                )));
            }
            tx.execute(
                "UPDATE team_deliveries
                 SET status='cancelled', updated_at=?2,
                     last_error_code=NULL, last_error_message=NULL
                 WHERE id=?1 AND status IN ('pending','deferred')",
                params![delivery.id, command.requested_at],
            )
            .map_err(|error| db_error("failed mirroring atomic delivery cancellation", error))?;
            delivery.status = "cancelled".into();
            delivery.last_error_code = None;
            delivery.last_error_message = None;
            delivery.updated_at = command.requested_at;
        }

        let result = AgentCancelMessageMutationResponse {
            operation_id: command.operation_id.clone(),
            deliveries,
        };
        let exact = serde_json::to_value(&result).map_err(|error| {
            RuntimeError::Bootstrap(format!("failed encoding cancellation receipt: {error}"))
        })?;
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            workspace_id.as_deref(),
            &exact,
            None,
            None,
            command.requested_at,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({"delivery_count":result.deliveries.len()})),
            created_at: command.requested_at,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing durable message cancellation", error))?;
        Ok(result)
    }
}
