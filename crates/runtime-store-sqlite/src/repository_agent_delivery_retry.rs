use runtime_core::{
    AgentRetryDeliveryMutationCommand, AgentRetryDeliveryMutationResponse, OperationPhase,
    OperationRecord, OperationTransitionRecord, RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};
use crate::repository_agent_delivery_cas::insert_agent_delivery_receipt;
use crate::repository_agent_delivery_guard::require_active_delivery_owners;
use crate::repository_workspace::operation_by_idempotency;
use crate::SqliteRuntimeRepository;

const RETRY_ADMISSION_KIND: &str = "agent_delivery_retry_admission";

impl SqliteRuntimeRepository {
    /// Admit a failed/deferred delivery once. The exact operation receipt
    /// intentionally commits the pending *queue admission*, not a guess at
    /// any later native provider outcome. Crash recovery owns that work.
    pub fn retry_agent_delivery_operator(
        &self,
        command: &AgentRetryDeliveryMutationCommand,
    ) -> Result<AgentRetryDeliveryMutationResponse, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting delivery retry admission", error))?;

        if let Some(existing) = operation_by_idempotency(
            &tx,
            command.actor.kind,
            &command.actor.identifier,
            command.idempotency_key.as_deref(),
        )? {
            if existing.kind != RETRY_ADMISSION_KIND
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
                    message: "delivery retry admission has no terminal receipt".into(),
                }
            })?;
            let receipt = serde_json::from_value(exact).map_err(|error| {
                RuntimeError::Bootstrap(format!(
                    "invalid durable delivery retry admission receipt: {error}"
                ))
            })?;
            tx.commit()
                .map_err(|error| db_error("failed finishing delivery retry replay", error))?;
            return Ok(receipt);
        }

        let mut delivery = tx
            .query_row(
                &format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE id=?1"),
                [&command.delivery_id],
                decode_delivery,
            )
            .optional()
            .map_err(|error| db_error("failed reading retry recipient", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", command.delivery_id)))?;
        if !matches!(delivery.status.as_str(), "failed" | "deferred") {
            return Err(RuntimeError::Conflict(format!(
                "delivery {} cannot be re-admitted from {}",
                delivery.id, delivery.status
            )));
        }
        let legacy_status: Option<String> = tx
            .query_row(
                "SELECT status FROM team_deliveries WHERE id=?1",
                [&delivery.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading retry legacy mirror", error))?;
        if let Some(ref status) = legacy_status {
            if !matches!(status.as_str(), "failed" | "deferred") {
                return Err(RuntimeError::Conflict(format!(
                    "legacy delivery {} is in {}, cannot safely retry",
                    delivery.id, status
                )));
            }
        }
        let admitted_at = command
            .requested_at
            .max(delivery.updated_at.saturating_add(1));

        let workspace_id = require_active_delivery_owners(&tx, &delivery)?;

        let authority = OperationAuthorityTransaction::new(&tx);
        authority.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: workspace_id.clone(),
            kind: RETRY_ADMISSION_KIND.into(),
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
            "agent_delivery_retry_admission",
            &command.delivery_id,
            command.requested_at,
        )?;

        let changed = tx
            .execute(
                "UPDATE agent_deliveries
                 SET status='pending', injection_strategy=NULL, injected_turn_id=NULL,
                     last_error_code=NULL, last_error_message=NULL, updated_at=?2
                 WHERE id=?1 AND status IN ('failed','deferred')",
                params![delivery.id, admitted_at],
            )
            .map_err(|error| db_error("failed admitting canonical delivery retry", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "delivery {} changed during retry admission",
                delivery.id
            )));
        }
        if legacy_status.is_some() {
            let mirrored = tx
                .execute(
                    "UPDATE team_deliveries
                     SET status='pending', injection_strategy=NULL, injected_turn_id=NULL,
                         last_error_code=NULL, last_error_message=NULL, updated_at=?2
                     WHERE id=?1 AND status IN ('failed','deferred')",
                    params![delivery.id, admitted_at],
                )
                .map_err(|error| db_error("failed admitting legacy delivery retry", error))?;
            if mirrored != 1 {
                return Err(RuntimeError::Conflict(format!(
                    "legacy delivery {} changed during retry admission",
                    delivery.id
                )));
            }
        }
        let previous = delivery.clone();
        delivery.status = "pending".into();
        delivery.injection_strategy = None;
        delivery.injected_turn_id = None;
        delivery.last_error_code = None;
        delivery.last_error_message = None;
        delivery.updated_at = admitted_at;
        // Admission is not native dispatch; it is nevertheless a Critical
        // delivery generation and must not survive without its journal row.
        // The operator's completed receipt and the optional legacy mirror
        // share exactly the same SQLite commit.
        insert_agent_delivery_receipt(&tx, &previous, &delivery)?;

        let result = AgentRetryDeliveryMutationResponse {
            operation_id: command.operation_id.clone(),
            delivery,
        };
        let exact = serde_json::to_value(&result).map_err(|error| {
            RuntimeError::Bootstrap(format!("failed encoding retry admission receipt: {error}"))
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
            evidence: Some(serde_json::json!({"retry_admitted":true})),
            created_at: command.requested_at,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing durable delivery retry", error))?;
        Ok(result)
    }
}
