use runtime_core::{
    AgentRetryDeliveryMutationCommand, AgentRetryDeliveryMutationResponse, OperationPhase,
    OperationRecord, OperationTransitionRecord, RuntimeError,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};
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

        let workspace_id: Option<Option<String>> = tx
            .query_row(
                "SELECT COALESCE(message.workspace_id,sender.workspace_id)
                 FROM agent_messages message
                 LEFT JOIN workspace_agents sender ON sender.session_id=message.sender_agent_id
                 WHERE message.id=?1",
                [&delivery.message_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed locating retry origin workspace", error))?;
        let workspace_id = workspace_id.ok_or_else(|| {
            RuntimeError::NotFound(format!("retry message {}", delivery.message_id))
        })?;

        // A retirement may be frozen while its public lifecycle still reads
        // active. Retry admission is a new provider-effect intent, so refuse
        // it inside this same serialized SQLite transaction for both sender
        // and recipient workspaces, not just after the provider call.
        let mut owner_workspaces = Vec::new();
        if let Some(ref workspace_id) = workspace_id {
            owner_workspaces.push(workspace_id.clone());
        }
        let sender_id: String = tx
            .query_row(
                "SELECT sender_agent_id FROM agent_messages WHERE id=?1",
                [&delivery.message_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed inspecting retry sender", error))?;
        for (role, session_id) in [
            ("sender", &sender_id),
            ("recipient", &delivery.recipient_agent_id),
        ] {
            let ownership: Option<(String, String)> = tx
                .query_row(
                    "SELECT workspace_id,lifecycle_state FROM workspace_agents WHERE session_id=?1",
                    [session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed inspecting retry recipient ownership", error))?;
            if let Some((owner, lifecycle)) = ownership {
                if lifecycle != "active" {
                    return Err(RuntimeError::Conflict(format!(
                        "{role} {session_id} is archived and cannot accept a retry",
                    )));
                }
                owner_workspaces.push(owner);
            }
        }
        for owner in owner_workspaces {
            let state: Option<(String, bool)> = tx
                .query_row(
                    "SELECT lifecycle_state,EXISTS(
                         SELECT 1 FROM workspace_retirement_fences
                         WHERE workspace_id=?1)
                     FROM workspaces WHERE workspace_id=?1",
                    [&owner],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed inspecting retry workspace retirement", error))?;
            let active = state.is_some_and(|(lifecycle, fenced)| lifecycle == "active" && !fenced);
            if !active {
                return Err(RuntimeError::Conflict(format!(
                    "workspace {owner} is retired or frozen; delivery retry refused"
                )));
            }
        }

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
                params![delivery.id, command.requested_at],
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
                    params![delivery.id, command.requested_at],
                )
                .map_err(|error| db_error("failed admitting legacy delivery retry", error))?;
            if mirrored != 1 {
                return Err(RuntimeError::Conflict(format!(
                    "legacy delivery {} changed during retry admission",
                    delivery.id
                )));
            }
        }
        delivery.status = "pending".into();
        delivery.injection_strategy = None;
        delivery.injected_turn_id = None;
        delivery.last_error_code = None;
        delivery.last_error_message = None;
        delivery.updated_at = command.requested_at;

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
