use runtime_core::{
    OperationEffectRecord, OperationOutboxReceiptRecord, OperationOutboxRecord, OperationPhase,
    OperationRecord, OperationResourceClaimRecord, OperationTransitionRecord, RuntimeError,
};
use rusqlite::{params, OptionalExtension, Transaction};

use crate::db::{db_error, json_to_string};

pub(crate) struct OperationAuthorityTransaction<'a, 'connection> {
    transaction: &'a Transaction<'connection>,
}

impl<'a, 'connection> OperationAuthorityTransaction<'a, 'connection> {
    pub(crate) const fn new(transaction: &'a Transaction<'connection>) -> Self {
        Self { transaction }
    }

    pub(crate) fn insert_operation(&self, operation: &OperationRecord) -> Result<(), RuntimeError> {
        self.transaction
            .execute(
                "INSERT INTO runtime_operations (
                    operation_id, workspace_id, kind, actor_kind, actor_id, idempotency_key,
                    normalized_request_hash, normalized_request_json, phase,
                    exact_terminal_result_json, error_code, error_message, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    operation.operation_id,
                    operation.workspace_id,
                    operation.kind,
                    operation.actor.kind.as_str(),
                    operation.actor.identifier,
                    operation.idempotency_key,
                    operation.normalized_request_hash,
                    json_to_string(&operation.normalized_request)?,
                    operation.phase.as_str(),
                    operation
                        .exact_terminal_result
                        .as_ref()
                        .map(json_to_string)
                        .transpose()?,
                    operation.error_code,
                    operation.error_message,
                    operation.created_at,
                    operation.updated_at,
                ],
            )
            .map_err(|error| db_error("failed inserting durable operation", error))?;
        Ok(())
    }

    pub(crate) fn acquire_claim(
        &self,
        operation_id: &str,
        resource_kind: &str,
        resource_id: &str,
        acquired_at: i64,
    ) -> Result<OperationResourceClaimRecord, RuntimeError> {
        let previous = self
            .transaction
            .query_row(
                "SELECT last_generation FROM runtime_operation_resource_fences
                 WHERE resource_kind = ?1 AND resource_id = ?2",
                params![resource_kind, resource_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading operation resource fence", error))?
            .unwrap_or(0);
        let next = previous.checked_add(1).ok_or_else(|| {
            RuntimeError::Bootstrap("operation resource fence generation overflow".to_string())
        })?;
        self.transaction
            .execute(
                "INSERT INTO runtime_operation_resource_fences (resource_kind, resource_id, last_generation)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(resource_kind, resource_id) DO UPDATE SET last_generation = excluded.last_generation",
                params![resource_kind, resource_id, next],
            )
            .map_err(|error| db_error("failed issuing operation resource fence", error))?;
        self.transaction
            .execute(
                "INSERT INTO runtime_operation_resource_claims (
                    resource_kind, resource_id, claim_mode, owner_operation_id, fence_generation, acquired_at
                 ) VALUES (?1, ?2, 'exclusive', ?3, ?4, ?5)",
                params![resource_kind, resource_id, operation_id, next, acquired_at],
            )
            .map_err(|error| db_error("failed acquiring operation resource claim", error))?;
        Ok(OperationResourceClaimRecord {
            resource_kind: resource_kind.to_string(),
            resource_id: resource_id.to_string(),
            claim_mode: "exclusive".to_string(),
            owner_operation_id: operation_id.to_string(),
            fence_generation: u64::try_from(next).map_err(|_| {
                RuntimeError::Bootstrap("negative operation resource fence".to_string())
            })?,
            acquired_at,
        })
    }

    pub(crate) fn release_claim(
        &self,
        claim: &OperationResourceClaimRecord,
    ) -> Result<(), RuntimeError> {
        let generation = i64::try_from(claim.fence_generation).map_err(|_| {
            RuntimeError::Bootstrap("operation resource fence generation overflow".to_string())
        })?;
        let deleted = self
            .transaction
            .execute(
                "DELETE FROM runtime_operation_resource_claims
                 WHERE resource_kind = ?1 AND resource_id = ?2 AND claim_mode = ?3
                   AND owner_operation_id = ?4 AND fence_generation = ?5",
                params![
                    claim.resource_kind,
                    claim.resource_id,
                    claim.claim_mode,
                    claim.owner_operation_id,
                    generation,
                ],
            )
            .map_err(|error| db_error("failed releasing exact operation resource claim", error))?;
        if deleted != 1 {
            return Err(RuntimeError::Conflict(format!(
                "operation {} no longer owns claim {}:{} at fence {}",
                claim.owner_operation_id,
                claim.resource_kind,
                claim.resource_id,
                claim.fence_generation
            )));
        }
        Ok(())
    }

    pub(crate) fn append_transition(
        &self,
        transition: &OperationTransitionRecord,
    ) -> Result<(), RuntimeError> {
        let sequence = i64::try_from(transition.sequence)
            .map_err(|_| RuntimeError::Bootstrap("operation sequence overflow".to_string()))?;
        self.transaction
            .execute(
                "INSERT INTO runtime_operation_transitions (
                    operation_id, sequence, from_phase, to_phase, evidence_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    transition.operation_id,
                    sequence,
                    transition.from_phase.map(OperationPhase::as_str),
                    transition.to_phase.as_str(),
                    transition
                        .evidence
                        .as_ref()
                        .map(json_to_string)
                        .transpose()?,
                    transition.created_at,
                ],
            )
            .map_err(|error| db_error("failed appending operation transition", error))?;
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn insert_effect(&self, effect: &OperationEffectRecord) -> Result<(), RuntimeError> {
        let attempt_count = i64::try_from(effect.attempt_count)
            .map_err(|_| RuntimeError::Bootstrap("effect attempt count overflow".to_string()))?;
        self.transaction
            .execute(
                "INSERT INTO runtime_operation_effects (
                    effect_id, operation_id, effect_kind, target_kind, target_id, phase,
                    idempotency_key, evidence_json, attempt_count, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    effect.effect_id,
                    effect.operation_id,
                    effect.effect_kind,
                    effect.target_kind,
                    effect.target_id,
                    effect.phase,
                    effect.idempotency_key,
                    effect.evidence.as_ref().map(json_to_string).transpose()?,
                    attempt_count,
                    effect.created_at,
                    effect.updated_at,
                ],
            )
            .map_err(|error| db_error("failed inserting operation effect evidence", error))?;
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn insert_outbox(&self, outbox: &OperationOutboxRecord) -> Result<(), RuntimeError> {
        let attempt_count = i64::try_from(outbox.attempt_count)
            .map_err(|_| RuntimeError::Bootstrap("outbox attempt count overflow".to_string()))?;
        self.transaction
            .execute(
                "INSERT INTO runtime_operation_outbox (
                    outbox_id, operation_id, delivery_kind, idempotency_key, payload_json,
                    state, attempt_count, next_attempt_at, last_error_json,
                    created_at, updated_at, delivered_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    outbox.outbox_id,
                    outbox.operation_id,
                    outbox.delivery_kind,
                    outbox.idempotency_key,
                    json_to_string(&outbox.payload)?,
                    outbox.state,
                    attempt_count,
                    outbox.next_attempt_at,
                    outbox.last_error.as_ref().map(json_to_string).transpose()?,
                    outbox.created_at,
                    outbox.updated_at,
                    outbox.delivered_at,
                ],
            )
            .map_err(|error| db_error("failed inserting operation outbox row", error))?;
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn record_outbox_receipt(
        &self,
        receipt: &OperationOutboxReceiptRecord,
    ) -> Result<(), RuntimeError> {
        self.transaction
            .execute(
                "INSERT INTO runtime_operation_outbox_receipts (
                    outbox_id, idempotency_key, received_at, receipt_json
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![
                    receipt.outbox_id,
                    receipt.idempotency_key,
                    receipt.received_at,
                    receipt.receipt.as_ref().map(json_to_string).transpose()?,
                ],
            )
            .map_err(|error| db_error("failed recording operation outbox receipt", error))?;
        Ok(())
    }

    pub(crate) fn terminalize(
        &self,
        operation_id: &str,
        expected_phase: OperationPhase,
        terminal_phase: OperationPhase,
        workspace_id: Option<&str>,
        exact_terminal_result: &serde_json::Value,
        error_code: Option<&str>,
        error_message: Option<&str>,
        updated_at: i64,
    ) -> Result<(), RuntimeError> {
        if !matches!(
            terminal_phase,
            OperationPhase::Completed | OperationPhase::Failed
        ) {
            return Err(RuntimeError::InvalidState(format!(
                "operation terminalization requires terminal phase, got {}",
                terminal_phase.as_str()
            )));
        }
        let updated = self
            .transaction
            .execute(
                "UPDATE runtime_operations
                 SET workspace_id = COALESCE(?4, workspace_id), phase = ?3,
                     exact_terminal_result_json = ?5, error_code = ?6, error_message = ?7,
                     updated_at = ?8
                 WHERE operation_id = ?1 AND phase = ?2",
                params![
                    operation_id,
                    expected_phase.as_str(),
                    terminal_phase.as_str(),
                    workspace_id,
                    json_to_string(exact_terminal_result)?,
                    error_code,
                    error_message,
                    updated_at,
                ],
            )
            .map_err(|error| db_error("failed terminalizing durable operation", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(format!(
                "operation {operation_id} is not in expected phase {}",
                expected_phase.as_str()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{apply_schema, open_connection},
        SqliteRuntimeRepository,
    };
    use runtime_core::{OperationActor, OperationActorKind};
    use rusqlite::TransactionBehavior;

    #[test]
    fn related_operation_authority_rows_commit_atomically() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let database_path = temp_dir.path().join("runtime.sqlite3");
        let mut connection = open_connection(&database_path).expect("connection");
        apply_schema(&mut connection).expect("schema");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("transaction");
        let authority = OperationAuthorityTransaction::new(&transaction);
        let operation_id = "op_transaction_test";
        authority
            .insert_operation(&OperationRecord {
                operation_id: operation_id.to_string(),
                workspace_id: None,
                kind: "test_operation".to_string(),
                actor: OperationActor {
                    kind: OperationActorKind::System,
                    identifier: "test_system".to_string(),
                },
                idempotency_key: Some("test-key".to_string()),
                normalized_request_hash: "hash".to_string(),
                normalized_request: serde_json::json!({"test":true}),
                phase: OperationPhase::Requested,
                exact_terminal_result: None,
                error_code: None,
                error_message: None,
                created_at: 10,
                updated_at: 10,
            })
            .expect("operation");
        authority
            .append_transition(&OperationTransitionRecord {
                operation_id: operation_id.to_string(),
                sequence: 1,
                from_phase: None,
                to_phase: OperationPhase::Requested,
                evidence: None,
                created_at: 10,
            })
            .expect("requested transition");
        let claim = authority
            .acquire_claim(operation_id, "test_resource", "resource_1", 10)
            .expect("claim");
        authority
            .insert_effect(&OperationEffectRecord {
                effect_id: "effect_1".to_string(),
                operation_id: operation_id.to_string(),
                effect_kind: "test_effect".to_string(),
                target_kind: "test_resource".to_string(),
                target_id: "resource_1".to_string(),
                phase: "finalized".to_string(),
                idempotency_key: "effect-key".to_string(),
                evidence: Some(serde_json::json!({"observed":true})),
                attempt_count: 1,
                created_at: 11,
                updated_at: 11,
            })
            .expect("effect");
        authority
            .insert_outbox(&OperationOutboxRecord {
                outbox_id: "outbox_1".to_string(),
                operation_id: operation_id.to_string(),
                delivery_kind: "test_delivery".to_string(),
                idempotency_key: "delivery-key".to_string(),
                payload: serde_json::json!({"payload":true}),
                state: "delivered".to_string(),
                attempt_count: 1,
                next_attempt_at: None,
                last_error: None,
                created_at: 11,
                updated_at: 12,
                delivered_at: Some(12),
            })
            .expect("outbox");
        authority
            .record_outbox_receipt(&OperationOutboxReceiptRecord {
                outbox_id: "outbox_1".to_string(),
                idempotency_key: "delivery-key".to_string(),
                received_at: 12,
                receipt: Some(serde_json::json!({"received":true})),
            })
            .expect("receipt");
        let exact_result = serde_json::json!({"ok":false,"code":"test_failure"});
        authority
            .terminalize(
                operation_id,
                OperationPhase::Requested,
                OperationPhase::Failed,
                None,
                &exact_result,
                Some("test_failure"),
                Some("intentional test failure"),
                12,
            )
            .expect("terminalize");
        authority
            .append_transition(&OperationTransitionRecord {
                operation_id: operation_id.to_string(),
                sequence: 2,
                from_phase: Some(OperationPhase::Requested),
                to_phase: OperationPhase::Failed,
                evidence: Some(serde_json::json!({"code":"test_failure"})),
                created_at: 12,
            })
            .expect("terminal transition");
        authority.release_claim(&claim).expect("release claim");
        transaction.commit().expect("commit");
        drop(connection);

        let repository = SqliteRuntimeRepository::new(database_path);
        let details = repository
            .get_operation(operation_id)
            .expect("query operation")
            .expect("operation exists");
        assert_eq!(details.operation.phase, OperationPhase::Failed);
        assert_eq!(details.operation.exact_terminal_result, Some(exact_result));
        assert_eq!(details.transitions.len(), 2);
        assert_eq!(details.effects.len(), 1);
        assert_eq!(details.outbox.len(), 1);
        assert_eq!(details.receipts.len(), 1);
        assert!(details.claims.is_empty());
    }
}
