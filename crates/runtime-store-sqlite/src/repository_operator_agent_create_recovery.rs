use runtime_core::{OperationPhase, OperationTransitionRecord, RuntimeError};
use rusqlite::{params, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_agent_create::{CLAIM_KIND, EFFECT_KIND, OPERATION_KIND};
use crate::repository_workspace::operation_by_id;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// An error after native creation admission is not proof that either Git,
    /// approved shell hooks, or the provider has *not* already run. No retry
    /// can allocate another session or reissue those native effects.
    pub fn mark_operator_agent_create_uncertain(
        &self,
        operation_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting provider creation quarantine", error))?;
        let record = operation_by_id(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("agent creation {operation_id}")))?;
        if record.kind != OPERATION_KIND {
            return Err(RuntimeError::Conflict(
                "cannot quarantine unrelated provider operation".into(),
            ));
        }
        if record.phase == OperationPhase::ManualReview {
            return Ok(());
        }
        if record.phase != OperationPhase::Requested {
            return Err(RuntimeError::Conflict(
                "cannot quarantine terminal agent creation".into(),
            ));
        }
        let diagnostic: String = reason.chars().take(240).collect();
        let effect_count = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='uncertain',attempt_count=1,
             evidence_json=json_set(evidence_json,'$.diagnostic',?3),updated_at=?2
             WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![operation_id, at_ms, diagnostic, EFFECT_KIND],
            )
            .map_err(|error| {
                db_error("failed recording uncertain provider creation effect", error)
            })?;
        if effect_count != 1 {
            return Err(RuntimeError::Conflict(
                "native provider creation intent disappeared".into(),
            ));
        }
        let changed = tx.execute(
            "UPDATE runtime_operations SET phase='manual_review',error_code='outcome_unresolved',
             error_message=?2,updated_at=?3 WHERE operation_id=?1 AND phase='requested'",
            params![operation_id,diagnostic,at_ms],
        ).map_err(|error| db_error("failed quarantining agent creation", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "agent creation operation phase changed".into(),
            ));
        }
        OperationAuthorityTransaction::new(&tx).append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::ManualReview,
            evidence: Some(serde_json::json!({"reason":"native_create_outcome_unknown"})),
            created_at: at_ms,
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing uncertain native creation", error))
    }

    /// No live provider call is allowed for a pre-crash, unreceipted creation.
    /// An agent may exist natively without the corresponding durable session
    /// row, and any independently created Git checkout must also be preserved.
    pub fn quarantine_started_operator_agent_creates(
        &self,
        at_ms: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        let ids = {
            let connection = open_connection(&self.database_path)?;
            let mut stmt = connection
                .prepare(
                    "SELECT o.operation_id FROM runtime_operations o
                 JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
                 JOIN runtime_operation_resource_claims c ON c.owner_operation_id=o.operation_id
                 WHERE o.kind=?1 AND o.phase='requested' AND e.effect_kind=?2
                   AND e.phase='intended' AND c.resource_kind=?3 AND c.claim_mode='exclusive'
                 GROUP BY o.operation_id ORDER BY o.created_at,o.operation_id",
                )
                .map_err(|error| {
                    db_error("failed preparing native agent creation recovery", error)
                })?;
            let rows = stmt
                .query_map(params![OPERATION_KIND, EFFECT_KIND, CLAIM_KIND], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| db_error("failed reading started native agent creation", error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed decoding native creation recovery IDs", error))?;
            rows
        };
        for id in &ids {
            self.mark_operator_agent_create_uncertain(
                id,
                "startup found native agent creation without exact session/onboarding receipt; Git/hooks/provider outcomes unknown",
                at_ms,
            )?;
        }
        Ok(ids)
    }
}
