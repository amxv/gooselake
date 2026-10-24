use runtime_core::{
    ProviderRetirementCloseEvidence, ProviderRetirementCloseProofKind, RuntimeError,
    WorkspaceRetirementCloseAttempt,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection, string_to_json};
use crate::repository_workspace_retirement_close_receipt::{
    archive_verified_close, current_native_identity,
};
use crate::SqliteRuntimeRepository;

/// A restore/reopen invalidates old closure evidence even when the native
/// provider reuses its session identifier. Only exact archived state matches.
pub(crate) fn verified_prior_close(
    db: &Connection,
    workspace_id: &str,
    agent_id: &str,
) -> Result<Option<serde_json::Value>, RuntimeError> {
    let receipt: Option<(String, String)> = db.query_row(
        "SELECT e.operation_id,e.evidence_json FROM runtime_operation_effects e
         JOIN runtime_operations op ON op.operation_id=e.operation_id
         JOIN workspace_session_ownership o ON o.session_id=e.target_id
         JOIN sessions s ON s.id=o.session_id
         LEFT JOIN workspace_agents a ON a.session_id=s.id
         WHERE o.workspace_id=?1 AND s.id=?2 AND op.workspace_id=?1
           AND op.kind='workspace_retirement' AND op.phase='completed'
           AND e.effect_kind='workspace_provider_close' AND e.phase='finalized'
           AND json_extract(e.evidence_json,'$.verified')=1
           AND s.status='closed' AND s.active_turn_id IS NULL
           AND (a.session_id IS NULL OR a.lifecycle_state='archived')
           AND json_extract(e.evidence_json,'$.provider')=s.provider
           AND json_extract(e.evidence_json,'$.provider_session_ref')=
               s.provider_session_ref
           AND json_extract(e.evidence_json,'$.canonical_provider_session_ref') IS s.canonical_provider_session_ref
           AND json_extract(e.evidence_json,'$.closed_at')=s.closed_at
           AND json_extract(e.evidence_json,'$.session_updated_at')=s.updated_at
           AND json_extract(e.evidence_json,'$.agent_revision') IS a.revision
         ORDER BY op.updated_at DESC,e.operation_id LIMIT 1",
        params![workspace_id,agent_id], |row| Ok((row.get(0)?,row.get(1)?)),
    ).optional().map_err(|error| db_error("failed checking prior verified native close",error))?;
    receipt
        .map(|(operation_id, raw)| {
            let mut evidence = string_to_json(raw).map_err(|error| {
                db_error("failed decoding verified native close receipt", error)
            })?;
            evidence["reused_from_operation_id"] = serde_json::json!(operation_id);
            Ok(evidence)
        })
        .transpose()
}

impl SqliteRuntimeRepository {
    /// Claims exactly one native close before contacting its provider. A
    /// claimed effect is never silently re-dispatched after a crash.
    pub fn claim_workspace_retirement_native_close(
        &self,
        operation_id: &str,
        agent_id: &str,
        at_ms: i64,
    ) -> Result<WorkspaceRetirementCloseAttempt, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native retirement close claim", error))?;
        let (provider, provider_session_ref, active_turn_id): (
            String,
            Option<String>,
            Option<String>,
        ) = tx
            .query_row(
                "SELECT s.provider,s.provider_session_ref,s.active_turn_id
                 FROM sessions s JOIN workspace_session_ownership o ON o.session_id=s.id
                 LEFT JOIN workspace_agents a ON a.session_id=s.id
                 JOIN workspace_retirement_fences f ON f.workspace_id=o.workspace_id
                 JOIN runtime_operations op ON op.operation_id=f.operation_id
                 WHERE s.id=?2 AND f.operation_id=?1 AND f.phase='frozen'
                   AND op.phase='requested'",
                params![operation_id, agent_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|error| {
                db_error(
                    "failed proving retirement provider attachment authority",
                    error,
                )
            })?;
        if active_turn_id.is_some() {
            return Err(RuntimeError::Conflict(format!(
                "agent {agent_id} still has an active logical turn; retire its turn first"
            )));
        }
        let provider_session_ref = provider_session_ref.ok_or_else(|| {
            RuntimeError::Conflict(format!(
                "agent {agent_id} has no proven native provider session identity"
            ))
        })?;
        let attempt = WorkspaceRetirementCloseAttempt {
            operation_id: operation_id.into(),
            agent_id: agent_id.into(),
            provider,
            provider_session_ref,
        };
        let identity = current_native_identity(&tx, operation_id, agent_id)?;
        let updated = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='started',
                evidence_json=?4,attempt_count=attempt_count+1,updated_at=?5
             WHERE operation_id=?1 AND effect_kind='workspace_provider_close' AND target_id=?2
               AND phase='intended' AND idempotency_key=?3",
                params![
                    operation_id,
                    agent_id,
                    format!("workspace_provider_close:{agent_id}"),
                    json_to_string(&serde_json::json!({"provider":attempt.provider,
                    "provider_session_ref":attempt.provider_session_ref,
                    "native_identity":identity,
                    "started_at":at_ms}))?,
                    at_ms
                ],
            )
            .map_err(|error| {
                db_error(
                    "failed marking retirement native close as dispatched",
                    error,
                )
            })?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(format!(
                "agent {agent_id} native close was already dispatched or requires reconciliation"
            )));
        }
        tx.commit().map_err(|error| {
            db_error(
                "failed committing native close intent before dispatch",
                error,
            )
        })?;
        Ok(attempt)
    }

    /// Native acknowledgement and database archival share a commit. If the
    /// acknowledgement happened but this transaction failed, the started
    /// intent remains ambiguous, and replay is forbidden without review.
    pub fn finalize_workspace_retirement_native_close(
        &self,
        attempt: &WorkspaceRetirementCloseAttempt,
        evidence: &ProviderRetirementCloseEvidence,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        if evidence.runtime_session_id != attempt.agent_id
            || evidence.provider_session_ref != attempt.provider_session_ref
            || evidence.proof_kind == ProviderRetirementCloseProofKind::OperatorVerifiedClosed
        {
            return Err(RuntimeError::Conflict(
                "provider retirement close evidence does not match admitted native identity".into(),
            ));
        }
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting verified provider close receipt", error))?;
        let saved: Option<String> = tx
            .query_row(
                "SELECT e.evidence_json FROM runtime_operation_effects e
             JOIN runtime_operations op ON op.operation_id=e.operation_id
             JOIN workspace_retirement_fences f ON f.operation_id=op.operation_id
             WHERE e.operation_id=?1 AND e.effect_kind='workspace_provider_close'
               AND e.target_id=?2 AND e.phase='started'
               AND f.phase='frozen' AND op.phase='requested'",
                params![attempt.operation_id, attempt.agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading native close dispatch evidence", error))?;
        let saved = saved.ok_or_else(|| {
            RuntimeError::Conflict("native retirement close not in started state".into())
        })?;
        let saved = string_to_json(saved)
            .map_err(|error| db_error("failed decoding immutable native close intent", error))?;
        if saved.get("provider").and_then(serde_json::Value::as_str)
            != Some(attempt.provider.as_str())
            || saved
                .get("provider_session_ref")
                .and_then(serde_json::Value::as_str)
                != Some(attempt.provider_session_ref.as_str())
        {
            return Err(RuntimeError::Conflict(
                "native retirement close intent changed before receipt".into(),
            ));
        }
        let identity = current_native_identity(&tx, &attempt.operation_id, &attempt.agent_id)?;
        if saved.get("native_identity")
            != Some(&serde_json::to_value(&identity).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid native close identity: {error}"))
            })?)
        {
            return Err(RuntimeError::Conflict(
                "native identity or archive generation changed after close intent".into(),
            ));
        }
        archive_verified_close(
            &tx,
            attempt,
            &identity,
            "started",
            evidence.proof_kind,
            serde_json::json!({"prior_evidence":saved}),
            at_ms,
        )?;
        tx.commit().map_err(|error| {
            db_error(
                "failed committing native close and archive atomically",
                error,
            )
        })?;
        Ok(())
    }

    pub fn quarantine_workspace_retirement_native_close(
        &self,
        attempt: &WorkspaceRetirementCloseAttempt,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting retirement close quarantine", error))?;
        let changed = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='manual_review',
            evidence_json=json_set(?3,'$.prior_evidence',json(evidence_json)),updated_at=?4 WHERE operation_id=?1
            AND effect_kind='workspace_provider_close' AND target_id=?2 AND phase='started'",
                params![
                    attempt.operation_id,
                    attempt.agent_id,
                    json_to_string(&serde_json::json!({"outcome":"native_close_unresolved",
                    "provider":attempt.provider,"provider_session_ref":attempt.provider_session_ref,
                    "reason":reason}))?,
                    at_ms
                ],
            )
            .map_err(|error| db_error("failed recording unknown native close outcome", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "native close was not in started state".into(),
            ));
        }
        tx.execute(
            "UPDATE workspace_retirement_fences SET phase='manual_review',updated_at=?2
            WHERE operation_id=?1 AND phase='frozen'",
            params![attempt.operation_id, at_ms],
        )
        .map_err(|error| db_error("failed freezing ambiguous retirement", error))?;
        tx.execute(
            "UPDATE runtime_operations SET phase='manual_review',updated_at=?2
            WHERE operation_id=?1 AND phase='requested'",
            params![attempt.operation_id, at_ms],
        )
        .map_err(|error| db_error("failed quarantining retirement operation", error))?;
        tx.commit()
            .map_err(|error| db_error("failed committing retirement close quarantine", error))?;
        Ok(())
    }
}
