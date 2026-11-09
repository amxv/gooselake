use runtime_core::{
    OperationPhase, OperationResourceClaimRecord, OperationTransitionRecord,
    OperatorAgentRestoreCommand, RuntimeError, SessionRecord, WorkspaceAgentLifecycleState,
    WorkspaceAgentRecord,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_agent_restore::{
    assert_restore_identity, restore_state, CLAIM_KIND, EFFECT_KIND, OPERATION_KIND,
};
use crate::repository_workspace::operation_by_id;
use crate::repository_workspace_agent_lifecycle::apply_workspace_agent_lifecycle_tx;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// A provider acknowledgement is not a durable roster update. The restored
    /// session identity, lifecycle, lead notices, exact result and claim release
    /// are ONE SQLite commit. If anything fails, the original intent stays
    /// unresolved and the caller quarantines it without retrying the provider.
    pub fn complete_operator_agent_restore(
        &self,
        command: &OperatorAgentRestoreCommand,
        session: &SessionRecord,
        at_ms: i64,
    ) -> Result<WorkspaceAgentRecord, RuntimeError> {
        if session.id != command.agent_id
            || session.provider != command.provider
            || session.status != "ready"
            || session
                .provider_session_ref
                .as_deref()
                .is_none_or(str::is_empty)
            || session.updated_at != at_ms
            || session.closed_at.is_some()
            || session.active_turn_id.is_some()
        {
            return Err(RuntimeError::Conflict(
                "native restore receipt does not describe a ready restored session".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting atomic operator restore receipt", error))?;
        let operation = operation_by_id(&tx, &command.operation_id)?.ok_or_else(|| {
            RuntimeError::NotFound(format!("restore operation {}", command.operation_id))
        })?;
        if operation.kind != OPERATION_KIND
            || operation.phase != OperationPhase::Requested
            || operation.actor != command.actor
            || operation.workspace_id.as_deref() != Some(&command.workspace_id)
            || operation.normalized_request_hash != command.normalized_request_hash
            || operation.normalized_request != command.normalized_request
        {
            return Err(RuntimeError::Conflict(
                "native restore intent changed after provider invocation".into(),
            ));
        }
        let effect_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3 AND phase='intended'",
                params![command.operation_id, EFFECT_KIND, command.agent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed verifying admitted restore effect", error))?;
        if effect_count != 1 {
            return Err(RuntimeError::Conflict(
                "exact native restore effect is not intended".into(),
            ));
        }
        let (generation, acquired_at): (i64, i64) = tx.query_row(
            "SELECT fence_generation,acquired_at FROM runtime_operation_resource_claims
             WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3 AND claim_mode='exclusive'",
            params![CLAIM_KIND, command.agent_id, command.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional().map_err(|error| db_error("failed reading exact restore claim", error))?
            .ok_or_else(|| RuntimeError::Conflict("native restore operation has lost its exclusive claim".into()))?;
        let claim = OperationResourceClaimRecord {
            resource_kind: CLAIM_KIND.into(),
            resource_id: command.agent_id.clone(),
            claim_mode: "exclusive".into(),
            owner_operation_id: command.operation_id.clone(),
            fence_generation: u64::try_from(generation)
                .map_err(|_| RuntimeError::Bootstrap("invalid restore fence generation".into()))?,
            acquired_at,
        };
        let state = restore_state(&tx, &command.agent_id)?;
        assert_restore_identity(&state, command, "archived", "closed")?;
        if session.cwd.as_deref() != Some(state.policy.authoritative_cwd.as_str())
            || session.model != state.policy.model
            || session.permission_mode != state.policy.permission_intent.resolved_mode()
            || session.system_prompt != state.policy.system_prompt
        {
            return Err(RuntimeError::Conflict(
                "restored session differs from persisted recreation policy".into(),
            ));
        }
        let retirement_frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&command.workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking restoration retirement authority", error))?;
        if retirement_frozen {
            return Err(RuntimeError::Conflict(
                "workspace retirement started while native restoration was in flight".into(),
            ));
        }

        let exact = apply_workspace_agent_lifecycle_tx(
            &tx,
            session,
            &command.agent_id,
            WorkspaceAgentLifecycleState::Active,
            None,
            at_ms,
            None,
            Some(&command.operation_id),
        )?;
        if exact.revision
            != command
                .expected_revision
                .checked_add(1)
                .ok_or_else(|| RuntimeError::InvalidState("restore revision overflow".into()))?
            || exact.provider_session_ref != session.provider_session_ref
            || exact.canonical_provider_session_ref != session.canonical_provider_session_ref
        {
            return Err(RuntimeError::Conflict(
                "restored agent receipt differs from native attachment".into(),
            ));
        }

        let receipt = serde_json::to_value(&exact).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid exact agent restore receipt: {error}"))
        })?;
        let effect_update = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?2,
             attempt_count=1,updated_at=?3
             WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![
                    command.operation_id,
                    json_to_string(&serde_json::json!({
                        "provider":"acknowledged_resume",
                        "provider_session_ref":session.provider_session_ref,
                        "canonical_provider_session_ref":session.canonical_provider_session_ref,
                        "agent_revision":exact.revision,
                    }))?,
                    at_ms,
                    EFFECT_KIND
                ],
            )
            .map_err(|error| db_error("failed persisting native resume evidence", error))?;
        if effect_update != 1 {
            return Err(RuntimeError::Conflict(
                "native restore intent was changed before receipt".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&command.workspace_id),
            &receipt,
            None,
            None,
            at_ms,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(), sequence: 2,
            from_phase: Some(OperationPhase::Requested), to_phase: OperationPhase::Completed,
            evidence: Some(serde_json::json!({"native_resume":"acknowledged", "agent_revision":exact.revision})),
            created_at: at_ms,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing atomic native restore receipt", error))?;
        Ok(exact)
    }

    pub fn mark_operator_agent_restore_uncertain(
        &self,
        operation_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native restore quarantine", error))?;
        let record = operation_by_id(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("restore operation {operation_id}")))?;
        if record.kind != OPERATION_KIND {
            return Err(RuntimeError::Conflict(
                "cannot quarantine another native operation".into(),
            ));
        }
        if record.phase == OperationPhase::ManualReview {
            return Ok(());
        }
        if record.phase != OperationPhase::Requested {
            return Err(RuntimeError::Conflict(
                "cannot quarantine terminal agent restoration".into(),
            ));
        }
        let diagnostic: String = reason.chars().take(240).collect();
        let updated = tx
            .execute(
                "UPDATE runtime_operation_effects SET phase='uncertain',attempt_count=1,
             evidence_json=json_set(evidence_json,'$.diagnostic',?3),updated_at=?2
             WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
                params![operation_id, at_ms, diagnostic, EFFECT_KIND],
            )
            .map_err(|error| db_error("failed recording unknown native agent resume", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "restore effect was not intended".into(),
            ));
        }
        let changed = tx.execute(
            "UPDATE runtime_operations SET phase='manual_review',error_code='outcome_unresolved',
             error_message=?2,updated_at=?3 WHERE operation_id=?1 AND phase='requested'",
            params![operation_id, diagnostic, at_ms],
        ).map_err(|error| db_error("failed marking restoration manual review", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "restore operation phase changed".into(),
            ));
        }
        OperationAuthorityTransaction::new(&tx).append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::ManualReview,
            evidence: Some(serde_json::json!({"reason":"native_provider_restore_outcome_unknown"})),
            created_at: at_ms,
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing native restoration quarantine", error))
    }
}
