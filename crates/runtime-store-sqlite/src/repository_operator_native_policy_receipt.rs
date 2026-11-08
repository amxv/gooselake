use runtime_core::{
    OperationPhase, OperationResourceClaimRecord, OperationTransitionRecord,
    OperatorNativePolicyCommand, OperatorNativePolicyKind, RuntimeError, SessionRecord,
    WorkspaceAgentRecreationPolicy,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::Value;

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_native_policy::{CLAIM_KIND, EFFECT_KIND, OPERATION_KIND};
use crate::repository_workspace::operation_by_id;
use crate::repository_workspace_agent::update_session;
use crate::SqliteRuntimeRepository;

fn intended_operation(
    tx: &Transaction<'_>,
    command: &OperatorNativePolicyCommand,
) -> Result<(), RuntimeError> {
    let record = operation_by_id(tx, &command.operation_id)?
        .ok_or_else(|| RuntimeError::NotFound(format!("operation {}", command.operation_id)))?;
    if record.kind != OPERATION_KIND
        || record.phase != OperationPhase::Requested
        || record.workspace_id.as_deref() != Some(&command.workspace_id)
        || record.normalized_request_hash != command.normalized_request_hash
        || record.normalized_request != command.normalized_request
    {
        return Err(RuntimeError::Conflict(
            "provider policy operation has changed since native intent admission".into(),
        ));
    }
    let count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_effects WHERE operation_id=?1
         AND effect_kind=?2 AND target_id=?3 AND phase='intended'",
            params![command.operation_id, EFFECT_KIND, command.agent_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading admitted native policy effect", error))?;
    if count != 1 {
        return Err(RuntimeError::Conflict(
            "native provider policy intent is absent".into(),
        ));
    }
    Ok(())
}

fn exact_claim(
    tx: &Transaction<'_>,
    command: &OperatorNativePolicyCommand,
) -> Result<OperationResourceClaimRecord, RuntimeError> {
    let claim: Option<(i64, i64)> = tx.query_row(
        "SELECT fence_generation, acquired_at FROM runtime_operation_resource_claims
         WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3 AND claim_mode='exclusive'",
        params![CLAIM_KIND, command.agent_id, command.operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional().map_err(|error| db_error("failed reading native policy claim", error))?;
    let (generation, acquired_at) = claim.ok_or_else(|| {
        RuntimeError::Conflict(
            "native provider policy claim is no longer owned by operation".into(),
        )
    })?;
    Ok(OperationResourceClaimRecord {
        resource_kind: CLAIM_KIND.into(),
        resource_id: command.agent_id.clone(),
        claim_mode: "exclusive".into(),
        owner_operation_id: command.operation_id.clone(),
        fence_generation: u64::try_from(generation)
            .map_err(|_| RuntimeError::Bootstrap("invalid policy fence generation".into()))?,
        acquired_at,
    })
}

impl SqliteRuntimeRepository {
    /// The provider's acknowledged result and the new authoritative policy
    /// are committed in ONE transaction. A failed transaction leaves the
    /// durable native intent/claim unresolved, never an invented success.
    pub fn complete_operator_native_policy(
        &self,
        command: &OperatorNativePolicyCommand,
        session: &SessionRecord,
        policy: &WorkspaceAgentRecreationPolicy,
        exact_result: &Value,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        if session.id != command.agent_id
            || session.provider != command.provider
            || session.provider_session_ref != command.provider_session_ref
            || session.canonical_provider_session_ref != command.canonical_provider_session_ref
            || session.cwd.as_deref() != Some(policy.authoritative_cwd.as_str())
            || session.model != policy.model
        {
            return Err(RuntimeError::Conflict(
                "provider policy receipt does not match intended native session".into(),
            ));
        }
        let new_revision = command
            .expected_revision
            .checked_add(1)
            .ok_or_else(|| RuntimeError::InvalidState("agent revision overflow".into()))?;
        if exact_result["revision"] != new_revision {
            return Err(RuntimeError::Conflict(
                "provider policy receipt has incorrect revision".into(),
            ));
        }
        match command.kind {
            OperatorNativePolicyKind::Preferences
                if exact_result["current_preferences"]
                    != serde_json::to_value(&policy.current_preferences)
                        .map_err(|e| RuntimeError::Bootstrap(e.to_string()))? =>
            {
                return Err(RuntimeError::Conflict(
                    "provider preference receipt differs from committed policy".into(),
                ));
            }
            OperatorNativePolicyKind::Permission
                if exact_result["permission_intent"]
                    != serde_json::to_value(&policy.permission_intent)
                        .map_err(|e| RuntimeError::Bootstrap(e.to_string()))? =>
            {
                return Err(RuntimeError::Conflict(
                    "provider permission receipt differs from committed policy".into(),
                ));
            }
            _ => {}
        }

        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting exact provider policy receipt", error))?;
        intended_operation(&tx, command)?;
        let claim = exact_claim(&tx, command)?;
        let identity: Option<(
            String,
            String,
            i64,
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
            Option<String>,
            String,
            String,
            String,
        )> = tx
            .query_row(
                "SELECT a.workspace_id,a.lifecycle_state,a.revision,s.provider,s.status,
                    s.provider_session_ref,s.canonical_provider_session_ref,s.updated_at,
                    s.active_turn_id,a.recreation_policy_json,w.lifecycle_state,s.cwd
             FROM workspace_agents a JOIN workspaces w ON w.workspace_id=a.workspace_id
             JOIN sessions s ON s.id=a.session_id WHERE a.session_id=?1",
                [&command.agent_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| db_error("failed rechecking provider policy authority", error))?;
        let Some((
            workspace_id,
            agent_state,
            revision,
            provider,
            status,
            session_ref,
            canonical_ref,
            updated_at,
            active_turn,
            policy_json,
            workspace_state,
            cwd,
        )) = identity
        else {
            return Err(RuntimeError::NotFound(format!(
                "workspace agent {}",
                command.agent_id
            )));
        };
        let old: WorkspaceAgentRecreationPolicy =
            serde_json::from_str(&policy_json).map_err(|error| {
                RuntimeError::Bootstrap(format!("corrupt durable agent policy: {error}"))
            })?;
        let mut expected = old.clone();
        match command.kind {
            OperatorNativePolicyKind::Preferences => {
                expected.current_preferences = policy.current_preferences.clone()
            }
            OperatorNativePolicyKind::Permission => {
                expected.permission_intent = policy.permission_intent.clone()
            }
        }
        if expected != *policy
            || workspace_id != command.workspace_id
            || workspace_state != "active"
            || agent_state != "active"
            || matches!(status.as_str(), "closed" | "failed")
            || active_turn.is_some()
            || provider != command.provider
            || old.provider.as_str() != command.provider
            || revision
                != i64::try_from(command.expected_revision)
                    .map_err(|_| RuntimeError::InvalidState("revision overflow".into()))?
            || session_ref != command.provider_session_ref
            || canonical_ref != command.canonical_provider_session_ref
            || updated_at != command.session_updated_at
            || cwd != old.authoritative_cwd
        {
            return Err(RuntimeError::Conflict(
                "provider policy authority changed after native acknowledgement".into(),
            ));
        }

        update_session(&tx, session)?;
        let changed = tx.execute(
            "UPDATE workspace_agents SET recreation_policy_json=?3,revision=revision+1,updated_at=?4
             WHERE session_id=?1 AND revision=?2 AND lifecycle_state='active'",
            params![command.agent_id, command.expected_revision, json_to_string(&serde_json::to_value(policy).map_err(|e| RuntimeError::Bootstrap(e.to_string()))?)?, at_ms],
        ).map_err(|error| db_error("failed committing provider policy revision", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "provider policy revision raced after native acknowledgement".into(),
            ));
        }
        let updated = tx.execute(
            "UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?2,
             attempt_count=1,updated_at=?3 WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
            params![command.operation_id, json_to_string(&serde_json::json!({"exact_result":exact_result}))?, at_ms, EFFECT_KIND],
        ).map_err(|error| db_error("failed recording provider policy exact receipt", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "provider policy native intent changed before receipt".into(),
            ));
        }
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            &command.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            Some(&command.workspace_id),
            exact_result,
            None,
            None,
            at_ms,
        )?;
        authority.append_transition(&OperationTransitionRecord {
            operation_id: command.operation_id.clone(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(
                serde_json::json!({"receipt":"acknowledged_native_policy_and_durable_revision"}),
            ),
            created_at: at_ms,
        })?;
        authority.release_claim(&claim)?;
        tx.commit()
            .map_err(|error| db_error("failed committing exact provider policy operation", error))
    }

    /// A provider error or lost post-dispatch receipt cannot prove absence of
    /// effect. Quarantine the intent and retain the exclusive agent fence.
    pub fn mark_operator_native_policy_uncertain(
        &self,
        operation_id: &str,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting provider policy manual review", error))?;
        let record = operation_by_id(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("operation {operation_id}")))?;
        if record.kind != OPERATION_KIND {
            return Err(RuntimeError::Conflict(
                "cannot quarantine unrelated native effect".into(),
            ));
        }
        if record.phase == OperationPhase::ManualReview {
            return Ok(());
        }
        if record.phase != OperationPhase::Requested {
            return Err(RuntimeError::Conflict(
                "cannot quarantine terminal policy mutation".into(),
            ));
        }
        let diagnostic: String = reason.chars().take(240).collect();
        let updated = tx.execute(
            "UPDATE runtime_operation_effects SET phase='uncertain',attempt_count=1,updated_at=?2,
             evidence_json=json_set(evidence_json,'$.diagnostic',?3)
             WHERE operation_id=?1 AND effect_kind=?4 AND phase='intended'",
            params![operation_id,at_ms,diagnostic,EFFECT_KIND],
        ).map_err(|error| db_error("failed recording unknown provider policy effect", error))?;
        if updated != 1 {
            return Err(RuntimeError::Conflict(
                "policy effect is no longer intended".into(),
            ));
        }
        let changed = tx.execute(
            "UPDATE runtime_operations SET phase='manual_review',error_code='outcome_unresolved',
             error_message=?2,updated_at=?3 WHERE operation_id=?1 AND phase='requested'",
            params![operation_id,diagnostic,at_ms],
        ).map_err(|error| db_error("failed quarantining provider policy operation", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "provider policy phase changed".into(),
            ));
        }
        OperationAuthorityTransaction::new(&tx).append_transition(&OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::ManualReview,
            evidence: Some(serde_json::json!({"reason":"native_provider_outcome_unknown"})),
            created_at: at_ms,
        })?;
        tx.commit()
            .map_err(|error| db_error("failed committing provider policy manual review", error))
    }
}
