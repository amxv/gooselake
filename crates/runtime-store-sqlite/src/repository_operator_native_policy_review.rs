use runtime_core::{
    OperationActorKind, OperationPhase, OperationResourceClaimRecord, OperatorNativePolicyKind,
    OperatorNativePolicyReviewCommand, OperatorNativePolicyReviewResponse, RuntimeError,
    WorkspaceAgentRecreationPolicy,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, open_connection};
use crate::repository_operator_native_policy::{CLAIM_KIND, EFFECT_KIND, OPERATION_KIND};
use crate::repository_workspace::operation_by_id;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn operator_native_policy_reattachment_pending(
        &self,
        agent_id: &str,
    ) -> Result<bool, RuntimeError> {
        let conn = open_connection(&self.database_path)?;
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions
             WHERE id=?1 AND status='policy_recovery_required')",
            [agent_id],
            |row| row.get(0),
        )
        .map_err(|error| {
            db_error(
                "failed reading operator native policy reattachment fence",
                error,
            )
        })
    }

    /// Return an exact historical review receipt without changing policy authority.
    /// Fresh prose cannot consume an uncertain effect, advance revision, or clear its claim.
    pub fn resolve_operator_native_policy_review(
        &self,
        command: &OperatorNativePolicyReviewCommand,
    ) -> Result<OperatorNativePolicyReviewResponse, RuntimeError> {
        let input = &command.input;
        let evidence = input.evidence.trim();
        if command.actor.kind != OperationActorKind::Operator
            || command.actor.identifier.trim().is_empty()
            || command.operation_id.trim().is_empty()
            || command.agent_id.trim().is_empty()
            || evidence.len() < 8
            || evidence.len() > 2048
            || input.provider.trim().is_empty()
            || input.provider_session_ref.trim().is_empty()
        {
            return Err(RuntimeError::InvalidState(
                "native policy review requires exact provider identity and 8-2048 evidence bytes"
                    .into(),
            ));
        }
        let mut normalized_input = input.clone();
        normalized_input.evidence = evidence.to_string();
        let request_json = serde_json::to_value(&normalized_input)
            .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting exact native policy review", error))?;
        let record = operation_by_id(&tx, &command.operation_id)?.ok_or_else(|| {
            RuntimeError::NotFound(format!("native policy operation {}", command.operation_id))
        })?;
        if record.kind != OPERATION_KIND
            || record.actor != command.actor
            || record.normalized_request["agent_id"] != command.agent_id
            || record.normalized_request["expected_revision"] != input.expected_revision
            || runtime_core::normalized_json_hash(&record.normalized_request)?
                != record.normalized_request_hash
        {
            return Err(RuntimeError::Conflict(
                "operator native policy review does not match immutable operation authority".into(),
            ));
        }
        let (effect_phase, raw_evidence): (String, String) = tx
            .query_row(
                "SELECT phase,evidence_json FROM runtime_operation_effects
                 WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3",
                params![command.operation_id, EFFECT_KIND, command.agent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading exact native policy review target", error))?
            .ok_or_else(|| RuntimeError::Conflict("native policy effect is absent".into()))?;
        let old_effect: Value = serde_json::from_str(&raw_evidence).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid native policy effect evidence: {error}"))
        })?;
        if matches!(
            record.phase,
            OperationPhase::Completed | OperationPhase::Failed
        ) {
            let review = &old_effect["operator_review"];
            if effect_phase == "finalized" && review["input"] == request_json {
                return serde_json::from_value(review["response"].clone()).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "invalid durable policy review replay: {error}"
                    ))
                });
            }
            return Err(RuntimeError::Conflict(
                "native policy operation already has a different terminal outcome".into(),
            ));
        }
        if record.phase != OperationPhase::ManualReview || effect_phase != "uncertain" {
            return Err(RuntimeError::Conflict(
                "only a quarantined native policy effect may receive operator evidence".into(),
            ));
        }
        let kind = match record.normalized_request["kind"].as_str() {
            Some("preferences") => OperatorNativePolicyKind::Preferences,
            Some("permission") => OperatorNativePolicyKind::Permission,
            _ => {
                return Err(RuntimeError::Bootstrap(
                    "invalid admitted native policy kind".into(),
                ))
            }
        };
        if old_effect["kind"] != kind.as_str()
            || old_effect["provider"] != input.provider
            || old_effect["provider_session_ref"] != input.provider_session_ref
            || old_effect["canonical_provider_session_ref"]
                != json!(input.canonical_provider_session_ref)
            || old_effect["session_updated_at"] != input.session_updated_at
            || old_effect["expected_revision"] != input.expected_revision
        {
            return Err(RuntimeError::Conflict(
                "operator evidence is not for the exact originally admitted native session".into(),
            ));
        }

        let state: Option<(
            String,
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
            Option<String>,
            String,
        )> = tx
            .query_row(
                "SELECT a.workspace_id,a.lifecycle_state,w.lifecycle_state,a.revision,
                        s.provider,s.status,s.provider_session_ref,
                        s.canonical_provider_session_ref,s.updated_at,s.active_turn_id,
                        s.cwd,s.model,a.recreation_policy_json
                 FROM workspace_agents a JOIN sessions s ON s.id=a.session_id
                 JOIN workspaces w ON w.workspace_id=a.workspace_id
                 WHERE a.session_id=?1",
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
                        row.get(12)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| {
                db_error("failed verifying current policy review attachment", error)
            })?;
        let Some((
            workspace_id,
            agent_state,
            workspace_state,
            revision,
            provider,
            status,
            native_ref,
            canonical_ref,
            session_updated_at,
            active_turn,
            cwd,
            model,
            policy_json,
        )) = state
        else {
            return Err(RuntimeError::NotFound(format!(
                "agent {}",
                command.agent_id
            )));
        };
        let policy: WorkspaceAgentRecreationPolicy = serde_json::from_str(&policy_json)
            .map_err(|error| RuntimeError::Bootstrap(format!("invalid native policy: {error}")))?;
        let expected_revision = i64::try_from(input.expected_revision)
            .map_err(|_| RuntimeError::InvalidState("agent revision overflow".into()))?;
        if record.workspace_id.as_deref() != Some(workspace_id.as_str())
            || agent_state != "active"
            || workspace_state != "active"
            || matches!(
                status.as_str(),
                "closed" | "failed" | "policy_recovery_required"
            )
            || active_turn.is_some()
            || revision != expected_revision
            || provider != input.provider
            || policy.provider.as_str() != provider
            || native_ref.as_deref() != Some(input.provider_session_ref.as_str())
            || canonical_ref != input.canonical_provider_session_ref
            || session_updated_at != input.session_updated_at
            || cwd != policy.authoritative_cwd
            || model != policy.model
        {
            return Err(RuntimeError::Conflict(
                "native policy review cannot settle a changed workspace, agent, or provider attachment".into(),
            ));
        }
        let frozen: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
                [&workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed checking native policy review retirement fence",
                    error,
                )
            })?;
        if frozen {
            return Err(RuntimeError::Conflict(
                "workspace is frozen by retirement".into(),
            ));
        }
        let (claim_generation, acquired_at): (i64, i64) = tx
            .query_row(
                "SELECT fence_generation,acquired_at FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3
                   AND claim_mode='exclusive'",
                params![CLAIM_KIND, command.agent_id, command.operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading exact native policy review claim", error))?
            .ok_or_else(|| {
                RuntimeError::Conflict("native policy review lost its resource claim".into())
            })?;
        let _claim = OperationResourceClaimRecord {
            resource_kind: CLAIM_KIND.into(),
            resource_id: command.agent_id.clone(),
            claim_mode: "exclusive".into(),
            owner_operation_id: command.operation_id.clone(),
            fence_generation: u64::try_from(claim_generation)
                .map_err(|_| RuntimeError::Bootstrap("invalid native review fence".into()))?,
            acquired_at,
        };

        Err(RuntimeError::operation_unresolved(
            command.operation_id.clone(),
            "prose assertions cannot settle an uncertain native policy change; inspect the original operation; native retry is not authorized",
        ))
    }
}
