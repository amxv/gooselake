use runtime_core::{
    ProviderKind, ProviderRetirementCloseProofKind, RuntimeError, WorkspaceRetirementCloseAttempt,
    WorkspaceRetirementNativeIdentity,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::Value;

use crate::db::{db_error, json_to_string};

/// The current native attachment and archive generation, read under the same
/// write transaction that will admit or finalize its retirement close.
pub(crate) fn current_native_identity(
    db: &Connection,
    operation_id: &str,
    agent_id: &str,
) -> Result<WorkspaceRetirementNativeIdentity, RuntimeError> {
    let row: Option<(
        String,
        Option<String>,
        Option<String>,
        Option<i64>,
        i64,
        Option<String>,
    )> = db
        .query_row(
            "SELECT s.provider,s.provider_session_ref,s.canonical_provider_session_ref,
            a.revision,s.updated_at,s.active_turn_id
         FROM sessions s JOIN workspace_session_ownership o ON o.session_id=s.id
         JOIN workspace_retirement_fences f ON f.workspace_id=o.workspace_id
         LEFT JOIN workspace_agents a ON a.session_id=s.id
         WHERE f.operation_id=?1 AND s.id=?2",
            params![operation_id, agent_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| {
            db_error(
                "failed reading retirement native attachment identity",
                error,
            )
        })?;
    let (provider, native, canonical, revision, updated_at, turn) = row.ok_or_else(|| {
        RuntimeError::Conflict("native attachment is not owned by this retirement operation".into())
    })?;
    Ok(WorkspaceRetirementNativeIdentity {
        agent_id: agent_id.into(),
        provider: ProviderKind::from_str(&provider).ok_or_else(|| {
            RuntimeError::Conflict("retirement native provider identity is invalid".into())
        })?,
        provider_session_ref: native.filter(|value| !value.is_empty()).ok_or_else(|| {
            RuntimeError::Conflict("retirement native session identity is missing".into())
        })?,
        canonical_provider_session_ref: canonical,
        agent_revision: revision
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    RuntimeError::Bootstrap("invalid native archive generation".into())
                })
            })
            .transpose()?,
        session_updated_at: updated_at,
        active_turn_id: turn,
    })
}

/// Both strong adapter acknowledgement and explicit operator verification use
/// this atomic archival path, preserving prior unknown evidence and publishing
/// the same stable lifecycle receipt exactly once.
pub(crate) fn archive_verified_close(
    tx: &Transaction<'_>,
    attempt: &WorkspaceRetirementCloseAttempt,
    identity: &WorkspaceRetirementNativeIdentity,
    source_phase: &str,
    proof_kind: ProviderRetirementCloseProofKind,
    mut evidence: Value,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    if current_native_identity(tx, &attempt.operation_id, &attempt.agent_id)? != *identity
        || attempt.provider != identity.provider.as_str()
        || attempt.provider_session_ref != identity.provider_session_ref
    {
        return Err(RuntimeError::Conflict(
            "native identity or archive generation changed before closure receipt".into(),
        ));
    }
    if proof_kind == ProviderRetirementCloseProofKind::OperatorVerifiedClosed {
        let turns =
            crate::repository_workspace_retirement_review_turns::settle_closed_attachment_turns(
                tx, attempt, identity, &evidence, at_ms,
            )?;
        evidence["operator_settled_turn_ids"] = serde_json::json!(turns);
    } else if identity.active_turn_id.is_some() {
        return Err(RuntimeError::Conflict(
            "native adapter closure cannot settle an active logical turn".into(),
        ));
    }
    let pending: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_effects WHERE operation_id=?1
         AND phase<>'finalized' AND (effect_kind='workspace_process_settle'
           OR (effect_kind='workspace_turn_interrupt' AND target_id IN
             (SELECT id FROM turns WHERE session_id=?2)))",
            params![attempt.operation_id, attempt.agent_id],
            |row| row.get(0),
        )
        .map_err(|error| {
            db_error(
                "failed proving retirement turn and process settlement",
                error,
            )
        })?;
    if pending != 0 {
        return Err(RuntimeError::Conflict(
            "settle retirement turns and processes before verifying provider closure".into(),
        ));
    }
    let workspace_id: String = tx
        .query_row(
            "SELECT workspace_id FROM workspace_retirement_fences WHERE operation_id=?1",
            [&attempt.operation_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed finding retiring workspace for close receipt", error))?;
    tx.execute(
        "UPDATE workspaces SET lead_agent_id=NULL,revision=revision+1,updated_at=?3
        WHERE workspace_id=?1 AND lead_agent_id=?2",
        params![workspace_id, attempt.agent_id, at_ms],
    )
    .map_err(|error| db_error("failed clearing lead before verified archive", error))?;
    let changed = tx
        .execute(
            "UPDATE sessions SET status='closed',closed_at=?2,updated_at=?2
        WHERE id=?1 AND active_turn_id IS NULL",
            params![attempt.agent_id, at_ms],
        )
        .map_err(|error| db_error("failed recording native-closed session", error))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "native session changed before verified archive".into(),
        ));
    }
    tx.execute(
        "UPDATE workspace_agents SET lifecycle_state='archived',archived_at=?2,
        archive_reason='workspace_retirement_verified_close',revision=revision+1,updated_at=?2
        WHERE session_id=?1 AND lifecycle_state='active'",
        params![attempt.agent_id, at_ms],
    )
    .map_err(|error| db_error("failed archiving verified native attachment", error))?;
    let archived_revision: Option<i64> = tx
        .query_row(
            "SELECT revision FROM workspace_agents WHERE session_id=?1",
            [&attempt.agent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading verified archive revision", error))?;
    for (key, value) in serde_json::json!({
        "verified":true,"provider":attempt.provider,
        "provider_session_ref":attempt.provider_session_ref,
        "canonical_provider_session_ref":identity.canonical_provider_session_ref,
        "proof_kind":proof_kind,"verified_at":at_ms,"closed_at":at_ms,
        "session_updated_at":at_ms,"agent_revision":archived_revision,
        "verified_identity":identity,
    })
    .as_object()
    .expect("literal receipt object")
    {
        evidence[key] = value.clone();
    }
    let changed = tx
        .execute(
            "UPDATE runtime_operation_effects SET phase='finalized',
        evidence_json=?3,updated_at=?4 WHERE operation_id=?1
        AND effect_kind='workspace_provider_close' AND target_id=?2 AND phase=?5",
            params![
                attempt.operation_id,
                attempt.agent_id,
                json_to_string(&evidence)?,
                at_ms,
                source_phase
            ],
        )
        .map_err(|error| db_error("failed recording verified provider close receipt", error))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "native close receipt lost its exact effect intent".into(),
        ));
    }
    crate::repository_workspace_retirement_events::retirement_agent_archived(
        tx,
        &attempt.operation_id,
        &workspace_id,
        &attempt.agent_id,
        proof_kind,
        at_ms,
    )?;
    Ok(())
}
