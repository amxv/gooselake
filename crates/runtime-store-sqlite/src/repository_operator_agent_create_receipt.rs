use runtime_core::{
    OperationPhase, OperationResourceClaimRecord, OperationTransitionRecord,
    OperatorAgentCreateCommand, RuntimeError, SessionRecord, TeamHookPreAddContext,
    WorkspaceAgentOnboardingRequest, WorkspaceAgentRecord,
};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::json;

use crate::db::{db_error, json_to_string};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_operator_agent_create::{
    read_frozen_create_plan, verify_requested_create, CLAIM_KIND, EFFECT_KIND,
};

pub(crate) fn exact_create_claim(
    tx: &Transaction<'_>,
    command: &OperatorAgentCreateCommand,
) -> Result<OperationResourceClaimRecord, RuntimeError> {
    let pair: Option<(i64, i64)> = tx
        .query_row(
            "SELECT fence_generation,acquired_at FROM runtime_operation_resource_claims
         WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3
           AND claim_mode='exclusive'",
            params![CLAIM_KIND, command.reserved_agent_id, command.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| db_error("failed reading exact agent creation resource claim", error))?;
    let (generation, acquired_at) = pair.ok_or_else(|| {
        RuntimeError::Conflict(
            "original native agent creation no longer owns its session identity".into(),
        )
    })?;
    Ok(OperationResourceClaimRecord {
        resource_kind: CLAIM_KIND.into(),
        resource_id: command.reserved_agent_id.clone(),
        claim_mode: "exclusive".into(),
        owner_operation_id: command.operation_id.clone(),
        fence_generation: u64::try_from(generation).map_err(|_| {
            RuntimeError::Bootstrap("invalid agent creation native fence generation".into())
        })?,
        acquired_at,
    })
}

/// All input authority was persisted before native provider execution. Native
/// acknowledgement can advance the public roster only if this exact frozen
/// snapshot still matches, and only in the same transaction that saves the
/// session/profile/onboarding/claims/receipt and releases the original fence.
pub(crate) fn finalize_agent_create_receipt_tx(
    tx: &Transaction<'_>,
    command: &OperatorAgentCreateCommand,
    session: &SessionRecord,
    agent: &WorkspaceAgentRecord,
    onboarding: &WorkspaceAgentOnboardingRequest,
    hooks: Option<&TeamHookPreAddContext>,
) -> Result<(), RuntimeError> {
    verify_requested_create(tx, command)?;
    let snapshot = read_frozen_create_plan(tx, command)?.ok_or_else(|| {
        RuntimeError::Conflict("native agent creation has no frozen provider launch plan".into())
    })?;
    let mut expected_session = snapshot.session.clone();
    expected_session.provider_session_ref = session.provider_session_ref.clone();
    expected_session.canonical_provider_session_ref =
        session.canonical_provider_session_ref.clone();
    expected_session.updated_at = session.updated_at;
    let mut expected_agent = snapshot.agent.clone();
    expected_agent.provider_session_ref = agent.provider_session_ref.clone();
    expected_agent.canonical_provider_session_ref = agent.canonical_provider_session_ref.clone();
    expected_agent.updated_at = agent.updated_at;
    if expected_session != *session
        || expected_agent != *agent
        || snapshot.onboarding != *onboarding
        || snapshot.hooks.as_ref() != hooks
        || session
            .provider_session_ref
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        || session.provider_session_ref != agent.provider_session_ref
        || session.canonical_provider_session_ref != agent.canonical_provider_session_ref
        || session.updated_at < session.created_at
        || agent.updated_at < agent.created_at
    {
        return Err(RuntimeError::Conflict(
            "native agent creation receipt differs from the exact pre-provider launch snapshot"
                .into(),
        ));
    }
    let frozen: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
            [&command.workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking create receipt retirement fence", error))?;
    if frozen {
        return Err(RuntimeError::Conflict(
            "workspace retired while provider creation was in flight".into(),
        ));
    }
    let ownership: Option<String> = tx
        .query_row(
            "SELECT source_operation_id FROM workspace_session_ownership WHERE session_id=?1",
            [&command.reserved_agent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading native creation session ownership", error))?;
    if ownership.as_deref() != Some(command.operation_id.as_str()) {
        return Err(RuntimeError::Conflict(
            "agent was not durably owned by its admitted create operation".into(),
        ));
    }
    let claim = exact_create_claim(tx, command)?;
    let effect_evidence = json!({
        "native_result":"provider_session_created",
        "provider":session.provider,
        "provider_session_ref":session.provider_session_ref,
        "canonical_provider_session_ref":session.canonical_provider_session_ref,
        "agent_id":agent.agent_id,
        "launch_snapshot":snapshot,
    });
    let changed = tx
        .execute(
            "UPDATE runtime_operation_effects SET phase='finalized',attempt_count=1,
         evidence_json=?2,updated_at=?3
         WHERE operation_id=?1 AND effect_kind=?4 AND target_id=?5 AND phase='intended'",
            params![
                command.operation_id,
                json_to_string(&effect_evidence)?,
                session.updated_at,
                EFFECT_KIND,
                command.reserved_agent_id
            ],
        )
        .map_err(|error| db_error("failed storing exact native agent creation effect", error))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "native provider creation effect is not still intended".into(),
        ));
    }
    let exact = serde_json::to_value(agent).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid exact agent create receipt: {error}"))
    })?;
    let authority = OperationAuthorityTransaction::new(tx);
    authority.terminalize(
        &command.operation_id,
        OperationPhase::Requested,
        OperationPhase::Completed,
        Some(&command.workspace_id),
        &exact,
        None,
        None,
        session.updated_at,
    )?;
    authority.append_transition(&OperationTransitionRecord {
        operation_id: command.operation_id.clone(),
        sequence: 2,
        from_phase: Some(OperationPhase::Requested),
        to_phase: OperationPhase::Completed,
        evidence: Some(json!({"native_provider":"created", "agent_id":command.reserved_agent_id})),
        created_at: session.updated_at,
    })?;
    authority.release_claim(&claim)?;
    Ok(())
}
