use crate::db::{db_error, fetch_runtime_event_by_event_id};
use crate::repository_startup_reattachment::{claim_for, conflict, RESUME_CLAIM, STARTUP_KIND};
use crate::repository_turn_start::read_admission;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::repository_workspace_agent::{workspace_agent_from_row, AGENT_SELECT};
use runtime_core::{
    RuntimeError, SessionRecord, StartupAttachmentAuthority, StartupAttachmentScope,
    StartupTerminalMetadataWitness, TurnDispatchState, WorkspaceAgentLifecycleState,
};
use rusqlite::{params, OptionalExtension, Transaction};
pub(crate) fn current_authority(
    tx: &Transaction<'_>,
    session_id: &str,
) -> Result<(SessionRecord, StartupAttachmentAuthority), RuntimeError> {
    let session = read_session(tx, session_id)?;
    let owner: Option<String> = tx
        .query_row(
            "SELECT workspace_id FROM workspace_session_ownership WHERE session_id=?1",
            [session_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| db_error("failed reading startup session ownership", e))?;
    let has_agent: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_agents WHERE session_id=?1)",
            [session_id],
            |r| r.get(0),
        )
        .map_err(|e| db_error("failed reading startup agent existence", e))?;
    let agent = if has_agent {
        Some(
            tx.query_row(
                &format!("{AGENT_SELECT} WHERE a.session_id=?1"),
                [session_id],
                workspace_agent_from_row,
            )
            .optional()
            .map_err(|e| db_error("failed reconstructing startup agent authority", e))?
            .ok_or_else(|| conflict("existing workspace agent lacks its complete profile"))?,
        )
    } else {
        None
    };
    let scope = if let Some(agent) = agent.as_ref() {
        if owner.as_deref() != Some(agent.workspace_id.as_str())
            || agent.lifecycle_state != WorkspaceAgentLifecycleState::Active
        {
            return Err(conflict(
                "startup requires the active original workspace agent owner",
            ));
        }
        StartupAttachmentScope::WorkspaceAgent {
            workspace_id: agent.workspace_id.clone(),
            agent_revision: agent.revision,
        }
    } else {
        StartupAttachmentScope::StoredSession {
            workspace_id: owner,
        }
    };
    if let Some(workspace_id) = scope.workspace_id() {
        let lifecycle: Option<String> = tx
            .query_row(
                "SELECT lifecycle_state FROM workspaces WHERE workspace_id=?1",
                [workspace_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| db_error("failed reading startup workspace lifecycle", e))?;
        if lifecycle.as_deref() != Some("active") {
            return Err(conflict("startup session workspace is not active"));
        }
    }
    let (provider, request) =
        runtime_core::startup_resume_request_from_authority(&session, agent.as_ref())?;
    let authority = StartupAttachmentAuthority {
        scope,
        provider,
        request,
        worktree_id: session.worktree_id.clone(),
    };
    Ok((session, authority))
}
pub(crate) fn reject_weak_startup_review(session: &SessionRecord) -> Result<(), RuntimeError> {
    if runtime_core::startup_review_status_blocked(&session.status) {
        return Err(RuntimeError::Unsupported(format!("session {} requires approved structured recovery evidence; its historical review status cannot authorize native startup resume",session.id)));
    }
    Ok(())
}
pub(crate) fn guard_startup_native(
    tx: &Transaction<'_>,
    session: &SessionRecord,
    authority: &StartupAttachmentAuthority,
) -> Result<(), RuntimeError> {
    guard_startup_projection(tx, session)?;
    crate::repository_delivery_native::guard_startup_delivery_claims(tx, &session.id)?;
    crate::repository_legacy_spawn_obligations::guard_legacy_native_session(tx, &session.id)?;
    crate::repository_workspace_control::guard_unresolved_interrupt(tx, &session.id)?;
    let previous_startup:Option<String>=tx.query_row("SELECT op.operation_id FROM runtime_operations op WHERE op.kind=?1 AND op.phase IN ('requested','manual_review') AND (json_extract(op.normalized_request_json,'$.command.session_id')=?2 OR EXISTS(SELECT 1 FROM runtime_operation_effects e WHERE e.operation_id=op.operation_id AND e.target_kind='provider_session' AND e.target_id=?2)) ORDER BY op.operation_id LIMIT 1",params![STARTUP_KIND,session.id],|r|r.get(0)).optional().map_err(|e|db_error("failed checking prior startup attempts",e))?;
    if let Some(operation_id) = previous_startup {
        return Err(RuntimeError::operation_unresolved(
            operation_id,
            "the original startup native attempt remains unresolved",
        ));
    }
    for kind in [
        RESUME_CLAIM,
        crate::repository_operator_native_policy::CLAIM_KIND,
        crate::repository_operator_agent_archive::CLAIM_KIND,
        crate::repository_operator_agent_create::CLAIM_KIND,
    ] {
        if let Some(claim) = claim_for(tx, kind, &session.id)? {
            return Err(RuntimeError::operation_unresolved(
                claim.owner_operation_id,
                "another native attachment operation owns this session",
            ));
        }
    }
    let blockers:i64=tx.query_row("SELECT (SELECT COUNT(*) FROM workspace_agent_rebinds WHERE agent_id=?1 AND phase IN ('intended','manual_review'))+(SELECT COUNT(*) FROM session_compaction_runs WHERE session_id=?1 AND phase IN ('intended','accepted','started','compacting','manual_review'))+(SELECT COUNT(*) FROM session_edit_reruns WHERE session_id=?1 AND phase IN ('intended','native_started','fork_observed','manual_review'))",[&session.id],|r|r.get(0)).map_err(|e|db_error("failed checking startup native attachment blockers",e))?;
    if blockers != 0 {
        return Err(conflict("another session native effect remains unresolved"));
    }
    let retirement:Option<String>=tx.query_row("SELECT f.operation_id FROM workspace_retirement_fences f WHERE f.workspace_id=?1 UNION ALL SELECT r.operation_id FROM workspace_retirement_routes r JOIN sessions s ON (s.cwd=r.route OR substr(s.cwd,1,length(r.route)+1)=r.route||'/') WHERE s.id=?2 AND NOT EXISTS(SELECT 1 FROM workspace_session_ownership o WHERE o.session_id=s.id) LIMIT 1",params![authority.scope.workspace_id(),session.id],|r|r.get(0)).optional().map_err(|e|db_error("failed checking startup retirement authority",e))?;
    if let Some(operation_id) = retirement {
        return Err(RuntimeError::operation_unresolved(
            operation_id,
            "workspace retirement withholds startup native reattachment",
        ));
    }

    Ok(())
}
pub(crate) fn terminal_metadata_witness(
    tx: &Transaction<'_>,
    session: &SessionRecord,
) -> Result<Option<StartupTerminalMetadataWitness>, RuntimeError> {
    let Some(turn_id) = session.active_turn_id.as_deref() else {
        return Ok(None);
    };
    let turn = read_turn(tx, turn_id)?;
    if turn.session_id != session.id {
        return Err(conflict(
            "startup metadata witness belongs to another session",
        ));
    }
    if turn.completed_at.is_some() {
        return Ok(None);
    }
    let has_admission: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM turn_admissions WHERE turn_id=?1)",
            [turn_id],
            |r| r.get(0),
        )
        .map_err(|e| db_error("failed reading startup metadata witness admission", e))?;
    let admission = has_admission
        .then(|| read_admission(tx, turn_id))
        .transpose()?;
    match admission.as_ref() {
        Some(admission) => {
            if admission.session_id != session.id
                || admission.provider != session.provider
                || admission.dispatch_state != TurnDispatchState::Dispatched
            {
                return Ok(None);
            }
        }
        None => {
            if !matches!(turn.status.as_str(), "in_progress" | "waiting_for_approval") {
                return Ok(None);
            }
        }
    }
    if fetch_runtime_event_by_event_id(tx, &format!("evt_turn_{turn_id}_terminal"))?.is_some() {
        return Err(conflict(
            "nonterminal startup metadata witness already has a terminal receipt",
        ));
    }
    Ok(Some(StartupTerminalMetadataWitness { turn, admission }))
}

pub(crate) fn guard_startup_projection(
    tx: &Transaction<'_>,
    session: &SessionRecord,
) -> Result<(), RuntimeError> {
    reject_weak_startup_review(session)?;
    let turn = session
        .active_turn_id
        .as_deref()
        .map(|id| read_turn(tx, id))
        .transpose()?;
    let admission = if let Some(turn_id) = session.active_turn_id.as_deref() {
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM turn_admissions WHERE turn_id=?1)",
                [turn_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking startup turn admission", error))?;
        exists.then(|| read_admission(tx, turn_id)).transpose()?
    } else {
        None
    };
    let decision_pending: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM approvals WHERE session_id=?1 AND status='decision_pending')",
        params![session.id], |row| row.get(0))
        .map_err(|error| db_error("failed checking startup approval decision", error))?;
    if let Some(reason) = runtime_core::startup_projection_refusal(
        session,
        turn.as_ref(),
        admission.as_ref(),
        decision_pending,
    ) {
        return Err(conflict(reason));
    }
    Ok(())
}
