use runtime_core::{
    ProviderRetirementCloseProofKind, RuntimeError, RuntimeEventCriticality, RuntimeEventScope,
    WorkspaceRetirementPlan, WorkspaceSystemNoticeRecord,
};
use rusqlite::{params, Transaction};
use serde_json::{json, Value};

use crate::db::{db_error, fetch_runtime_event_by_event_id, json_to_string};
use crate::repository_workspace_notice::workspace_notice_policy_tx;
use crate::SqliteRuntimeRepository;

fn append_event(
    tx: &Transaction<'_>,
    operation_id: &str,
    workspace_id: &str,
    kind: &str,
    agent_id: Option<&str>,
    payload: &Value,
    at_ms: i64,
) -> Result<bool, RuntimeError> {
    let event_id = match agent_id {
        Some(agent) => format!("{operation_id}:{kind}:{agent}"),
        None => format!("{operation_id}:{kind}"),
    };
    let (scope, scope_id) = match agent_id {
        Some(agent) => (RuntimeEventScope::Session, agent),
        None => (RuntimeEventScope::Workspace, workspace_id),
    };
    if let Some(existing) = fetch_runtime_event_by_event_id(tx, &event_id)? {
        if existing.scope != scope
            || existing.scope_id != scope_id
            || existing.session_id.as_deref() != agent_id
            || existing.kind != kind
            || existing.payload != *payload
            || existing.criticality != RuntimeEventCriticality::Critical
            || existing.team_id.is_some()
            || existing.turn_id.is_some()
            || existing.provider.is_some()
            || existing.provider_seq.is_some()
        {
            return Err(RuntimeError::Conflict(
                "retirement event identity conflicts with its committed receipt".into(),
            ));
        }
        return Ok(false);
    }
    let sequence: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(seq),0)+1 FROM runtime_events WHERE scope=?1 AND scope_id=?2",
            params![scope.as_str(), scope_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed allocating retirement event sequence", error))?;
    tx.execute(
        "INSERT INTO runtime_events
         (event_id,scope,scope_id,session_id,seq,kind,critical,payload_json,created_at)
         VALUES(?1,?2,?3,?4,?5,?6,1,?7,?8)",
        params![
            event_id,
            scope.as_str(),
            scope_id,
            agent_id,
            sequence,
            kind,
            json_to_string(payload)?,
            at_ms
        ],
    )
    .map_err(|error| db_error("failed atomically recording retirement event", error))?;
    Ok(true)
}

pub(crate) fn retirement_started(
    tx: &Transaction<'_>,
    plan: &WorkspaceRetirementPlan,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    let inserted = append_event(
        tx,
        &plan.operation_id,
        &plan.workspace_id,
        "workspace.retirement_started",
        None,
        &json!({
            "operation_id":plan.operation_id,"workspace_id":plan.workspace_id,
            "agents":plan.agent_ids.len(),"turns":plan.turn_ids.len(),
            "processes":plan.managed_process_ids.len(),"deliveries":plan.delivery_ids.len(),
            "worktrees_preserved":plan.preserved_worktree_ids.len(),
        }),
        at_ms,
    )?;
    if !inserted
        || !workspace_notice_policy_tx(tx, &plan.workspace_id)?
            .policy
            .workspace_retirement
    {
        return Ok(());
    }
    let display_name: String = tx
        .query_row(
            "SELECT display_name FROM workspaces WHERE workspace_id=?1",
            [&plan.workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading retirement notice workspace name", error))?;
    let mut recipients = tx
        .prepare(
            "SELECT session_id FROM workspace_agents WHERE workspace_id=?1
         AND lifecycle_state='active' ORDER BY session_id",
        )
        .map_err(|error| db_error("failed preparing retirement notice recipients", error))?;
    let recipients = recipients
        .query_map([&plan.workspace_id], |row| row.get::<_, String>(0))
        .map_err(|error| db_error("failed reading retirement notice recipients", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed decoding retirement notice recipients", error))?;
    let event_id = format!("{}:workspace.retirement_started", plan.operation_id);
    for recipient in recipients {
        let mut notice = WorkspaceSystemNoticeRecord::lifecycle(
            &plan.workspace_id, &recipient, &event_id, "workspace_retirement",
            format!("Workspace \"{display_name}\" is retiring. New work is frozen. Retired agents require explicit restoration."),
            at_ms,
        );
        // Preserve lifecycle history without inventing a delivered model turn
        // or reopening a native session after the retirement freeze.
        notice.state = "cancelled".into();
        notice.last_error = Some("workspace_retirement_freezes_new_model_input".into());
        SqliteRuntimeRepository::insert_workspace_system_notice_tx(tx, &notice)?;
    }
    Ok(())
}

pub(crate) fn retirement_agent_archived(
    tx: &Transaction<'_>,
    operation_id: &str,
    workspace_id: &str,
    agent_id: &str,
    proof_kind: ProviderRetirementCloseProofKind,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    append_event(
        tx,
        operation_id,
        workspace_id,
        "session.archived",
        Some(agent_id),
        &json!({"operation_id":operation_id,"workspace_id":workspace_id,
            "agent_id":agent_id,"reason":"workspace_retirement",
            "native_close_verified":true,"proof_kind":proof_kind}),
        at_ms,
    )?;
    Ok(())
}

pub(crate) fn retirement_completed(
    tx: &Transaction<'_>,
    operation_id: &str,
    workspace_id: &str,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    append_event(
        tx,
        operation_id,
        workspace_id,
        "workspace.retired",
        None,
        &json!({"operation_id":operation_id,"workspace_id":workspace_id,
            "lifecycle_state":"retired"}),
        at_ms,
    )?;
    Ok(())
}

pub(crate) fn quiescent_retirement_completed(
    tx: &Transaction<'_>,
    operation_id: &str,
    workspace_id: &str,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    append_event(
        tx,
        operation_id,
        workspace_id,
        "workspace.retirement_started",
        None,
        &json!({"operation_id":operation_id,"workspace_id":workspace_id,
            "settlement":"quiescent"}),
        at_ms,
    )?;
    retirement_completed(tx, operation_id, workspace_id, at_ms)
}

/// Redacted authority snapshots for the finite local/restart retirement writers.
/// Hashes retain exact diagnostic binding without exposing command or output text.
pub(crate) fn journal_retirement_observation_tx(
    tx: &Transaction<'_>,
    operation_id: &str,
    workspace_id: &str,
    kind: &'static str,
    key: &str,
    before: &Value,
    after: &Value,
    at_ms: i64,
) -> Result<Option<crate::repository_team_hooks::CriticalEventProof>, RuntimeError> {
    if before == after {
        return Ok(None);
    }
    let payload = json!({
        "operation_id":operation_id, "workspace_id":workspace_id,
        "observation_key":key,
        "before_hash":runtime_core::normalized_json_hash(before)?,
        "after_hash":runtime_core::normalized_json_hash(after)?,
        "native_replayed":false,
    });
    let event = runtime_core::NewRuntimeEvent {
        event_id: format!(
            "evt_retirement_observation_{}",
            runtime_core::normalized_json_hash(&json!({"kind":kind,"payload":payload}))?
        ),
        scope: RuntimeEventScope::Workspace,
        scope_id: workspace_id.into(),
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: at_ms,
    };
    let recorded = crate::repository_team_hooks::insert_critical_receipt_tx(tx, &event)?;
    Ok(Some(crate::repository_team_hooks::CriticalEventProof {
        event,
        sequence: recorded.seq,
    }))
}

pub(crate) fn retirement_effect_authority_tx(
    tx: &Transaction<'_>,
    operation_id: &str,
    kind: &str,
    target_id: &str,
) -> Result<Value, RuntimeError> {
    let row:(String,String,i64,Option<String>,i64)=tx.query_row(
        "SELECT effect_id,phase,attempt_count,evidence_json,updated_at FROM runtime_operation_effects
         WHERE operation_id=?1 AND effect_kind=?2 AND target_id=?3",
        params![operation_id,kind,target_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)),
    ).map_err(|error| db_error("failed reading exact retirement effect receipt", error))?;
    Ok(
        json!({"effect_id":row.0,"phase":row.1,"attempt_count":row.2,
        "evidence":row.3,"updated_at":row.4}),
    )
}

pub(crate) fn retirement_effect_snapshot_tx(
    tx: &Transaction<'_>,
    operation_id: &str,
    kind: &str,
    target_id: &str,
) -> Result<Value, RuntimeError> {
    let mut snapshot = retirement_effect_authority_tx(tx, operation_id, kind, target_id)?;
    if kind == "workspace_hook_settle" {
        let keys: Vec<String> = serde_json::from_str(target_id).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid retirement hook receipt key: {error}"))
        })?;
        if keys.len() != 3 {
            return Err(RuntimeError::Bootstrap(
                "invalid retirement hook receipt key width".into(),
            ));
        }
        let event = runtime_core::TeamHookEventName::ALL
            .into_iter()
            .find(|event| event.as_str() == keys[1])
            .ok_or_else(|| {
                RuntimeError::Bootstrap("invalid retirement hook receipt event".into())
            })?;
        let workspace_id: String = tx
            .query_row(
                "SELECT workspace_id FROM runtime_operations WHERE operation_id=?1",
                [operation_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed reading retirement hook receipt owner", error))?;
        snapshot["hook_record"] =
            serde_json::to_value(crate::repository_team_hooks::read_team_hook_effect(
                tx,
                &workspace_id,
                &keys[0],
                event,
                &keys[2],
            )?)
            .map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid retirement hook receipt: {error}"))
            })?;
    }
    Ok(snapshot)
}

pub(crate) struct RetirementPhaseProof {
    operation_id: String,
    workspace_id: String,
    sequence: u64,
    before: runtime_core::OperationPhase,
    after: runtime_core::OperationPhase,
    evidence: Value,
    at_ms: i64,
    event: crate::repository_team_hooks::CriticalEventProof,
}
pub(crate) fn verify_retirement_phase_tx(
    tx: &Transaction<'_>,
    proof: &RetirementPhaseProof,
) -> Result<(), RuntimeError> {
    crate::repository_team_hooks::verify_critical_proof_tx(tx, &proof.event)?;
    let transition: (Option<String>, String, Option<String>, i64) = tx
        .query_row(
            "SELECT from_phase,to_phase,evidence_json,created_at FROM runtime_operation_transitions
         WHERE operation_id=?1 AND sequence=?2",
            params![proof.operation_id, proof.sequence],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| {
            db_error(
                "failed verifying retirement classification transition",
                error,
            )
        })?;
    let current:(String,String,i64)=tx.query_row(
        "SELECT op.phase,f.phase,op.updated_at FROM runtime_operations op JOIN workspace_retirement_fences f
         ON f.operation_id=op.operation_id WHERE op.operation_id=?1 AND op.workspace_id=?2",
        params![proof.operation_id,proof.workspace_id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    ).map_err(|error|db_error("failed verifying retirement classification authority",error))?;
    if transition.0.as_deref() != Some(proof.before.as_str())
        || transition.1 != proof.after.as_str()
        || transition
            .2
            .as_deref()
            .map(serde_json::from_str::<Value>)
            .transpose()
            .map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid retirement transition: {error}"))
            })?
            != Some(proof.evidence.clone())
        || transition.3 != proof.at_ms
        || current.0 != proof.after.as_str()
        || current.1 != "manual_review"
        || current.2 != proof.at_ms
    {
        return Err(RuntimeError::Conflict(
            "retirement classification differs from its Critical receipt".into(),
        ));
    }
    Ok(())
}
pub(crate) fn journal_retirement_phase_tx(
    tx: &Transaction<'_>,
    operation_id: &str,
    workspace_id: &str,
    before: runtime_core::OperationPhase,
    after: runtime_core::OperationPhase,
    origin: &'static str,
    at_ms: i64,
) -> Result<RetirementPhaseProof, RuntimeError> {
    if before == after {
        return Err(RuntimeError::InvalidState(
            "retirement phase receipt requires an actual transition".into(),
        ));
    }
    let sequence:u64=tx.query_row(
        "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_operation_transitions WHERE operation_id=?1",
        [operation_id],|row|row.get(0),
    ).map_err(|error|db_error("failed allocating retirement classification transition",error))?;
    let evidence = json!({"origin":origin,"native_replayed":false});
    let event_sequence = crate::repository_team_hooks::next_critical_sequence_tx(
        tx,
        &RuntimeEventScope::Workspace,
        workspace_id,
    )?;
    crate::operation_tx::OperationAuthorityTransaction::new(tx).append_transition(
        &runtime_core::OperationTransitionRecord {
            operation_id: operation_id.into(),
            sequence,
            from_phase: Some(before),
            to_phase: after,
            evidence: Some(evidence.clone()),
            created_at: at_ms,
        },
    )?;
    let event = runtime_core::NewRuntimeEvent {
        event_id: format!("{operation_id}:event:transition:{sequence}"),
        scope: RuntimeEventScope::Workspace,
        scope_id: workspace_id.into(),
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: "operation.transitioned".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: json!({"operation_id":operation_id,"workspace_id":workspace_id,
            "sequence":sequence,"from_phase":before.as_str(),"to_phase":after.as_str()}),
        provider: None,
        provider_seq: None,
        created_at: at_ms,
    };
    let proof = RetirementPhaseProof {
        operation_id: operation_id.into(),
        workspace_id: workspace_id.into(),
        sequence,
        before,
        after,
        evidence,
        at_ms,
        event: crate::repository_team_hooks::CriticalEventProof {
            event,
            sequence: event_sequence,
        },
    };
    verify_retirement_phase_tx(tx, &proof)?;
    Ok(proof)
}
