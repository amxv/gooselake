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
