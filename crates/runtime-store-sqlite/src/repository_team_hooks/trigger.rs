//! Hook trigger snapshots are admitted in the same transaction as the member
//! or message mutation that caused them. No background observer reconstructs
//! an effect from a mutable current roster or configuration file.

use std::collections::HashSet;

use runtime_core::{RuntimeError, TeamHookEventName, TeamHookResolvedEntry};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string};

#[allow(clippy::too_many_arguments)]
pub(crate) fn queue_team_hook_trigger_tx(
    tx: &Transaction<'_>,
    workspace_id: &str,
    event: TeamHookEventName,
    event_id: &str,
    actor_id: Option<&str>,
    subject: Value,
    eligible_targets: &[String],
    created_at: i64,
) -> Result<(), RuntimeError> {
    // Generation-1 migration fixtures may legitimately create workspace
    // agents while the schema is only at v8. Hook storage was introduced in
    // v16, so post-hooks are inert until that migration is committed.
    let hook_schema_ready: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=16)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking hook schema readiness", error))?;
    if !hook_schema_ready {
        return Ok(());
    }
    let approved_json: Option<String> = tx
        .query_row(
            "SELECT approved_hooks_json FROM workspace_hook_approvals WHERE workspace_id=?1",
            [workspace_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed snapshotting approved team hooks", error))?;
    let Some(approved_json) = approved_json else {
        return Ok(());
    };
    let approved: Vec<TeamHookResolvedEntry> = serde_json::from_str(&approved_json)
        .map_err(|error| RuntimeError::Bootstrap(format!("corrupt approved hooks: {error}")))?;
    let active_hooks: Vec<_> = approved
        .iter()
        .filter(|hook| hook.event_name == event)
        .collect();
    if active_hooks.is_empty() {
        return Ok(());
    }

    // Only a presently active workspace member can receive `inject.caller`.
    // Non-member operator labels never become authenticated agent identities.
    let actor = match actor_id {
        Some(id) if is_active_workspace_agent(tx, workspace_id, id)? => Some(id),
        _ => None,
    };
    let mut targets = Vec::new();
    let mut seen = HashSet::new();
    for target in eligible_targets.iter().take(128) {
        if seen.insert(target.as_str()) && is_active_agent(tx, target)? {
            targets.push(target.clone());
        }
    }
    // Broad broadcast snapshots remain bounded; the hook receives an explicit
    // truncation marker rather than an arbitrarily large model context input.
    let input = json!({
        "schema_version": 1,
        "event_id": event_id,
        "hook_event": event,
        "workspace_id": workspace_id,
        "actor_agent_id": actor,
        "subject": subject,
        "eligible_target_agent_ids": targets,
        "eligible_targets_truncated": eligible_targets.len() > 128,
    });
    let input_json = json_to_string(&input)?;
    if input_json.len() > 64 * 1024 {
        return Err(RuntimeError::InvalidState(
            "hook event snapshot exceeds 64 KiB".into(),
        ));
    }

    let sequence: i64 = tx.query_row(
        "SELECT COALESCE(MAX(seq),0)+1 FROM runtime_events WHERE scope='workspace' AND scope_id=?1",
        [workspace_id],
        |row| row.get(0),
    ).map_err(|error| db_error("failed allocating hook parent event sequence", error))?;
    tx.execute(
        "INSERT INTO runtime_events
         (event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
         VALUES(?1,'workspace',?2,?3,'workspace.hook_parent',0,?4,?5)",
        params![event_id, workspace_id, sequence, input_json, created_at],
    )
    .map_err(|error| db_error("failed atomically admitting hook parent event", error))?;

    for hook in active_hooks {
        let hook_json = json_to_string(&json!(hook))?;
        tx.execute(
            "INSERT INTO workspace_hook_effects
             (workspace_id,parent_event_id,event_name,hook_name,hook_json,input_json,
              phase,attempt_count,created_at_ms,updated_at_ms)
             VALUES(?1,?2,?3,?4,?5,?6,'intended',0,?7,?7)",
            params![
                workspace_id,
                event_id,
                event.as_str(),
                hook.name,
                hook_json,
                input_json,
                created_at
            ],
        )
        .map_err(|error| db_error("failed atomically admitting approved hook intent", error))?;
    }
    Ok(())
}

/// A lifecycle hook may address only current members of its own workspace.
/// Returned IDs have a stable order; the trigger adds a truncation marker when
/// its input snapshots more than 128 eligible recipients.
pub(crate) fn active_workspace_hook_targets_tx(
    tx: &Transaction<'_>,
    workspace_id: &str,
) -> Result<Vec<String>, RuntimeError> {
    let mut statement = tx
        .prepare(
            "SELECT session_id FROM workspace_agents
             WHERE workspace_id=?1 AND lifecycle_state='active' ORDER BY session_id LIMIT 129",
        )
        .map_err(|error| db_error("failed preparing active hook recipient roster", error))?;
    let rows = statement
        .query_map([workspace_id], |row| row.get::<_, String>(0))
        .map_err(|error| db_error("failed snapshotting active hook recipient roster", error))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed loading active hook recipient roster", error))
}

fn is_active_workspace_agent(
    tx: &Transaction<'_>,
    workspace_id: &str,
    agent_id: &str,
) -> Result<bool, RuntimeError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM workspace_agents
         WHERE workspace_id=?1 AND session_id=?2 AND lifecycle_state='active')",
        params![workspace_id, agent_id],
        |row| row.get(0),
    )
    .map_err(|error| db_error("failed checking hook actor identity", error))
}

fn is_active_agent(tx: &Transaction<'_>, agent_id: &str) -> Result<bool, RuntimeError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM workspace_agents
         WHERE session_id=?1 AND lifecycle_state='active')",
        [agent_id],
        |row| row.get(0),
    )
    .map_err(|error| db_error("failed checking hook recipient identity", error))
}
