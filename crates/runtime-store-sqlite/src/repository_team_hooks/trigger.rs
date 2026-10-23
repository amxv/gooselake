//! Hook trigger snapshots are admitted in the same transaction as the member
//! or message mutation that caused them. No background observer reconstructs
//! an effect from a mutable current roster or configuration file.

use std::collections::HashSet;

use runtime_core::{
    RuntimeError, TeamHookEffectRecord, TeamHookEventName, TeamHookPreAddAdmission,
    TeamHookResolvedEntry,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

use super::effects::read_effect;
use crate::db::{db_error, json_to_string, open_connection};
use crate::SqliteRuntimeRepository;

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
) -> Result<Vec<TeamHookResolvedEntry>, RuntimeError> {
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
        return Ok(Vec::new());
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
        return Ok(Vec::new());
    };
    let approved: Vec<TeamHookResolvedEntry> = serde_json::from_str(&approved_json)
        .map_err(|error| RuntimeError::Bootstrap(format!("corrupt approved hooks: {error}")))?;
    let active_hooks: Vec<_> = approved
        .iter()
        .filter(|hook| hook.event_name == event)
        .collect();
    if active_hooks.is_empty() {
        return Ok(Vec::new());
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

    for hook in &active_hooks {
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
    Ok(active_hooks.into_iter().map(Clone::clone).collect())
}

impl SqliteRuntimeRepository {
    pub fn admit_pre_add_team_hooks(
        &self,
        request: &TeamHookPreAddAdmission,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        if !request.reserved_agent_id.starts_with("sess_")
            || request.reserved_agent_id.len() > 128
            || request.planned_cwd.len() > 4096
        {
            return Err(RuntimeError::InvalidState(
                "invalid reserved pre-add identity or route".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting pre-add hook admission", error))?;
        let active: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id=?1 AND lifecycle_state='active')",
            [&request.workspace_id],
            |row| row.get(0),
        ).map_err(|error| db_error("failed validating pre-add workspace", error))?;
        if !active {
            return Err(RuntimeError::Conflict(
                "cannot add member to inactive workspace".into(),
            ));
        }
        if let Some(actor) = &request.actor_agent_id {
            if !is_active_workspace_agent(&tx, &request.workspace_id, actor)? {
                return Err(RuntimeError::Conflict(
                    "pre-add actor is not an active member".into(),
                ));
            }
        }
        let event_id = format!("workspace-hook:pre-add:{}", request.reserved_agent_id);
        let subject = json!({
            "kind":"member", "action":"preparing",
            "member_agent_id": request.reserved_agent_id,
            "provider":request.provider, "model":request.model,
            "planned_cwd":request.planned_cwd,
            "member_title":request.member_title,
            "onboarding_prompt":request.onboarding_prompt,
        });
        let existing: Option<(String, String, String, String)> = tx
            .query_row(
                "SELECT scope,scope_id,kind,payload_json FROM runtime_events WHERE event_id=?1",
                [&event_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|error| db_error("failed checking duplicate pre-add parent", error))?;
        if let Some((scope, owner, kind, payload_json)) = existing {
            let payload: Value = serde_json::from_str(&payload_json).map_err(|error| {
                RuntimeError::Bootstrap(format!("corrupt pre-add event snapshot: {error}"))
            })?;
            if scope != "workspace"
                || owner != request.workspace_id
                || kind != "workspace.hook_parent"
                || payload["subject"] != subject
                || payload["actor_agent_id"].as_str() != request.actor_agent_id.as_deref()
                || payload["hook_event"] != "pre_teammate_add"
            {
                return Err(RuntimeError::Conflict(
                    "reserved pre-add identity was used for a different operation".into(),
                ));
            }
            let mut stmt = tx
                .prepare(
                    "SELECT hook_name FROM workspace_hook_effects WHERE workspace_id=?1
                 AND parent_event_id=?2 AND event_name='pre_teammate_add' ORDER BY hook_name",
                )
                .map_err(|error| db_error("failed listing replayed pre-add effects", error))?;
            let names = stmt
                .query_map(params![request.workspace_id, event_id], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| db_error("failed reading replayed pre-add effects", error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    db_error("failed decoding replayed pre-add effect names", error)
                })?;
            if names.is_empty() {
                return Err(RuntimeError::Bootstrap(
                    "pre-add parent has no durable effect rows".into(),
                ));
            }
            let mut effects = Vec::with_capacity(names.len());
            for name in names {
                effects.push(
                    read_effect(
                        &tx,
                        &request.workspace_id,
                        &event_id,
                        TeamHookEventName::PreTeammateAdd,
                        &name,
                    )?
                    .ok_or_else(|| {
                        RuntimeError::Bootstrap("pre-add replay effect missing".into())
                    })?,
                );
            }
            return Ok(effects);
        }
        let targets = request.actor_agent_id.iter().cloned().collect::<Vec<_>>();
        let hooks = queue_team_hook_trigger_tx(
            &tx,
            &request.workspace_id,
            TeamHookEventName::PreTeammateAdd,
            &event_id,
            request.actor_agent_id.as_deref(),
            subject,
            &targets,
            request.created_at_ms,
        )?;
        let mut effects = Vec::with_capacity(hooks.len());
        for hook in hooks {
            let effect = read_effect(
                &tx,
                &request.workspace_id,
                &event_id,
                hook.event_name,
                &hook.name,
            )?
            .ok_or_else(|| RuntimeError::Bootstrap("pre-add admitted effect missing".into()))?;
            effects.push(effect);
        }
        tx.commit()
            .map_err(|error| db_error("failed committing pre-add effects", error))?;
        Ok(effects)
    }
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
