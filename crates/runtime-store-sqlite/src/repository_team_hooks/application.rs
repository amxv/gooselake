//! Script completion and recipient-scoped model-context delivery have
//! independent durable receipts. This transaction is safe to replay after an
//! interrupted worker: either all notices and the receipt commit or none do.

use std::collections::HashSet;

use runtime_core::{
    team_hooks::validate_team_hook_script_output, RuntimeError, TeamHookEffectRecord,
    TeamHookEventName, WorkspaceSystemNoticeRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::Value;

use super::effects::{hook_is_approved, read_effect};
use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn list_dispatchable_team_hook_effects(
        &self,
        limit: usize,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        // Revoked or retired intents remain durable evidence but cannot
        // occupy the head of the runnable queue and starve later effects.
        self.list_hook_effects_by_state(
            "e.phase='intended' AND e.event_name IN
                ('post_teammate_add','post_teammate_remove',
                 'post_direct_message_send','post_broadcast_message_send')
             AND (e.event_name NOT IN
                ('post_direct_message_send','post_broadcast_message_send')
                OR NOT EXISTS (SELECT 1 FROM agent_deliveries pending
                  WHERE pending.message_id=json_extract(e.input_json,'$.subject.message_id')
                    AND pending.status IN ('pending','deferred','injecting')))
             AND EXISTS (SELECT 1 FROM runtime_events parent
               WHERE parent.event_id=e.parent_event_id AND parent.kind='workspace.hook_parent'
                 AND parent.scope='workspace' AND parent.scope_id=e.workspace_id)
             AND EXISTS (
                SELECT 1 FROM workspaces w JOIN workspace_hook_approvals a
                  ON a.workspace_id=w.workspace_id,
                  json_each(a.approved_hooks_json) approved
                WHERE w.workspace_id=e.workspace_id AND w.lifecycle_state='active'
                  AND approved.value=e.hook_json)",
            limit,
        )
    }

    pub fn list_unapplied_team_hook_effects(
        &self,
        limit: usize,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        self.list_hook_effects_by_state(
            "e.application_state='pending' AND e.event_name IN
             ('post_teammate_add','post_teammate_remove',
              'post_direct_message_send','post_broadcast_message_send')",
            limit,
        )
    }

    fn list_hook_effects_by_state(
        &self,
        clause: &str,
        limit: usize,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        // The clause is selected from two constants above, never caller SQL.
        let connection = open_connection(&self.database_path)?;
        let query = format!(
            "SELECT e.workspace_id,e.parent_event_id,e.event_name,e.hook_name
             FROM workspace_hook_effects e WHERE {clause}
             ORDER BY e.created_at_ms,e.parent_event_id,e.hook_name LIMIT ?1"
        );
        let mut statement = connection
            .prepare(&query)
            .map_err(|error| db_error("failed preparing hook work queue", error))?;
        let rows = statement
            .query_map([i64::try_from(limit.min(256)).unwrap_or(256)], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|error| db_error("failed reading hook work queue", error))?;
        let mut effects = Vec::new();
        for row in rows {
            let (workspace, parent, event, name) =
                row.map_err(|error| db_error("invalid hook work row", error))?;
            let event = TeamHookEventName::ALL
                .into_iter()
                .find(|candidate| candidate.as_str() == event)
                .ok_or_else(|| RuntimeError::Bootstrap("invalid durable hook event".into()))?;
            if let Some(record) = read_effect(&connection, &workspace, &parent, event, &name)? {
                effects.push(record);
            }
        }
        Ok(effects)
    }

    pub fn apply_team_hook_effect_output(
        &self,
        effect: &TeamHookEffectRecord,
        now: i64,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting hook application receipt", error))?;
        let record = read_effect(
            &tx,
            &effect.workspace_id,
            &effect.parent_event_id,
            effect.hook.event_name,
            &effect.hook.name,
        )?
        .ok_or_else(|| RuntimeError::NotFound("hook effect no longer exists".into()))?;
        if record.application_state != "pending" {
            return Ok(record);
        }

        let prepared = prepare_scoped_notices(&tx, &record, now)?;
        let (application_state, error_code) = match prepared {
            Ok(notices) => {
                for notice in &notices {
                    SqliteRuntimeRepository::insert_workspace_system_notice_tx(&tx, notice)?;
                }
                ("applied", None)
            }
            Err(reason) => ("rejected", Some(reason)),
        };
        tx.execute(
            "UPDATE workspace_hook_effects
             SET application_state=?5,application_error_code=?6,applied_at_ms=?7
             WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name=?3
               AND hook_name=?4 AND application_state='pending'",
            params![
                record.workspace_id,
                record.parent_event_id,
                record.hook.event_name.as_str(),
                record.hook.name,
                application_state,
                error_code,
                now
            ],
        )
        .map_err(|error| db_error("failed recording hook context application", error))?;
        let settled = read_effect(
            &tx,
            &record.workspace_id,
            &record.parent_event_id,
            record.hook.event_name,
            &record.hook.name,
        )?
        .ok_or_else(|| RuntimeError::Bootstrap("hook application receipt disappeared".into()))?;
        tx.commit()
            .map_err(|error| db_error("failed committing hook context notices", error))?;
        Ok(settled)
    }
}

fn prepare_scoped_notices(
    tx: &Transaction<'_>,
    effect: &TeamHookEffectRecord,
    now: i64,
) -> Result<Result<Vec<WorkspaceSystemNoticeRecord>, &'static str>, RuntimeError> {
    let parent_payload: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM runtime_events WHERE event_id=?1
             AND scope='workspace' AND scope_id=?2 AND kind='workspace.hook_parent'",
            params![effect.parent_event_id, effect.workspace_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed validating hook parent event", error))?;
    let Some(parent_payload) = parent_payload else {
        return Ok(Err("untrusted_parent_event"));
    };
    let parent: Value = serde_json::from_str(&parent_payload)
        .map_err(|error| RuntimeError::Bootstrap(format!("corrupt hook parent: {error}")))?;
    if parent != effect.input || !hook_is_approved(tx, &effect.workspace_id, &effect.hook)? {
        return Ok(Err("parent_or_grant_changed"));
    }
    let active: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspaces
         WHERE workspace_id=?1 AND lifecycle_state='active')",
            [&effect.workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking hook application workspace", error))?;
    if !active {
        return Ok(Err("workspace_not_active"));
    }
    let receipt = effect.receipt.as_ref();
    let Some(output) = receipt.and_then(|receipt| receipt.validated_output.as_ref()) else {
        return Ok(Err("missing_validated_output"));
    };
    let encoded = serde_json::to_vec(output).map_err(|error| {
        RuntimeError::Bootstrap(format!("invalid durable hook output: {error}"))
    })?;
    let Ok(output) = validate_team_hook_script_output(effect.hook.event_name, &encoded) else {
        return Ok(Err("invalid_durable_output"));
    };
    let Some(inject) = output.inject else {
        return Ok(Ok(Vec::new()));
    };

    let subject = effect.input.get("subject");
    let subject_kind = subject
        .and_then(|subject| subject.get("kind"))
        .and_then(Value::as_str);
    if !matches!(subject_kind, Some("member" | "message")) {
        return Ok(Err("invalid_subject"));
    }
    if subject_kind == Some("message") && !message_snapshot_matches(tx, effect, subject)? {
        return Ok(Err("message_snapshot_changed"));
    }

    let actor_id = effect.input.get("actor_agent_id").and_then(Value::as_str);
    let allowed: HashSet<&str> = effect
        .input
        .get("eligible_target_agent_ids")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|ids| ids.iter().filter_map(Value::as_str))
        .collect();
    let mut texts = Vec::new();
    if let Some(caller) = inject.caller {
        let Some(actor_id) = actor_id else {
            return Ok(Err("caller_is_not_an_agent"));
        };
        texts.push((actor_id.to_owned(), caller.text, true));
    }
    for target in inject.targets {
        if !allowed.contains(target.agent_id.as_str()) {
            return Ok(Err("unauthorized_hook_recipient"));
        }
        texts.push((target.agent_id, target.text, false));
    }
    let mut notices = Vec::new();
    let mut seen = HashSet::new();
    for (agent_id, text, is_caller) in texts {
        if !seen.insert(agent_id.clone()) {
            return Ok(Err("duplicate_hook_recipient"));
        }
        let workspace: Option<String> = tx
            .query_row(
                "SELECT a.workspace_id FROM workspace_agents a JOIN workspaces w
             ON w.workspace_id=a.workspace_id
             WHERE a.session_id=?1 AND a.lifecycle_state='active'
               AND w.lifecycle_state='active'",
                [&agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed verifying hook notice recipient", error))?;
        let Some(workspace) = workspace else {
            return Ok(Err("hook_recipient_not_active"));
        };
        if (is_caller
            || subject_kind == Some("member")
            || effect.hook.event_name == TeamHookEventName::PostBroadcastMessageSend)
            && workspace != effect.workspace_id
        {
            return Ok(Err("cross_workspace_hook_recipient"));
        }
        let parent = format!("{}:{}", effect.parent_event_id, effect.hook.name);
        notices.push(WorkspaceSystemNoticeRecord::lifecycle(
            &workspace,
            &agent_id,
            &parent,
            "hook_context",
            format!(
                "Workspace hook '{}' ({}) returned the following context (not a user message):\n{}",
                effect.hook.name,
                effect.hook.event_name.as_str(),
                text
            ),
            now,
        ));
    }
    Ok(Ok(notices))
}

fn message_snapshot_matches(
    tx: &Transaction<'_>,
    effect: &TeamHookEffectRecord,
    subject: Option<&Value>,
) -> Result<bool, RuntimeError> {
    let Some(subject) = subject else {
        return Ok(false);
    };
    let Some(message_id) = subject.get("message_id").and_then(Value::as_str) else {
        return Ok(false);
    };
    let message: Option<(String, String, String)> = tx
        .query_row(
            "SELECT sender_agent_id,scope,recipient_agent_ids_json FROM agent_messages WHERE id=?1",
            [message_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| db_error("failed validating hook message snapshot", error))?;
    let Some((sender, scope, recipients)) = message else {
        return Ok(false);
    };
    let actual_recipients: Vec<String> = serde_json::from_str(&recipients).map_err(|error| {
        RuntimeError::Bootstrap(format!("corrupt hook message roster: {error}"))
    })?;
    let listed_recipients =
        serde_json::json!(actual_recipients.iter().take(128).collect::<Vec<_>>());
    Ok(
        subject.get("sender_agent_id").and_then(Value::as_str) == Some(&sender)
            && subject.get("scope").and_then(Value::as_str) == Some(&scope)
            && subject.get("recipient_agent_ids") == Some(&listed_recipients)
            && subject.get("recipient_count").and_then(Value::as_u64)
                == u64::try_from(actual_recipients.len()).ok()
            && subject.get("recipients_truncated").and_then(Value::as_bool)
                == Some(actual_recipients.len() > 128)
            && effect.input.get("actor_agent_id").and_then(Value::as_str) == Some(&sender),
    )
}
