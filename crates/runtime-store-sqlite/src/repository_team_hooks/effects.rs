use runtime_core::{
    RuntimeError, TeamHookEffectAdmission, TeamHookEffectPhase, TeamHookEffectReceipt,
    TeamHookEffectRecord, TeamHookEventName, TeamHookResolvedEntry,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::SqliteRuntimeRepository;

fn decode_json<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, RuntimeError> {
    serde_json::from_str(text)
        .map_err(|error| RuntimeError::Bootstrap(format!("corrupt team-hook effect: {error}")))
}

const EFFECT_SELECT: &str = "SELECT hook_json, input_json, phase, attempt_count,
    receipt_json, created_at_ms, updated_at_ms, application_state,
    application_error_code, applied_at_ms FROM workspace_hook_effects
    WHERE workspace_id = ?1 AND parent_event_id = ?2 AND event_name = ?3 AND hook_name = ?4";

pub(super) fn read_effect(
    connection: &Connection,
    workspace_id: &str,
    parent_event_id: &str,
    event_name: TeamHookEventName,
    hook_name: &str,
) -> Result<Option<TeamHookEffectRecord>, RuntimeError> {
    let row: Option<(
        String,
        String,
        String,
        i64,
        Option<String>,
        i64,
        i64,
        String,
        Option<String>,
        Option<i64>,
    )> = connection
        .query_row(
            EFFECT_SELECT,
            params![
                workspace_id,
                parent_event_id,
                event_name.as_str(),
                hook_name
            ],
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
                ))
            },
        )
        .optional()
        .map_err(|error| db_error("failed reading team-hook effect", error))?;
    let Some((
        hook_json,
        input_json,
        phase,
        count,
        receipt_json,
        created,
        updated,
        application_state,
        application_error_code,
        applied_at_ms,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(TeamHookEffectRecord {
        workspace_id: workspace_id.to_owned(),
        parent_event_id: parent_event_id.to_owned(),
        hook: decode_json::<TeamHookResolvedEntry>(&hook_json)?,
        input: decode_json::<serde_json::Value>(&input_json)?,
        phase: TeamHookEffectPhase::from_str(&phase)
            .ok_or_else(|| RuntimeError::Bootstrap("corrupt team-hook effect phase".into()))?,
        attempt_count: u32::try_from(count).map_err(|_| {
            RuntimeError::Bootstrap("invalid team-hook effect attempt count".into())
        })?,
        receipt: receipt_json
            .as_deref()
            .map(decode_json::<TeamHookEffectReceipt>)
            .transpose()?,
        created_at_ms: created,
        updated_at_ms: updated,
        application_state,
        application_error_code,
        applied_at_ms,
    }))
}

impl SqliteRuntimeRepository {
    /// Admission is valid only for a trusted canonical event which explicitly
    /// belongs to this workspace, and an exact operator-approved hook image.
    /// No caller can supply a new command through this API.
    pub fn admit_team_hook_effect(
        &self,
        admission: &TeamHookEffectAdmission,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        let input_json = json_to_string(&admission.input)?;
        if !admission.input.is_object() || input_json.len() > 64 * 1024 {
            return Err(RuntimeError::InvalidState(
                "hook input must be a bounded object".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting team-hook effect admission", error))?;
        if let Some(existing) = read_effect(
            &tx,
            &admission.workspace_id,
            &admission.parent_event_id,
            admission.hook.event_name,
            &admission.hook.name,
        )? {
            if existing.hook != admission.hook || existing.input != admission.input {
                return Err(RuntimeError::Conflict(
                    "hook event/key was already admitted with different input".into(),
                ));
            }
            return Ok(existing);
        }
        if admission
            .input
            .get("event_id")
            .and_then(serde_json::Value::as_str)
            != Some(admission.parent_event_id.as_str())
            || admission
                .input
                .get("hook_event")
                .and_then(serde_json::Value::as_str)
                != Some(admission.hook.event_name.as_str())
        {
            return Err(RuntimeError::InvalidState(
                "hook effect input does not match its canonical parent event and hook event".into(),
            ));
        }
        let valid_parent: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_events e JOIN workspaces w
                 ON w.workspace_id = ?1 AND w.lifecycle_state = 'active'
                 WHERE e.event_id = ?2 AND json_valid(e.payload_json)
                 AND json_extract(e.payload_json,'$.workspace_id') = ?1)",
                params![admission.workspace_id, admission.parent_event_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed verifying canonical hook parent event", error))?;
        if !valid_parent {
            return Err(RuntimeError::InvalidState(
                "hook effect needs a persisted canonical workspace parent event".into(),
            ));
        }
        if !hook_is_approved(&tx, &admission.workspace_id, &admission.hook)? {
            return Err(RuntimeError::Conflict(
                "hook command is not operator approved".into(),
            ));
        }
        tx.execute(
            "INSERT INTO workspace_hook_effects (
                workspace_id,parent_event_id,event_name,hook_name,hook_json,input_json,
                phase,attempt_count,receipt_json,created_at_ms,updated_at_ms
             ) VALUES (?1,?2,?3,?4,?5,?6,'intended',0,NULL,?7,?7)",
            params![
                admission.workspace_id,
                admission.parent_event_id,
                admission.hook.event_name.as_str(),
                admission.hook.name,
                json_to_string(&serde_json::to_value(&admission.hook).map_err(|error| {
                    RuntimeError::Bootstrap(format!("invalid hook image: {error}"))
                })?)?,
                input_json,
                admission.created_at_ms,
            ],
        )
        .map_err(|error| db_error("failed persisting intended team-hook effect", error))?;
        let result = read_effect(
            &tx,
            &admission.workspace_id,
            &admission.parent_event_id,
            admission.hook.event_name,
            &admission.hook.name,
        )?
        .ok_or_else(|| RuntimeError::Bootstrap("admitted hook effect missing".into()))?;
        tx.commit()
            .map_err(|error| db_error("failed committing hook effect intent", error))?;
        Ok(result)
    }

    pub fn get_team_hook_effect(
        &self,
        workspace_id: &str,
        parent_event_id: &str,
        event: TeamHookEventName,
        hook_name: &str,
    ) -> Result<Option<TeamHookEffectRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        read_effect(&connection, workspace_id, parent_event_id, event, hook_name)
    }

    /// A persisted STARTED claim comes strictly before any native shell call.
    /// No retry may acquire it if execution might already have happened.
    pub fn claim_team_hook_effect(
        &self,
        workspace_id: &str,
        parent_event_id: &str,
        event: TeamHookEventName,
        hook_name: &str,
        at_ms: i64,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting hook claim", error))?;
        let current = read_effect(&tx, workspace_id, parent_event_id, event, hook_name)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("hook effect {parent_event_id}/{hook_name}"))
            })?;
        if current.phase != TeamHookEffectPhase::Intended {
            return Err(RuntimeError::Conflict(
                "hook effect already claimed or terminal".into(),
            ));
        }
        let workspace_active: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id=?1 AND lifecycle_state='active')",
                [workspace_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking hook execution workspace", error))?;
        if !workspace_active {
            return Err(RuntimeError::Conflict(
                "retired workspaces cannot dispatch hook effects".into(),
            ));
        }
        if !hook_is_approved(&tx, workspace_id, &current.hook)? {
            return Err(RuntimeError::Conflict(
                "hook approval was revoked or changed before execution".into(),
            ));
        }
        tx.execute(
            "UPDATE workspace_hook_effects SET phase='started',attempt_count=attempt_count+1,updated_at_ms=?5
             WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name=?3 AND hook_name=?4 AND phase='intended'",
            params![workspace_id, parent_event_id, event.as_str(), hook_name, at_ms],
        )
        .map_err(|error| db_error("failed claiming hook effect", error))?;
        let claimed = read_effect(&tx, workspace_id, parent_event_id, event, hook_name)?
            .ok_or_else(|| RuntimeError::Bootstrap("claimed hook effect missing".into()))?;
        tx.commit()
            .map_err(|error| db_error("failed committing hook claim", error))?;
        Ok(claimed)
    }

    pub fn finish_team_hook_effect(
        &self,
        workspace_id: &str,
        parent_event_id: &str,
        event: TeamHookEventName,
        hook_name: &str,
        outcome: TeamHookEffectPhase,
        receipt: TeamHookEffectReceipt,
        at_ms: i64,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        if !matches!(
            outcome,
            TeamHookEffectPhase::Succeeded
                | TeamHookEffectPhase::Failed
                | TeamHookEffectPhase::TimedOut
        ) {
            return Err(RuntimeError::InvalidState(
                "invalid hook terminal outcome".into(),
            ));
        }
        let receipt_json =
            json_to_string(&serde_json::to_value(&receipt).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid hook receipt: {error}"))
            })?)?;
        if receipt_json.len() > 16 * 1024
            || receipt.error_code.as_deref().is_some_and(|code| {
                code.len() > 64
                    || !code
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            })
        {
            return Err(RuntimeError::InvalidState(
                "unbounded or unsafe hook receipt".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting hook receipt", error))?;
        let prior = read_effect(&tx, workspace_id, parent_event_id, event, hook_name)?.ok_or_else(
            || RuntimeError::NotFound(format!("hook effect {parent_event_id}/{hook_name}")),
        )?;
        if prior.phase == outcome && prior.receipt.as_ref() == Some(&receipt) {
            return Ok(prior);
        }
        if prior.phase != TeamHookEffectPhase::Started {
            return Err(RuntimeError::Conflict(
                "hook effect was not claimable for terminal receipt".into(),
            ));
        }
        tx.execute(
            "UPDATE workspace_hook_effects SET phase=?5,receipt_json=?6,updated_at_ms=?7,
             application_state=CASE WHEN ?5='succeeded' THEN 'pending' ELSE 'rejected' END
             WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name=?3 AND hook_name=?4 AND phase='started'",
            params![workspace_id, parent_event_id, event.as_str(), hook_name, outcome.as_str(), receipt_json, at_ms],
        )
        .map_err(|error| db_error("failed recording hook receipt", error))?;
        let recorded = read_effect(&tx, workspace_id, parent_event_id, event, hook_name)?
            .ok_or_else(|| RuntimeError::Bootstrap("received hook effect missing".into()))?;
        tx.commit()
            .map_err(|error| db_error("failed committing hook receipt", error))?;
        Ok(recorded)
    }

    /// Run only at a single-runtime restart boundary, before new hook work.
    /// A shell may have run, so never transition this state back to intended.
    pub fn quarantine_started_team_hook_effects(&self, at_ms: i64) -> Result<usize, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed opening hook recovery transaction", error))?;
        let quarantined = tx
            .execute(
                "UPDATE workspace_hook_effects SET phase='manual_review',updated_at_ms=?1
                 WHERE phase='started'",
                [at_ms],
            )
            .map_err(|error| db_error("failed quarantining ambiguous hook execution", error))?;
        // Pre-add is blocking: it has no background dispatcher or agent yet.
        // After a crash we cannot infer whether native creation happened from
        // the shell receipt. Preserve evidence and require operator review.
        tx.execute(
            "UPDATE workspace_hook_effects SET phase='manual_review',
               application_state='rejected',
               application_error_code='pre_add_orphaned_before_execution',
               updated_at_ms=?1
             WHERE event_name='pre_teammate_add' AND phase='intended'",
            [at_ms],
        )
        .map_err(|error| db_error("failed classifying abandoned pre-add intents", error))?;
        tx.execute(
            "UPDATE workspace_hook_effects SET application_state='rejected',
               application_error_code='pre_add_provider_outcome_unknown',updated_at_ms=?1
             WHERE event_name='pre_teammate_add' AND application_state='pending'",
            [at_ms],
        )
        .map_err(|error| db_error("failed classifying unapplied pre-add outputs", error))?;
        tx.commit()
            .map_err(|error| db_error("failed committing hook restart review", error))?;
        Ok(quarantined)
    }

    pub fn list_team_hook_effects(
        &self,
        workspace_id: &str,
        limit: usize,
    ) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let mut statement = connection.prepare(
            "SELECT parent_event_id,event_name,hook_name FROM workspace_hook_effects
             WHERE workspace_id=?1 ORDER BY created_at_ms DESC,parent_event_id DESC,hook_name DESC LIMIT ?2",
        ).map_err(|error| db_error("failed preparing team-hook effect listing", error))?;
        let rows = statement
            .query_map(
                params![workspace_id, i64::try_from(limit.min(100)).unwrap_or(100)],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .map_err(|error| db_error("failed listing team-hook effects", error))?;
        let mut items = Vec::new();
        for row in rows {
            let (parent, event, name) =
                row.map_err(|error| db_error("failed reading team-hook effect list", error))?;
            let event = TeamHookEventName::ALL
                .into_iter()
                .find(|e| e.as_str() == event)
                .ok_or_else(|| RuntimeError::Bootstrap("corrupt team-hook event name".into()))?;
            if let Some(effect) = read_effect(&connection, workspace_id, &parent, event, &name)? {
                items.push(effect);
            }
        }
        Ok(items)
    }
}

pub(crate) fn hook_is_approved(
    connection: &Connection,
    workspace_id: &str,
    hook: &TeamHookResolvedEntry,
) -> Result<bool, RuntimeError> {
    let approved: Option<String> = connection
        .query_row(
            "SELECT approved_hooks_json FROM workspace_hook_approvals WHERE workspace_id=?1",
            [workspace_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed checking team-hook execution grant", error))?;
    approved
        .map(|json| {
            serde_json::from_str::<Vec<TeamHookResolvedEntry>>(&json)
                .map(|hooks| hooks.contains(hook))
                .map_err(|error| RuntimeError::Bootstrap(format!("corrupt hook grant: {error}")))
        })
        .transpose()
        .map(|allowed| allowed.unwrap_or(false))
}
