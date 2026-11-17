//! Enumerate only affected keys at startup; never hydrate historical hook
//! rows just to discover a changed subset. All changed receipts commit together.
use super::effects::read_effect;
use super::journal::{journal_hook_change_tx, verify_hook_proofs_tx};
use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;
use runtime_core::{RuntimeError, TeamHookEffectPhase, TeamHookEffectRecord, TeamHookEventName};
use rusqlite::{params, Transaction, TransactionBehavior};

fn admitted_create_keeps_output(
    tx: &Transaction<'_>,
    effect: &TeamHookEffectRecord,
) -> Result<bool, RuntimeError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM runtime_operations op
         JOIN runtime_operation_effects effect ON effect.operation_id=op.operation_id
         WHERE op.kind='operator_agent_create' AND op.workspace_id=?1
           AND op.phase IN ('requested','manual_review')
           AND effect.effect_kind='provider_session_create'
           AND effect.phase IN ('intended','uncertain')
           AND effect.target_id=json_extract(?2,'$.subject.member_agent_id'))",
        params![
            effect.workspace_id,
            serde_json::to_string(&effect.input).map_err(|error| RuntimeError::Bootstrap(
                format!("invalid admitted hook input: {error}")
            ))?
        ],
        |row| row.get(0),
    )
    .map_err(|error| db_error("failed preserving admitted create hook output", error))
}

impl SqliteRuntimeRepository {
    /// Restart never reissues a shell. The original admitted-create exception
    /// preserves its output without inferring provider creation from that output.
    pub fn quarantine_started_team_hook_effects(&self, at_ms: i64) -> Result<usize, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed opening hook recovery transaction", error))?;
        let keys = {
            let mut statement = tx.prepare(
                "SELECT workspace_id,parent_event_id,event_name,hook_name FROM workspace_hook_effects
                 WHERE phase='started'
                    OR (event_name='pre_teammate_add' AND phase='intended')
                    OR (event_name='pre_teammate_add' AND application_state='pending'
                        AND NOT EXISTS (SELECT 1 FROM runtime_operations op
                          JOIN runtime_operation_effects effect ON effect.operation_id=op.operation_id
                          WHERE op.kind='operator_agent_create'
                            AND op.workspace_id=workspace_hook_effects.workspace_id
                            AND op.phase IN ('requested','manual_review')
                            AND effect.effect_kind='provider_session_create'
                            AND effect.phase IN ('intended','uncertain')
                            AND effect.target_id=json_extract(
                                workspace_hook_effects.input_json,'$.subject.member_agent_id')))
                 ORDER BY workspace_id,parent_event_id,event_name,hook_name"
            ).map_err(|error| db_error("failed preparing changed hook restart keys", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(|error| db_error("failed reading changed hook restart keys", error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed decoding changed hook restart keys", error))?;
            rows
        };
        // Freeze only the selected carriers before any receipt can affect a later key.
        let mut selected = Vec::with_capacity(keys.len());
        for (workspace, parent, event, name) in keys {
            let event = TeamHookEventName::ALL
                .into_iter()
                .find(|candidate| candidate.as_str() == event)
                .ok_or_else(|| RuntimeError::Bootstrap("invalid changed hook event".into()))?;
            let before = read_effect(&tx, &workspace, &parent, event, &name)?.ok_or_else(|| {
                RuntimeError::Bootstrap("changed hook restart key disappeared".into())
            })?;
            selected.push((workspace, parent, event, name, before));
        }
        let mut quarantined = 0;
        let mut proofs = Vec::with_capacity(selected.len());
        for (workspace, parent, event, name, before) in selected {
            if read_effect(&tx, &workspace, &parent, event, &name)?.as_ref() != Some(&before) {
                return Err(RuntimeError::Conflict(
                    "selected hook restart authority changed before classification".into(),
                ));
            }
            let mut after = before.clone();
            if before.phase == TeamHookEffectPhase::Started {
                after.phase = TeamHookEffectPhase::ManualReview;
                quarantined += 1;
            }
            if event == TeamHookEventName::PreTeammateAdd
                && before.phase == TeamHookEffectPhase::Intended
            {
                after.phase = TeamHookEffectPhase::ManualReview;
                after.application_state = "rejected".into();
                after.application_error_code = Some("pre_add_orphaned_before_execution".into());
            }
            if event == TeamHookEventName::PreTeammateAdd
                && after.application_state == "pending"
                && !admitted_create_keeps_output(&tx, &before)?
            {
                after.application_state = "rejected".into();
                after.application_error_code = Some("pre_add_provider_outcome_unknown".into());
            }
            after.updated_at_ms = at_ms;
            let changed = tx
                .execute(
                    "UPDATE workspace_hook_effects SET phase=?5,application_state=?6,
                   application_error_code=?7,updated_at_ms=?8
                 WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name=?3 AND hook_name=?4
                   AND phase=?9 AND application_state=?10 AND updated_at_ms=?11",
                    params![
                        workspace,
                        parent,
                        event.as_str(),
                        name,
                        after.phase.as_str(),
                        after.application_state,
                        after.application_error_code,
                        at_ms,
                        before.phase.as_str(),
                        before.application_state,
                        before.updated_at_ms
                    ],
                )
                .map_err(|error| {
                    db_error(
                        "failed recording changed hook startup classification",
                        error,
                    )
                })?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "hook restart classification changed concurrently".into(),
                ));
            }
            proofs.push(journal_hook_change_tx(
                &tx,
                Some(&before),
                &after,
                "startup",
                at_ms,
            )?);
        }
        verify_hook_proofs_tx(&tx, &proofs)?;
        tx.commit()
            .map_err(|error| db_error("failed committing hook restart review", error))?;
        Ok(quarantined)
    }
}
