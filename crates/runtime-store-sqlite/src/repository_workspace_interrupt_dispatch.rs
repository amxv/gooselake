//! The first interrupt claim retains the legacy fence and effect update in
//! one writer transaction. Known interrupt outcomes use existing receipts.
use crate::db::{db_error, open_connection};
use crate::repository_workspace_control::INTERRUPT_EFFECT_KIND;
use crate::SqliteRuntimeRepository;
use runtime_core::RuntimeError;
use rusqlite::{params, TransactionBehavior};

impl SqliteRuntimeRepository {
    pub fn mark_workspace_interrupt_started(
        &self,
        operation_id: &str,
        agent_id: &str,
        turn_id: &str,
        changed_at: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning first workspace interrupt", error))?;
        let first_interrupt: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_operation_effects
             WHERE operation_id=?1 AND effect_kind=?5 AND target_id=?2
               AND phase='intended' AND json_extract(evidence_json,'$.turn_id')=?3
               AND typeof(attempt_count)='integer' AND attempt_count>=0 AND attempt_count<?4)",
                params![
                    operation_id,
                    agent_id,
                    turn_id,
                    i64::MAX,
                    INTERRUPT_EFFECT_KIND
                ],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error("failed checking first workspace interrupt authority", error)
            })?;
        if !first_interrupt {
            return Err(RuntimeError::Conflict(format!(
                "workspace interrupt effect for agent {agent_id} is not safely dispatchable"
            )));
        }
        crate::repository_legacy_spawn_obligations::guard_legacy_native_session(&tx, agent_id)?;
        crate::repository_startup_reattachment::guard_resume_claim(&tx, agent_id)?;
        let changed = tx
            .execute(
                "UPDATE runtime_operation_effects
                 SET phase = 'started', attempt_count = attempt_count + 1, updated_at = ?4
                 WHERE operation_id = ?1 AND effect_kind = ?5 AND target_id = ?2
                   AND phase = 'intended' AND json_extract(evidence_json, '$.turn_id') = ?3
                   AND typeof(attempt_count)='integer' AND attempt_count >= 0
                   AND attempt_count < ?6",
                params![
                    operation_id,
                    agent_id,
                    turn_id,
                    changed_at,
                    INTERRUPT_EFFECT_KIND,
                    i64::MAX
                ],
            )
            .map_err(|error| db_error("failed marking workspace interrupt started", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "workspace interrupt effect for agent {agent_id} is not safely dispatchable"
            )));
        }
        tx.commit()
            .map_err(|error| db_error("failed committing first workspace interrupt", error))?;
        Ok(())
    }
}
