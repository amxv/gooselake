use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeClaimTransition, NewRuntimeEvent, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    /// A claim transition has no native Git effect. Commit the state AND its
    /// critical journal entry in one immediate transaction. An exact retry is
    /// a durable no-op; it cannot reset claim timestamps or duplicate history.
    pub fn transition_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
        transition: ManagedWorktreeClaimTransition,
    ) -> Result<ManagedWorktreeClaimRecord, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting worktree claim event transaction", error))?;

        let existing = transaction
            .query_row(
                "SELECT claim_role, created_at, released_at FROM managed_worktree_claims
                 WHERE worktree_id = ?1 AND session_id = ?2",
                params![record.worktree_id, record.session_id],
                |row| {
                    Ok(ManagedWorktreeClaimRecord {
                        worktree_id: record.worktree_id.clone(),
                        session_id: record.session_id.clone(),
                        claim_role: row.get(0)?,
                        created_at: row.get(1)?,
                        released_at: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|error| db_error("failed reading worktree claim transition", error))?;

        let updated = match transition {
            ManagedWorktreeClaimTransition::Claim => {
                if record.released_at.is_some() {
                    return Err(RuntimeError::InvalidState(
                        "claim activation cannot carry a release timestamp".into(),
                    ));
                }
                // Enforce cross-worktree exclusivity across independent
                // processes, not just the service's in-memory repository lock.
                let foreign_claim: Option<String> = transaction
                    .query_row(
                        "SELECT worktree_id FROM managed_worktree_claims
                         WHERE session_id = ?1 AND worktree_id != ?2 AND released_at IS NULL
                         LIMIT 1",
                        params![record.session_id, record.worktree_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| {
                        db_error("failed checking foreign active worktree claim", error)
                    })?;
                if let Some(foreign_claim) = foreign_claim {
                    return Err(RuntimeError::Conflict(format!(
                        "session {} already claims worktree {}",
                        record.session_id, foreign_claim
                    )));
                }
                if let Some(active) = existing
                    .as_ref()
                    .filter(|claim| claim.released_at.is_none())
                {
                    if active.claim_role == record.claim_role {
                        return Ok(active.clone());
                    }
                    return Err(RuntimeError::Conflict(
                        "active worktree claim role cannot be overwritten".into(),
                    ));
                }
                record.clone()
            }
            ManagedWorktreeClaimTransition::Release => {
                let existing = existing.ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "worktree claim {}:{}",
                        record.worktree_id, record.session_id
                    ))
                })?;
                if existing.released_at.is_some() {
                    return Ok(existing);
                }
                let released_at = record.released_at.ok_or_else(|| {
                    RuntimeError::InvalidState("worktree release needs a timestamp".into())
                })?;
                ManagedWorktreeClaimRecord {
                    released_at: Some(released_at),
                    ..existing
                }
            }
        };

        transaction
            .execute(
                "INSERT INTO managed_worktree_claims (
                    worktree_id, session_id, claim_role, created_at, released_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(worktree_id, session_id) DO UPDATE SET
                    claim_role = excluded.claim_role,
                    created_at = excluded.created_at,
                    released_at = excluded.released_at",
                params![
                    updated.worktree_id,
                    updated.session_id,
                    updated.claim_role,
                    updated.created_at,
                    updated.released_at,
                ],
            )
            .map_err(|error| db_error("failed persisting worktree claim transition", error))?;

        let next_seq: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM runtime_events
                 WHERE scope = 'worktree' AND scope_id = ?1",
                [&updated.worktree_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed computing worktree claim event identity", error))?;
        // The scope sequence is serialized under the transaction's immediate
        // lock. It makes the event ID stable for this committed transition;
        // retries detect the effective state before allocating another ID.
        let event = NewRuntimeEvent {
            event_id: format!(
                "evt_wt_claim_{}_{}_{}",
                updated.worktree_id.len(),
                updated.worktree_id,
                next_seq
            ),
            scope: RuntimeEventScope::Worktree,
            scope_id: updated.worktree_id.clone(),
            session_id: Some(updated.session_id.clone()),
            team_id: None,
            turn_id: None,
            kind: match transition {
                ManagedWorktreeClaimTransition::Claim => "worktree.claimed",
                ManagedWorktreeClaimTransition::Release => "worktree.released",
            }
            .into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: serde_json::json!({ "claim": updated }),
            provider: None,
            provider_seq: None,
            created_at: updated.released_at.unwrap_or(updated.created_at),
        };
        insert_runtime_event_in_transaction(&transaction, &event)?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing atomic worktree claim event", error))?;
        Ok(updated)
    }
}
