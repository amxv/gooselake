use std::collections::BTreeSet;

use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeClaimTransition, NewRuntimeEvent, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::SqliteRuntimeRepository;

/// Validate all raw hydrated claim keys/values and newly created target
/// absence in the caller's writer transaction, then apply changed historical
/// claim rows. This is value-snapshot CAS, not generation-aware ABA safety.
pub(crate) fn repair_historical_claims_in_transaction(
    transaction: &Transaction<'_>,
    originals: Option<&[ManagedWorktreeClaimRecord]>,
    repairs: &[ManagedWorktreeClaimRecord],
) -> Result<(), RuntimeError> {
    let mut hydrated_keys = BTreeSet::<(String, String)>::new();
    if let Some(originals) = originals {
        for original in originals {
            if !hydrated_keys.insert((original.worktree_id.clone(), original.session_id.clone())) {
                return Err(RuntimeError::Conflict(
                    "historical startup claim snapshot contains duplicate source keys".into(),
                ));
            }
            let durable = transaction
                .query_row(
                    "SELECT claim_role,created_at,released_at FROM managed_worktree_claims
                     WHERE worktree_id=?1 AND session_id=?2",
                    params![original.worktree_id, original.session_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| db_error("failed comparing historical raw claim source", error))?;
            if durable
                .as_ref()
                .is_none_or(|(role, created_at, released_at)| {
                    role != &original.claim_role
                        || *created_at != original.created_at
                        || *released_at != original.released_at
                })
            {
                return Err(RuntimeError::Conflict(
                    "historical startup claim source changed since hydration".into(),
                ));
            }
        }
    }
    for record in repairs {
        require_no_unresolved_rebind_for_claim_mutation(&transaction, &record.session_id)?;
        let existing = transaction
            .query_row(
                "SELECT claim_role,created_at,released_at FROM managed_worktree_claims
                 WHERE worktree_id=?1 AND session_id=?2",
                params![record.worktree_id, record.session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| db_error("failed reading historical claim repair authority", error))?;
        if originals.is_some()
            && !hydrated_keys.contains(&(record.worktree_id.clone(), record.session_id.clone()))
            && existing.is_some()
        {
            return Err(RuntimeError::Conflict(
                "historical startup claim target was created after hydration".into(),
            ));
        }
        if let Some((prior_role, prior_created_at, prior_released_at)) = existing {
            // Exact historical retries never UPDATE, including active
            // journal-backed or terminal rows.
            if record.released_at == prior_released_at
                && record.claim_role == prior_role
                && record.created_at == prior_created_at
            {
                continue;
            }
            if prior_released_at.is_some() {
                // Only a journaled Claim may reactivate a released row.
                return Err(RuntimeError::Conflict(
                    "historical startup claim repair cannot rewrite a terminal claim".into(),
                ));
            }

            // A committed Release then re-Claim may leave this row active
            // again. The Critical journal, not old hydration, is authority.
            // Legacy unjournaled active claims may still be normalized.
            let journaled: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM runtime_events
                     WHERE scope = 'worktree' AND scope_id = ?1 AND session_id = ?2
                       AND critical = 1
                       AND kind IN ('worktree.claimed','worktree.released'))",
                    params![record.worktree_id, record.session_id],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    db_error(
                        "failed reading active worktree claim journal authority",
                        error,
                    )
                })?;
            if journaled {
                return Err(RuntimeError::Conflict(
                    "historical startup claim repair cannot rewrite a journaled active claim"
                        .into(),
                ));
            }
        }
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
                    record.worktree_id,
                    record.session_id,
                    record.claim_role,
                    record.created_at,
                    record.released_at,
                ],
            )
            .map_err(|error| db_error("failed upserting managed worktree claim", error))?;
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// Reconstruct old claims at startup only when normalization changed the
    /// historical row. This repair path is not Critical-journaled, so fence
    /// unresolved provider rebind authority under the same writer lock as
    /// journaled Claim/Release and native rebind admission.
    pub fn upsert_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
    ) -> Result<(), RuntimeError> {
        self.repair_managed_worktree_claims_atomically(&[record.clone()])
    }

    /// All historical changed claim rows share one SQLite IMMEDIATE writer
    /// transaction. An independently committed later Release/re-Claim or
    /// unresolved native rebind blocks the whole batch, rather than leaving
    /// earlier unjournaled role normalizations committed. Journaled native
    /// transitions and the separate worktree RECORD plan remain untouched.
    pub fn repair_managed_worktree_claims_atomically(
        &self,
        repairs: &[ManagedWorktreeClaimRecord],
    ) -> Result<(), RuntimeError> {
        self.persist_historical_claim_batch(None, repairs)
    }

    /// Startup's reconstructed role/identity must still be based on the raw
    /// durable source rows it hydrated, even when it remaps a source key.
    /// A new target key must also remain absent until the writer lock.
    /// This value-snapshot guard is not a generation-aware ABA guarantee.
    pub fn repair_managed_worktree_claims_if_unchanged_atomically(
        &self,
        originals: &[ManagedWorktreeClaimRecord],
        repairs: &[ManagedWorktreeClaimRecord],
    ) -> Result<(), RuntimeError> {
        self.persist_historical_claim_batch(Some(originals), repairs)
    }

    fn persist_historical_claim_batch(
        &self,
        originals: Option<&[ManagedWorktreeClaimRecord]>,
        repairs: &[ManagedWorktreeClaimRecord],
    ) -> Result<(), RuntimeError> {
        if repairs.is_empty() {
            return Ok(());
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed starting historical worktree claim repair batch",
                    error,
                )
            })?;
        repair_historical_claims_in_transaction(&transaction, originals, repairs)?;
        transaction.commit().map_err(|error| {
            db_error(
                "failed committing historical worktree claim repair batch",
                error,
            )
        })?;
        Ok(())
    }

    /// A missing claim is a pure PK read. Never widen the successful case's
    /// native cleanup safety authorization using this limited projection.
    pub fn get_managed_worktree_claim(
        &self,
        worktree_id: &str,
        session_id: &str,
    ) -> Result<Option<ManagedWorktreeClaimRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT worktree_id,session_id,claim_role,created_at,released_at
                 FROM managed_worktree_claims
                 WHERE worktree_id=?1 AND session_id=?2",
                params![worktree_id, session_id],
                |row| {
                    Ok(ManagedWorktreeClaimRecord {
                        worktree_id: row.get(0)?,
                        session_id: row.get(1)?,
                        claim_role: row.get(2)?,
                        created_at: row.get(3)?,
                        released_at: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(|error| db_error("failed reading exact managed worktree claim", error))
    }

    /// Legacy best-effort member removal needs only this removed session's
    /// active claims in original hydrated (created_at, worktree_id) order.
    /// Schema 48 already supplies the partial seek index; the subsequent
    /// durable claim releases and native cleanup retain their own fences.
    pub fn list_active_managed_worktree_claims_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<ManagedWorktreeClaimRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let mut statement = connection
            .prepare(
                "SELECT worktree_id,session_id,claim_role,created_at,released_at
                 FROM managed_worktree_claims
                 WHERE session_id=?1 AND released_at IS NULL
                 ORDER BY created_at ASC,worktree_id ASC,session_id ASC",
            )
            .map_err(|error| db_error("failed preparing removed member's active claims", error))?;
        let rows = statement
            .query_map([session_id], |row| {
                Ok(ManagedWorktreeClaimRecord {
                    worktree_id: row.get(0)?,
                    session_id: row.get(1)?,
                    claim_role: row.get(2)?,
                    created_at: row.get(3)?,
                    released_at: row.get(4)?,
                })
            })
            .map_err(|error| db_error("failed reading removed member's active claims", error))?;
        collect_rows(rows)
    }

    /// A claim transition has no native Git effect. Commit the state AND its
    /// critical journal entry in one immediate transaction. An exact retry is
    /// a durable no-op; it cannot reset claim timestamps or duplicate history.
    pub fn transition_managed_worktree_claim(
        &self,
        record: &ManagedWorktreeClaimRecord,
        transition: ManagedWorktreeClaimTransition,
    ) -> Result<ManagedWorktreeClaimRecord, RuntimeError> {
        self.transition_managed_worktree_claim_with_guard(record, transition, None)
    }

    /// Used exclusively by explicit legacy worktree release. The service's
    /// hydrated preflight cannot fence an independent SQLite session writer;
    /// recheck the binding under the claim/event writer lock instead.
    pub fn release_managed_worktree_claim_if_unbound(
        &self,
        record: &ManagedWorktreeClaimRecord,
        observed: &ManagedWorktreeClaimRecord,
        expected_worktree_cwd: &str,
    ) -> Result<ManagedWorktreeClaimRecord, RuntimeError> {
        self.transition_managed_worktree_claim_with_guard(
            record,
            ManagedWorktreeClaimTransition::Release,
            Some((observed, expected_worktree_cwd)),
        )
    }

    fn transition_managed_worktree_claim_with_guard(
        &self,
        record: &ManagedWorktreeClaimRecord,
        transition: ManagedWorktreeClaimTransition,
        explicit_release_snapshot: Option<(&ManagedWorktreeClaimRecord, &str)>,
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
                // A separate writer can admit a native rebind after the
                // service preflight. Serialize claim admission with rebind
                // intent under this same IMMEDIATE transaction.
                require_no_unresolved_rebind_for_claim_mutation(&transaction, &record.session_id)?;
                record.clone()
            }
            ManagedWorktreeClaimTransition::Release => {
                let existing = existing.ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "worktree claim {}:{}",
                        record.worktree_id, record.session_id
                    ))
                })?;
                if let Some((observed, expected_cwd)) = explicit_release_snapshot {
                    // A separate writer may have released and re-claimed the
                    // same PK since the service read its original claim. Do
                    // not let that stale release revoke the newer Critical
                    // claim, even when the session has no native binding.
                    if &existing != observed {
                        return Err(RuntimeError::Conflict(
                            "explicit worktree claim changed since release preflight".into(),
                        ));
                    }
                    if observed.worktree_id != record.worktree_id
                        || observed.session_id != record.session_id
                    {
                        return Err(RuntimeError::Conflict(
                            "explicit worktree claim release identity mismatch".into(),
                        ));
                    }
                    // Both the current persisted route and the route observed
                    // by the service must be checked. Another writer may have
                    // changed the worktree cwd before binding the session.
                    let current_cwd: String = transaction
                        .query_row(
                            "SELECT worktree_cwd FROM managed_worktrees WHERE id = ?1",
                            [&record.worktree_id],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|error| {
                            db_error("failed reading explicit release worktree route", error)
                        })?
                        .ok_or_else(|| {
                            RuntimeError::NotFound(format!("worktree {}", record.worktree_id))
                        })?;
                    let live_binding: bool = transaction
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM sessions
                             WHERE id = ?1 AND status != 'closed'
                               AND (worktree_id = ?2 OR cwd = ?3 OR cwd = ?4))",
                            params![
                                record.session_id,
                                record.worktree_id,
                                expected_cwd,
                                current_cwd
                            ],
                            |row| row.get(0),
                        )
                        .map_err(|error| {
                            db_error("failed checking explicit release native binding", error)
                        })?;
                    if live_binding {
                        return Err(RuntimeError::Conflict(
                            "cannot release a worktree still bound to a live provider session; rebind or close the agent first".into(),
                        ));
                    }
                }
                if existing.released_at.is_some() {
                    return Ok(existing);
                }
                // Member removal runs after the roster commit; it must never
                // invalidate a reserved destination or previous claim while
                // the provider rebind outcome is unresolved.
                require_no_unresolved_rebind_for_claim_mutation(&transaction, &record.session_id)?;
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

// Native rebind admission and verified finalization also acquire SQLite's
// immediate writer lock. They update reservations in their own authority
// transactions, never through this generic claim/release entry point.
fn require_no_unresolved_rebind_for_claim_mutation(
    connection: &rusqlite::Connection,
    session_id: &str,
) -> Result<(), RuntimeError> {
    // Claims existed in migration 1, but the native rebind authority table
    // was not introduced until migration 10. Old databases legitimately
    // write claims before upgrading; a missing table is safe ONLY with a
    // complete recorded pre-rebind migration chain. Do not treat missing or
    // damaged current schema as an empty set of pending native effects.
    const REBIND_SCHEMA_VERSION: i64 = 10;
    let rebind_table_exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'workspace_agent_rebinds')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking native rebind schema", error))?;
    if !rebind_table_exists {
        let (first, latest, count): (Option<i64>, Option<i64>, i64) = connection
            .query_row(
                "SELECT MIN(version), MAX(version), COUNT(*) FROM schema_migrations",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|error| db_error("failed checking legacy claim migration authority", error))?;
        if first == Some(1)
            && latest.is_some_and(|version| (1..REBIND_SCHEMA_VERSION).contains(&version))
            && latest == Some(count)
        {
            return Ok(());
        }
        return Err(RuntimeError::Bootstrap(
            "native rebind authority table missing from migrated worktree claim schema".into(),
        ));
    }

    let unresolved: i64 = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_agent_rebinds
             WHERE agent_id = ?1 AND phase IN ('intended','manual_review'))",
            [session_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed fencing worktree claim against native rebind", error))?;
    if unresolved != 0 {
        return Err(RuntimeError::Conflict(
            "agent has an unresolved provider rebind; worktree claim changes are fenced".into(),
        ));
    }
    Ok(())
}
