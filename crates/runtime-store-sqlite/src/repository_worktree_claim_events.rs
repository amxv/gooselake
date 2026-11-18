use std::collections::BTreeSet;

use runtime_core::{
    ManagedWorktreeClaimRecord, ManagedWorktreeClaimTransition, NewRuntimeEvent, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{collect_rows, db_error, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_worktree_claim_generation as lifetime;
use crate::SqliteRuntimeRepository;

/// Compare exact raw source lifetimes, then preserve each existing destination's
/// own token or allocate one for a still-absent key. No historical repair may
/// reactivate a terminal claim or borrow a token from another primary key.
pub(crate) fn repair_historical_claims_in_transaction(
    transaction: &Transaction<'_>,
    originals: Option<&[ManagedWorktreeClaimRecord]>,
    repairs: &[ManagedWorktreeClaimRecord],
) -> Result<(), RuntimeError> {
    let mut hydrated_keys = BTreeSet::new();
    if let Some(originals) = originals {
        for original in originals {
            lifetime::generation(original)?;
            if !hydrated_keys.insert((original.worktree_id.clone(), original.session_id.clone())) {
                return Err(RuntimeError::Conflict(
                    "historical startup claim snapshot contains duplicate source keys".into(),
                ));
            }
            if lifetime::read(transaction, &original.worktree_id, &original.session_id)?.as_ref()
                != Some(original)
            {
                return Err(RuntimeError::Conflict(
                    "historical startup claim source changed since hydration".into(),
                ));
            }
        }
    }
    let mut planned_keys = BTreeSet::new();
    let mut written = Vec::new();
    for record in repairs {
        if !planned_keys.insert((record.worktree_id.clone(), record.session_id.clone())) {
            return Err(RuntimeError::Conflict(
                "historical claim repair repeats a destination key".into(),
            ));
        }
        require_no_unresolved_rebind_for_claim_mutation(transaction, &record.session_id)?;
        let existing = lifetime::read(transaction, &record.worktree_id, &record.session_id)?;
        if originals.is_some()
            && !hydrated_keys.contains(&(record.worktree_id.clone(), record.session_id.clone()))
            && existing.is_some()
        {
            return Err(RuntimeError::Conflict(
                "historical startup claim target was created after hydration".into(),
            ));
        }
        let mut updated = record.clone();
        if let Some(prior) = existing {
            lifetime::require_observed(record, &prior)?;
            if record == &prior {
                continue;
            }
            if prior.released_at.is_some() {
                return Err(RuntimeError::Conflict(
                    "historical startup claim repair cannot rewrite a terminal claim".into(),
                ));
            }
            let journaled: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM runtime_events
                 WHERE scope='worktree' AND scope_id=?1 AND session_id=?2 AND critical=1
                   AND kind IN ('worktree.claimed','worktree.released'))",
                    params![record.worktree_id, record.session_id],
                    |row| row.get(0),
                )
                .map_err(|e| {
                    db_error("failed reading active worktree claim journal authority", e)
                })?;
            if journaled {
                return Err(RuntimeError::Conflict(
                    "historical startup claim repair cannot rewrite a journaled active claim"
                        .into(),
                ));
            }
            updated.claim_generation = prior.claim_generation;
        } else {
            // None is an unallocated insertion intent. An explicit first-token
            // fixture/intent may assert 1, but never choose a later generation.
            if record.claim_generation.is_some_and(|value| value != 1) {
                return Err(RuntimeError::Conflict(
                    "new historical claim cannot import another key's lifetime token".into(),
                ));
            }
            updated.claim_generation = Some(1);
        }
        lifetime::write(transaction, &updated)?;
        written.push(updated);
    }
    // A later trigger/write must not alter an earlier carrier in this batch.
    for record in written {
        if lifetime::read(transaction, &record.worktree_id, &record.session_id)?.as_ref()
            != Some(&record)
        {
            return Err(RuntimeError::Conflict(
                "historical claim batch carrier changed before commit".into(),
            ));
        }
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
    /// Exact lifetime tokens fence same-value release/reclaim as well as value changes.
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
        crate::repository_legacy_native_targets::guard_legacy_native_repairs(
            &transaction,
            &[],
            repairs,
        )?;
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
        lifetime::read(&connection, worktree_id, session_id)
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
        let mut statement = connection.prepare(&format!(
            "{} WHERE session_id=?1 AND released_at IS NULL ORDER BY created_at ASC,worktree_id ASC,session_id ASC",
            lifetime::SELECT)).map_err(|e| db_error("failed preparing removed member's active claims", e))?;
        let rows = statement
            .query_map([session_id], lifetime::from_row)
            .map_err(|e| db_error("failed reading removed member's active claims", e))?;
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

        let existing = lifetime::read(&transaction, &record.worktree_id, &record.session_id)?;

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
                ManagedWorktreeClaimRecord {
                    claim_generation: Some(lifetime::next(existing.as_ref())?),
                    ..record.clone()
                }
            }
            ManagedWorktreeClaimTransition::Release => {
                let existing = existing.ok_or_else(|| {
                    RuntimeError::NotFound(format!(
                        "worktree claim {}:{}",
                        record.worktree_id, record.session_id
                    ))
                })?;
                // Lifetime equality precedes terminal replay as well as mutation.
                lifetime::require_observed(record, &existing)?;
                if let Some((observed, expected_cwd)) = explicit_release_snapshot {
                    lifetime::require_observed(observed, &existing)?;
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
                               AND (worktree_id = ?2 OR cwd = ?3 OR cwd = ?4
                                 OR substr(cwd, 1, length(?3) + 1) = ?3 || '/'
                                 OR substr(cwd, 1, length(?4) + 1) = ?4 || '/'))",
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
                // Best-effort member removal hydrates active claims before
                // releasing them. Another SQLite writer may have journaled a
                // Release and re-Claim for the same key in the meantime.
                // The generic path deliberately does not enforce the
                // explicit release's live-native-binding policy, but must
                // still reject a different claim generation under this
                // writer lock. Historical unjournaled trim/case-only role
                // normalization is equivalent and may race a generic
                // Release; do not break that long-standing compatibility.
                // The token guard above already distinguishes identical-value ABA.
                if existing.created_at != record.created_at
                    || !existing
                        .claim_role
                        .trim()
                        .eq_ignore_ascii_case(record.claim_role.trim())
                {
                    return Err(RuntimeError::Conflict(
                        "worktree claim changed since release observation".into(),
                    ));
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

        let mut legacy_targets =
            crate::repository_legacy_native_targets::LegacyNativeTargetBatch::default();
        legacy_targets.claim(&transaction, &updated)?;
        if let Some((_, observed_cwd)) = explicit_release_snapshot {
            legacy_targets.route(&transaction, Some(&record.worktree_id), Some(observed_cwd))?;
        }
        legacy_targets.guard(&transaction)?;

        lifetime::write(&transaction, &updated)?;

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
        if lifetime::read(&transaction, &updated.worktree_id, &updated.session_id)?.as_ref()
            != Some(&updated)
        {
            return Err(RuntimeError::Conflict(
                "worktree claim carrier changed during Critical event publication".into(),
            ));
        }
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
