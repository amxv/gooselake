use runtime_core::{ProviderCompactSessionOutcome, RuntimeError, SessionCompactionRun};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

fn load(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<SessionCompactionRun>, RuntimeError> {
    let raw: Option<String> = connection
        .query_row(
            "SELECT record_json FROM session_compaction_runs WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed reading compaction run", error))?;
    raw.map(|record| {
        serde_json::from_str(&record)
            .map_err(|error| RuntimeError::Bootstrap(format!("corrupt compaction run: {error}")))
    })
    .transpose()
}

fn encode(run: &SessionCompactionRun) -> Result<String, RuntimeError> {
    serde_json::to_string(run)
        .map_err(|error| RuntimeError::Bootstrap(format!("invalid compaction run: {error}")))
}

impl SqliteRuntimeRepository {
    pub fn recover_session_compactions(
        &self,
        now_ms: i64,
    ) -> Result<Vec<SessionCompactionRun>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning compaction startup recovery", error))?;
        let raws = {
            let mut stmt = tx
                .prepare(
                    "SELECT record_json FROM session_compaction_runs
                          WHERE phase IN ('intended', 'accepted') ORDER BY run_id",
                )
                .map_err(|error| db_error("failed querying compaction recovery", error))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| db_error("failed loading pending compactions", error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed collecting compaction recovery", error))?;
            rows
        };
        let mut recovered = Vec::new();
        for raw in raws {
            let mut run: SessionCompactionRun = serde_json::from_str(&raw).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid compaction recovery row: {error}"))
            })?;
            let old_phase = run.phase.clone();
            run.phase = "manual_review".into();
            run.error_code = Some("restart_native_compaction_outcome_unverified".into());
            run.updated_at_ms = now_ms.max(run.updated_at_ms);
            run.validate()?;
            let changed = tx
                .execute(
                    "UPDATE session_compaction_runs SET phase = 'manual_review',
                 record_json = ?2, updated_at_ms = ?3 WHERE run_id = ?1 AND phase = ?4",
                    params![run.run_id, encode(&run)?, run.updated_at_ms, old_phase],
                )
                .map_err(|error| db_error("failed fencing unverified compaction", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "concurrent compaction changed during startup reconciliation".into(),
                ));
            }
            recovered.push(run);
        }
        tx.commit()
            .map_err(|error| db_error("failed committing compaction startup recovery", error))?;
        Ok(recovered)
    }

    pub fn resolve_session_compaction_review(
        &self,
        run_id: &str,
        phase: &str,
        evidence: &str,
        now_ms: i64,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        let evidence = evidence.trim();
        if !matches!(phase, "completed" | "failed") || evidence.len() < 8 || evidence.len() > 2048 {
            return Err(RuntimeError::InvalidState(
                "manual compaction review requires completed/failed and 8-2048 evidence characters"
                    .into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning compaction review", error))?;
        let mut run = load(&tx, run_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("compaction run {run_id}")))?;
        if run.phase != "manual_review" {
            return Err(RuntimeError::Conflict(
                "only unresolved manual-review compactions can be explicitly settled".into(),
            ));
        }
        if phase == "completed" {
            let current: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM sessions s
                     JOIN workspace_agents a ON a.session_id = s.id
                     WHERE s.id = ?1 AND s.provider = ?2
                       AND s.provider_session_ref = ?3
                       AND s.canonical_provider_session_ref IS ?4
                       AND a.revision = ?5 AND a.lifecycle_state = 'active'",
                    params![
                        run.agent_id,
                        run.provider.as_str(),
                        run.provider_session_ref,
                        run.canonical_provider_session_ref,
                        i64::try_from(run.agent_revision).unwrap_or(-1),
                    ],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    db_error("failed checking reviewed compaction attachment", error)
                })?;
            if current != 1 {
                return Err(RuntimeError::Conflict(
                    "cannot confirm completion against a superseded provider attachment".into(),
                ));
            }
        }
        run.phase = phase.into();
        run.outcome = (phase == "completed").then_some(ProviderCompactSessionOutcome::Accepted);
        run.error_code = (phase == "failed").then_some("operator_verified_not_completed".into());
        run.review_evidence = Some(evidence.into());
        run.updated_at_ms = now_ms.max(run.updated_at_ms);
        run.validate()?;
        tx.execute(
            "UPDATE session_compaction_runs SET phase = ?2, record_json = ?3, updated_at_ms = ?4
             WHERE run_id = ?1 AND phase = 'manual_review'",
            params![run_id, phase, encode(&run)?, run.updated_at_ms],
        )
        .map_err(|error| db_error("failed committing operator compaction review", error))?;
        tx.commit()
            .map_err(|error| db_error("failed committing reviewed compaction outcome", error))?;
        Ok(run)
    }

    pub fn get_session_compaction(
        &self,
        run_id: &str,
    ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
        load(&open_connection(&self.database_path)?, run_id)
    }

    pub fn get_session_compaction_by_key(
        &self,
        agent_id: &str,
        key: &str,
    ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let run_id: Option<String> = connection
            .query_row(
                "SELECT run_id FROM session_compaction_runs
                 WHERE session_id = ?1 AND idempotency_key = ?2",
                params![agent_id, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed looking up compaction idempotency key", error))?;
        run_id
            .map(|id| load(&connection, &id))
            .transpose()
            .map(Option::flatten)
    }

    pub fn unresolved_session_compaction(&self, agent_id: &str) -> Result<bool, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM session_compaction_runs
                 WHERE session_id = ?1 AND phase IN ('intended','accepted','manual_review')",
                [agent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking unresolved compaction", error))?;
        Ok(count != 0)
    }

    /// Commit before provider dispatch. Identity, policy revision and idle
    /// state are read atomically against the same session row.
    pub fn begin_session_compaction(
        &self,
        run: &SessionCompactionRun,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        run.validate()?;
        if run.phase != "intended" || run.outcome.is_some() {
            return Err(RuntimeError::InvalidState(
                "new compaction must be an intended operation".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed opening compaction admission", error))?;
        if let Some(key) = run.idempotency_key.as_deref() {
            let existing: Option<String> = tx
                .query_row(
                    "SELECT run_id FROM session_compaction_runs
                     WHERE session_id = ?1 AND idempotency_key = ?2",
                    params![run.agent_id, key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| db_error("failed checking compaction replay", error))?;
            if let Some(id) = existing {
                return load(&tx, &id)?.ok_or_else(|| RuntimeError::NotFound(id));
            }
        }
        let eligible: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM sessions s
                 JOIN workspace_agents a ON a.session_id = s.id
                 JOIN workspaces w ON w.workspace_id = a.workspace_id
                 WHERE s.id = ?1 AND s.provider = ?2
                   AND s.provider_session_ref = ?3
                   AND s.canonical_provider_session_ref IS ?4
                   AND a.revision = ?5 AND s.updated_at = ?6
                   AND s.status = 'ready' AND s.active_turn_id IS NULL
                   AND a.lifecycle_state = 'active' AND w.lifecycle_state = 'active'",
                params![
                    run.agent_id,
                    run.provider.as_str(),
                    run.provider_session_ref,
                    run.canonical_provider_session_ref,
                    i64::try_from(run.agent_revision).map_err(|_| RuntimeError::InvalidState(
                        "agent revision exceeds SQLite integer range".into()
                    ))?,
                    run.expected_session_updated_at,
                ],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed verifying current compaction attachment", error))?;
        if eligible != 1 {
            return Err(RuntimeError::Conflict(
                "compaction admission requires current idle agent and exact provider attachment"
                    .into(),
            ));
        }
        let pending_rebind: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM workspace_agent_rebinds
                 WHERE agent_id = ?1 AND phase IN ('intended','manual_review')",
                [&run.agent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking pending worktree rebind", error))?;
        if pending_rebind != 0 {
            return Err(RuntimeError::Conflict(
                "compaction refused during unresolved workspace rebind".into(),
            ));
        }
        let pending: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM session_compaction_runs
                 WHERE session_id = ?1 AND phase IN ('intended','accepted','manual_review')",
                [&run.agent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking active compaction", error))?;
        if pending != 0 {
            return Err(RuntimeError::Conflict(
                "another manual compaction is unresolved; inspect its operation".into(),
            ));
        }
        tx.execute(
            "INSERT INTO session_compaction_runs
             (run_id,session_id,idempotency_key,phase,record_json,created_at_ms,updated_at_ms)
             VALUES (?1,?2,?3,'intended',?4,?5,?6)",
            params![
                run.run_id,
                run.agent_id,
                run.idempotency_key,
                encode(run)?,
                run.created_at_ms,
                run.updated_at_ms,
            ],
        )
        .map_err(|error| db_error("failed admitting durable compaction operation", error))?;
        tx.commit()
            .map_err(|error| db_error("failed committing compaction admission", error))?;
        Ok(run.clone())
    }

    pub fn transition_session_compaction(
        &self,
        run_id: &str,
        phase: &str,
        outcome: Option<ProviderCompactSessionOutcome>,
        error_code: Option<&str>,
        updated_at_ms: i64,
    ) -> Result<SessionCompactionRun, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting compaction transition", error))?;
        let mut run = load(&tx, run_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("compaction run {run_id}")))?;
        if run.phase == phase {
            if run.outcome == outcome && run.error_code.as_deref() == error_code {
                return Ok(run);
            }
            return Err(RuntimeError::Conflict(
                "duplicate compaction phase contains contradictory provider evidence".into(),
            ));
        }
        let allowed = matches!(
            (run.phase.as_str(), phase),
            ("intended", "accepted" | "not_performed" | "manual_review")
                | ("accepted", "completed" | "failed" | "manual_review")
                | ("manual_review", "completed" | "failed")
        );
        if !allowed || updated_at_ms < run.created_at_ms {
            return Err(RuntimeError::Conflict(format!(
                "invalid compaction transition {} -> {phase}",
                run.phase
            )));
        }
        if phase == "completed" {
            let current: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM sessions s
                     JOIN workspace_agents a ON a.session_id = s.id
                     WHERE s.id = ?1 AND s.provider = ?2
                       AND s.provider_session_ref = ?3
                       AND s.canonical_provider_session_ref IS ?4
                       AND a.revision = ?5 AND a.lifecycle_state = 'active'",
                    params![
                        run.agent_id,
                        run.provider.as_str(),
                        run.provider_session_ref,
                        run.canonical_provider_session_ref,
                        i64::try_from(run.agent_revision).unwrap_or(-1),
                    ],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    db_error("failed verifying compaction completion identity", error)
                })?;
            if current != 1 {
                return Err(RuntimeError::Conflict(
                    "stale provider compaction completion cannot update current attachment".into(),
                ));
            }
        }
        let before = run.phase.clone();
        run.phase = phase.to_string();
        run.outcome = outcome;
        run.error_code = error_code.map(str::to_string);
        run.updated_at_ms = updated_at_ms.max(run.updated_at_ms);
        run.validate()?;
        let changed = tx
            .execute(
                "UPDATE session_compaction_runs SET phase = ?2, record_json = ?3, updated_at_ms = ?4
                 WHERE run_id = ?1 AND phase = ?5",
                params![run_id, phase, encode(&run)?, run.updated_at_ms, before],
            )
            .map_err(|error| db_error("failed transitioning compaction operation", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "concurrent compaction state update".into(),
            ));
        }
        tx.commit()
            .map_err(|error| db_error("failed committing compaction transition", error))?;
        Ok(run)
    }
}
