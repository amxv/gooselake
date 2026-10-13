use runtime_core::{ProviderSession, RuntimeError, SessionEditRerunOperation};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, open_connection};
use crate::SqliteRuntimeRepository;

fn encode(operation: &SessionEditRerunOperation) -> Result<String, RuntimeError> {
    serde_json::to_string(operation)
        .map_err(|e| RuntimeError::Bootstrap(format!("invalid edit-and-rerun operation: {e}")))
}

fn read(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
    let row: Option<String> = connection
        .query_row(
            "SELECT record_json FROM session_edit_reruns WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| db_error("failed reading edit-and-rerun", e))?;
    row.map(|json| {
        serde_json::from_str(&json)
            .map_err(|e| RuntimeError::Bootstrap(format!("corrupt edit-and-rerun row: {e}")))
    })
    .transpose()
}

fn update(
    tx: &Transaction<'_>,
    operation: &SessionEditRerunOperation,
    before: &str,
) -> Result<(), RuntimeError> {
    operation.validate()?;
    let changed = tx
        .execute(
            "UPDATE session_edit_reruns SET phase=?2,record_json=?3,updated_at_ms=?4
         WHERE operation_id=?1 AND phase=?5",
            params![
                operation.operation_id,
                operation.phase,
                encode(operation)?,
                operation.updated_at_ms,
                before
            ],
        )
        .map_err(|e| db_error("failed recording edit-and-rerun transition", e))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "concurrent edit-and-rerun transition".into(),
        ));
    }
    Ok(())
}

/// Rebuild the bounded, derived transcript against the provider's retained
/// prefix before committing a verified native fork. Historical turns and
/// admissions remain intact for audit, but the active session must not expose
/// assistant/user messages that the child provider thread no longer contains.
fn fork_prefix_metadata(
    tx: &Transaction<'_>,
    session_id: &str,
    target_turn_id: &str,
) -> Result<String, RuntimeError> {
    let metadata_json: String = tx
        .query_row(
            "SELECT metadata_json FROM sessions WHERE id=?1",
            [session_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading session transcript before edit", error))?;
    let mut metadata: Value = serde_json::from_str(&metadata_json).map_err(|error| {
        RuntimeError::Bootstrap(format!("corrupt session metadata before edit: {error}"))
    })?;
    if !metadata.is_object() {
        metadata = json!({});
    }
    let mut statement = tx
        .prepare(
            "SELECT t.input_json,t.usage_json FROM turns t
             WHERE t.session_id=?1
               AND t.rowid < (SELECT target.rowid FROM turns target WHERE target.id=?2)
               AND t.status IN ('completed','interrupted')
               AND NOT EXISTS (
                 SELECT 1 FROM session_edit_rerun_superseded_turns st WHERE st.turn_id=t.id
               )
             ORDER BY t.rowid ASC",
        )
        .map_err(|error| db_error("failed preparing retained edit transcript", error))?;
    let rows = statement
        .query_map(params![session_id, target_turn_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|error| db_error("failed querying retained edit transcript", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed collecting retained edit transcript", error))?;

    let mut entries = Vec::new();
    for (input_json, usage_json) in rows {
        let input: Value = serde_json::from_str(&input_json).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid historical input during edit: {error}"))
        })?;
        let text = input
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|item| {
                item.get("text")
                    .and_then(Value::as_str)
                    .or_else(|| item.as_str())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        if !text.is_empty() {
            entries.push(json!({"role":"user","text":text}));
        }
        if let Some(usage_json) = usage_json {
            let usage: Value = serde_json::from_str(&usage_json).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid historical usage during edit: {error}"))
            })?;
            if let Some(text) = [
                "last_message",
                "lastMessage",
                "assistant_text",
                "assistantText",
            ]
            .iter()
            .find_map(|key| usage.get(key).and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            {
                entries.push(json!({"role":"assistant","text":text}));
            }
        }
    }
    if entries.len() > 80 {
        entries.drain(..entries.len() - 80);
    }
    let object = metadata.as_object_mut().expect("normalized object");
    object.remove("codex_transcript");
    object.insert("session_transcript".into(), Value::Array(entries));
    serde_json::to_string(&metadata).map_err(|error| {
        RuntimeError::Bootstrap(format!("failed encoding edit transcript: {error}"))
    })
}

impl SqliteRuntimeRepository {
    pub fn get_session_edit_rerun(
        &self,
        operation_id: &str,
    ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
        read(&open_connection(&self.database_path)?, operation_id)
    }

    pub fn get_session_edit_rerun_by_key(
        &self,
        agent_id: &str,
        key: &str,
    ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
        let conn = open_connection(&self.database_path)?;
        let id: Option<String> = conn.query_row(
            "SELECT operation_id FROM session_edit_reruns WHERE session_id=?1 AND idempotency_key=?2",
            params![agent_id,key], |row| row.get(0)
        ).optional().map_err(|e| db_error("failed querying edit idempotency",e))?;
        id.map(|id| read(&conn, &id))
            .transpose()
            .map(Option::flatten)
    }

    pub fn unresolved_session_edit_rerun(&self, agent_id: &str) -> Result<bool, RuntimeError> {
        let conn = open_connection(&self.database_path)?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_edit_reruns WHERE session_id=?1
             AND phase IN ('intended','native_started','fork_observed','manual_review')",
                [agent_id],
                |row| row.get(0),
            )
            .map_err(|e| db_error("failed querying active edit", e))?;
        Ok(count != 0)
    }

    /// Admit and fence an edit before *any* provider-native fork request.
    /// Revalidate all mutable state inside one SQLite IMMEDIATE transaction.
    pub fn begin_session_edit_rerun(
        &self,
        operation: &SessionEditRerunOperation,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        operation.validate()?;
        if operation.phase != "intended"
            || operation.child_provider_session_ref.is_some()
            || operation.replacement_turn_id.is_some()
        {
            return Err(RuntimeError::InvalidState(
                "new edit must have intended state".into(),
            ));
        }
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting edit admission", e))?;
        if let Some(key) = operation.idempotency_key.as_deref() {
            let id: Option<String> = tx.query_row(
                "SELECT operation_id FROM session_edit_reruns WHERE session_id=?1 AND idempotency_key=?2",
                params![operation.agent_id,key], |row| row.get(0)
            ).optional().map_err(|e| db_error("failed checking edit replay",e))?;
            if let Some(id) = id {
                let previous = read(&tx, &id)?.ok_or_else(|| RuntimeError::NotFound(id.clone()))?;
                if previous.request_hash != operation.request_hash {
                    return Err(RuntimeError::Conflict(
                        "Idempotency-Key reused with different edit input".into(),
                    ));
                }
                return Ok(previous);
            }
        }
        let eligible: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM sessions s JOIN workspace_agents a ON a.session_id=s.id
             JOIN workspaces w ON w.workspace_id=a.workspace_id
             JOIN turn_admissions ta ON ta.session_id=s.id AND ta.turn_id=?2
             JOIN turns t ON t.id=ta.turn_id
             WHERE s.id=?1 AND s.provider=?3 AND s.provider_session_ref=?4
               AND s.canonical_provider_session_ref IS ?5
               AND s.updated_at=?6 AND s.status='ready' AND s.active_turn_id IS NULL
               AND a.revision=?7 AND a.lifecycle_state='active' AND w.lifecycle_state='active'
               AND ta.projection_source='user_visible' AND ta.provider_native_turn_id IS NOT NULL
               AND t.status='completed' AND NOT EXISTS (
                  SELECT 1 FROM session_edit_rerun_superseded_turns st
                  WHERE st.turn_id=ta.turn_id
               )",
                params![
                    operation.agent_id,
                    operation.target_turn_id,
                    operation.provider.as_str(),
                    operation.source_provider_session_ref,
                    operation.source_canonical_provider_session_ref,
                    operation.expected_session_updated_at,
                    i64::try_from(operation.agent_revision).unwrap_or(-1)
                ],
                |row| row.get(0),
            )
            .map_err(|e| db_error("failed validating historical edit authority", e))?;
        if eligible != 1 {
            return Err(RuntimeError::Conflict(
                "edit requires an idle current agent and an unsuperseded completed user turn with native identity".into()
            ));
        }
        let pending: i64 = tx.query_row(
            "SELECT (SELECT COUNT(*) FROM session_compaction_runs
               WHERE session_id=?1 AND phase IN ('intended','accepted','manual_review'))
               +(SELECT COUNT(*) FROM workspace_agent_rebinds
               WHERE agent_id=?1 AND phase IN ('intended','manual_review'))
               +(SELECT COUNT(*) FROM session_edit_reruns
               WHERE session_id=?1 AND phase IN ('intended','native_started','fork_observed','manual_review'))",
            [&operation.agent_id], |row| row.get(0)
        ).map_err(|e| db_error("failed checking competing provider operation",e))?;
        if pending != 0 {
            return Err(RuntimeError::Conflict(
                "another provider operation requires reconciliation".into(),
            ));
        }
        tx.execute(
            "INSERT INTO session_edit_reruns
             (operation_id,session_id,target_turn_id,idempotency_key,request_hash,phase,record_json,created_at_ms,updated_at_ms)
             VALUES (?1,?2,?3,?4,?5,'intended',?6,?7,?8)",
            params![operation.operation_id,operation.agent_id,operation.target_turn_id,
                operation.idempotency_key,operation.request_hash,encode(operation)?,
                operation.created_at_ms,operation.updated_at_ms]
        ).map_err(|e| db_error("failed admitting edit before native fork",e))?;
        tx.commit()
            .map_err(|e| db_error("failed committing edit admission", e))?;
        Ok(operation.clone())
    }

    pub fn advance_session_edit_rerun(
        &self,
        operation_id: &str,
        phase: &str,
        child: Option<&ProviderSession>,
        replacement_turn_id: Option<&str>,
        error_code: Option<&str>,
        now: i64,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting edit transition", e))?;
        let mut operation = read(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("edit operation {operation_id}")))?;
        if operation.phase == phase {
            if operation.child_provider_session_ref.as_deref()
                == child.map(|c| c.provider_session_ref.as_str())
                && operation.replacement_turn_id.as_deref() == replacement_turn_id
            {
                return Ok(operation);
            }
            return Err(RuntimeError::Conflict(
                "conflicting edit operation replay".into(),
            ));
        }
        let allowed = matches!(
            (operation.phase.as_str(), phase),
            ("intended", "native_started" | "failed")
                | ("native_started", "fork_observed" | "manual_review")
                | ("intended", "manual_review")
                | ("fork_observed", "completed" | "manual_review")
        );
        if !allowed {
            return Err(RuntimeError::Conflict(format!(
                "invalid edit transition {} -> {phase}",
                operation.phase
            )));
        }
        if phase == "fork_observed" {
            let child = child
                .ok_or_else(|| RuntimeError::InvalidState("fork requires provider proof".into()))?;
            if child.runtime_session_id != operation.agent_id
                || child.provider_session_ref.trim().is_empty()
                || child.provider_session_ref == operation.source_provider_session_ref
            {
                return Err(RuntimeError::ProtocolViolation(
                    "fork returned missing, foreign or unchanged native identity".into(),
                ));
            }
            let monotonic = now.max(operation.expected_session_updated_at.saturating_add(1));
            let retained_metadata =
                fork_prefix_metadata(&tx, &operation.agent_id, &operation.target_turn_id)?;
            let changed = tx.execute(
                "UPDATE sessions SET provider_session_ref=?2,canonical_provider_session_ref=?3,updated_at=?4,
                   metadata_json=?9
                 WHERE id=?1 AND provider=?5 AND provider_session_ref=?6
                   AND canonical_provider_session_ref IS ?7 AND updated_at=?8
                   AND status='ready' AND active_turn_id IS NULL",
                params![operation.agent_id,child.provider_session_ref,child.canonical_provider_session_ref,
                    monotonic,operation.provider.as_str(),operation.source_provider_session_ref,
                    operation.source_canonical_provider_session_ref,operation.expected_session_updated_at,
                    retained_metadata]
            ).map_err(|e| db_error("failed committing verified provider fork",e))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "session changed during native hard fork; manual review required".into(),
                ));
            }
            // Provider identity is stored on sessions and joined into the
            // workspace-agent projection. Advance the agent revision in the
            // same transaction to fence stale policy/context observations.
            let changed_agent = tx
                .execute(
                    "UPDATE workspace_agents
                 SET revision=revision+1,updated_at=?2
                 WHERE session_id=?1 AND revision=?3 AND lifecycle_state='active'",
                    params![
                        operation.agent_id,
                        monotonic,
                        i64::try_from(operation.agent_revision).unwrap_or(-1)
                    ],
                )
                .map_err(|e| db_error("failed committing child workspace agent identity", e))?;
            if changed_agent != 1 {
                return Err(RuntimeError::Conflict(
                    "agent revision or provider identity changed during edit fork".into(),
                ));
            }
            // Preserve all old rows, native mappings and user input as audit
            // history while marking which turns are no longer on the fork.
            // A turn's SQLite rowid is its durable admission order: the
            // millisecond timestamp and lexicographical logical IDs can tie
            // or disagree with that order when several turns are admitted
            // within the same millisecond.
            tx.execute(
                "INSERT INTO session_edit_rerun_superseded_turns (operation_id,turn_id)
                 SELECT ?1,t.id FROM turns t
                 WHERE t.session_id=?2 AND t.rowid >=
                   (SELECT target.rowid FROM turns target WHERE target.id=?3)",
                params![
                    operation.operation_id,
                    operation.agent_id,
                    operation.target_turn_id
                ],
            )
            .map_err(|e| db_error("failed preserving superseded historical turn lineage", e))?;
            tx.execute(
                "DELETE FROM session_context_limit_snapshots WHERE session_id=?1",
                [&operation.agent_id],
            )
            .map_err(|e| db_error("failed invalidating old thread context observation", e))?;
            operation.child_provider_session_ref = Some(child.provider_session_ref.clone());
            operation.child_canonical_provider_session_ref =
                child.canonical_provider_session_ref.clone();
        } else if phase == "completed" {
            let replacement_id = replacement_turn_id.ok_or_else(|| {
                RuntimeError::InvalidState("missing admitted replacement turn".into())
            })?;
            let evidence: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM turn_admissions WHERE turn_id=?1 AND session_id=?2
                 AND json_extract(correlation_json,'$.correlation_id')=?3
                 AND projection_source='user_visible'",
                    params![replacement_id, operation.agent_id, operation.operation_id],
                    |row| row.get(0),
                )
                .map_err(|e| db_error("failed checking replacement admission", e))?;
            if evidence != 1 {
                return Err(RuntimeError::Conflict(
                    "replacement was not durably admitted for this edit".into(),
                ));
            }
            operation.replacement_turn_id = Some(replacement_id.to_string());
        }
        let previous = operation.phase.clone();
        operation.phase = phase.to_string();
        operation.error_code = error_code.map(str::to_string);
        operation.updated_at_ms = now.max(operation.updated_at_ms);
        update(&tx, &operation, &previous)?;
        tx.commit()
            .map_err(|e| db_error("failed committing edit operation", e))?;
        Ok(operation)
    }

    pub fn recover_session_edit_reruns(
        &self,
        now: i64,
    ) -> Result<Vec<SessionEditRerunOperation>, RuntimeError> {
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed beginning edit startup recovery", e))?;
        let raw = {
            let mut stmt = tx
                .prepare(
                    "SELECT record_json FROM session_edit_reruns
                WHERE phase IN ('intended','native_started','fork_observed')
                   OR (phase='manual_review' AND
                       json_extract(record_json,'$.child_provider_session_ref') IS NOT NULL)
                ORDER BY operation_id",
                )
                .map_err(|e| db_error("failed preparing edit recovery", e))?;
            let collected = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| db_error("failed reading edits for recovery", e))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| db_error("failed collecting edits for recovery", e))?;
            collected
        };
        let mut recovered = Vec::new();
        for json in raw {
            let mut op: SessionEditRerunOperation = serde_json::from_str(&json).map_err(|e| {
                RuntimeError::Bootstrap(format!("corrupt edit during recovery: {e}"))
            })?;
            let before = op.phase.clone();
            let replacement: Option<String> = tx
                .query_row(
                    "SELECT turn_id FROM turn_admissions WHERE session_id=?1
                 AND json_extract(correlation_json,'$.correlation_id')=?2
                 ORDER BY admitted_at DESC,turn_id DESC LIMIT 1",
                    params![op.agent_id, op.operation_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| db_error("failed reconciling edited turn admission", e))?;
            if matches!(before.as_str(), "fork_observed" | "manual_review") && replacement.is_some()
            {
                op.phase = "completed".into();
                op.replacement_turn_id = replacement;
                op.error_code = None;
            } else if before == "manual_review" {
                continue;
            } else {
                op.phase = "manual_review".into();
                op.error_code = Some("restart_edit_native_outcome_unverified".into());
            }
            op.updated_at_ms = now.max(op.updated_at_ms);
            update(&tx, &op, &before)?;
            recovered.push(op);
        }
        tx.commit()
            .map_err(|e| db_error("failed committing edit recovery", e))?;
        Ok(recovered)
    }

    /// Operator may release a fork that was durably committed but whose
    /// replacement turn was never admitted. Unknown native child identity
    /// cannot be released on a mere human assertion.
    pub fn resolve_session_edit_rerun_review(
        &self,
        operation_id: &str,
        evidence: &str,
        now: i64,
    ) -> Result<SessionEditRerunOperation, RuntimeError> {
        if !(8..=2048).contains(&evidence.trim().len()) {
            return Err(RuntimeError::InvalidState(
                "edit review evidence must be 8-2048 characters".into(),
            ));
        }
        let mut conn = open_connection(&self.database_path)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed beginning edit review", e))?;
        let mut op = read(&tx, operation_id)?
            .ok_or_else(|| RuntimeError::NotFound(format!("edit operation {operation_id}")))?;
        if op.phase != "manual_review" {
            return Err(RuntimeError::Conflict(
                "only unresolved edit operations may be reviewed".into(),
            ));
        }
        let Some(child) = op.child_provider_session_ref.as_deref() else {
            return Err(RuntimeError::Conflict(
                "native fork outcome lacks child identity; inspect/reconcile provider externally before release".into()
            ));
        };
        let match_child: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id=?1 AND provider_session_ref=?2
             AND canonical_provider_session_ref IS ?3 AND active_turn_id IS NULL",
                params![op.agent_id, child, op.child_canonical_provider_session_ref],
                |row| row.get(0),
            )
            .map_err(|e| db_error("failed validating reviewed child attachment", e))?;
        if match_child != 1 {
            return Err(RuntimeError::Conflict(
                "review cannot release an unverified or busy provider child".into(),
            ));
        }
        let admitted: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM turn_admissions WHERE session_id=?1
             AND json_extract(correlation_json,'$.correlation_id')=?2",
                params![op.agent_id, op.operation_id],
                |row| row.get(0),
            )
            .map_err(|e| db_error("failed checking reviewed rerun admission", e))?;
        if admitted != 0 {
            return Err(RuntimeError::Conflict(
                "review cannot discard an already admitted edited turn".into(),
            ));
        }
        let previous = op.phase.clone();
        op.phase = "failed".into();
        op.error_code = Some("operator_verified_no_replacement_turn".into());
        op.review_evidence = Some(evidence.trim().to_string());
        op.updated_at_ms = now.max(op.updated_at_ms);
        update(&tx, &op, &previous)?;
        tx.commit()
            .map_err(|e| db_error("failed committing edit review", e))?;
        Ok(op)
    }
}
