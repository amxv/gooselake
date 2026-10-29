use runtime_core::{
    ApprovalRecord, OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord,
    OperationTransitionRecord, OperatorUserTurnCommand, RuntimeError, SessionRecord,
    TurnAdmissionRecord, TurnDispatchState, TurnInputProjectionSource, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::db::{db_error, json_to_string, open_connection, opt_json_to_string};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_workspace::operation_by_idempotency;
use crate::SqliteRuntimeRepository;

pub(crate) const OPERATOR_TURN_KIND: &str = "operator_user_turn_send";
pub(crate) const OPERATOR_TURN_CLAIM: &str = "operator_user_turn_native";
pub(crate) const OPERATOR_TURN_EFFECT: &str = "operator_turn_dispatch";

impl SqliteRuntimeRepository {
    pub fn admit_turn(
        &self,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        self.admit_turn_internal(admission, turn, session, approval, None)
    }

    pub fn admit_operator_user_turn(
        &self,
        command: &OperatorUserTurnCommand,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        self.admit_turn_internal(admission, turn, session, approval, Some(command))
    }

    fn admit_turn_internal(
        &self,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
        command: Option<&OperatorUserTurnCommand>,
    ) -> Result<(), RuntimeError> {
        validate_admission_bundle(admission, turn, session, approval)?;
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting turn admission transaction", error))?;

        if let Some(command) = command {
            validate_operator_user_turn_command(&transaction, command, admission, turn, session)?;
            if let Some(existing) = operation_by_idempotency(
                &transaction,
                command.actor.kind,
                &command.actor.identifier,
                Some(&command.idempotency_key),
            )? {
                if existing.kind != OPERATOR_TURN_KIND
                    || existing.normalized_request_hash != command.normalized_request_hash
                    || existing.normalized_request["agent_id"] != command.agent_id
                {
                    return Err(RuntimeError::Conflict(format!(
                        "Idempotency-Key belongs to operation {} with different input",
                        existing.operation_id
                    )));
                }
                return Err(RuntimeError::operation_unresolved(
                    existing.operation_id,
                    "operator logical turn was already admitted; inspect its durable receipt",
                ));
            }
        }

        let active_native_claim: Option<String> = transaction
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
                 WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",
                params![OPERATOR_TURN_CLAIM, session.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking unresolved operator turn claim", error))?;
        if let Some(operation_id) = active_native_claim {
            return Err(RuntimeError::operation_unresolved(
                operation_id,
                "an operator turn has unresolved native dispatch; inspect its operation before sending another turn",
            ));
        }
        guard_process_completion_admission(&transaction, admission)?;
        let rebind_pending: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM workspace_agent_rebinds
             WHERE agent_id = ?1 AND phase IN ('intended', 'manual_review')",
                params![session.id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error(
                    "failed checking provider route before turn admission",
                    error,
                )
            })?;
        if rebind_pending != 0 {
            return Err(RuntimeError::Conflict(format!(
                "session {} has an unresolved provider worktree binding",
                session.id
            )));
        }
        // The manager also serializes these operations in-memory, but the
        // SQLite fence must hold when more than one runtime instance (or a
        // direct store caller) races a native compaction admission.
        let compaction_pending: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM session_compaction_runs
                 WHERE session_id = ?1
                   AND phase IN ('intended','accepted','started','compacting','manual_review')",
                params![session.id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking compaction before turn admission", error))?;
        if compaction_pending != 0 {
            return Err(RuntimeError::Conflict(format!(
                "session {} has an unresolved provider compaction",
                session.id
            )));
        }
        // Only the replacement belonging to the durably verified child fork
        // may enter while an edit operation owns this session. Callers cannot
        // bypass this fence by issuing ordinary API send requests.
        let pending_edit: Option<(String, String)> = transaction
            .query_row(
                "SELECT operation_id,phase FROM session_edit_reruns WHERE session_id=?1
                 AND phase IN ('intended','native_started','fork_observed','manual_review')",
                params![session.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| {
                db_error("failed checking native edit before turn admission", error)
            })?;
        if let Some((operation_id, phase)) = pending_edit {
            if phase != "fork_observed"
                || admission.correlation.correlation_id.as_deref() != Some(operation_id.as_str())
            {
                return Err(RuntimeError::Conflict(format!(
                    "session {} has an unresolved edit-and-rerun; inspect operation {}",
                    session.id, operation_id
                )));
            }
        }

        let persisted = transaction
            .query_row(
                "SELECT status, active_turn_id FROM sessions WHERE id = ?1",
                params![session.id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading session for turn admission", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("session {}", session.id)))?;
        if matches!(persisted.0.as_str(), "closed" | "failed") {
            return Err(RuntimeError::InvalidState(format!(
                "session {} is not writable in status {}",
                session.id, persisted.0
            )));
        }
        if let Some(active_turn_id) = persisted.1 {
            return Err(RuntimeError::Conflict(format!(
                "session {} already has active turn {}",
                session.id, active_turn_id
            )));
        }

        if let Some(command) = command {
            let authority = OperationAuthorityTransaction::new(&transaction);
            authority.insert_operation(&OperationRecord {
                operation_id: command.operation_id.clone(),
                workspace_id: Some(command.workspace_id.clone()),
                kind: OPERATOR_TURN_KIND.into(),
                actor: command.actor.clone(),
                idempotency_key: Some(command.idempotency_key.clone()),
                normalized_request_hash: command.normalized_request_hash.clone(),
                normalized_request: command.normalized_request.clone(),
                phase: OperationPhase::Requested,
                exact_terminal_result: None,
                error_code: None,
                error_message: None,
                created_at: command.requested_at,
                updated_at: command.requested_at,
            })?;
            authority.append_transition(&OperationTransitionRecord {
                operation_id: command.operation_id.clone(),
                sequence: 1,
                from_phase: None,
                to_phase: OperationPhase::Requested,
                evidence: Some(serde_json::json!({"logical_turn_id":turn.id})),
                created_at: command.requested_at,
            })?;
            authority.acquire_claim(
                &command.operation_id,
                OPERATOR_TURN_CLAIM,
                &session.id,
                command.requested_at,
            )?;
            authority.insert_effect(&OperationEffectRecord {
                effect_id: format!("fx_{}", command.operation_id),
                operation_id: command.operation_id.clone(),
                effect_kind: OPERATOR_TURN_EFFECT.into(),
                target_kind: "logical_turn".into(),
                target_id: turn.id.clone(),
                phase: "intended".into(),
                idempotency_key: "provider_turn_send".into(),
                evidence: Some(serde_json::json!({"agent_id":session.id,"turn_id":turn.id})),
                attempt_count: 0,
                created_at: command.requested_at,
                updated_at: command.requested_at,
            })?;
        }

        insert_turn(&transaction, turn)?;
        insert_admission(&transaction, admission)?;
        if let Some(approval) = approval {
            insert_approval(&transaction, approval)?;
        }
        update_session_for_admission(&transaction, session)?;

        transaction
            .commit()
            .map_err(|error| db_error("failed committing turn admission", error))?;
        Ok(())
    }

    pub fn upsert_turn_admission(&self, record: &TurnAdmissionRecord) -> Result<(), RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .execute(
                "INSERT INTO turn_admissions (
                    turn_id, session_id, provider, projection_source, user_input_snapshot_json,
                    dispatch_policy_json, correlation_json, dispatch_state,
                    provider_native_turn_id, dispatch_error_json, admitted_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(turn_id) DO UPDATE SET
                    dispatch_state = excluded.dispatch_state,
                    provider_native_turn_id = excluded.provider_native_turn_id,
                    dispatch_error_json = excluded.dispatch_error_json,
                    updated_at = excluded.updated_at",
                admission_params(record)?,
            )
            .map_err(|error| db_error("failed upserting turn admission", error))?;
        Ok(())
    }

    pub fn list_turn_admissions(&self) -> Result<Vec<TurnAdmissionRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let mut statement = connection
            .prepare(
                "SELECT turn_id, session_id, provider, projection_source,
                        user_input_snapshot_json, dispatch_policy_json, correlation_json,
                        dispatch_state, provider_native_turn_id, dispatch_error_json,
                        admitted_at, updated_at
                 FROM turn_admissions
                 ORDER BY admitted_at ASC, turn_id ASC",
            )
            .map_err(|error| db_error("failed preparing turn admission query", error))?;
        let rows = statement
            .query_map([], turn_admission_from_row)
            .map_err(|error| db_error("failed querying turn admissions", error))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| db_error("failed collecting turn admissions", error))
    }
}

fn validate_operator_user_turn_command(
    transaction: &Transaction<'_>,
    command: &OperatorUserTurnCommand,
    admission: &TurnAdmissionRecord,
    turn: &TurnRecord,
    session: &SessionRecord,
) -> Result<(), RuntimeError> {
    if command.actor.kind != OperationActorKind::Operator
        || command.actor.identifier.trim().is_empty()
        || command.agent_id != session.id
        || command.workspace_id.is_empty()
        || command.operation_id.is_empty()
        || command.idempotency_key.is_empty()
        || command.idempotency_key.len() > 512
        || command.idempotency_key.trim() != command.idempotency_key
        || admission.projection_source != TurnInputProjectionSource::UserVisible
        || admission.correlation.correlation_id.is_some()
        || command.normalized_request_hash
            != runtime_core::normalized_json_hash(&command.normalized_request)?
        || command.normalized_request["agent_id"].as_str() != Some(session.id.as_str())
        || command.normalized_request["input"] != turn.input
        || command.normalized_request["expected_turn_id"]
            != serde_json::to_value(&admission.correlation.expected_turn_id)
                .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?
        || command.normalized_request["turn_settings"]
            != serde_json::to_value(&admission.dispatch_policy.turn_settings)
                .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?
        || !matches!(
            &command.normalized_request["permission_mode"],
            Value::Null | Value::String(_)
        )
    {
        return Err(RuntimeError::InvalidState(
            "invalid trusted operator user turn input or admission identity".into(),
        ));
    }
    let authority: Option<(String, String, String, String)> = transaction
        .query_row(
            "SELECT a.workspace_id, a.lifecycle_state, w.lifecycle_state,
                    a.recreation_policy_json
             FROM workspace_agents a JOIN workspaces w ON w.workspace_id=a.workspace_id
             WHERE a.session_id=?1",
            [&command.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|error| db_error("failed verifying operator turn workspace owner", error))?;
    let Some((workspace_id, agent_state, workspace_state, recreation_policy_json)) = authority
    else {
        return Err(RuntimeError::NotFound(format!(
            "active workspace agent {}",
            command.agent_id
        )));
    };
    if workspace_id != command.workspace_id
        || agent_state != "active"
        || workspace_state != "active"
    {
        return Err(RuntimeError::Conflict(
            "operator turn agent or workspace authority changed before admission".into(),
        ));
    }
    if let Some(effort) = admission.dispatch_policy.turn_settings.thinking_effort {
        let policy: runtime_core::WorkspaceAgentRecreationPolicy =
            serde_json::from_str(&recreation_policy_json).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid durable agent policy: {error}"))
            })?;
        if policy.current_preferences.thinking_effort != Some(effort) {
            return Err(RuntimeError::Conflict(
                "agent thinking preference changed before keyed operator turn admission".into(),
            ));
        }
    }
    let frozen: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace_retirement_fences WHERE workspace_id=?1)",
            [&workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking operator turn retirement fence", error))?;
    if frozen {
        return Err(RuntimeError::Conflict(
            "operator turn blocked by workspace retirement".into(),
        ));
    }
    Ok(())
}

fn validate_admission_bundle(
    admission: &TurnAdmissionRecord,
    turn: &TurnRecord,
    session: &SessionRecord,
    approval: Option<&ApprovalRecord>,
) -> Result<(), RuntimeError> {
    if admission.turn_id != turn.id
        || admission.session_id != turn.session_id
        || admission.session_id != session.id
        || session.active_turn_id.as_deref() != Some(turn.id.as_str())
    {
        return Err(RuntimeError::ProtocolViolation(
            "turn admission bundle identities do not match".to_string(),
        ));
    }
    if let Some(approval) = approval {
        if approval.session_id != session.id || approval.turn_id != turn.id {
            return Err(RuntimeError::ProtocolViolation(
                "turn admission approval identity does not match".to_string(),
            ));
        }
    }
    Ok(())
}

fn guard_process_completion_admission(
    transaction: &Transaction<'_>,
    admission: &TurnAdmissionRecord,
) -> Result<(), RuntimeError> {
    if admission.projection_source != TurnInputProjectionSource::AutomationContext {
        return Ok(());
    }
    let Some(correlation) = admission.correlation.correlation_id.as_deref() else {
        return Ok(());
    };
    let Some(process_id) = correlation.strip_prefix("process_completion:") else {
        return Ok(());
    };
    let owner: Option<Option<String>> = transaction
        .query_row(
            "SELECT owner_session_id FROM managed_processes WHERE process_id=?1",
            [process_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed checking process completion admission owner", error))?;
    if owner.flatten().as_deref() != Some(admission.session_id.as_str()) {
        return Err(RuntimeError::Conflict(
            "completion input does not identify an owned managed process".into(),
        ));
    }
    // A worker that claimed before retirement may wake after explicit restore.
    // Correlation authority outlives its claim and prevents a second model input
    // even when another completion turn has already finished and the session is idle.
    let already_admitted: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM turn_admissions
             WHERE session_id=?1 AND projection_source='automation_context'
               AND json_extract(correlation_json,'$.correlation_id')=?2
               AND dispatch_state<>'not_dispatched')",
            params![admission.session_id, correlation],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking prior process completion admission", error))?;
    if already_admitted {
        return Err(RuntimeError::Conflict(
            "process completion already has admitted or dispatched input".into(),
        ));
    }
    Ok(())
}

fn insert_turn(transaction: &Transaction<'_>, record: &TurnRecord) -> Result<(), RuntimeError> {
    transaction
        .execute(
            "INSERT INTO turns (
                id, session_id, provider_turn_ref, status, input_json, source,
                started_at, completed_at, usage_json, error_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.id,
                record.session_id,
                record.provider_turn_ref,
                record.status,
                json_to_string(&record.input)?,
                record.source,
                record.started_at,
                record.completed_at,
                opt_json_to_string(record.usage.as_ref())?,
                opt_json_to_string(record.error.as_ref())?,
            ],
        )
        .map_err(|error| db_error("failed inserting admitted turn", error))?;
    Ok(())
}

fn insert_admission(
    transaction: &Transaction<'_>,
    record: &TurnAdmissionRecord,
) -> Result<(), RuntimeError> {
    transaction
        .execute(
            "INSERT INTO turn_admissions (
                turn_id, session_id, provider, projection_source, user_input_snapshot_json,
                dispatch_policy_json, correlation_json, dispatch_state,
                provider_native_turn_id, dispatch_error_json, admitted_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            admission_params(record)?,
        )
        .map_err(|error| db_error("failed inserting turn admission", error))?;
    Ok(())
}

fn insert_approval(
    transaction: &Transaction<'_>,
    record: &ApprovalRecord,
) -> Result<(), RuntimeError> {
    transaction
        .execute(
            "INSERT INTO approvals (
                id, session_id, turn_id, origin, tool_call_id, provider_approval_ref, status,
                request_json, response_json, created_at, resolved_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                record.id,
                record.session_id,
                record.turn_id,
                record.origin,
                record.tool_call_id,
                record.provider_approval_ref,
                record.status,
                json_to_string(&record.request)?,
                opt_json_to_string(record.response.as_ref())?,
                record.created_at,
                record.resolved_at,
            ],
        )
        .map_err(|error| db_error("failed inserting turn admission approval", error))?;
    Ok(())
}

fn update_session_for_admission(
    transaction: &Transaction<'_>,
    record: &SessionRecord,
) -> Result<(), RuntimeError> {
    let changed = transaction
        .execute(
            "UPDATE sessions
             SET status = ?1, active_turn_id = ?2, updated_at = ?3
             WHERE id = ?4 AND active_turn_id IS NULL AND status NOT IN ('closed', 'failed')",
            params![
                record.status,
                record.active_turn_id,
                record.updated_at,
                record.id,
            ],
        )
        .map_err(|error| db_error("failed claiming session for turn admission", error))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(format!(
            "session {} changed during turn admission",
            record.id
        )));
    }
    Ok(())
}

fn admission_params(
    record: &TurnAdmissionRecord,
) -> Result<rusqlite::ParamsFromIter<Vec<rusqlite::types::Value>>, RuntimeError> {
    let values = vec![
        record.turn_id.clone().into(),
        record.session_id.clone().into(),
        record.provider.clone().into(),
        record.projection_source.as_str().to_string().into(),
        encode(&record.user_input_snapshot)?.into(),
        encode(&record.dispatch_policy)?.into(),
        encode(&record.correlation)?.into(),
        record.dispatch_state.as_str().to_string().into(),
        record.provider_native_turn_id.clone().into(),
        opt_json_to_string(record.dispatch_error.as_ref())?.into(),
        record.admitted_at.into(),
        record.updated_at.into(),
    ];
    Ok(rusqlite::params_from_iter(values))
}

fn turn_admission_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnAdmissionRecord> {
    let projection_source: String = row.get(3)?;
    let dispatch_state: String = row.get(7)?;
    let dispatch_error_json: Option<String> = row.get(9)?;
    Ok(TurnAdmissionRecord {
        turn_id: row.get(0)?,
        session_id: row.get(1)?,
        provider: row.get(2)?,
        projection_source: TurnInputProjectionSource::from_str(&projection_source).ok_or_else(
            || invalid_data(3, format!("invalid projection source {projection_source}")),
        )?,
        user_input_snapshot: decode(row.get::<_, String>(4)?, 4)?,
        dispatch_policy: decode(row.get::<_, String>(5)?, 5)?,
        correlation: decode(row.get::<_, String>(6)?, 6)?,
        dispatch_state: TurnDispatchState::from_str(&dispatch_state)
            .ok_or_else(|| invalid_data(7, format!("invalid dispatch state {dispatch_state}")))?,
        provider_native_turn_id: row.get(8)?,
        dispatch_error: dispatch_error_json
            .map(|value| decode::<Value>(value, 9))
            .transpose()?,
        admitted_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn encode<T: Serialize>(value: &T) -> Result<String, RuntimeError> {
    serde_json::to_string(value).map_err(|error| {
        RuntimeError::Bootstrap(format!("failed serializing turn authority: {error}"))
    })
}

fn decode<T: DeserializeOwned>(value: String, column: usize) -> rusqlite::Result<T> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn invalid_data(column: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}
