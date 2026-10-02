use runtime_core::{
    ApprovalRecord, RuntimeError, SessionRecord, TurnAdmissionRecord, TurnDispatchState,
    TurnInputProjectionSource, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::db::{db_error, json_to_string, open_connection, opt_json_to_string};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn admit_turn(
        &self,
        admission: &TurnAdmissionRecord,
        turn: &TurnRecord,
        session: &SessionRecord,
        approval: Option<&ApprovalRecord>,
    ) -> Result<(), RuntimeError> {
        validate_admission_bundle(admission, turn, session, approval)?;
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting turn admission transaction", error))?;

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
