use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    SessionRecord,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_turn_terminal::read_session;
use crate::SqliteRuntimeRepository;

fn validate_lifecycle(
    before: Option<&SessionRecord>,
    after: &SessionRecord,
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    if event.scope != RuntimeEventScope::Session
        || event.scope_id != after.id
        || event.session_id.as_deref() != Some(after.id.as_str())
        || event.turn_id.is_some()
        || event.team_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || after.id.is_empty()
        || after.provider.is_empty()
    {
        return Err(RuntimeError::ProtocolViolation(
            "session lifecycle event subject or criticality mismatch".into(),
        ));
    }
    match (event.kind.as_str(), before) {
        ("session.created", None) => {
            if event.event_id != format!("evt_session_{}_created", after.id)
                || event.payload != serde_json::json!({"provider":after.provider})
                || after.status != "ready"
                || after.active_turn_id.is_some()
                || after.closed_at.is_some()
                || after.failure_code.is_some()
                || after.failure_message.is_some()
                || after.updated_at < after.created_at
                || after.provider_session_ref.is_none()
            {
                return Err(RuntimeError::ProtocolViolation(
                    "provider session creation and critical event mismatch".into(),
                ));
            }
        }
        ("session.resumed", Some(previous)) => {
            let mut expected = previous.clone();
            expected.provider_session_ref = after.provider_session_ref.clone();
            expected.canonical_provider_session_ref = after.canonical_provider_session_ref.clone();
            if expected.status != "closed" {
                expected.status = "ready".into();
            }
            expected.updated_at = after.updated_at;
            if !event
                .event_id
                .starts_with(&format!("evt_session_{}_resumed_", after.id))
                || event.event_id.ends_with("_resumed_")
                || event.payload != serde_json::json!({})
                || after.updated_at < previous.updated_at
                || after.provider_session_ref.is_none()
                || *after != expected
            {
                return Err(RuntimeError::ProtocolViolation(
                    "provider session resume state and critical event mismatch".into(),
                ));
            }
        }
        ("session.closed", Some(previous)) => {
            let reason = event.payload["reason"].as_str().unwrap_or_default();
            let mut expected = previous.clone();
            expected.status = "closed".into();
            expected.closed_at = after.closed_at;
            expected.updated_at = after.updated_at;
            expected.active_turn_id = None;
            if event.event_id != format!("evt_session_{}_closed", after.id)
                || event.payload != serde_json::json!({"reason":reason})
                || reason.is_empty()
                || previous.status == "closed"
                || after.closed_at.is_none()
                || after.closed_at < Some(previous.created_at)
                || after.updated_at < previous.updated_at
                || *after != expected
            {
                return Err(RuntimeError::ProtocolViolation(
                    "native session close state and critical event mismatch".into(),
                ));
            }
        }
        _ => {
            return Err(RuntimeError::ProtocolViolation(
                "unsupported session lifecycle transition for critical journal".into(),
            ));
        }
    }
    if before.is_some_and(|old| old.id != after.id) {
        return Err(RuntimeError::ProtocolViolation(
            "session lifecycle cannot change durable identity".into(),
        ));
    }
    Ok(())
}

fn exact_receipt(previous: &RuntimeEventRecord, event: &NewRuntimeEvent) -> bool {
    previous.event_id == event.event_id
        && previous.kind == event.kind
        && previous.scope == event.scope
        && previous.scope_id == event.scope_id
        && previous.session_id == event.session_id
        && previous.turn_id == event.turn_id
        && previous.team_id == event.team_id
        && previous.criticality == event.criticality
        && previous.provider == event.provider
        && previous.provider_seq == event.provider_seq
        && previous.payload == event.payload
}

impl SqliteRuntimeRepository {
    /// Session creation happens after a native provider creation; resume and
    /// close likewise follow native calls. All local authority and its critical
    /// event commit once, or neither does. Unknown post-native storage errors
    /// must never be converted into permission to invoke that native action.
    pub fn commit_session_lifecycle(
        &self,
        before: Option<&SessionRecord>,
        after: &SessionRecord,
        event: &NewRuntimeEvent,
    ) -> Result<(SessionRecord, Option<RuntimeEventRecord>), RuntimeError> {
        validate_lifecycle(before, after, event)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error("failed starting provider session lifecycle journal", error)
            })?;

        if let Some(receipt) = fetch_runtime_event_by_event_id(&tx, &event.event_id)? {
            if !exact_receipt(&receipt, event) || read_session(&tx, &after.id)? != *after {
                return Err(RuntimeError::Conflict(
                    "session lifecycle receipt collides with changed state or owner".into(),
                ));
            }
            return Ok((after.clone(), None));
        }

        match before {
            None => {
                if tx
                    .query_row("SELECT 1 FROM sessions WHERE id=?1", [&after.id], |row| {
                        row.get::<_, i64>(0)
                    })
                    .optional()
                    .map_err(|error| db_error("failed checking session ID ownership", error))?
                    .is_some()
                {
                    return Err(RuntimeError::Conflict(
                        "provider session already persisted without this exact creation receipt"
                            .into(),
                    ));
                }
                tx.execute(
                    "INSERT INTO sessions (
                         id,provider,status,cwd,model,permission_mode,system_prompt,metadata_json,
                         provider_session_ref,canonical_provider_session_ref,active_turn_id,worktree_id,
                         created_at,updated_at,closed_at,failure_code,failure_message
                     ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
                    params![after.id,after.provider,after.status,after.cwd,after.model,
                        after.permission_mode,after.system_prompt,json_to_string(&after.metadata)?,
                        after.provider_session_ref,after.canonical_provider_session_ref,
                        after.active_turn_id,after.worktree_id,after.created_at,after.updated_at,
                        after.closed_at,after.failure_code,after.failure_message],
                ).map_err(|error| db_error("failed inserting native session creation", error))?;
            }
            Some(previous) => {
                if read_session(&tx, &after.id)? != *previous {
                    return Err(RuntimeError::Conflict(
                        "native session lifecycle ownership changed during provider effect".into(),
                    ));
                }
                let changed = tx
                    .execute(
                        "UPDATE sessions SET status=?1, provider_session_ref=?2,
                         canonical_provider_session_ref=?3, active_turn_id=?4,
                         closed_at=?5, updated_at=?6
                     WHERE id=?7 AND status=?8 AND updated_at=?9 AND active_turn_id IS ?10",
                        params![
                            after.status,
                            after.provider_session_ref,
                            after.canonical_provider_session_ref,
                            after.active_turn_id,
                            after.closed_at,
                            after.updated_at,
                            after.id,
                            previous.status,
                            previous.updated_at,
                            previous.active_turn_id
                        ],
                    )
                    .map_err(|error| db_error("failed updating native session lifecycle", error))?;
                if changed != 1 {
                    return Err(RuntimeError::Conflict(
                        "native session lifecycle compare-and-swap lost".into(),
                    ));
                }
            }
        }
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        tx.commit().map_err(|error| {
            db_error(
                "failed committing atomic session lifecycle and critical event",
                error,
            )
        })?;
        Ok((after.clone(), Some(receipt)))
    }
}
