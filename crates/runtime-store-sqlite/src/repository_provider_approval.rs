use runtime_core::{
    ApprovalRecord, NewRuntimeEvent, ProviderApprovalAdmission, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionRecord, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{
    db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection, opt_json_to_string,
    opt_string_to_json, string_to_json,
};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

fn existing_provider_approval(
    tx: &Transaction<'_>,
    approval: &ApprovalRecord,
) -> Result<Option<ApprovalRecord>, RuntimeError> {
    tx.query_row(
        "SELECT id, session_id, turn_id, origin, tool_call_id, provider_approval_ref,
                status, request_json, response_json, created_at, resolved_at
         FROM approvals
         WHERE session_id=?1 AND turn_id=?2 AND provider_approval_ref=?3",
        params![
            approval.session_id,
            approval.turn_id,
            approval.provider_approval_ref
        ],
        |row| {
            Ok(ApprovalRecord {
                id: row.get(0)?,
                session_id: row.get(1)?,
                turn_id: row.get(2)?,
                origin: row.get(3)?,
                tool_call_id: row.get(4)?,
                provider_approval_ref: row.get(5)?,
                status: row.get(6)?,
                request: string_to_json(row.get(7)?)?,
                response: opt_string_to_json(row.get(8)?)?,
                created_at: row.get(9)?,
                resolved_at: row.get(10)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed inspecting native provider approval identity", error))
}

impl SqliteRuntimeRepository {
    /// Native provider callback has already occurred. Publish its exact
    /// approval identity, attached waiting turn/session and canonical journal
    /// in one SQLite transaction. The provider-ref index is the cross-process
    /// dedupe key, including when a caller has lost its commit acknowledgment.
    pub fn admit_provider_approval(
        &self,
        previous_turn: &TurnRecord,
        updated_turn: &TurnRecord,
        previous_session: &SessionRecord,
        updated_session: &SessionRecord,
        approval: &ApprovalRecord,
        event: &NewRuntimeEvent,
    ) -> Result<ProviderApprovalAdmission, RuntimeError> {
        if approval.id.is_empty()
            || approval.origin != "provider"
            || approval.status != "pending"
            || approval.response.is_some()
            || approval.resolved_at.is_some()
            || approval
                .provider_approval_ref
                .as_deref()
                .is_none_or(|s| s.trim().is_empty())
            || previous_turn.id != approval.turn_id
            || updated_turn.id != previous_turn.id
            || updated_turn.session_id != previous_turn.session_id
            || previous_session.id != approval.session_id
            || updated_session.id != previous_session.id
            || updated_turn.session_id != approval.session_id
            || previous_session.active_turn_id.as_deref() != Some(approval.turn_id.as_str())
            || updated_session.active_turn_id != previous_session.active_turn_id
            || !matches!(
                previous_session.status.as_str(),
                "turn_admitted" | "turn_running" | "waiting_for_approval"
            )
            || !matches!(
                previous_turn.status.as_str(),
                "admitted" | "in_progress" | "waiting_for_approval"
            )
            || previous_turn.completed_at.is_some()
            || updated_turn.completed_at.is_some()
            || updated_turn.status != "waiting_for_approval"
            || updated_session.status != "waiting_for_approval"
            || event.event_id != format!("evt_approval_{}_requested", approval.id)
            || event.scope != RuntimeEventScope::Session
            || event.scope_id != approval.session_id
            || event.session_id.as_deref() != Some(approval.session_id.as_str())
            || event.turn_id.as_deref() != Some(approval.turn_id.as_str())
            || event.kind != "approval.requested"
            || event.criticality != RuntimeEventCriticality::Critical
            || event.payload["approval_id"] != approval.id
            || event.payload["provider_approval_ref"].as_str()
                != approval.provider_approval_ref.as_deref()
            || event.payload["origin"] != "provider"
        {
            return Err(RuntimeError::ProtocolViolation(
                "native provider approval state and critical journal mismatch".into(),
            ));
        }

        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native approval transaction", error))?;

        if let Some(existing) = existing_provider_approval(&tx, approval)? {
            if existing.origin != "provider"
                || existing.request != approval.request
                || existing.tool_call_id != approval.tool_call_id
            {
                return Err(RuntimeError::Conflict(
                    "provider approval identity reused with different native request".into(),
                ));
            }
            // Legacy approvals may lack a journal event. Do not backfill a
            // fabricated event or alter already-resolved provider authority.
            if let Some(receipt) = fetch_runtime_event_by_event_id(
                &tx,
                &format!("evt_approval_{}_requested", existing.id),
            )? {
                if receipt.kind != "approval.requested"
                    || receipt.scope != RuntimeEventScope::Session
                    || receipt.scope_id != existing.session_id
                    || receipt.turn_id.as_deref() != Some(existing.turn_id.as_str())
                    || receipt.criticality != RuntimeEventCriticality::Critical
                    || receipt.payload["approval_id"] != existing.id
                    || receipt.payload["provider_approval_ref"].as_str()
                        != existing.provider_approval_ref.as_deref()
                {
                    return Err(RuntimeError::Conflict(
                        "native provider approval receipt conflicts with historical authority"
                            .into(),
                    ));
                }
            }
            return Ok(ProviderApprovalAdmission {
                approval: existing,
                turn: read_turn(&tx, &approval.turn_id)?,
                session: read_session(&tx, &approval.session_id)?,
                event: None,
            });
        }
        if fetch_runtime_event_by_event_id(&tx, &event.event_id)?.is_some() {
            return Err(RuntimeError::Conflict(
                "native provider approval event ID is already reserved".into(),
            ));
        }

        let dispatched: Option<String> = tx
            .query_row(
                "SELECT dispatch_state FROM turn_admissions
                 WHERE turn_id=?1 AND session_id=?2",
                params![approval.turn_id, approval.session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking durable native turn dispatch", error))?;
        if dispatched.as_deref() != Some("dispatched") {
            return Err(RuntimeError::Conflict(
                "native approval requires durable dispatched turn authority".into(),
            ));
        }

        let changed_turn = tx
            .execute(
                "UPDATE turns SET status='waiting_for_approval'
                 WHERE id=?1 AND session_id=?2 AND status=?3 AND completed_at IS ?4",
                params![
                    approval.turn_id,
                    approval.session_id,
                    previous_turn.status,
                    previous_turn.completed_at,
                ],
            )
            .map_err(|error| db_error("failed transitioning native approval turn", error))?;
        if changed_turn != 1 {
            return Err(RuntimeError::Conflict(
                "turn state changed before native approval admission".into(),
            ));
        }
        let changed_session = tx
            .execute(
                "UPDATE sessions SET status='waiting_for_approval', updated_at=?1
                 WHERE id=?2 AND active_turn_id=?3 AND status=?4 AND updated_at=?5",
                params![
                    updated_session.updated_at,
                    approval.session_id,
                    approval.turn_id,
                    previous_session.status,
                    previous_session.updated_at,
                ],
            )
            .map_err(|error| db_error("failed transitioning native approval session", error))?;
        if changed_session != 1 {
            return Err(RuntimeError::Conflict(
                "session authority changed before native approval admission".into(),
            ));
        }
        tx.execute(
            "INSERT INTO approvals (
                id,session_id,turn_id,origin,tool_call_id,provider_approval_ref,
                status,request_json,response_json,created_at,resolved_at
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                approval.id,
                approval.session_id,
                approval.turn_id,
                approval.origin,
                approval.tool_call_id,
                approval.provider_approval_ref,
                approval.status,
                json_to_string(&approval.request)?,
                opt_json_to_string(approval.response.as_ref())?,
                approval.created_at,
                approval.resolved_at,
            ],
        )
        .map_err(|error| db_error("failed recording provider-native approval", error))?;
        let receipt = insert_runtime_event_in_transaction(&tx, event)?;
        let result = ProviderApprovalAdmission {
            approval: approval.clone(),
            turn: read_turn(&tx, &approval.turn_id)?,
            session: read_session(&tx, &approval.session_id)?,
            event: Some(receipt),
        };
        tx.commit()
            .map_err(|error| db_error("failed committing native provider approval", error))?;
        Ok(result)
    }
}
