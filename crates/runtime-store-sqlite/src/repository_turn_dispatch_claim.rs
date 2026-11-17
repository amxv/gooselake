//! A first provider dispatch consumes the exact durable Pending state.
//! Known provider outcomes continue through their existing receipt writers.
use crate::db::{db_error, open_connection};
use crate::repository_turn_authority::turn_admission_from_row;
use crate::SqliteRuntimeRepository;
use runtime_core::{RuntimeError, TurnAdmissionRecord, TurnDispatchState};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

impl SqliteRuntimeRepository {
    pub(crate) fn claim_pending_turn_dispatch(
        &self,
        requested: &TurnAdmissionRecord,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed beginning first turn dispatch claim", error))?;
        let current = tx
            .query_row(
                "SELECT turn_id,session_id,provider,projection_source,user_input_snapshot_json,
                    dispatch_policy_json,correlation_json,dispatch_state,
                    provider_native_turn_id,dispatch_error_json,admitted_at,updated_at
             FROM turn_admissions WHERE turn_id=?1",
                [&requested.turn_id],
                turn_admission_from_row,
            )
            .optional()
            .map_err(|error| db_error("failed reading original pending turn dispatch", error))?
            .ok_or_else(|| {
                RuntimeError::Conflict(
                    "first provider dispatch requires an original durable turn admission".into(),
                )
            })?;
        if current.dispatch_state != TurnDispatchState::Pending
            || current.provider_native_turn_id.is_some()
        {
            return Err(RuntimeError::TurnDispatchNotClaimed {
                turn_id: requested.turn_id.clone(),
                blocking_operation_id: None,
            });
        }
        let mut expected = current;
        expected.dispatch_state = TurnDispatchState::Dispatching;
        expected.dispatch_error = None;
        expected.updated_at = requested.updated_at;
        if &expected != requested {
            return Err(RuntimeError::Conflict(
                "first provider dispatch does not match its immutable admitted target and input"
                    .into(),
            ));
        }
        crate::repository_legacy_spawn_obligations::guard_legacy_native_session(
            &tx,
            &requested.session_id,
        )?;
        if let Some(claim) = crate::repository_startup_reattachment::claim_for(
            &tx,
            crate::repository_startup_reattachment::RESUME_CLAIM,
            &requested.session_id,
        )? {
            return Err(RuntimeError::TurnDispatchNotClaimed {
                turn_id: requested.turn_id.clone(),
                blocking_operation_id: Some(claim.owner_operation_id),
            });
        }
        let changed = tx.execute(
            "UPDATE turn_admissions SET dispatch_state='dispatching',dispatch_error_json=NULL,updated_at=?2
             WHERE turn_id=?1 AND session_id=?3 AND dispatch_state='pending'
               AND provider_native_turn_id IS NULL",
            params![requested.turn_id, requested.updated_at, requested.session_id],
        ).map_err(|error| db_error("failed claiming first provider turn dispatch", error))?;
        if changed != 1 {
            return Err(RuntimeError::TurnDispatchNotClaimed {
                turn_id: requested.turn_id.clone(),
                blocking_operation_id: None,
            });
        }
        tx.commit()
            .map_err(|error| db_error("failed committing first turn dispatch claim", error))
    }
}
