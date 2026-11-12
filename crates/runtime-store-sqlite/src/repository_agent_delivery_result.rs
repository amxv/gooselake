//! A positively acknowledged agent-message turn has one durable result.
//! An injecting claim alone is never permission to infer native success or
//! to resend a turn: correlated runtime admission and the Critical receipt
//! must agree in the same transaction as status, turn ID and legacy mirror.

use runtime_core::{AgentDeliveryRecord, RuntimeError, RuntimeEventCriticality};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};
use crate::repository_agent_delivery_cas::{
    agent_delivery_transition_event, insert_agent_delivery_receipt,
};
use crate::repository_legacy_team_messages::event_matches;
use crate::SqliteRuntimeRepository;

fn require_claim_receipt(
    tx: &Transaction<'_>,
    before: &AgentDeliveryRecord,
) -> Result<(), RuntimeError> {
    let mut statement = tx
        .prepare(
            "SELECT event_id FROM runtime_events
             WHERE kind='agent_delivery.injecting' AND critical=1
               AND session_id=?1
               AND json_extract(payload_json,'$.delivery.id')=?2
               AND json_extract(payload_json,'$.delivery.updated_at')=?3",
        )
        .map_err(|error| db_error("failed preparing native delivery claim receipt", error))?;
    let event_ids = statement
        .query_map(
            params![before.recipient_agent_id, before.id, before.updated_at],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| db_error("failed querying delivery claim receipt", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed reading delivery claim receipt", error))?;
    if event_ids.len() != 1 {
        return Err(RuntimeError::Conflict(format!(
            "delivery {} lacks one certified native dispatch claim",
            before.id
        )));
    }
    let event = fetch_runtime_event_by_event_id(tx, &event_ids[0])?
        .ok_or_else(|| RuntimeError::Conflict("native delivery claim event vanished".into()))?;
    let claim_generation = event.payload["before_updated_at"].as_i64();
    if event.kind != "agent_delivery.injecting"
        || event.criticality != RuntimeEventCriticality::Critical
        || event.turn_id.is_some()
        || event.created_at != before.updated_at
        || event.session_id.as_deref() != Some(before.recipient_agent_id.as_str())
        || event.payload["delivery"] != serde_json::json!(before)
        || claim_generation.is_none_or(|generation| {
            generation >= before.updated_at
                || event.event_id
                    != format!("evt_agent_delivery_{}_injecting_{generation}", before.id)
        })
    {
        return Err(RuntimeError::Conflict(
            "native delivery dispatch claim receipt was modified or forged".into(),
        ));
    }
    Ok(())
}

fn require_correlated_admitted_turn(
    tx: &Transaction<'_>,
    before: &AgentDeliveryRecord,
    turn_id: &str,
) -> Result<(), RuntimeError> {
    // An arbitrary turn ID, an unknown native dispatch, and a different
    // provider/recipient/correlation are NOT positive acknowledgement.
    let admitted: bool = tx
        .query_row(
            "SELECT EXISTS(
               SELECT 1 FROM turns turn
               JOIN turn_admissions a ON a.turn_id=turn.id
               WHERE turn.id=?1 AND turn.session_id=?2 AND a.session_id=?2
                 AND turn.source='agent_message_delivery_transport'
                 AND a.projection_source='agent_message_delivery_transport'
                 AND a.provider=?3
                 AND json_extract(a.correlation_json,'$.correlation_id')=?4
                 AND (a.dispatch_state='dispatched'
                      OR (a.dispatch_state='pending'
                          AND EXISTS (SELECT 1 FROM approvals approval
                              WHERE approval.turn_id=turn.id AND approval.status='pending')))
             )",
            params![
                turn_id,
                before.recipient_agent_id,
                before.provider,
                before.message_id
            ],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading correlated native turn", error))?;
    let in_use_elsewhere: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_deliveries
                 WHERE injected_turn_id=?1 AND id<>?2)
                 OR EXISTS(SELECT 1 FROM team_deliveries
                 WHERE injected_turn_id=?1 AND id<>?2)",
            params![turn_id, before.id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking reused agent turn identity", error))?;
    if !admitted || in_use_elsewhere {
        return Err(RuntimeError::Conflict(
            "agent delivery turn lacks unique correlated, committed native admission".into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn commit_agent_delivery_injected(
        &self,
        before: &AgentDeliveryRecord,
        turn_id: &str,
        updated_at: i64,
    ) -> Result<AgentDeliveryRecord, RuntimeError> {
        if before.status != "injecting"
            || before.injected_turn_id.is_some()
            || before.id.trim().is_empty()
            || turn_id.trim() != turn_id
            || turn_id.is_empty()
        {
            return Err(RuntimeError::ProtocolViolation(
                "native delivery acknowledgement requires the claimed generation and turn ID"
                    .into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting native delivery result", error))?;
        let observed = tx
            .query_row(
                &format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE id=?1"),
                [&before.id],
                decode_delivery,
            )
            .optional()
            .map_err(|error| db_error("failed reading native delivery generation", error))?
            .ok_or_else(|| RuntimeError::NotFound(format!("delivery {}", before.id)))?;
        let same_identity = observed.id == before.id
            && observed.message_id == before.message_id
            && observed.recipient_agent_id == before.recipient_agent_id
            && observed.provider == before.provider
            && observed.created_at == before.created_at;
        if !same_identity {
            return Err(RuntimeError::Conflict(
                "native delivery immutable recipient authority changed".into(),
            ));
        }
        require_claim_receipt(&tx, before)?;
        require_correlated_admitted_turn(&tx, before, turn_id)?;
        let event_id = format!(
            "evt_agent_delivery_{}_injected_{}",
            before.id, before.updated_at
        );
        if observed.status == "injected" && observed.injected_turn_id.as_deref() == Some(turn_id) {
            let event = agent_delivery_transition_event(&tx, before, &observed)?;
            let receipt = fetch_runtime_event_by_event_id(&tx, &event_id)?;
            if !receipt
                .as_ref()
                .is_some_and(|row| event_matches(row, &event))
            {
                return Err(RuntimeError::Conflict(
                    "injected delivery lacks its exact correlated Critical receipt".into(),
                ));
            }
            tx.commit()
                .map_err(|error| db_error("failed finishing acknowledged replay", error))?;
            return Ok(observed);
        }
        if observed != *before || updated_at <= before.updated_at {
            return Err(RuntimeError::Conflict(
                "native delivery claim is stale or its result is unresolved".into(),
            ));
        }
        let mut after = before.clone();
        after.status = "injected".into();
        after.injected_turn_id = Some(turn_id.into());
        after.injection_strategy = Some("runtime_send_turn".into());
        after.last_error_code = None;
        after.last_error_message = None;
        after.updated_at = updated_at;
        let legacy: Option<(String, i64)> = tx
            .query_row(
                "SELECT status,updated_at FROM team_deliveries WHERE id=?1",
                [&before.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed reading native delivery legacy mirror", error))?;
        if legacy
            .as_ref()
            .is_some_and(|(status, at)| status != &before.status || *at != before.updated_at)
        {
            return Err(RuntimeError::Conflict(
                "native delivery legacy mirror has a different generation".into(),
            ));
        }
        let changed = tx
            .execute(
                "UPDATE agent_deliveries SET status='injected', injection_strategy=?2,
                  injected_turn_id=?3,last_error_code=NULL,last_error_message=NULL,
                  updated_at=?4 WHERE id=?1 AND status='injecting' AND updated_at=?5",
                params![
                    before.id,
                    after.injection_strategy,
                    turn_id,
                    updated_at,
                    before.updated_at
                ],
            )
            .map_err(|error| db_error("failed committing native delivery state", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "native delivery acknowledgement lost generation ownership".into(),
            ));
        }
        if legacy.is_some() {
            let mirrored = tx
                .execute(
                    "UPDATE team_deliveries SET status='injected',injection_strategy=?2,
                      injected_turn_id=?3,last_error_code=NULL,last_error_message=NULL,
                      updated_at=?4 WHERE id=?1 AND status='injecting' AND updated_at=?5",
                    params![
                        before.id,
                        after.injection_strategy,
                        turn_id,
                        updated_at,
                        before.updated_at
                    ],
                )
                .map_err(|error| db_error("failed committing native delivery mirror", error))?;
            if mirrored != 1 {
                return Err(RuntimeError::Conflict(
                    "native delivery mirror lost generation ownership".into(),
                ));
            }
        }
        insert_agent_delivery_receipt(&tx, before, &after)?;
        tx.commit()
            .map_err(|error| db_error("failed committing native delivery result receipt", error))?;
        Ok(after)
    }
}
