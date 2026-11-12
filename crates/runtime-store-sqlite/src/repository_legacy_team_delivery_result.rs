//! A native turn acknowledgement does not permit the old cache-first legacy
//! status upsert and best-effort event append. The exact injecting generation,
//! its admitted native turn, the injected projection and Critical receipt
//! must converge in one SQLite transaction; a lost commit reply never grants
//! permission to dispatch a second provider turn.

use std::collections::HashSet;

use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, TeamDeliveryRecord,
    TeamMessageRecord,
};
use rusqlite::{params, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_legacy_team_messages::{deliveries_by_message, event_matches, message_by_id};
use crate::SqliteRuntimeRepository;

fn validate(
    before: &TeamDeliveryRecord,
    after: &TeamDeliveryRecord,
    event: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let mut expected = before.clone();
    expected.status = "injected".into();
    expected.injection_strategy = Some("runtime_send_turn".into());
    expected.injected_turn_id = after.injected_turn_id.clone();
    expected.last_error_code = None;
    expected.last_error_message = None;
    expected.updated_at = after.updated_at;
    let trigger = event.payload["trigger"].as_str().unwrap_or_default();
    if before.status != "injecting"
        || before.id.is_empty()
        || before.message_id.is_empty()
        || before.team_id.is_empty()
        || before.recipient_agent_id.is_empty()
        || before.injected_turn_id.is_some()
        || after.updated_at <= before.updated_at
        || after
            .injected_turn_id
            .as_deref()
            .is_none_or(|id| id.trim() != id || id.is_empty())
        || *after != expected
        || !matches!(
            trigger,
            "queue" | "retry" | "turncompletedboundary" | "startuprecovery"
        )
        || event.event_id
            != format!(
                "evt_legacy_delivery_{}_injected_{}",
                before.id, before.updated_at
            )
        || event.kind != "team_delivery.injected"
        || event.criticality != RuntimeEventCriticality::Critical
        || event.scope != RuntimeEventScope::Team
        || event.scope_id != before.team_id
        || event.team_id.as_deref() != Some(before.team_id.as_str())
        || event.session_id.as_deref() != Some(before.recipient_agent_id.as_str())
        || event.turn_id.as_deref() != after.injected_turn_id.as_deref()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.created_at != after.updated_at
        || event.payload
            != serde_json::json!({
                "delivery":after,"before_updated_at":before.updated_at,"trigger":trigger
            })
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy acknowledged native turn does not match one exact injecting generation".into(),
        ));
    }
    Ok(())
}

fn verify_native_turn_admission(
    tx: &Transaction<'_>,
    before: &TeamDeliveryRecord,
    turn_id: &str,
) -> Result<(), RuntimeError> {
    // This is runtime acknowledgement evidence, not inference from a
    // caller-provided string. For an approved waiting turn, pre-dispatch
    // admission may still be pending; do not pretend the native provider ran.
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
        .map_err(|error| {
            db_error(
                "failed inspecting acknowledged team native turn authority",
                error,
            )
        })?;
    let elsewhere: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM team_deliveries
             WHERE injected_turn_id=?1 AND id<>?2)
         OR EXISTS(SELECT 1 FROM agent_deliveries
             WHERE injected_turn_id=?1 AND id<>?2)",
            params![turn_id, before.id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking reused legacy native turn id", error))?;
    if !admitted || elsewhere {
        return Err(RuntimeError::Conflict(
            "legacy delivery native turn has no matching unique committed runtime admission".into(),
        ));
    }
    Ok(())
}

pub(crate) fn complete_recipient_snapshot(
    message: &TeamMessageRecord,
    rows: &[TeamDeliveryRecord],
) -> bool {
    let Some(recipients) = message.recipient_agent_ids.as_array() else {
        return false;
    };
    if recipients.len() != rows.len() {
        return false;
    }
    let mut seen = HashSet::new();
    rows.iter().all(|row| {
        row.team_id == message.team_id
            && row.message_id == message.id
            && seen.insert(row.recipient_agent_id.as_str())
            && recipients
                .iter()
                .any(|value| value.as_str() == Some(row.recipient_agent_id.as_str()))
    })
}

pub(crate) fn completed_event(
    message: &TeamMessageRecord,
    rows: &[TeamDeliveryRecord],
) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: format!("evt_legacy_message_{}_completed_delivery", message.id),
        scope: RuntimeEventScope::Team,
        scope_id: message.team_id.clone(),
        session_id: None,
        team_id: Some(message.team_id.clone()),
        turn_id: None,
        kind: "team_message.completed".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"message_id":message.id,"deliveries":rows}),
        provider: None,
        provider_seq: None,
        created_at: rows
            .iter()
            .map(|row| row.updated_at)
            .max()
            .unwrap_or(message.created_at),
    }
}

impl SqliteRuntimeRepository {
    pub fn commit_legacy_team_delivery_injected(
        &self,
        before: &TeamDeliveryRecord,
        after: &TeamDeliveryRecord,
        event: &NewRuntimeEvent,
    ) -> Result<TeamDeliveryRecord, RuntimeError> {
        validate(before, after, event)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                db_error(
                    "failed starting acknowledged legacy delivery journal",
                    error,
                )
            })?;
        let message = message_by_id(&tx, &before.message_id)?.ok_or_else(|| {
            RuntimeError::Conflict("native legacy delivery message vanished".into())
        })?;
        if message.team_id != before.team_id
            || !message.recipient_agent_ids.as_array().is_some_and(|ids| {
                ids.iter()
                    .any(|id| id.as_str() == Some(before.recipient_agent_id.as_str()))
            })
        {
            return Err(RuntimeError::Conflict(
                "native legacy delivery message identity changed".into(),
            ));
        }
        let current = deliveries_by_message(&tx, &before.message_id)?
            .into_iter()
            .find(|row| row.id == before.id)
            .ok_or_else(|| RuntimeError::Conflict("native legacy delivery row vanished".into()))?;
        let claim_id = format!(
            "evt_legacy_delivery_{}_injecting_{}",
            before.id, before.updated_at
        );
        let claim = fetch_runtime_event_by_event_id(&tx, &claim_id)?
            .ok_or_else(|| RuntimeError::Conflict("unproven native injection claim".into()))?;
        if claim.kind != "team_delivery.injecting"
            || claim.criticality != RuntimeEventCriticality::Critical
            || claim.scope != RuntimeEventScope::Team
            || claim.scope_id != before.team_id
            || claim.team_id.as_deref() != Some(before.team_id.as_str())
            || claim.session_id.as_deref() != Some(before.recipient_agent_id.as_str())
            || claim.turn_id.is_some()
            || claim.provider.is_some()
            || claim.provider_seq.is_some()
            || claim.created_at != before.updated_at
            || claim.payload["delivery"] != serde_json::json!(before)
            || !claim.payload["trigger"].as_str().is_some_and(|trigger| {
                matches!(
                    trigger,
                    "queue" | "retry" | "turncompletedboundary" | "startuprecovery"
                )
            })
        {
            return Err(RuntimeError::Conflict(
                "native injecting Critical claim differs from durable generation".into(),
            ));
        }
        let canonical: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agent_deliveries WHERE id=?1)",
                [&before.id],
                |row| row.get(0),
            )
            .map_err(|error| {
                db_error("failed checking canonical native delivery ownership", error)
            })?;
        if canonical {
            return Err(RuntimeError::Conflict(
                "canonical agent delivery owns native result".into(),
            ));
        }
        verify_native_turn_admission(
            &tx,
            before,
            after
                .injected_turn_id
                .as_deref()
                .expect("validated turn id"),
        )?;
        let injected_receipt = fetch_runtime_event_by_event_id(&tx, &event.event_id)?;
        if current == *after {
            // A lost SQLite response may have committed the positive result;
            // exact event + state proves replay, but does NOT authorize another send.
            if injected_receipt
                .as_ref()
                .is_some_and(|receipt| event_matches(receipt, event))
            {
                return Ok(current);
            }
            return Err(RuntimeError::Conflict(
                "native delivery injected without a matching Critical receipt".into(),
            ));
        }
        if current != *before {
            return Err(RuntimeError::Conflict(
                "another writer changed injecting delivery authority".into(),
            ));
        }
        if injected_receipt.is_some() {
            return Err(RuntimeError::Conflict(
                "native result event ID consumed before delivery commit".into(),
            ));
        }
        let changed = tx
            .execute(
                "UPDATE team_deliveries
               SET status='injected',injection_strategy=?1,injected_turn_id=?2,
                   last_error_code=NULL,last_error_message=NULL,updated_at=?3
             WHERE id=?4 AND message_id=?5 AND status='injecting' AND updated_at=?6",
                params![
                    after.injection_strategy,
                    after.injected_turn_id,
                    after.updated_at,
                    before.id,
                    before.message_id,
                    before.updated_at
                ],
            )
            .map_err(|error| {
                db_error("failed writing acknowledged native delivery result", error)
            })?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(
                "native result generation lost compare-and-swap".into(),
            ));
        }
        let saved = insert_runtime_event_in_transaction(&tx, event)?;
        if !event_matches(&saved, event) {
            return Err(RuntimeError::Conflict(
                "native result Critical event mismatched committed state".into(),
            ));
        }

        let all = deliveries_by_message(&tx, &message.id)?;
        if !complete_recipient_snapshot(&message, &all) {
            return Err(RuntimeError::Conflict(
                "native delivery completion has incomplete recipient snapshot".into(),
            ));
        }
        let terminal = all
            .iter()
            .all(|row| matches!(row.status.as_str(), "injected" | "failed" | "cancelled"));
        let completion = completed_event(&message, &all);
        if terminal {
            if fetch_runtime_event_by_event_id(&tx, &completion.event_id)?.is_some() {
                return Err(RuntimeError::Conflict(
                    "legacy completion Critical ID consumed before last delivery".into(),
                ));
            }
            let saved = insert_runtime_event_in_transaction(&tx, &completion)?;
            if !event_matches(&saved, &completion) {
                return Err(RuntimeError::Conflict(
                    "legacy completion Critical receipt does not match final snapshot".into(),
                ));
            }
        } else if fetch_runtime_event_by_event_id(&tx, &completion.event_id)?.is_some() {
            return Err(RuntimeError::Conflict(
                "legacy completion receipt exists with nonterminal delivery".into(),
            ));
        }
        tx.commit().map_err(|error| {
            db_error(
                "failed committing native delivery result and critical event",
                error,
            )
        })?;
        Ok(after.clone())
    }
}
