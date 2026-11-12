//! The older /v1 message admission is pre-native authority: its message,
//! recipient snapshot, delivery rows and every initial critical team event
//! must either all commit or none commit. A returned existing receipt never
//! implies permission to inject a recipient's native turn again.

use std::collections::{HashMap, HashSet};

use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord, RuntimeEventScope,
    TeamDeliveryRecord, TeamMessageAck, TeamMessageRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{
    collect_rows, db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection,
    string_to_json,
};
use crate::repository::insert_runtime_event_in_transaction;
use crate::SqliteRuntimeRepository;

fn message_by_id(
    tx: &Transaction<'_>,
    message_id: &str,
) -> Result<Option<TeamMessageRecord>, RuntimeError> {
    tx.query_row(
        "SELECT id,team_id,scope,sender_agent_id,recipient_agent_ids_json,input_json,
                image_paths_json,priority,policy,correlation_id,reply_to_message_id,
                idempotency_key,created_at FROM team_messages WHERE id=?1",
        [message_id],
        |row| {
            Ok(TeamMessageRecord {
                id: row.get(0)?,
                team_id: row.get(1)?,
                scope: row.get(2)?,
                sender_agent_id: row.get(3)?,
                recipient_agent_ids: string_to_json(row.get(4)?)?,
                input: string_to_json(row.get(5)?)?,
                image_paths: string_to_json(row.get(6)?)?,
                priority: row.get(7)?,
                policy: row.get(8)?,
                correlation_id: row.get(9)?,
                reply_to_message_id: row.get(10)?,
                idempotency_key: row.get(11)?,
                created_at: row.get(12)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading legacy message admission", error))
}

fn deliveries_by_message(
    tx: &Transaction<'_>,
    message_id: &str,
) -> Result<Vec<TeamDeliveryRecord>, RuntimeError> {
    let mut statement = tx
        .prepare(
            "SELECT id,message_id,team_id,recipient_agent_id,provider,status,
                    effective_policy,injection_strategy,injected_turn_id,last_error_code,
                    last_error_message,created_at,updated_at
             FROM team_deliveries WHERE message_id=?1 ORDER BY created_at,id",
        )
        .map_err(|error| db_error("failed preparing legacy delivery receipt", error))?;
    let rows = statement
        .query_map([message_id], |row| {
            Ok(TeamDeliveryRecord {
                id: row.get(0)?,
                message_id: row.get(1)?,
                team_id: row.get(2)?,
                recipient_agent_id: row.get(3)?,
                provider: row.get(4)?,
                status: row.get(5)?,
                effective_policy: row.get(6)?,
                injection_strategy: row.get(7)?,
                injected_turn_id: row.get(8)?,
                last_error_code: row.get(9)?,
                last_error_message: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        })
        .map_err(|error| db_error("failed querying legacy delivery receipt", error))?;
    collect_rows(rows)
}

fn event_matches(row: &RuntimeEventRecord, expected: &NewRuntimeEvent) -> bool {
    row.event_id == expected.event_id
        && row.kind == expected.kind
        && row.scope == expected.scope
        && row.scope_id == expected.scope_id
        && row.team_id == expected.team_id
        && row.session_id == expected.session_id
        && row.turn_id == expected.turn_id
        && row.criticality == expected.criticality
        && row.provider == expected.provider
        && row.provider_seq == expected.provider_seq
        && row.payload == expected.payload
        && row.created_at == expected.created_at
}

fn message_intent_matches(existing: &TeamMessageRecord, requested: &TeamMessageRecord) -> bool {
    existing.team_id == requested.team_id
        && existing.scope == requested.scope
        && existing.sender_agent_id == requested.sender_agent_id
        && existing.recipient_agent_ids == requested.recipient_agent_ids
        && existing.input == requested.input
        && existing.image_paths == requested.image_paths
        && existing.priority == requested.priority
        && existing.policy == requested.policy
        && existing.correlation_id == requested.correlation_id
        && existing.reply_to_message_id == requested.reply_to_message_id
        && existing.idempotency_key == requested.idempotency_key
}

fn validate(
    message: &TeamMessageRecord,
    deliveries: &[TeamDeliveryRecord],
    events: &[NewRuntimeEvent],
) -> Result<(), RuntimeError> {
    let recipients = message.recipient_agent_ids.as_array().ok_or_else(|| {
        RuntimeError::ProtocolViolation("legacy message recipients are not an array".into())
    })?;
    if message.id.is_empty()
        || message.team_id.is_empty()
        || message.sender_agent_id.is_empty()
        || !matches!(message.scope.as_str(), "direct" | "broadcast")
        || events.len() != deliveries.len() + 1
        || recipients.len() != deliveries.len()
        || !message.image_paths.is_array()
        || message
            .idempotency_key
            .as_deref()
            .is_some_and(|key| key.trim().is_empty())
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy message admission shape or scope mismatch".into(),
        ));
    }
    if message.scope == "direct" && recipients.len() != 1 {
        return Err(RuntimeError::ProtocolViolation(
            "legacy direct message must have exactly one recipient".into(),
        ));
    }
    let mut seen_recipients = HashSet::new();
    let mut seen_delivery_ids = HashSet::new();
    for (recipient, delivery) in recipients.iter().zip(deliveries) {
        if recipient.as_str() != Some(delivery.recipient_agent_id.as_str())
            || !seen_recipients.insert(&delivery.recipient_agent_id)
            || !seen_delivery_ids.insert(&delivery.id)
            || delivery.id.is_empty()
            || delivery.message_id != message.id
            || delivery.team_id != message.team_id
            || delivery.provider.is_empty()
            || delivery.status != "pending"
            || delivery.effective_policy.as_deref() != Some(message.policy.as_str())
            || delivery.injection_strategy.is_some()
            || delivery.injected_turn_id.is_some()
            || delivery.last_error_code.is_some()
            || delivery.last_error_message.is_some()
            || delivery.created_at != message.created_at
            || delivery.updated_at != message.created_at
        {
            return Err(RuntimeError::ProtocolViolation(
                "legacy initial delivery is not an exact pending recipient snapshot".into(),
            ));
        }
    }
    let mut ids = HashSet::new();
    for (index, event) in events.iter().enumerate() {
        let (id, kind, subject, payload) = if index == 0 {
            (
                format!("evt_legacy_message_{}_created", message.id),
                "team_message.created",
                message.sender_agent_id.as_str(),
                serde_json::json!({"message":message,"deliveries":deliveries}),
            )
        } else {
            let delivery = &deliveries[index - 1];
            (
                format!("evt_legacy_delivery_{}_pending", delivery.id),
                "team_delivery.pending",
                delivery.recipient_agent_id.as_str(),
                serde_json::json!({"delivery":delivery}),
            )
        };
        if event.event_id != id
            || !ids.insert(event.event_id.as_str())
            || event.kind != kind
            || event.scope != RuntimeEventScope::Team
            || event.scope_id != message.team_id
            || event.team_id.as_deref() != Some(message.team_id.as_str())
            || event.session_id.as_deref() != Some(subject)
            || event.turn_id.is_some()
            || event.criticality != RuntimeEventCriticality::Critical
            || event.provider.is_some()
            || event.provider_seq.is_some()
            || event.payload != payload
            || event.created_at != message.created_at
        {
            return Err(RuntimeError::ProtocolViolation(
                "legacy message/delivery event differs from admission authority".into(),
            ));
        }
    }
    Ok(())
}

fn verify_stored_admission(
    tx: &Transaction<'_>,
    message: &TeamMessageRecord,
    deliveries: &[TeamDeliveryRecord],
) -> Result<(), RuntimeError> {
    // Old pre-journal legacy rows stay intact but cannot be retroactively
    // certified as an atomic receipt or used to silently re-dispatch native.
    let id = format!("evt_legacy_message_{}_created", message.id);
    let event = fetch_runtime_event_by_event_id(tx, &id)?.ok_or_else(|| {
        RuntimeError::Conflict("legacy message has no atomic critical admission receipt".into())
    })?;
    let originally_admitted = event.payload["deliveries"]
        .as_array()
        .ok_or_else(|| RuntimeError::Conflict("legacy message receipt has no deliveries".into()))?;
    if event.kind != "team_message.created"
        || event.criticality != RuntimeEventCriticality::Critical
        || event.scope != RuntimeEventScope::Team
        || event.scope_id != message.team_id
        || event.team_id.as_deref() != Some(message.team_id.as_str())
        || event.session_id.as_deref() != Some(message.sender_agent_id.as_str())
        || event.turn_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.payload["message"] != serde_json::json!(message)
        || event.created_at != message.created_at
        || originally_admitted.len() != deliveries.len()
    {
        return Err(RuntimeError::Conflict(
            "legacy message admission receipt drifted".into(),
        ));
    }
    let current_by_id: HashMap<_, _> = deliveries
        .iter()
        .map(|delivery| (delivery.id.as_str(), delivery))
        .collect();
    for initial_value in originally_admitted {
        let initial: TeamDeliveryRecord =
            serde_json::from_value(initial_value.clone()).map_err(|_| {
                RuntimeError::Conflict("legacy pending delivery receipt is malformed".into())
            })?;
        let current = current_by_id.get(initial.id.as_str()).ok_or_else(|| {
            RuntimeError::Conflict("legacy admission receipt delivery row is missing".into())
        })?;
        let id = format!("evt_legacy_delivery_{}_pending", initial.id);
        let pending = fetch_runtime_event_by_event_id(tx, &id)?.ok_or_else(|| {
            RuntimeError::Conflict("legacy pending delivery has no critical event".into())
        })?;
        if pending.kind != "team_delivery.pending"
            || pending.criticality != RuntimeEventCriticality::Critical
            || pending.scope != RuntimeEventScope::Team
            || pending.scope_id != message.team_id
            || pending.team_id.as_deref() != Some(message.team_id.as_str())
            || pending.session_id.as_deref() != Some(initial.recipient_agent_id.as_str())
            || pending.turn_id.is_some()
            || pending.provider.is_some()
            || pending.provider_seq.is_some()
            || pending.payload != serde_json::json!({"delivery":initial})
            || pending.created_at != message.created_at
            || initial.status != "pending"
            || initial.effective_policy.as_deref() != Some(message.policy.as_str())
            || initial.injection_strategy.is_some()
            || initial.injected_turn_id.is_some()
            || initial.last_error_code.is_some()
            || initial.last_error_message.is_some()
            || initial.updated_at != initial.created_at
            || initial.id != current.id
            || initial.message_id != current.message_id
            || initial.team_id != current.team_id
            || initial.recipient_agent_id != current.recipient_agent_id
            || initial.provider != current.provider
            || initial.created_at != current.created_at
        {
            return Err(RuntimeError::Conflict(
                "legacy pending delivery receipt drifted".into(),
            ));
        }
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn commit_legacy_team_message_admission(
        &self,
        message: &TeamMessageRecord,
        deliveries: &[TeamDeliveryRecord],
        events: &[NewRuntimeEvent],
    ) -> Result<TeamMessageAck, RuntimeError> {
        validate(message, deliveries, events)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting legacy message critical journal", error))?;

        let existing_key_id: Option<String> = if let Some(key) = message.idempotency_key.as_deref()
        {
            tx.query_row(
                "SELECT id FROM team_messages WHERE team_id=?1 AND sender_agent_id=?2
                    AND scope=?3 AND idempotency_key=?4",
                params![message.team_id, message.sender_agent_id, message.scope, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading legacy message idempotency key", error))?
        } else {
            None
        };
        let existing = match existing_key_id.as_deref() {
            Some(id) => message_by_id(&tx, id)?,
            None => message_by_id(&tx, &message.id)?,
        };
        if let Some(existing) = existing {
            if !message_intent_matches(&existing, message)
                || (existing_key_id.is_none()
                    && existing.id == message.id
                    && existing.created_at != message.created_at)
            {
                return Err(RuntimeError::Conflict(
                    "legacy message ID or idempotency key was already consumed by different input"
                        .into(),
                ));
            }
            let persisted_deliveries = deliveries_by_message(&tx, &existing.id)?;
            verify_stored_admission(&tx, &existing, &persisted_deliveries)?;
            let requested_by_recipient: HashMap<_, _> = deliveries
                .iter()
                .map(|delivery| (delivery.recipient_agent_id.as_str(), delivery))
                .collect();
            if persisted_deliveries.len() != deliveries.len()
                || persisted_deliveries.iter().any(|persisted| {
                    requested_by_recipient
                        .get(persisted.recipient_agent_id.as_str())
                        .is_none_or(|requested| {
                            requested.provider != persisted.provider
                                || requested.effective_policy != persisted.effective_policy
                        })
                })
            {
                return Err(RuntimeError::Conflict(
                    "legacy message delivery recipient/provider snapshot was changed".into(),
                ));
            }
            return Ok(TeamMessageAck {
                message: existing,
                deliveries: persisted_deliveries,
                disposition: "existing".into(),
            });
        }

        // Pre-check exact globally unique event IDs. The shared insert helper
        // returns an existing row for generic idempotent journal replay; for
        // a *new* message that would be an unacceptable forged authority.
        for event in events {
            if fetch_runtime_event_by_event_id(&tx, &event.event_id)?.is_some() {
                return Err(RuntimeError::Conflict(
                    "legacy message critical event ID already belongs to another admission".into(),
                ));
            }
        }
        let team_active: bool = tx
            .query_row(
                "SELECT deleted_at IS NULL FROM teams WHERE id=?1",
                [&message.team_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking legacy message team authority", error))?
            .unwrap_or(false);
        if !team_active {
            return Err(RuntimeError::Conflict(
                "legacy message team no longer exists".into(),
            ));
        }
        for agent_id in std::iter::once(message.sender_agent_id.as_str())
            .chain(deliveries.iter().map(|row| row.recipient_agent_id.as_str()))
        {
            let member: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM team_members WHERE team_id=?1 AND agent_id=?2)",
                    params![message.team_id, agent_id],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    db_error("failed checking legacy message recipient authority", error)
                })?;
            if !member {
                return Err(RuntimeError::Conflict(
                    "legacy message sender/recipient no longer belongs to team".into(),
                ));
            }
        }

        tx.execute(
            "INSERT INTO team_messages (
                id,team_id,scope,sender_agent_id,recipient_agent_ids_json,input_json,
                image_paths_json,priority,policy,correlation_id,reply_to_message_id,
                idempotency_key,created_at
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                message.id,
                message.team_id,
                message.scope,
                message.sender_agent_id,
                json_to_string(&message.recipient_agent_ids)?,
                json_to_string(&message.input)?,
                json_to_string(&message.image_paths)?,
                message.priority,
                message.policy,
                message.correlation_id,
                message.reply_to_message_id,
                message.idempotency_key,
                message.created_at,
            ],
        )
        .map_err(|error| db_error("failed inserting atomic legacy message", error))?;
        for delivery in deliveries {
            tx.execute(
                "INSERT INTO team_deliveries (
                    id,message_id,team_id,recipient_agent_id,provider,status,effective_policy,
                    injection_strategy,injected_turn_id,last_error_code,last_error_message,
                    created_at,updated_at
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
                params![
                    delivery.id,
                    delivery.message_id,
                    delivery.team_id,
                    delivery.recipient_agent_id,
                    delivery.provider,
                    delivery.status,
                    delivery.effective_policy,
                    delivery.injection_strategy,
                    delivery.injected_turn_id,
                    delivery.last_error_code,
                    delivery.last_error_message,
                    delivery.created_at,
                    delivery.updated_at,
                ],
            )
            .map_err(|error| db_error("failed inserting atomic legacy pending delivery", error))?;
        }
        for event in events {
            let inserted = insert_runtime_event_in_transaction(&tx, event)?;
            if !event_matches(&inserted, event) {
                return Err(RuntimeError::Conflict(
                    "legacy critical event receipt collision".into(),
                ));
            }
        }
        tx.commit().map_err(|error| {
            db_error("failed committing legacy message critical journal", error)
        })?;
        Ok(TeamMessageAck {
            message: message.clone(),
            deliveries: deliveries.to_vec(),
            disposition: "created".into(),
        })
    }
}
