use runtime_core::{AgentDeliveryRecord, AgentMessageRecord, RuntimeError};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{collect_rows, db_error, json_to_string, open_connection};
use crate::repository_team_hooks::queue_team_hook_trigger_tx;
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn insert_agent_message_with_deliveries(
        &self,
        message: &AgentMessageRecord,
        deliveries: &[AgentDeliveryRecord],
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting agent message transaction", error))?;

        tx.execute(
            "INSERT INTO agent_messages (
                id, scope, context_kind, workspace_id, legacy_team_id, sender_agent_id,
                recipient_agent_ids_json, input_json, image_paths_json, priority, policy,
                correlation_id, reply_to_message_id, idempotency_key, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                message.id,
                message.scope,
                message.context_kind.as_str(),
                message.workspace_id,
                message.legacy_team_id,
                message.sender_agent_id,
                json_to_string(&serde_json::json!(message.recipient_agent_ids))?,
                json_to_string(&message.input)?,
                json_to_string(&serde_json::json!(message.image_paths))?,
                message.priority,
                message.policy,
                message.correlation_id,
                message.reply_to_message_id,
                message.idempotency_key,
                message.created_at,
            ],
        )
        .map_err(|error| db_error("failed inserting agent message", error))?;

        for delivery in deliveries {
            if delivery.message_id != message.id {
                return Err(RuntimeError::ProtocolViolation(format!(
                    "delivery {} does not belong to message {}",
                    delivery.id, message.id
                )));
            }
            insert_delivery(&tx, delivery)?;
        }

        // The sender's workspace, not a shared-team assumption, owns the
        // event. A cross-workspace direct recipient remains eligible only
        // because the immutable message recipient snapshot explicitly names it.
        let sender_workspace: Option<String> = tx
            .query_row(
                "SELECT w.workspace_id FROM workspace_agents a JOIN workspaces w
             ON w.workspace_id=a.workspace_id WHERE a.session_id=?1
             AND a.lifecycle_state='active' AND w.lifecycle_state='active'",
                [&message.sender_agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed identifying hook message sender workspace", error))?;
        // Publishing the redacted message event is part of the authoritative
        // message + delivery commit, never an after-commit best effort. The
        // sender workspace is the primary scope, while the subject projection
        // makes it independently addressable under the message ID.
        let (event_scope, event_scope_id) = sender_workspace
            .as_deref()
            .map(|id| ("workspace", id))
            .unwrap_or(("message", message.id.as_str()));
        let event_seq: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(seq),0)+1 FROM runtime_events WHERE scope=?1 AND scope_id=?2",
                params![event_scope, event_scope_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed assigning message event sequence", error))?;
        tx.execute(
            "INSERT INTO runtime_events
             (event_id,scope,scope_id,session_id,seq,kind,critical,payload_json,created_at)
             VALUES (?1,?2,?3,?4,?5,'message.sent',1,?6,?7)",
            params![
                format!("agent-message:{}:sent", message.id),
                event_scope,
                event_scope_id,
                message.sender_agent_id,
                event_seq,
                json_to_string(&serde_json::json!({
                    "message_id": message.id,
                    "workspace_id": sender_workspace,
                    "scope": message.scope,
                    "recipient_count": message.recipient_agent_ids.len(),
                }))?,
                message.created_at,
            ],
        )
        .map_err(|error| db_error("failed atomically publishing message event", error))?;

        if let Some(workspace_id) = sender_workspace.as_deref() {
            let event = match message.scope.as_str() {
                "direct" => Some(runtime_core::TeamHookEventName::PostDirectMessageSend),
                "broadcast" => Some(runtime_core::TeamHookEventName::PostBroadcastMessageSend),
                _ => None,
            };
            if let Some(event) = event {
                queue_team_hook_trigger_tx(
                    &tx,
                    workspace_id,
                    event,
                    &format!("workspace-hook:message:{}", message.id),
                    Some(&message.sender_agent_id),
                    serde_json::json!({
                        "kind":"message", "message_id":message.id,
                        "sender_agent_id":message.sender_agent_id,
                        "recipient_agent_ids":message.recipient_agent_ids.iter().take(128).collect::<Vec<_>>(),
                        "recipient_count":message.recipient_agent_ids.len(),
                        "recipients_truncated":message.recipient_agent_ids.len() > 128,
                        "scope":message.scope,
                    }),
                    &message.recipient_agent_ids,
                    message.created_at,
                )?;
            }
        }

        tx.commit()
            .map_err(|error| db_error("failed committing agent message transaction", error))?;
        Ok(())
    }

    pub fn upsert_agent_delivery(
        &self,
        delivery: &AgentDeliveryRecord,
    ) -> Result<(), RuntimeError> {
        // Historical raw-upsert compatibility cannot claim a native effect
        // or forge its correlated turn ID. Those statuses are exclusively
        // written by Critical-journaled generation CAS/result authority.
        if matches!(delivery.status.as_str(), "injecting" | "injected")
            || delivery.injected_turn_id.is_some()
        {
            return Err(RuntimeError::Conflict(
                "native delivery status requires the certified receipt path".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting raw canonical delivery guard", error))?;
        let existing_message: Option<String> = tx
            .query_row(
                "SELECT message_id FROM agent_deliveries WHERE id=?1",
                [&delivery.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking raw canonical delivery origin", error))?;
        let certified: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_events
                 WHERE (kind='message.sent' AND critical=1 AND event_id IN (?1,?2))
                   OR (kind LIKE 'agent_delivery.%' AND critical=1
                       AND json_extract(payload_json,'$.delivery.id')=?3))",
                params![
                    format!("agent-message:{}:sent", delivery.message_id),
                    existing_message.map(|id| format!("agent-message:{id}:sent")),
                    delivery.id,
                ],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking certified canonical delivery", error))?;
        if certified {
            return Err(RuntimeError::Conflict(
                "certified canonical delivery requires the Critical generation transaction; raw upsert cannot update or recreate it".into(),
            ));
        }
        // Preserve old unjournaled legacy migration fixtures only; new
        // canonical message admission always creates the Critical sent event.
        let changed = tx
            .execute(
                "INSERT INTO agent_deliveries (
                    id, message_id, recipient_agent_id, provider, status, effective_policy,
                    injection_strategy, injected_turn_id, last_error_code, last_error_message,
                    created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(id) DO UPDATE SET
                    message_id = excluded.message_id,
                    recipient_agent_id = excluded.recipient_agent_id,
                    provider = excluded.provider,
                    status = excluded.status,
                    effective_policy = excluded.effective_policy,
                    injection_strategy = excluded.injection_strategy,
                    injected_turn_id = excluded.injected_turn_id,
                    last_error_code = excluded.last_error_code,
                    last_error_message = excluded.last_error_message,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at
                 WHERE agent_deliveries.status NOT IN ('cancelled','injecting','injected')
                   AND excluded.updated_at >= agent_deliveries.updated_at
                   AND NOT (agent_deliveries.status='injecting'
                            AND excluded.status='pending')
                   AND NOT (agent_deliveries.status='injected'
                            AND excluded.status!='injected')",
                params![
                    delivery.id,
                    delivery.message_id,
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
            .map_err(|error| db_error("failed upserting agent delivery", error))?;
        if changed == 0 {
            return Err(RuntimeError::Conflict(format!(
                "delivery {} has a newer or terminal durable authority",
                delivery.id
            )));
        }
        tx.commit()
            .map_err(|error| db_error("failed committing guarded raw delivery update", error))?;
        Ok(())
    }

    pub fn get_agent_delivery(
        &self,
        id: &str,
    ) -> Result<Option<AgentDeliveryRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                &format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE id=?1"),
                [id],
                decode_delivery,
            )
            .optional()
            .map_err(|error| db_error("failed reading authoritative agent delivery", error))
    }

    pub fn list_agent_delivery_records(
        &self,
        request: &runtime_core::AgentDeliveryListRequest,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        let mut clauses = Vec::new();
        let mut values = Vec::new();
        if let Some(id) = request.message_id.as_deref() {
            clauses.push("message_id=?");
            values.push(id);
        }
        if let Some(id) = request.recipient_agent_id.as_deref() {
            clauses.push("recipient_agent_id=?");
            values.push(id);
        }
        let filter = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let mut statement = connection
            .prepare(&format!(
                "SELECT {DELIVERY_COLUMNS} FROM agent_deliveries{filter} ORDER BY created_at,id"
            ))
            .map_err(|error| db_error("failed preparing authoritative delivery list", error))?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(values), decode_delivery)
            .map_err(|error| db_error("failed listing authoritative agent deliveries", error))?;
        collect_rows(rows)
    }

    /// Indexed delivery keyset page. The `message_id` predicate is required
    /// for the `(message_id, created_at, id)` index; unlike the full listing
    /// used for internal recovery, this never loads the whole message fanout.
    pub fn list_agent_delivery_records_page(
        &self,
        request: &runtime_core::AgentDeliveryListRequest,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> Result<Vec<AgentDeliveryRecord>, RuntimeError> {
        let message_id = request.message_id.as_deref().ok_or_else(|| {
            RuntimeError::InvalidState("paged deliveries require a message ID".into())
        })?;
        if !(1..=101).contains(&limit) {
            return Err(RuntimeError::InvalidState(
                "paged delivery limit must be between 1 and 101".into(),
            ));
        }
        let connection = open_connection(&self.database_path)?;
        if let Some((created_at, id)) = cursor.as_ref() {
            // The keyset anchor must be a delivery the caller could have
            // obtained from this message/recipient collection. Otherwise a
            // stale or foreign cursor could silently omit valid deliveries.
            let present: i64 = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM agent_deliveries
                     WHERE id=?1 AND created_at=?2 AND message_id=?3
                       AND (?4 IS NULL OR recipient_agent_id=?4))",
                    params![
                        id,
                        created_at,
                        message_id,
                        request.recipient_agent_id.as_deref()
                    ],
                    |row| row.get(0),
                )
                .map_err(|error| db_error("failed validating delivery page cursor", error))?;
            if present == 0 {
                return Err(RuntimeError::InvalidState(
                    "delivery cursor is not in the selected message/recipient collection".into(),
                ));
            }
        }
        let mut sql = format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE message_id=?");
        let mut values: Vec<rusqlite::types::Value> = vec![message_id.to_owned().into()];
        if let Some(recipient) = request.recipient_agent_id.as_deref() {
            sql.push_str(" AND recipient_agent_id=?");
            values.push(recipient.to_owned().into());
        }
        if let Some((created_at, id)) = cursor {
            sql.push_str(" AND (created_at, id) > (?, ?)");
            values.push(created_at.into());
            values.push(id.into());
        }
        sql.push_str(" ORDER BY created_at, id LIMIT ?");
        values.push((limit as i64).into());
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| db_error("failed preparing indexed delivery page", error))?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(values), decode_delivery)
            .map_err(|error| db_error("failed listing indexed agent deliveries", error))?;
        collect_rows(rows)
    }
}

pub(crate) const DELIVERY_COLUMNS: &str =
    "id,message_id,recipient_agent_id,provider,status,effective_policy,
    injection_strategy,injected_turn_id,last_error_code,last_error_message,created_at,updated_at";

pub(crate) fn decode_delivery(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentDeliveryRecord> {
    Ok(AgentDeliveryRecord {
        id: row.get(0)?,
        message_id: row.get(1)?,
        recipient_agent_id: row.get(2)?,
        provider: row.get(3)?,
        status: row.get(4)?,
        effective_policy: row.get(5)?,
        injection_strategy: row.get(6)?,
        injected_turn_id: row.get(7)?,
        last_error_code: row.get(8)?,
        last_error_message: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn insert_delivery(
    connection: &rusqlite::Connection,
    delivery: &AgentDeliveryRecord,
) -> Result<(), RuntimeError> {
    connection
        .execute(
            "INSERT INTO agent_deliveries (
                id, message_id, recipient_agent_id, provider, status, effective_policy,
                injection_strategy, injected_turn_id, last_error_code, last_error_message,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                delivery.id,
                delivery.message_id,
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
        .map_err(|error| db_error("failed inserting agent delivery", error))?;
    Ok(())
}
