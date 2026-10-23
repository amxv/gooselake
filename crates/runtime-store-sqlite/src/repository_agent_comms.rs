use runtime_core::{AgentDeliveryRecord, AgentMessageRecord, RuntimeError};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
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
        if let Some(workspace_id) = sender_workspace {
            let event = match message.scope.as_str() {
                "direct" => Some(runtime_core::TeamHookEventName::PostDirectMessageSend),
                "broadcast" => Some(runtime_core::TeamHookEventName::PostBroadcastMessageSend),
                _ => None,
            };
            if let Some(event) = event {
                queue_team_hook_trigger_tx(
                    &tx,
                    &workspace_id,
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
        let connection = open_connection(&self.database_path)?;
        connection
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
                    updated_at = excluded.updated_at",
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
        Ok(())
    }
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
