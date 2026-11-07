use runtime_core::{AgentMessageListRequest, AgentMessageListResponse, RuntimeError};
use rusqlite::{params_from_iter, types::Value as SqlValue, OptionalExtension};

use crate::db::{collect_rows, db_error, open_connection};
use crate::repository_hydration::decode_agent_message;
use crate::SqliteRuntimeRepository;

const MESSAGE_COLUMNS: &str = "id,scope,context_kind,workspace_id,legacy_team_id,
    sender_agent_id,recipient_agent_ids_json,input_json,image_paths_json,priority,policy,
    correlation_id,reply_to_message_id,idempotency_key,created_at";

impl SqliteRuntimeRepository {
    /// Indexed chronological keyset paging. JSON input is decoded only for the
    /// returned page, never for the entire stored message history.
    pub fn list_agent_messages_page(
        &self,
        request: &AgentMessageListRequest,
    ) -> Result<AgentMessageListResponse, RuntimeError> {
        let limit = request.limit.unwrap_or(100);
        if !(1..=500).contains(&limit) {
            return Err(RuntimeError::InvalidState(
                "message history page limit must be between 1 and 500".into(),
            ));
        }
        let connection = open_connection(&self.database_path)?;
        let after = if let Some(cursor) = request.cursor.as_deref() {
            let anchor: Option<(i64, Option<String>, String)> = connection
                .query_row(
                    "SELECT created_at,workspace_id,sender_agent_id
                     FROM agent_messages WHERE id=?1",
                    [cursor],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(|error| db_error("failed resolving message history cursor", error))?;
            let (created_at, workspace_id, sender) = anchor.ok_or_else(|| {
                RuntimeError::InvalidState(format!("cursor message {cursor} not found"))
            })?;
            if request
                .workspace_id
                .as_ref()
                .is_some_and(|expected| workspace_id.as_deref() != Some(expected.as_str()))
                || request
                    .sender_agent_id
                    .as_ref()
                    .is_some_and(|expected| expected != &sender)
            {
                return Err(RuntimeError::InvalidState(format!(
                    "cursor message {cursor} is outside the requested collection"
                )));
            }
            Some((created_at, cursor))
        } else {
            None
        };

        let mut sql = format!("SELECT {MESSAGE_COLUMNS} FROM agent_messages WHERE 1=1");
        let mut args = Vec::<SqlValue>::new();
        if let Some(workspace_id) = request.workspace_id.as_ref() {
            sql.push_str(" AND workspace_id=?");
            args.push(SqlValue::Text(workspace_id.clone()));
        }
        if let Some(sender) = request.sender_agent_id.as_ref() {
            sql.push_str(" AND sender_agent_id=?");
            args.push(SqlValue::Text(sender.clone()));
        }
        if let Some((created_at, cursor)) = after {
            sql.push_str(" AND (created_at,id)>(?,?)");
            args.push(SqlValue::Integer(created_at));
            args.push(SqlValue::Text(cursor.to_string()));
        }
        sql.push_str(" ORDER BY created_at,id LIMIT ?");
        args.push(SqlValue::Integer((limit + 1) as i64));

        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| db_error("failed preparing indexed message history", error))?;
        let rows = statement
            .query_map(params_from_iter(args.iter()), decode_agent_message)
            .map_err(|error| db_error("failed querying indexed message history", error))?;
        let mut messages = collect_rows(rows)?;
        let has_more = messages.len() > limit;
        messages.truncate(limit);
        let next_cursor = if has_more {
            messages.last().map(|message| message.id.clone())
        } else {
            None
        };
        Ok(AgentMessageListResponse {
            messages,
            next_cursor,
        })
    }
}
