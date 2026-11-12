use runtime_core::{AgentDeliveryRecord, RuntimeError};
use rusqlite::{params, OptionalExtension, Transaction};

use crate::db::db_error;

/// The check and the delivery claim that follows it must be inside the *same*
/// IMMEDIATE transaction. A retirement fence may be admitted while the public
/// workspace still says active; neither sender nor recipient may issue a new
/// native provider effect after that admission.
pub(crate) fn require_active_delivery_owners(
    tx: &Transaction<'_>,
    delivery: &AgentDeliveryRecord,
) -> Result<Option<String>, RuntimeError> {
    let source: Option<(Option<String>, String, i64)> = tx
        .query_row(
            "SELECT COALESCE(message.workspace_id,sender.workspace_id),message.sender_agent_id,
                    (SELECT COUNT(*) FROM json_each(message.recipient_agent_ids_json)
                     WHERE value=?2)
             FROM agent_messages message
             LEFT JOIN workspace_agents sender ON sender.session_id=message.sender_agent_id
             WHERE message.id=?1",
            params![delivery.message_id, delivery.recipient_agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| db_error("failed inspecting delivery source authority", error))?;
    let (workspace_id, sender_id, is_snapshot_recipient) = source.ok_or_else(|| {
        RuntimeError::NotFound(format!("delivery message {}", delivery.message_id))
    })?;
    if is_snapshot_recipient != 1 {
        return Err(RuntimeError::Conflict(format!(
            "delivery {} recipient is absent or duplicated in its immutable message snapshot",
            delivery.id
        )));
    }
    let mut owner_workspaces = Vec::new();
    if let Some(ref owner) = workspace_id {
        owner_workspaces.push(owner.clone());
    }
    for (role, session_id) in [
        ("sender", sender_id.as_str()),
        ("recipient", delivery.recipient_agent_id.as_str()),
    ] {
        let ownership: Option<(String, String)> = tx
            .query_row(
                "SELECT workspace_id,lifecycle_state FROM workspace_agents WHERE session_id=?1",
                [session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed inspecting delivery agent ownership", error))?;
        if let Some((owner, lifecycle)) = ownership {
            if lifecycle != "active" {
                return Err(RuntimeError::Conflict(format!(
                    "{role} {session_id} is archived and cannot dispatch a delivery",
                )));
            }
            owner_workspaces.push(owner);
        }
    }
    for owner in owner_workspaces {
        let state: Option<(String, bool)> = tx
            .query_row(
                "SELECT lifecycle_state,EXISTS(
                     SELECT 1 FROM workspace_retirement_fences
                     WHERE workspace_id=?1)
                 FROM workspaces WHERE workspace_id=?1",
                [&owner],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed inspecting delivery owner retirement", error))?;
        if !state.is_some_and(|(lifecycle, fenced)| lifecycle == "active" && !fenced) {
            return Err(RuntimeError::Conflict(format!(
                "workspace {owner} is retired or frozen; delivery dispatch refused"
            )));
        }
    }
    Ok(workspace_id)
}
