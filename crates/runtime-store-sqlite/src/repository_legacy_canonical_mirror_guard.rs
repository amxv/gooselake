//! Verify raw migrated legacy mirrors against their canonical delivery owner.
//! This guard is not used by the authoritative atomic status/result paths.

use runtime_core::{RuntimeError, TeamDeliveryRecord};
use rusqlite::{OptionalExtension, Transaction};

use crate::db::db_error;
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};

pub(crate) fn require_exact_canonical_mirror(
    tx: &Transaction<'_>,
    record: &TeamDeliveryRecord,
) -> Result<(), RuntimeError> {
    // A migrated legacy mirror is a projection of its canonical owner.
    // It can be harmlessly re-applied when identical, but cannot invent
    // a newer status, recipient, provider or native turn behind the
    // generation-specific canonical Critical receipt. Legacy-only rows
    // remain independent and keep their existing historic safeguards.
    // Generation-one legacy databases can write legacy deliveries before
    // the canonical agent_deliveries table is even installed. That is
    // historical-only authority, not a reason to weaken the exact mirror
    // guard once the modern schema actually exists.
    let has_canonical_table: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master
                 WHERE type='table' AND name='agent_deliveries')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking canonical delivery schema", error))?;
    let canonical: Option<runtime_core::AgentDeliveryRecord> = if has_canonical_table {
        tx.query_row(
            &format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE id=?1"),
            [&record.id],
            decode_delivery,
        )
        .optional()
        .map_err(|error| db_error("failed loading canonical legacy mirror owner", error))?
    } else {
        None
    };
    if let Some(owner) = canonical {
        let mapped_team: Option<String> = tx
            .query_row(
                "SELECT legacy_team_id FROM agent_messages WHERE id=?1",
                [&owner.message_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading canonical legacy team mapping", error))?
            .flatten();
        if mapped_team.as_deref() != Some(record.team_id.as_str())
            || owner.id != record.id
            || owner.message_id != record.message_id
            || owner.recipient_agent_id != record.recipient_agent_id
            || owner.provider != record.provider
            || owner.status != record.status
            || owner.effective_policy != record.effective_policy
            || owner.injection_strategy != record.injection_strategy
            || owner.injected_turn_id != record.injected_turn_id
            || owner.last_error_code != record.last_error_code
            || owner.last_error_message != record.last_error_message
            || owner.created_at != record.created_at
            || owner.updated_at != record.updated_at
        {
            return Err(RuntimeError::Conflict(
                "legacy mirror must exactly match its canonical delivery authority and mapped team"
                    .into(),
            ));
        }
    }

    Ok(())
}
