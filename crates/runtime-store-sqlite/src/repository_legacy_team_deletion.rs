//! A team tombstone, every cancellable pre-native recipient and all required
//! Critical deletion/completion receipts belong to one SQLite transaction.
//! Unknown native injecting results and canonical-owned mirrors fail closed.

use std::collections::{BTreeSet, HashMap};

use runtime_core::{
    NewRuntimeEvent, RuntimeError, RuntimeEventCriticality, RuntimeEventScope, TeamDeliveryRecord,
    TeamMessageRecord, TeamRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_legacy_team_delivery_result::{complete_recipient_snapshot, completed_event};
use crate::repository_legacy_team_delivery_transition::require_legacy_pre_native_lineage;
use crate::repository_legacy_team_events::read_team;
use crate::repository_legacy_team_messages::{deliveries_by_message, event_matches, message_by_id};
use crate::SqliteRuntimeRepository;

fn same_event(record: &NewRuntimeEvent, expected: &NewRuntimeEvent) -> bool {
    record.event_id == expected.event_id
        && record.scope == expected.scope
        && record.scope_id == expected.scope_id
        && record.session_id == expected.session_id
        && record.team_id == expected.team_id
        && record.turn_id == expected.turn_id
        && record.kind == expected.kind
        && record.criticality == expected.criticality
        && record.payload == expected.payload
        && record.provider == expected.provider
        && record.provider_seq == expected.provider_seq
        && record.created_at == expected.created_at
}

fn deleted_event(team: &TeamRecord) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: format!("evt_team_{}_deleted", team.id),
        scope: RuntimeEventScope::Team,
        scope_id: team.id.clone(),
        session_id: Some(team.created_by.clone()),
        team_id: Some(team.id.clone()),
        turn_id: None,
        kind: "team.deleted".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"team":team}),
        provider: None,
        provider_seq: None,
        created_at: team.updated_at,
    }
}

fn cancelled_event(before: &TeamDeliveryRecord, cancelled: &TeamDeliveryRecord) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: format!(
            "evt_legacy_team_delete_{}_delivery_{}_cancelled_{}",
            before.team_id, before.id, before.updated_at
        ),
        scope: RuntimeEventScope::Team,
        scope_id: before.team_id.clone(),
        session_id: Some(before.recipient_agent_id.clone()),
        team_id: Some(before.team_id.clone()),
        turn_id: None,
        kind: "team_delivery.cancelled".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({
            "delivery":cancelled,
            "before_status":before.status,
            "before_updated_at":before.updated_at,
        }),
        provider: None,
        provider_seq: None,
        created_at: cancelled.updated_at,
    }
}

fn sort_rows(rows: &mut [TeamDeliveryRecord]) {
    rows.sort_by(|a, b| (&a.message_id, &a.id).cmp(&(&b.message_id, &b.id)));
}

fn team_delivery_snapshots(
    tx: &Transaction<'_>,
    team_id: &str,
) -> Result<(HashMap<String, TeamMessageRecord>, Vec<TeamDeliveryRecord>), RuntimeError> {
    let message_ids = {
        let mut stmt = tx
            .prepare("SELECT id FROM team_messages WHERE team_id=?1 ORDER BY created_at,id")
            .map_err(|error| {
                db_error(
                    "failed inspecting legacy team deletion message snapshot",
                    error,
                )
            })?;
        let rows = stmt
            .query_map([team_id], |row| row.get::<_, String>(0))
            .map_err(|error| db_error("failed listing legacy team deletion messages", error))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| db_error("failed decoding legacy team deletion message ids", error))?
    };
    let mut messages = HashMap::new();
    let mut deliveries = Vec::new();
    for id in message_ids {
        let message = message_by_id(tx, &id)?
            .ok_or_else(|| RuntimeError::Conflict("legacy deletion message vanished".into()))?;
        let rows = deliveries_by_message(tx, &id)?;
        if !complete_recipient_snapshot(&message, &rows) {
            return Err(RuntimeError::Conflict(
                "legacy team deletion message has an incomplete recipient snapshot".into(),
            ));
        }
        deliveries.extend(rows);
        messages.insert(id, message);
    }
    let count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM team_deliveries WHERE team_id=?1",
            [team_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed counting total legacy deletion deliveries", error))?;
    if i64::try_from(deliveries.len()).ok() != Some(count) {
        return Err(RuntimeError::Conflict(
            "legacy team deletion has an orphan or cross-team recipient".into(),
        ));
    }
    sort_rows(&mut deliveries);
    Ok((messages, deliveries))
}

fn validate_team(before: &TeamRecord, after: &TeamRecord) -> Result<(), RuntimeError> {
    let mut expected = before.clone();
    expected.updated_at = after.updated_at;
    expected.deleted_at = Some(after.updated_at);
    if before.id.is_empty()
        || before.created_by.is_empty()
        || before.deleted_at.is_some()
        || after.updated_at <= before.updated_at
        || *after != expected
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy deletion must tombstone exactly one active team generation".into(),
        ));
    }
    Ok(())
}

fn verify_plan(
    before: &TeamRecord,
    after: &TeamRecord,
    all: &[TeamDeliveryRecord],
    cancelled: &[TeamDeliveryRecord],
    events: &[NewRuntimeEvent],
) -> Result<(), RuntimeError> {
    validate_team(before, after)?;
    let mut sorted = all.to_vec();
    sort_rows(&mut sorted);
    if sorted != all {
        return Err(RuntimeError::ProtocolViolation(
            "legacy deletion requires a sorted complete recipient snapshot".into(),
        ));
    }
    let cancellable = all
        .iter()
        .filter(|row| matches!(row.status.as_str(), "pending" | "deferred"))
        .collect::<Vec<_>>();
    if all.iter().any(|row| {
        row.team_id != before.id
            || row.status == "injecting"
            || !matches!(
                row.status.as_str(),
                "pending" | "deferred" | "failed" | "injected" | "cancelled"
            )
    }) || cancellable.len() != cancelled.len()
        || events.len() != cancelled.len() + 1
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy deletion cannot omit recipients or assert an unknown native result".into(),
        ));
    }
    for (index, (previous, result)) in cancellable.into_iter().zip(cancelled.iter()).enumerate() {
        let mut expected = previous.clone();
        expected.status = "cancelled".into();
        expected.updated_at = result.updated_at;
        expected.injection_strategy = None;
        expected.injected_turn_id = None;
        expected.last_error_code = None;
        expected.last_error_message = None;
        if result != &expected
            || result.updated_at <= previous.updated_at
            || result.updated_at != after.updated_at
            || !same_event(&events[index], &cancelled_event(previous, result))
        {
            return Err(RuntimeError::ProtocolViolation(
                "legacy deletion cancelled row and Critical event disagree".into(),
            ));
        }
    }
    if !same_event(
        events.last().expect("checked deletion event count"),
        &deleted_event(after),
    ) {
        return Err(RuntimeError::ProtocolViolation(
            "legacy deletion event is not one exact Critical tombstone receipt".into(),
        ));
    }
    Ok(())
}

/// A successful transaction may lose its reply. The next caller generates a
/// different clock sample, so replay must be proved from the *persisted*
/// tombstone and each generation-specific receipt, never from its proposal.
fn committed_deletion_replay(
    tx: &Transaction<'_>,
    before: &TeamRecord,
    tombstone: &TeamRecord,
    old_deliveries: &[TeamDeliveryRecord],
    current: &[TeamDeliveryRecord],
    messages: &HashMap<String, TeamMessageRecord>,
) -> Result<(), RuntimeError> {
    let mut expected_team = before.clone();
    expected_team.updated_at = tombstone.updated_at;
    expected_team.deleted_at = Some(tombstone.updated_at);
    if *tombstone != expected_team || tombstone.updated_at <= before.updated_at {
        return Err(RuntimeError::Conflict(
            "legacy team deletion replay has unrelated tombstone authority".into(),
        ));
    }
    let deletion = deleted_event(tombstone);
    if fetch_runtime_event_by_event_id(tx, &deletion.event_id)?
        .as_ref()
        .is_none_or(|event| !event_matches(event, &deletion))
    {
        return Err(RuntimeError::Conflict(
            "legacy deletion replay has no exact committed Critical tombstone".into(),
        ));
    }
    let mut expected_current = old_deliveries.to_vec();
    let mut completed_messages = BTreeSet::new();
    for row in &mut expected_current {
        if matches!(row.status.as_str(), "pending" | "deferred") {
            let previous = row.clone();
            row.status = "cancelled".into();
            row.updated_at = tombstone.updated_at;
            row.injection_strategy = None;
            row.injected_turn_id = None;
            row.last_error_code = None;
            row.last_error_message = None;
            let cancelled = cancelled_event(&previous, row);
            if fetch_runtime_event_by_event_id(tx, &cancelled.event_id)?
                .as_ref()
                .is_none_or(|event| !event_matches(event, &cancelled))
            {
                return Err(RuntimeError::Conflict(
                    "legacy deletion replay has missing/forged cancellation Critical receipt"
                        .into(),
                ));
            }
            completed_messages.insert(row.message_id.clone());
        }
    }
    if current != expected_current {
        return Err(RuntimeError::Conflict(
            "legacy deletion replay recipient generations no longer match exact tombstone".into(),
        ));
    }
    for id in completed_messages {
        let message = messages.get(&id).ok_or_else(|| {
            RuntimeError::Conflict("legacy deletion replay message vanished".into())
        })?;
        let mut rows = expected_current
            .iter()
            .filter(|row| row.message_id == id)
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| (a.created_at, a.id.as_str()).cmp(&(b.created_at, b.id.as_str())));
        if !complete_recipient_snapshot(message, &rows)
            || !rows
                .iter()
                .all(|row| matches!(row.status.as_str(), "injected" | "failed" | "cancelled"))
        {
            return Err(RuntimeError::Conflict(
                "legacy deletion replay has incomplete terminal recipient snapshot".into(),
            ));
        }
        let completion = completed_event(message, &rows);
        if fetch_runtime_event_by_event_id(tx, &completion.event_id)?
            .as_ref()
            .is_none_or(|event| !event_matches(event, &completion))
        {
            return Err(RuntimeError::Conflict(
                "legacy deletion replay has no exact terminal Critical completion".into(),
            ));
        }
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn commit_legacy_team_deletion(
        &self,
        before: &TeamRecord,
        after: &TeamRecord,
        all_deliveries: &[TeamDeliveryRecord],
        cancelled_deliveries: &[TeamDeliveryRecord],
        events: &[NewRuntimeEvent],
    ) -> Result<TeamRecord, RuntimeError> {
        verify_plan(before, after, all_deliveries, cancelled_deliveries, events)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting atomic legacy team deletion", error))?;
        let (messages, actual) = team_delivery_snapshots(&tx, &before.id)?;
        let mut expected_after = all_deliveries.to_vec();
        let replacements = cancelled_deliveries
            .iter()
            .map(|row| (row.id.as_str(), row))
            .collect::<HashMap<_, _>>();
        for row in &mut expected_after {
            if let Some(cancelled) = replacements.get(row.id.as_str()) {
                *row = (*cancelled).clone();
            }
        }
        let stored_team = read_team(&tx, &before.id)?;
        // Reconcile an already committed (possibly different timestamp)
        // tombstone before attempting any state mutation or event insertion.
        if let Some(winner) = stored_team
            .as_ref()
            .filter(|team| team.deleted_at.is_some())
        {
            committed_deletion_replay(&tx, before, winner, all_deliveries, &actual, &messages)?;
            return Ok(winner.clone());
        }
        if stored_team.as_ref() != Some(before) || actual != all_deliveries {
            return Err(RuntimeError::Conflict(
                "legacy team changed before deletion Critical authority".into(),
            ));
        }
        // The tombstone and a new caller-bound spawn admission compete for
        // the same SQLite IMMEDIATE write lock. Once native spawn has been
        // admitted, deletion must not win until its exact response is
        // committed or *all* native compensation is certified rolled back.
        // An unknown/missing stage, including a forged `completed` stage
        // without its original response, is not clearance to delete.
        let unsettled_keyed: Option<String> = tx
            .query_row(
                "SELECT a.operation_id FROM team_spawn_admissions a
                 LEFT JOIN team_operation_journal j
                   ON j.operation_id=a.operation_id AND j.team_id=a.team_id
                  AND j.kind='spawn_member_with_worktree'
                 WHERE a.team_id=?1 AND (
                     j.operation_id IS NULL OR
                     (j.stage!='rolled_back' AND NOT (
                         j.stage='completed' AND a.completed_response_json IS NOT NULL
                     ))
                 ) LIMIT 1",
                [&before.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking keyed spawn deletion fence", error))?;
        if let Some(operation_id) = unsettled_keyed {
            return Err(RuntimeError::Conflict(format!(
                "team deletion blocked by unresolved admitted spawn {operation_id}; inspect its original native outcome"
            )));
        }
        // Historical unkeyed spawns still have a planned native journal,
        // but no caller-bound retry identity. Never interpret their absence
        // of a successful HTTP response as proof that no native work ran.
        let unsettled_unkeyed: Option<String> = tx
            .query_row(
                "SELECT j.operation_id FROM team_operation_journal j
                 WHERE j.team_id=?1 AND j.kind='spawn_member_with_worktree'
                   AND j.stage NOT IN ('completed','rolled_back')
                   AND NOT EXISTS (
                       SELECT 1 FROM team_spawn_admissions a
                       WHERE a.operation_id=j.operation_id
                   ) LIMIT 1",
                [&before.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking unkeyed spawn deletion fence", error))?;
        if let Some(operation_id) = unsettled_unkeyed {
            return Err(RuntimeError::Conflict(format!(
                "team deletion blocked by unresolved legacy spawn {operation_id}; inspect its original native outcome"
            )));
        }
        for cancelled in cancelled_deliveries {
            let canonical: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM agent_deliveries WHERE id=?1)",
                    [&cancelled.id],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    db_error("failed inspecting canonical-owned team deletion", error)
                })?;
            if canonical {
                return Err(RuntimeError::Conflict(
                    "legacy team deletion cannot overwrite a canonical-owned delivery".into(),
                ));
            }
        }
        // Only journal-certified pre-native generations can be cancelled;
        // an old direct upsert must never become cancellation authority.
        for row in all_deliveries
            .iter()
            .filter(|row| matches!(row.status.as_str(), "pending" | "deferred" | "failed"))
        {
            require_legacy_pre_native_lineage(&tx, row)?;
        }
        // Stable transaction event order is part of the external cursor.
        let mut affected_messages = BTreeSet::new();
        {
            let changed = tx
                .execute(
                    "UPDATE teams SET updated_at=?1,deleted_at=?2
                 WHERE id=?3 AND updated_at=?4 AND deleted_at IS NULL",
                    params![
                        after.updated_at,
                        after.deleted_at,
                        after.id,
                        before.updated_at
                    ],
                )
                .map_err(|error| db_error("failed tombstoning certified legacy team", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "legacy deletion team revision CAS lost".into(),
                ));
            }
            for (previous, cancelled) in all_deliveries
                .iter()
                .filter(|row| matches!(row.status.as_str(), "pending" | "deferred"))
                .zip(cancelled_deliveries)
            {
                let changed = tx
                    .execute(
                        "UPDATE team_deliveries
                     SET status='cancelled',injection_strategy=NULL,injected_turn_id=NULL,
                         last_error_code=NULL,last_error_message=NULL,updated_at=?1
                     WHERE id=?2 AND team_id=?3 AND message_id=?4
                       AND status=?5 AND updated_at=?6",
                        params![
                            cancelled.updated_at,
                            previous.id,
                            previous.team_id,
                            previous.message_id,
                            previous.status,
                            previous.updated_at,
                        ],
                    )
                    .map_err(|error| {
                        db_error("failed cancelling legacy deletion recipient", error)
                    })?;
                if changed != 1 {
                    return Err(RuntimeError::Conflict(
                        "legacy team deletion recipient CAS lost".into(),
                    ));
                }
                affected_messages.insert(previous.message_id.clone());
            }
        }
        for event in &events[..cancelled_deliveries.len()] {
            let saved = insert_runtime_event_in_transaction(&tx, event)?;
            if !event_matches(&saved, event) {
                return Err(RuntimeError::Conflict(
                    "legacy deletion Critical cancellation receipt changed at insert".into(),
                ));
            }
        }
        for message_id in affected_messages {
            let message = messages.get(&message_id).ok_or_else(|| {
                RuntimeError::Conflict("legacy team deletion cancellation has no message".into())
            })?;
            let mut rows = expected_after
                .iter()
                .filter(|row| row.message_id == message_id)
                .cloned()
                .collect::<Vec<_>>();
            rows.sort_by(|a, b| (a.created_at, a.id.as_str()).cmp(&(b.created_at, b.id.as_str())));
            if !complete_recipient_snapshot(message, &rows) {
                return Err(RuntimeError::Conflict(
                    "legacy deletion completion has incomplete recipient snapshot".into(),
                ));
            }
            if rows
                .iter()
                .all(|row| matches!(row.status.as_str(), "injected" | "failed" | "cancelled"))
            {
                let completion = completed_event(message, &rows);
                let saved = insert_runtime_event_in_transaction(&tx, &completion)?;
                if !event_matches(&saved, &completion) {
                    return Err(RuntimeError::Conflict(
                        "legacy deletion completion Critical receipt changed".into(),
                    ));
                }
            }
        }
        let deletion = events.last().expect("verified event count");
        let saved = insert_runtime_event_in_transaction(&tx, deletion)?;
        if !event_matches(&saved, deletion) {
            return Err(RuntimeError::Conflict(
                "legacy team deletion Critical tombstone changed at commit".into(),
            ));
        }
        tx.commit()
            .map_err(|error| db_error("failed committing atomic legacy team deletion", error))?;
        Ok(after.clone())
    }
}
