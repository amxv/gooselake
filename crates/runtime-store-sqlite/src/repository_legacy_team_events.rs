//! Older /v1 team roster writes previously mutated cache/SQLite first and
//! discarded failures from separately appended critical events. This module
//! owns an all-or-nothing durable roster+event transaction. It deliberately
//! does NOT attempt to certify asynchronous message delivery or native spawn.

use std::collections::HashSet;

use runtime_core::{
    LegacyTeamMutation, RuntimeError, RuntimeEventCriticality, RuntimeEventRecord,
    RuntimeEventScope, TeamMemberRecord, TeamRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, fetch_runtime_event_by_event_id, open_connection};
use crate::repository::insert_runtime_event_in_transaction;
use crate::SqliteRuntimeRepository;

fn read_team(tx: &Transaction<'_>, team_id: &str) -> Result<Option<TeamRecord>, RuntimeError> {
    tx.query_row(
        "SELECT id,name,lead_agent_id,created_by,created_at,updated_at,deleted_at
         FROM teams WHERE id=?1",
        [team_id],
        |row| {
            Ok(TeamRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                lead_agent_id: row.get(2)?,
                created_by: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
                deleted_at: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading atomic legacy team authority", error))
}

fn read_member(
    tx: &Transaction<'_>,
    team_id: &str,
    agent_id: &str,
) -> Result<Option<TeamMemberRecord>, RuntimeError> {
    tx.query_row(
        "SELECT team_id,agent_id,title,joined_at,added_by,creator_agent_id,
                creator_compaction_subscription,worktree_id
         FROM team_members WHERE team_id=?1 AND agent_id=?2",
        params![team_id, agent_id],
        |row| {
            Ok(TeamMemberRecord {
                team_id: row.get(0)?,
                agent_id: row.get(1)?,
                title: row.get(2)?,
                joined_at: row.get(3)?,
                added_by: row.get(4)?,
                creator_agent_id: row.get(5)?,
                creator_compaction_subscription: row.get(6)?,
                worktree_id: row.get(7)?,
            })
        },
    )
    .optional()
    .map_err(|error| db_error("failed reading legacy team membership authority", error))
}

fn exact_receipt(previous: &RuntimeEventRecord, mutation: &LegacyTeamMutation) -> bool {
    let event = &mutation.event;
    previous.kind == event.kind
        && previous.scope == event.scope
        && previous.scope_id == event.scope_id
        && previous.team_id == event.team_id
        && previous.session_id == event.session_id
        && previous.turn_id == event.turn_id
        && previous.criticality == event.criticality
        && previous.provider == event.provider
        && previous.provider_seq == event.provider_seq
        && previous.payload == event.payload
}

fn validate(m: &LegacyTeamMutation) -> Result<(), RuntimeError> {
    let event = &m.event;
    let after = &m.after_team;
    if after.id.is_empty()
        || after.name.trim().is_empty()
        || after.lead_agent_id.is_empty()
        || after.deleted_at.is_some()
        || after.updated_at < after.created_at
        || event.scope != RuntimeEventScope::Team
        || event.scope_id != after.id
        || event.team_id.as_deref() != Some(after.id.as_str())
        || event.turn_id.is_some()
        || event.provider.is_some()
        || event.provider_seq.is_some()
        || event.criticality != RuntimeEventCriticality::Critical
        || !event
            .event_id
            .starts_with(&format!("evt_team_{}_", after.id))
    {
        return Err(RuntimeError::ProtocolViolation(
            "legacy team mutation subject or critical event authority mismatch".into(),
        ));
    }
    let (subject, expected_payload) = match event.kind.as_str() {
        "team.created" => {
            let mut members = HashSet::new();
            if m.before_team.is_some()
                || m.before_member.is_some()
                || m.after_member.is_some()
                || m.initial_members.is_empty()
                || after.updated_at != after.created_at
                || event.event_id != format!("evt_team_{}_created", after.id)
                || m.initial_members.iter().any(|member| {
                    member.team_id != after.id
                        || member.joined_at != after.created_at
                        || member.agent_id.is_empty()
                        || !members.insert(member.agent_id.clone())
                })
                || !members.contains(&after.lead_agent_id)
            {
                return Err(RuntimeError::ProtocolViolation(
                    "legacy team creation needs one exact lead-bearing initial roster".into(),
                ));
            }
            (
                after.lead_agent_id.as_str(),
                serde_json::json!({"team":after,"members":m.initial_members}),
            )
        }
        "team.member_joined" | "team.member_left" | "team.lead_changed" => {
            if !m.initial_members.is_empty() {
                return Err(RuntimeError::ProtocolViolation(
                    "legacy roster delta cannot rewrite initial membership".into(),
                ));
            }
            let before = m.before_team.as_ref().ok_or_else(|| {
                RuntimeError::ProtocolViolation("legacy roster delta lacks prior team".into())
            })?;
            let mut expected = before.clone();
            expected.updated_at = after.updated_at;
            if event.kind == "team.lead_changed" {
                expected.lead_agent_id = after.lead_agent_id.clone();
            }
            if *after != expected
                || after.updated_at <= before.updated_at
                || before.deleted_at.is_some()
            {
                return Err(RuntimeError::ProtocolViolation(
                    "legacy roster delta attempts to mutate unrelated team authority".into(),
                ));
            }
            match event.kind.as_str() {
                "team.member_joined" => {
                    let member = m.after_member.as_ref().ok_or_else(|| {
                        RuntimeError::ProtocolViolation("legacy team join lacks new member".into())
                    })?;
                    if m.before_member.is_some()
                        || member.team_id != after.id
                        || member.agent_id.is_empty()
                        || member.joined_at < after.created_at
                    {
                        return Err(RuntimeError::ProtocolViolation(
                            "legacy team join member identity mismatch".into(),
                        ));
                    }
                    (
                        member.agent_id.as_str(),
                        serde_json::json!({"team":after,"member":member}),
                    )
                }
                "team.member_left" => {
                    let member = m.before_member.as_ref().ok_or_else(|| {
                        RuntimeError::ProtocolViolation(
                            "legacy team leave lacks prior member".into(),
                        )
                    })?;
                    if m.after_member.is_some()
                        || member.team_id != after.id
                        || member.agent_id == after.lead_agent_id
                    {
                        return Err(RuntimeError::ProtocolViolation(
                            "legacy team removal cannot remove the current lead".into(),
                        ));
                    }
                    (
                        member.agent_id.as_str(),
                        serde_json::json!({"team":after,"agent_id":member.agent_id}),
                    )
                }
                _ => {
                    if m.before_member.is_some()
                        || m.after_member.is_some()
                        || before.lead_agent_id == after.lead_agent_id
                    {
                        return Err(RuntimeError::ProtocolViolation(
                            "legacy lead change cannot mutate membership or remain unchanged"
                                .into(),
                        ));
                    }
                    (
                        after.lead_agent_id.as_str(),
                        serde_json::json!({"team":after,"lead_agent_id":after.lead_agent_id}),
                    )
                }
            }
        }
        _ => {
            return Err(RuntimeError::ProtocolViolation(
                "legacy team journal kind is not an authorized roster mutation".into(),
            ));
        }
    };
    if event.session_id.as_deref() != Some(subject) || event.payload != expected_payload {
        return Err(RuntimeError::ProtocolViolation(
            "legacy team critical event does not match the exact roster delta".into(),
        ));
    }
    Ok(())
}

fn insert_team(tx: &Transaction<'_>, team: &TeamRecord) -> Result<(), RuntimeError> {
    tx.execute(
        "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at,deleted_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            team.id,
            team.name,
            team.lead_agent_id,
            team.created_by,
            team.created_at,
            team.updated_at,
            team.deleted_at
        ],
    )
    .map_err(|error| db_error("failed inserting legacy team creation", error))?;
    Ok(())
}

fn insert_member(tx: &Transaction<'_>, member: &TeamMemberRecord) -> Result<(), RuntimeError> {
    tx.execute(
        "INSERT INTO team_members(team_id,agent_id,title,joined_at,added_by,
             creator_agent_id,creator_compaction_subscription,worktree_id)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            member.team_id,
            member.agent_id,
            member.title,
            member.joined_at,
            member.added_by,
            member.creator_agent_id,
            member.creator_compaction_subscription,
            member.worktree_id
        ],
    )
    .map_err(|error| db_error("failed inserting legacy team membership", error))?;
    Ok(())
}

fn member_delta_matches(
    tx: &Transaction<'_>,
    m: &LegacyTeamMutation,
) -> Result<bool, RuntimeError> {
    let target = m.after_member.as_ref().or(m.before_member.as_ref());
    match target {
        Some(member) => Ok(read_member(tx, &m.after_team.id, &member.agent_id)? == m.after_member),
        None => {
            for member in &m.initial_members {
                if read_member(tx, &m.after_team.id, &member.agent_id)?.as_ref() != Some(member) {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

impl SqliteRuntimeRepository {
    pub fn commit_legacy_team_mutation(
        &self,
        mutation: &LegacyTeamMutation,
    ) -> Result<RuntimeEventRecord, RuntimeError> {
        validate(mutation)?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting legacy roster journal", error))?;
        let after = &mutation.after_team;

        if let Some(previous) = fetch_runtime_event_by_event_id(&tx, &mutation.event.event_id)? {
            if !exact_receipt(&previous, mutation)
                || read_team(&tx, &after.id)?.as_ref() != Some(after)
                || !member_delta_matches(&tx, mutation)?
            {
                return Err(RuntimeError::Conflict(
                    "legacy team critical receipt collides with changed roster authority".into(),
                ));
            }
            return Ok(previous);
        }
        if read_team(&tx, &after.id)? != mutation.before_team {
            return Err(RuntimeError::Conflict(
                "legacy team roster changed before its critical journal receipt".into(),
            ));
        }
        match mutation.event.kind.as_str() {
            "team.created" => {
                insert_team(&tx, after)?;
                for member in &mutation.initial_members {
                    insert_member(&tx, member)?;
                }
            }
            "team.member_joined" => {
                let member = mutation
                    .after_member
                    .as_ref()
                    .expect("validated join member");
                if read_member(&tx, &after.id, &member.agent_id)?.is_some() {
                    return Err(RuntimeError::Conflict(
                        "legacy team member already exists or another join won".into(),
                    ));
                }
                insert_member(&tx, member)?;
            }
            "team.member_left" => {
                let member = mutation
                    .before_member
                    .as_ref()
                    .expect("validated leave member");
                if read_member(&tx, &after.id, &member.agent_id)?.as_ref() != Some(member) {
                    return Err(RuntimeError::Conflict(
                        "legacy team member changed before removal".into(),
                    ));
                }
                if tx
                    .query_row(
                        "SELECT COUNT(*) FROM team_members WHERE team_id=?1",
                        [&after.id],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(|error| {
                        db_error("failed counting remaining legacy team members", error)
                    })?
                    <= 1
                {
                    return Err(RuntimeError::Conflict(
                        "legacy team cannot remove final member".into(),
                    ));
                }
                tx.execute(
                    "DELETE FROM team_members WHERE team_id=?1 AND agent_id=?2",
                    params![after.id, member.agent_id],
                )
                .map_err(|error| db_error("failed deleting atomic team member", error))?;
            }
            "team.lead_changed" => {
                if read_member(&tx, &after.id, &after.lead_agent_id)?.is_none() {
                    return Err(RuntimeError::Conflict(
                        "new legacy team lead is not a durable member".into(),
                    ));
                }
            }
            _ => unreachable!("validated legacy roster event kind"),
        }
        if mutation.before_team.is_some() {
            let before = mutation.before_team.as_ref().expect("checked existence");
            let changed = tx
                .execute(
                    "UPDATE teams SET lead_agent_id=?1,updated_at=?2
                 WHERE id=?3 AND updated_at=?4 AND lead_agent_id=?5 AND deleted_at IS NULL",
                    params![
                        after.lead_agent_id,
                        after.updated_at,
                        after.id,
                        before.updated_at,
                        before.lead_agent_id
                    ],
                )
                .map_err(|error| db_error("failed updating atomic legacy team roster", error))?;
            if changed != 1 {
                return Err(RuntimeError::Conflict(
                    "legacy team roster compare-and-swap lost".into(),
                ));
            }
        }
        let receipt = insert_runtime_event_in_transaction(&tx, &mutation.event)?;
        tx.commit().map_err(|error| {
            db_error(
                "failed committing legacy team roster and critical event",
                error,
            )
        })?;
        Ok(receipt)
    }
}
