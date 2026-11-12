use runtime_core::{
    RuntimeError, TeamMemberSpawnAdmission, TeamMemberSpawnRequest, TeamMemberSpawnResponse,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::db::{db_error, json_to_string, open_connection};
use crate::SqliteRuntimeRepository;

const KIND: &str = "spawn_member_with_worktree";

impl SqliteRuntimeRepository {
    /// Return the original immutable receipt, including after source session
    /// retirement. A missing or damaged terminal journal is never a green
    /// light to repeat provider/Git/onboarding effects.
    pub fn replay_team_member_spawn(
        &self,
        team_id: &str,
        source_session_id: &str,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<Option<TeamMemberSpawnAdmission>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        lookup_spawn_admission(
            &connection,
            team_id,
            source_session_id,
            idempotency_key,
            request_hash,
        )
    }

    /// The first caller takes an SQLite IMMEDIATE lock and atomically commits
    /// both an immutable keyed reservation and its operator recovery journal.
    /// Exactly one concurrent worker receives Execute; all others are fenced.
    pub fn admit_team_member_spawn(
        &self,
        operation_id: &str,
        request: &TeamMemberSpawnRequest,
        request_hash: &str,
        requested_at: i64,
    ) -> Result<TeamMemberSpawnAdmission, RuntimeError> {
        let key = request.idempotency_key.as_deref().ok_or_else(|| {
            RuntimeError::InvalidState("keyed team spawn admission requires a key".into())
        })?;
        if key.is_empty()
            || key.len() > 512
            || key.trim() != key
            || key.chars().any(char::is_control)
            || request.team_id.trim() != request.team_id
            || request.team_id.is_empty()
            || request.source_session_id.trim() != request.source_session_id
            || request.source_session_id.is_empty()
            || operation_id.is_empty()
            || request_hash
                != runtime_core::normalized_json_hash(&serde_json::to_value(request).map_err(
                    |error| {
                        RuntimeError::InvalidState(format!("invalid team spawn request: {error}"))
                    },
                )?)?
        {
            return Err(RuntimeError::InvalidState(
                "invalid trusted caller-bound team spawn admission".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed locking caller-bound spawn admission", error))?;
        if let Some(existing) = lookup_spawn_admission(
            &transaction,
            &request.team_id,
            &request.source_session_id,
            key,
            request_hash,
        )? {
            return Ok(existing);
        }

        // The legacy team cache can race a deletion by another SQLite
        // worker. The same write lock that reserves native admission must
        // prove this team still exists and is not tombstoned. A later
        // deletion still needs its own native-lifecycle fence; this check
        // only prevents work based on already-stale roster authority.
        let team_active = transaction
            .query_row(
                "SELECT 1 FROM teams WHERE id=?1 AND deleted_at IS NULL",
                [&request.team_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|error| db_error("failed checking team spawn roster authority", error))?;
        if team_active != Some(1) {
            return Err(RuntimeError::Conflict(
                "team is deleted or missing; legacy native spawn is not authorized".into(),
            ));
        }

        transaction
            .execute(
                "INSERT INTO team_operation_journal
                 (operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
                 VALUES (?1,?2,?3,'planned',?4,?5,?5)",
                params![
                    operation_id,
                    request.team_id,
                    KIND,
                    json_to_string(&serde_json::json!({
                        "source_session_id": request.source_session_id,
                        "worktree": request.worktree,
                        "caller_bound_admission": true,
                    }))?,
                    requested_at,
                ],
            )
            .map_err(|error| db_error("failed writing initial team spawn journal", error))?;
        transaction
            .execute(
                "INSERT INTO team_spawn_admissions
                 (team_id,source_session_id,idempotency_key,request_hash,operation_id,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?6)",
                params![
                    request.team_id,
                    request.source_session_id,
                    key,
                    request_hash,
                    operation_id,
                    requested_at,
                ],
            )
            .map_err(|error| db_error("failed reserving caller-bound team spawn", error))?;
        transaction
            .commit()
            .map_err(|error| db_error("failed committing caller-bound team spawn", error))?;
        Ok(TeamMemberSpawnAdmission::Execute {
            operation_id: operation_id.to_string(),
        })
    }

    /// Final exact response and the journal's terminal stage are one SQLite
    /// transaction. Missing terminal receipts cannot be reconstructed from a
    /// cached response or inferred from native state on an ordinary retry.
    pub fn complete_team_member_spawn(
        &self,
        operation_id: &str,
        response: &TeamMemberSpawnResponse,
        completed_at: i64,
    ) -> Result<(), RuntimeError> {
        if response.operation_id != operation_id
            || response.journal_stage != "completed"
            || response.spawned_session.id != response.spawned_member.agent_id
        {
            return Err(RuntimeError::InvalidState(
                "team spawn completion does not match original operation and session".into(),
            ));
        }
        let mut connection = open_connection(&self.database_path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed locking team spawn completion", error))?;
        let admitted_owner: Option<(String, String)> = transaction
            .query_row(
                "SELECT team_id,source_session_id FROM team_spawn_admissions
                 WHERE operation_id=?1 AND completed_response_json IS NULL",
                [operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| db_error("failed verifying keyed spawn owner", error))?;
        let Some((admitted_team, admitted_source)) = admitted_owner else {
            return Err(RuntimeError::Conflict(format!(
                "spawn {operation_id} has no matching unsettled caller-bound admission"
            )));
        };
        if admitted_team != response.team.team.id {
            return Err(RuntimeError::Conflict(format!(
                "spawn {operation_id} has no matching unsettled team authority"
            )));
        }
        // The stage was written *after* native session creation, roster
        // membership and durable onboarding admission. Do not let an
        // unrelated or forged response turn that stage into a terminal replay
        // receipt for the same caller-bound key. Check the original source,
        // native/session identity, joined roster and exact persisted message
        // inside this same IMMEDIATE transaction before committing terminal.
        validate_spawn_completion_lineage(&transaction, operation_id, response, &admitted_source)?;
        let changed = transaction
            .execute(
                "UPDATE team_operation_journal SET stage='completed',payload_json=?2,updated_at=?3
                 WHERE operation_id=?1 AND team_id=?4 AND kind=?5 AND stage='onboarding_sent'",
                params![
                    operation_id,
                    json_to_string(&serde_json::json!({
                        "spawned_session_id": response.spawned_session.id,
                        "worktree_id": response.worktree.as_ref().map(|row| &row.id),
                    }))?,
                    completed_at,
                    response.team.team.id,
                    KIND,
                ],
            )
            .map_err(|error| db_error("failed writing terminal team spawn stage", error))?;
        if changed != 1 {
            return Err(RuntimeError::Conflict(format!(
                "spawn {operation_id} has no certified onboarding stage"
            )));
        }
        let receipt_written = transaction
            .execute(
                "UPDATE team_spawn_admissions SET completed_response_json=?2,updated_at=?3
                 WHERE operation_id=?1 AND completed_response_json IS NULL",
                params![
                    operation_id,
                    json_to_string(&serde_json::to_value(response).map_err(|error| {
                        RuntimeError::InvalidState(format!("invalid team spawn response: {error}"))
                    })?)?,
                    completed_at,
                ],
            )
            .map_err(|error| db_error("failed storing exact spawn result", error))?;
        if receipt_written != 1 {
            return Err(RuntimeError::Conflict(format!(
                "spawn {operation_id} terminal reply was not durably recorded"
            )));
        }
        transaction
            .commit()
            .map_err(|error| db_error("failed committing exact team spawn receipt", error))
    }
}

fn validate_spawn_completion_lineage(
    transaction: &rusqlite::Transaction<'_>,
    operation_id: &str,
    response: &TeamMemberSpawnResponse,
    original_source: &str,
) -> Result<(), RuntimeError> {
    let mismatch = || {
        RuntimeError::Conflict(format!(
            "spawn {operation_id} native onboarding does not match original caller/session/team authority"
        ))
    };
    let team_id = &response.team.team.id;
    let spawned_id = &response.spawned_session.id;
    let worktree_id = response.worktree.as_ref().map(|row| row.id.as_str());
    if response.spawned_member.team_id != *team_id
        || response.spawned_member.added_by != original_source
        || response.spawned_member.worktree_id.as_deref() != worktree_id
        || response
            .team
            .members
            .iter()
            .filter(|member| member.agent_id == *spawned_id)
            .collect::<Vec<_>>()
            != vec![&response.spawned_member]
        || response.onboarding.get("status").and_then(|v| v.as_str()) != Some("sent")
    {
        return Err(mismatch());
    }
    let message_id = response
        .onboarding
        .get("message_id")
        .and_then(|v| v.as_str())
        .ok_or_else(mismatch)?;
    let delivery_ids = response
        .onboarding
        .get("delivery_ids")
        .and_then(|v| v.as_array())
        .filter(|ids| !ids.is_empty() && ids.iter().all(|id| id.as_str().is_some()))
        .ok_or_else(mismatch)?;
    let stage_payload: Option<String> = transaction
        .query_row(
            "SELECT payload_json FROM team_operation_journal
             WHERE operation_id=?1 AND team_id=?2 AND kind=?3 AND stage='onboarding_sent'",
            params![operation_id, team_id, KIND],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed loading original spawn onboarding stage", error))?;
    let stage = stage_payload
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .ok_or_else(mismatch)?;
    if stage
        != serde_json::json!({
            "source_session_id": original_source,
            "spawned_session_id": spawned_id,
            "provider": response.spawned_session.provider,
            "provider_session_ref": response.spawned_session.provider_session_ref,
            "canonical_provider_session_ref": response.spawned_session.canonical_provider_session_ref,
            "worktree_id": worktree_id,
            "message_id": message_id,
            "delivery_ids": delivery_ids,
        })
    {
        return Err(mismatch());
    }
    let stored_member: Option<(Option<String>, i64, String, Option<String>, String, Option<String>)> = transaction
        .query_row(
            "SELECT title,joined_at,added_by,creator_agent_id,creator_compaction_subscription,worktree_id
             FROM team_members WHERE team_id=?1 AND agent_id=?2",
            params![team_id, spawned_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .optional()
        .map_err(|error| db_error("failed checking original spawned team member", error))?;
    let original_session: Option<(String, Option<String>, Option<String>, Option<String>)> =
        transaction
            .query_row(
                "SELECT provider,provider_session_ref,canonical_provider_session_ref,worktree_id
             FROM sessions WHERE id=?1",
                [spawned_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|error| db_error("failed checking original spawned session", error))?;
    if stored_member
        != Some((
            response.spawned_member.title.clone(),
            response.spawned_member.joined_at,
            original_source.to_owned(),
            response.spawned_member.creator_agent_id.clone(),
            response
                .spawned_member
                .creator_compaction_subscription
                .clone(),
            worktree_id.map(str::to_owned),
        ))
        || original_session
            != Some((
                response.spawned_session.provider.clone(),
                response.spawned_session.provider_session_ref.clone(),
                response
                    .spawned_session
                    .canonical_provider_session_ref
                    .clone(),
                worktree_id.map(str::to_owned),
            ))
    {
        return Err(mismatch());
    }
    // Native worktree effects are not SQLite-atomic. A success receipt can
    // only name the original persisted worktree and active spawned claim;
    // it cannot certify a different checkout or an orphaned assignment.
    match (
        response.worktree.as_ref(),
        response.worktree_assignment_mode.as_str(),
    ) {
        (None, "none") if !response.worktree_created_by_operation => {}
        (Some(worktree), "created" | "reused") => {
            if response.worktree_created_by_operation
                != (response.worktree_assignment_mode == "created")
            {
                return Err(mismatch());
            }
            let stored_worktree: Option<Option<String>> = transaction
                .query_row(
                    "SELECT created_by_operation_id FROM managed_worktrees
                     WHERE id=?1 AND repo_root=?2 AND worktree_cwd=?3 AND branch_name=?4",
                    params![
                        worktree.id,
                        worktree.repo_root,
                        worktree.worktree_cwd,
                        worktree.branch_name
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| db_error("failed verifying original spawn worktree", error))?;
            if stored_worktree.is_none()
                || (response.worktree_created_by_operation
                    && stored_worktree.as_ref().and_then(Option::as_deref) != Some(operation_id))
            {
                return Err(mismatch());
            }
            let claim: Option<(String, Option<i64>)> = transaction
                .query_row(
                    "SELECT claim_role,released_at FROM managed_worktree_claims
                     WHERE worktree_id=?1 AND session_id=?2",
                    params![worktree.id, spawned_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| db_error("failed checking original worktree claim", error))?;
            if claim
                != Some((
                    if response.worktree_created_by_operation {
                        "owner"
                    } else {
                        "consumer"
                    }
                    .to_owned(),
                    None,
                ))
            {
                return Err(mismatch());
            }
        }
        _ => return Err(mismatch()),
    }
    let original_message: Option<(String, String, String, Option<String>, Option<String>)> =
        transaction
            .query_row(
                "SELECT sender_agent_id,recipient_agent_ids_json,scope,idempotency_key,correlation_id
                 FROM team_messages WHERE id=?1 AND team_id=?2",
                params![message_id, team_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .optional()
            .map_err(|error| db_error("failed checking original spawn onboarding message", error))?;
    let Some((sender, recipients, scope, key, correlation)) = original_message else {
        return Err(mismatch());
    };
    let original_key = format!("spawn-onboarding:{operation_id}");
    if sender != original_source
        || scope != "direct"
        || recipients != serde_json::json!([spawned_id]).to_string()
        || key.as_deref() != Some(original_key.as_str())
        || correlation.as_deref() != Some(original_key.as_str())
    {
        return Err(mismatch());
    }
    let mut statement = transaction
        // An onboarding message is direct, so at most one delivery can
        // certify completion. Read only two rows to detect any forged or
        // historical fan-out without enumerating its entire recipient set.
        .prepare(
            "SELECT id,team_id,recipient_agent_id
             FROM team_deliveries WHERE message_id=?1 LIMIT 2",
        )
        .map_err(|error| db_error("failed preparing spawn onboarding recipients", error))?;
    let actual_deliveries = statement
        .query_map([message_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|error| db_error("failed querying spawn onboarding recipients", error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| db_error("failed reading spawn onboarding recipients", error))?;
    let mut expected_ids = delivery_ids
        .iter()
        .filter_map(|id| id.as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    expected_ids.sort();
    let mut actual_ids = actual_deliveries
        .iter()
        .map(|row| row.0.clone())
        .collect::<Vec<_>>();
    actual_ids.sort();
    if actual_deliveries
        .iter()
        .any(|(_, team, recipient)| team != team_id || recipient != spawned_id)
        || actual_ids != expected_ids
        || expected_ids.len() != 1
    {
        return Err(mismatch());
    }
    Ok(())
}

fn lookup_spawn_admission(
    connection: &Connection,
    team_id: &str,
    source_session_id: &str,
    key: &str,
    request_hash: &str,
) -> Result<Option<TeamMemberSpawnAdmission>, RuntimeError> {
    let existing: Option<(
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = connection
        .query_row(
            "SELECT a.operation_id,a.request_hash,a.completed_response_json,
                        j.stage,j.team_id,j.kind
                 FROM team_spawn_admissions a
                 LEFT JOIN team_operation_journal j ON j.operation_id=a.operation_id
                 WHERE a.team_id=?1 AND a.source_session_id=?2 AND a.idempotency_key=?3",
            params![team_id, source_session_id, key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| db_error("failed loading exact caller-bound spawn", error))?;
    let Some((operation_id, original_hash, response_json, stage, journal_team, kind)) = existing
    else {
        return Ok(None);
    };
    if original_hash != request_hash {
        return Err(RuntimeError::Conflict(format!(
            "spawn key already belongs to a different immutable request ({operation_id})"
        )));
    }
    if stage.as_deref() == Some("completed")
        && journal_team.as_deref() == Some(team_id)
        && kind.as_deref() == Some(KIND)
    {
        if let Some(json) = response_json {
            let response: TeamMemberSpawnResponse =
                serde_json::from_str(&json).map_err(|error| {
                    RuntimeError::Conflict(format!(
                        "invalid certified spawn {operation_id} receipt: {error}"
                    ))
                })?;
            if response.operation_id != operation_id
                || response.team.team.id != team_id
                || response.journal_stage != "completed"
            {
                return Err(RuntimeError::Conflict(format!(
                    "certified spawn {operation_id} receipt identity mismatch"
                )));
            }
            return Ok(Some(TeamMemberSpawnAdmission::Replay { response }));
        }
    }
    Ok(Some(TeamMemberSpawnAdmission::Unresolved { operation_id }))
}
