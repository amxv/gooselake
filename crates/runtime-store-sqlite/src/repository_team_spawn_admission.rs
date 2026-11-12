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
        let admitted_team: Option<String> = transaction
            .query_row(
                "SELECT team_id FROM team_spawn_admissions
                 WHERE operation_id=?1 AND completed_response_json IS NULL",
                [operation_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed verifying keyed spawn owner", error))?;
        if admitted_team.as_deref() != Some(response.team.team.id.as_str()) {
            return Err(RuntimeError::Conflict(format!(
                "spawn {operation_id} has no matching unsettled caller-bound admission"
            )));
        }
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
        transaction
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
        transaction
            .commit()
            .map_err(|error| db_error("failed committing exact team spawn receipt", error))
    }
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
