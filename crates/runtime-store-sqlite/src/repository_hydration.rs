use runtime_core::{
    ApprovalRecord, CredentialRecord, ManagedWorktreeClaimRecord, ManagedWorktreeRecord,
    ProcessRecord, RuntimeError, RuntimeHydratedState, SessionRecord, TeamDeliveryRecord,
    TeamMemberRecord, TeamMessageRecord, TeamOperationDiagnosticRecord, TeamOperationJournalRecord,
    TeamRecord, TurnRecord,
};

use crate::db::{collect_rows, db_error, open_connection, opt_string_to_json, string_to_json};
use crate::SqliteRuntimeRepository;

impl SqliteRuntimeRepository {
    pub fn hydrate_runtime_state(&self) -> Result<RuntimeHydratedState, RuntimeError> {
        let connection = open_connection(&self.database_path)?;

        self.hydrate_runtime_state_from_connection(&connection)
    }

    pub(crate) fn hydrate_runtime_state_from_connection(
        &self,
        connection: &rusqlite::Connection,
    ) -> Result<RuntimeHydratedState, RuntimeError> {
        let sessions = {
            let mut statement = connection
                .prepare(
                    "SELECT id, provider, status, cwd, model, permission_mode, system_prompt,
                            metadata_json, provider_session_ref, canonical_provider_session_ref,
                            active_turn_id, worktree_id, created_at, updated_at, closed_at,
                            failure_code, failure_message
                     FROM sessions
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing session hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(SessionRecord {
                        id: row.get(0)?,
                        provider: row.get(1)?,
                        status: row.get(2)?,
                        cwd: row.get(3)?,
                        model: row.get(4)?,
                        permission_mode: row.get(5)?,
                        system_prompt: row.get(6)?,
                        metadata: string_to_json(row.get(7)?)?,
                        provider_session_ref: row.get(8)?,
                        canonical_provider_session_ref: row.get(9)?,
                        active_turn_id: row.get(10)?,
                        worktree_id: row.get(11)?,
                        created_at: row.get(12)?,
                        updated_at: row.get(13)?,
                        closed_at: row.get(14)?,
                        failure_code: row.get(15)?,
                        failure_message: row.get(16)?,
                    })
                })
                .map_err(|error| db_error("failed running session hydration query", error))?;
            collect_rows(rows)?
        };

        let turns = {
            let mut statement = connection
                .prepare(
                    "SELECT id, session_id, provider_turn_ref, status, input_json, source,
                            started_at, completed_at, usage_json, error_json
                     FROM turns
                     ORDER BY rowid ASC",
                )
                .map_err(|error| db_error("failed preparing turn hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    let usage_json: Option<String> = row.get(8)?;
                    let error_json: Option<String> = row.get(9)?;
                    Ok(TurnRecord {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        provider_turn_ref: row.get(2)?,
                        status: row.get(3)?,
                        input: string_to_json(row.get(4)?)?,
                        source: row.get(5)?,
                        started_at: row.get(6)?,
                        completed_at: row.get(7)?,
                        usage: opt_string_to_json(usage_json)?,
                        error: opt_string_to_json(error_json)?,
                    })
                })
                .map_err(|error| db_error("failed running turn hydration query", error))?;
            collect_rows(rows)?
        };

        let approvals = {
            let mut statement = connection
                .prepare(
                    "SELECT id, session_id, turn_id, tool_call_id, provider_approval_ref,
                            status, request_json, response_json, created_at, resolved_at
                     FROM approvals
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing approval hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    let response_json: Option<String> = row.get(7)?;
                    Ok(ApprovalRecord {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        turn_id: row.get(2)?,
                        tool_call_id: row.get(3)?,
                        provider_approval_ref: row.get(4)?,
                        status: row.get(5)?,
                        request: string_to_json(row.get(6)?)?,
                        response: opt_string_to_json(response_json)?,
                        created_at: row.get(8)?,
                        resolved_at: row.get(9)?,
                    })
                })
                .map_err(|error| db_error("failed running approval hydration query", error))?;
            collect_rows(rows)?
        };

        let teams = {
            let mut statement = connection
                .prepare(
                    "SELECT id, name, lead_agent_id, created_by, created_at, updated_at, deleted_at
                     FROM teams
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing team hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamRecord {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        lead_agent_id: row.get(2)?,
                        created_by: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                        deleted_at: row.get(6)?,
                    })
                })
                .map_err(|error| db_error("failed running team hydration query", error))?;
            collect_rows(rows)?
        };

        let team_members = {
            let mut statement = connection
                .prepare(
                    "SELECT team_id, agent_id, title, joined_at, added_by, creator_agent_id,
                            creator_compaction_subscription, worktree_id
                     FROM team_members
                     ORDER BY joined_at ASC, team_id ASC, agent_id ASC",
                )
                .map_err(|error| db_error("failed preparing team member hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
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
                })
                .map_err(|error| db_error("failed running team member hydration query", error))?;
            collect_rows(rows)?
        };

        let team_messages = {
            let mut statement = connection
                .prepare(
                    "SELECT id, team_id, scope, sender_agent_id, recipient_agent_ids_json,
                            input_json, image_paths_json, priority, policy, correlation_id,
                            reply_to_message_id, idempotency_key, created_at
                     FROM team_messages
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing team message hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
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
                })
                .map_err(|error| db_error("failed running team message hydration query", error))?;
            collect_rows(rows)?
        };

        let team_deliveries = {
            let mut statement = connection
                .prepare(
                    "SELECT id, message_id, team_id, recipient_agent_id, provider, status,
                            effective_policy, injection_strategy, injected_turn_id,
                            last_error_code, last_error_message, created_at, updated_at
                     FROM team_deliveries
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing team delivery hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
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
                .map_err(|error| db_error("failed running team delivery hydration query", error))?;
            collect_rows(rows)?
        };

        let managed_worktrees = {
            let mut statement = connection
                .prepare(
                    "SELECT id, repo_root, worktree_root, worktree_cwd, branch_name,
                            worktree_name, unified_workspace_path, deletion_policy,
                            created_by_session_id, created_by_operation_id, created_at, updated_at
                     FROM managed_worktrees
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing managed worktree hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(ManagedWorktreeRecord {
                        id: row.get(0)?,
                        repo_root: row.get(1)?,
                        worktree_root: row.get(2)?,
                        worktree_cwd: row.get(3)?,
                        branch_name: row.get(4)?,
                        worktree_name: row.get(5)?,
                        unified_workspace_path: row.get(6)?,
                        deletion_policy: row.get(7)?,
                        created_by_session_id: row.get(8)?,
                        created_by_operation_id: row.get(9)?,
                        created_at: row.get(10)?,
                        updated_at: row.get(11)?,
                    })
                })
                .map_err(|error| {
                    db_error("failed running managed worktree hydration query", error)
                })?;
            collect_rows(rows)?
        };

        let managed_worktree_claims = {
            let mut statement = connection
                .prepare(
                    "SELECT worktree_id, session_id, claim_role, created_at, released_at
                     FROM managed_worktree_claims
                     ORDER BY created_at ASC, worktree_id ASC, session_id ASC",
                )
                .map_err(|error| {
                    db_error(
                        "failed preparing managed worktree claim hydration query",
                        error,
                    )
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(ManagedWorktreeClaimRecord {
                        worktree_id: row.get(0)?,
                        session_id: row.get(1)?,
                        claim_role: row.get(2)?,
                        created_at: row.get(3)?,
                        released_at: row.get(4)?,
                    })
                })
                .map_err(|error| {
                    db_error(
                        "failed running managed worktree claim hydration query",
                        error,
                    )
                })?;
            collect_rows(rows)?
        };

        let team_operation_journal = {
            let mut statement = connection
                .prepare(
                    "SELECT operation_id, team_id, kind, stage, payload_json, created_at, updated_at
                     FROM team_operation_journal
                     ORDER BY updated_at ASC, operation_id ASC",
                )
                .map_err(|error| {
                    db_error("failed preparing team operation journal hydration query", error)
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamOperationJournalRecord {
                        operation_id: row.get(0)?,
                        team_id: row.get(1)?,
                        kind: row.get(2)?,
                        stage: row.get(3)?,
                        payload: string_to_json(row.get(4)?)?,
                        created_at: row.get(5)?,
                        updated_at: row.get(6)?,
                    })
                })
                .map_err(|error| {
                    db_error(
                        "failed running team operation journal hydration query",
                        error,
                    )
                })?;
            collect_rows(rows)?
        };

        let team_operation_diagnostics = {
            let mut statement = connection
                .prepare(
                    "SELECT id, operation_id, team_id, code, message, payload_json, created_at
                     FROM team_operation_diagnostics
                     ORDER BY id ASC",
                )
                .map_err(|error| {
                    db_error(
                        "failed preparing team operation diagnostics hydration query",
                        error,
                    )
                })?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TeamOperationDiagnosticRecord {
                        id: row.get(0)?,
                        operation_id: row.get(1)?,
                        team_id: row.get(2)?,
                        code: row.get(3)?,
                        message: row.get(4)?,
                        payload: string_to_json(row.get(5)?)?,
                        created_at: row.get(6)?,
                    })
                })
                .map_err(|error| {
                    db_error(
                        "failed running team operation diagnostics hydration query",
                        error,
                    )
                })?;
            collect_rows(rows)?
        };

        let processes = {
            let mut statement = connection
                .prepare(
                    "SELECT id, session_id, tool_call_id, pid, command_json, cwd, status,
                            exit_code, signal, stdout_path, stderr_path, started_at, ended_at, timeout_ms
                     FROM processes
                     ORDER BY started_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing process hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(ProcessRecord {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        tool_call_id: row.get(2)?,
                        pid: row.get(3)?,
                        command: string_to_json(row.get(4)?)?,
                        cwd: row.get(5)?,
                        status: row.get(6)?,
                        exit_code: row.get(7)?,
                        signal: row.get(8)?,
                        stdout_path: row.get(9)?,
                        stderr_path: row.get(10)?,
                        started_at: row.get(11)?,
                        ended_at: row.get(12)?,
                        timeout_ms: row.get(13)?,
                    })
                })
                .map_err(|error| db_error("failed running process hydration query", error))?;
            collect_rows(rows)?
        };

        let credentials = {
            let mut statement = connection
                .prepare(
                    "SELECT id, provider, profile, kind, encrypted_secret, metadata_json,
                            created_at, updated_at
                     FROM credentials
                     ORDER BY created_at ASC, id ASC",
                )
                .map_err(|error| db_error("failed preparing credential hydration query", error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(CredentialRecord {
                        id: row.get(0)?,
                        provider: row.get(1)?,
                        profile: row.get(2)?,
                        kind: row.get(3)?,
                        encrypted_secret: row.get(4)?,
                        metadata: string_to_json(row.get(5)?)?,
                        created_at: row.get(6)?,
                        updated_at: row.get(7)?,
                    })
                })
                .map_err(|error| db_error("failed running credential hydration query", error))?;
            collect_rows(rows)?
        };

        Ok(RuntimeHydratedState {
            sessions,
            turns,
            approvals,
            teams,
            team_members,
            team_messages,
            team_deliveries,
            managed_worktrees,
            managed_worktree_claims,
            team_operation_journal,
            team_operation_diagnostics,
            processes,
            credentials,
        })
    }
}
