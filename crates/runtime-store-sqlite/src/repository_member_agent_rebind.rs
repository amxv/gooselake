use runtime_core::{
    ManagedWorktreeRecord, MemberAgentRebindAdmission, MemberAgentRebindCommand, NewRuntimeEvent,
    OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord, RepositoryIdentity,
    RuntimeError, RuntimeEventCriticality, RuntimeEventScope, WorkspaceAgentLifecycleState,
    WorkspaceAgentRebindOperation, WorkspaceAgentRebindResponse, WorkspaceMembershipPolicy,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_operator_agent_restore::restored_session_snapshot;
use crate::repository_workspace::operation_by_id;
use crate::repository_workspace_agent::{workspace_agent_from_row, AGENT_SELECT};
use crate::SqliteRuntimeRepository;

pub(crate) const MEMBER_REBIND_EFFECT: &str = "model_member_rebind";

fn authorize_member(
    tx: &Transaction<'_>,
    workspace: &str,
    caller: &str,
    allow_non_lead: bool,
) -> Result<(), RuntimeError> {
    let lead: Option<Option<String>> = tx.query_row(
        "SELECT w.lead_agent_id FROM workspaces w
         JOIN workspace_agents a ON a.workspace_id=w.workspace_id JOIN sessions s ON s.id=a.session_id
         WHERE w.workspace_id=?1 AND a.session_id=?2 AND w.lifecycle_state='active'
           AND a.lifecycle_state='active' AND s.status NOT IN ('closed','failed')
           AND NOT EXISTS(SELECT 1 FROM workspace_retirement_fences f WHERE f.workspace_id=w.workspace_id)",
        params![workspace, caller], |row| row.get(0),
    ).optional().map_err(|error| db_error("failed checking member rebind caller", error))?;
    let lead = lead.ok_or_else(|| {
        RuntimeError::Conflict(
            "member rebind requires an active caller in the same active workspace".into(),
        )
    })?;
    if lead.as_deref().is_some_and(|id| id != caller) && !allow_non_lead {
        return Err(RuntimeError::Conflict(
            "caller cannot assign workspace members".into(),
        ));
    }
    Ok(())
}

fn worktree_snapshot(
    connection: &Connection,
    id: &str,
) -> Result<(ManagedWorktreeRecord, u64), RuntimeError> {
    let (record, revision): (ManagedWorktreeRecord, i64) = connection.query_row(
        "SELECT id,repo_root,worktree_root,worktree_cwd,branch_name,worktree_name,
                unified_workspace_path,deletion_policy,created_by_session_id,created_by_operation_id,
                created_at,updated_at,revision FROM managed_worktrees WHERE id=?1", [id], |row| {
            Ok((ManagedWorktreeRecord {
                id:row.get(0)?, repo_root:row.get(1)?, worktree_root:row.get(2)?, worktree_cwd:row.get(3)?,
                branch_name:row.get(4)?, worktree_name:row.get(5)?, unified_workspace_path:row.get(6)?,
                deletion_policy:row.get(7)?, created_by_session_id:row.get(8)?, created_by_operation_id:row.get(9)?,
                created_at:row.get(10)?, updated_at:row.get(11)?,
            }, row.get(12)?))
        },
    ).optional().map_err(|error| db_error("failed snapshotting member destination", error))?
        .ok_or_else(|| RuntimeError::NotFound("member destination worktree".into()))?;
    Ok((
        record,
        u64::try_from(revision)
            .map_err(|_| RuntimeError::Bootstrap("negative worktree revision".into()))?,
    ))
}

fn event(
    tx: &Transaction<'_>,
    command: &MemberAgentRebindCommand,
    status: &str,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    insert_runtime_event_in_transaction(
        tx,
        &NewRuntimeEvent {
            event_id: format!(
                "evt_model_rebind_{}_{}",
                command.operation.operation_id, status
            ),
            scope: RuntimeEventScope::Workspace,
            scope_id: command.operation.workspace_id.clone(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "model_tool.rebind_target_updated".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({"operation_id":command.parent_operation_id,"child_operation_id":command.operation.operation_id,
            "agent_id":command.operation.agent_id,"destination_worktree_id":command.destination.id,"status":status}),
            provider: None,
            provider_seq: None,
            created_at: at_ms,
        },
    )?;
    Ok(())
}

fn link(
    tx: &Transaction<'_>,
    command: &MemberAgentRebindCommand,
) -> Result<(OperationRecord, String, Value), RuntimeError> {
    let parent = operation_by_id(tx, &command.parent_operation_id)?
        .ok_or_else(|| RuntimeError::NotFound("model rebind parent".into()))?;
    if command.actor.kind != OperationActorKind::Agent
        || parent.actor != command.actor
        || parent.kind != "agent_model_tool_mutation"
        || parent.workspace_id.as_deref() != Some(command.operation.workspace_id.as_str())
        || parent.normalized_request["tool_name"] != "gg_team"
        || parent.normalized_request["args"]["mode"] != "assign_worktree"
        || runtime_core::normalized_json_hash(&parent.normalized_request)?
            != parent.normalized_request_hash
        || !matches!(
            parent.phase,
            OperationPhase::Requested | OperationPhase::Completed
        )
        || runtime_core::normalized_json_hash(&command.normalized_request)?
            != command.normalized_request_hash
    {
        return Err(RuntimeError::Conflict(
            "member rebind parent authority changed".into(),
        ));
    }
    let (phase, raw): (String,String) = tx.query_row(
        "SELECT phase,evidence_json FROM runtime_operation_effects WHERE effect_id=?1 AND operation_id=?2
         AND effect_kind=?3 AND target_id=?4",
        params![command.parent_effect_id,command.parent_operation_id,MEMBER_REBIND_EFFECT,command.operation.agent_id],
        |row| Ok((row.get(0)?,row.get(1)?)),
    ).optional().map_err(|error| db_error("failed loading member rebind plan", error))?
        .ok_or_else(|| RuntimeError::Conflict("original member rebind plan missing".into()))?;
    let evidence: Value =
        serde_json::from_str(&raw).map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
    let frozen: MemberAgentRebindCommand = serde_json::from_value(evidence["snapshot"].clone())
        .map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid member rebind snapshot: {error}"))
        })?;
    if frozen != *command {
        return Err(RuntimeError::Conflict(
            "member rebind snapshot was substituted".into(),
        ));
    }
    Ok((parent, phase, evidence))
}

pub(crate) fn command_for_record(
    tx: &Transaction<'_>,
    record: &OperationRecord,
) -> Result<Option<MemberAgentRebindCommand>, RuntimeError> {
    if record.actor.kind != OperationActorKind::Agent {
        return Ok(None);
    }
    let reference = &record.normalized_request["model_tool_parent"];
    let effect_id = reference["effect_id"]
        .as_str()
        .ok_or_else(|| RuntimeError::Conflict("member rebind effect missing".into()))?;
    let raw: String = tx
        .query_row(
            "SELECT evidence_json FROM runtime_operation_effects WHERE effect_id=?1",
            [effect_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed loading original member rebind command", error))?
        .ok_or_else(|| RuntimeError::Conflict("member rebind command missing".into()))?;
    let evidence: Value =
        serde_json::from_str(&raw).map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
    let command: MemberAgentRebindCommand = serde_json::from_value(evidence["snapshot"].clone())
        .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
    link(tx, &command)?;
    if record.kind != "workspace_agent_rebind"
        || record.operation_id != command.operation.operation_id
        || record.actor != command.actor
        || record.workspace_id.as_deref() != Some(command.operation.workspace_id.as_str())
        || record.idempotency_key != command.operation.idempotency_key
        || record.normalized_request != command.normalized_request
        || record.normalized_request_hash != command.normalized_request_hash
    {
        return Err(RuntimeError::Conflict(
            "member rebind child identity differs from parent plan".into(),
        ));
    }
    Ok(Some(command))
}

pub(crate) fn validate_frozen_target(
    tx: &Transaction<'_>,
    command: &MemberAgentRebindCommand,
) -> Result<(), RuntimeError> {
    let session = restored_session_snapshot(tx, &command.operation.agent_id)?;
    let agent = tx
        .query_row(
            &format!("{AGENT_SELECT} WHERE a.session_id=?1"),
            [&command.operation.agent_id],
            workspace_agent_from_row,
        )
        .map_err(|error| db_error("failed reading frozen rebind agent", error))?;
    let destination = worktree_snapshot(tx, &command.destination.id)?;
    let root: String = tx
        .query_row(
            "SELECT canonical_root FROM workspaces WHERE workspace_id=?1",
            [&command.operation.workspace_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed reading frozen rebind repository", error))?;
    if session != command.original_session
        || agent != command.original_agent
        || destination != (command.destination.clone(), command.destination_revision)
        || root != command.repository.canonical_root
    {
        return Err(RuntimeError::Conflict(
            "member rebind native target or destination changed since planning".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_member_admission(
    tx: &Transaction<'_>,
    command: &MemberAgentRebindCommand,
    new: bool,
) -> Result<(), RuntimeError> {
    let (parent, phase, evidence) = link(tx, command)?;
    if new {
        if parent.phase != OperationPhase::Requested
            || phase != "intended"
            || evidence["outcome"]["status"] != "not_admitted"
        {
            return Err(RuntimeError::operation_unresolved(
                &parent.operation_id,
                "member rebind is no longer available for first admission",
            ));
        }
        authorize_member(
            tx,
            &command.operation.workspace_id,
            &command.actor.identifier,
            command.non_lead_can_add_members,
        )?;
        validate_frozen_target(tx, command)?;
    }
    Ok(())
}

pub(crate) fn validate_member_receipt(
    tx: &Transaction<'_>,
    command: &MemberAgentRebindCommand,
    session: &runtime_core::SessionRecord,
    policy: &runtime_core::WorkspaceAgentRecreationPolicy,
    evidence: &runtime_core::ProviderWorkspaceRebindEvidence,
    changed_at: i64,
) -> Result<(), RuntimeError> {
    validate_frozen_target(tx, command)?;
    let mut expected_session = command.original_session.clone();
    expected_session.cwd = Some(command.destination.worktree_cwd.clone());
    expected_session.worktree_id = Some(command.destination.id.clone());
    expected_session.provider_session_ref = evidence.provider_session_ref.clone();
    expected_session.canonical_provider_session_ref =
        evidence.canonical_provider_session_ref.clone();
    expected_session.updated_at = changed_at;
    let mut expected_policy = command.original_agent.recreation_policy.clone();
    expected_policy.authoritative_cwd = command.destination.worktree_cwd.clone();
    let native_matches = match command.original_session.provider.as_str() {
        "codex" => {
            evidence.provider_session_ref == command.original_session.provider_session_ref
                && evidence.canonical_provider_session_ref
                    == command.original_session.canonical_provider_session_ref
        }
        "claude" => {
            evidence.binding_generation.is_some()
                && evidence
                    .canonical_provider_session_ref
                    .as_deref()
                    .is_some_and(|id| !id.trim().is_empty())
                && command
                    .original_session
                    .canonical_provider_session_ref
                    .as_deref()
                    .is_none_or(|id| evidence.canonical_provider_session_ref.as_deref() == Some(id))
        }
        _ => false,
    };
    if *session != expected_session
        || *policy != expected_policy
        || !native_matches
        || evidence
            .provider_session_ref
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
        || evidence.runtime_session_id != command.operation.agent_id
        || evidence.cwd != command.destination.worktree_cwd
    {
        return Err(RuntimeError::Conflict(
            "member rebind receipt differs from frozen native target and policy".into(),
        ));
    }
    Ok(())
}

pub(crate) fn update_parent(
    tx: &Transaction<'_>,
    record: &OperationRecord,
    phase: &str,
    status: &str,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    let Some(command) = command_for_record(tx, record)? else {
        return Ok(());
    };
    let (parent, old_phase, mut evidence) = link(tx, &command)?;
    if parent.phase != OperationPhase::Requested
        || !matches!(
            (old_phase.as_str(), phase),
            ("intended", "started")
                | ("started", "uncertain" | "finalized")
                | ("uncertain", "finalized")
        )
    {
        return Err(RuntimeError::Conflict(
            "member rebind parent outcome transition changed".into(),
        ));
    }
    evidence["outcome"] = json!({"status":status,"operation_id":record.operation_id});
    let count = tx.execute("UPDATE runtime_operation_effects SET phase=?2,evidence_json=?3,attempt_count=1,updated_at=?4
        WHERE effect_id=?1 AND phase=?5", params![command.parent_effect_id,phase,json_to_string(&evidence)?,at_ms,old_phase])
        .map_err(|error| db_error("failed linking member rebind outcome",error))?;
    if count != 1 {
        return Err(RuntimeError::Conflict("member rebind outcome raced".into()));
    }
    event(tx, &command, status, at_ms)
}

pub(crate) fn verify_model_assign_result(
    tx: &Transaction<'_>,
    parent: &OperationRecord,
    result: &Value,
) -> Result<(), RuntimeError> {
    if parent.normalized_request["args"]["mode"] != "assign_worktree" {
        return Ok(());
    }
    let count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_effects WHERE operation_id=?1",
            [&parent.operation_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed counting model rebind targets", error))?;
    if count != 1 {
        return Err(RuntimeError::Conflict(
            "model assignment needs its exact frozen target".into(),
        ));
    }
    let raw: String = tx.query_row("SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1 AND effect_kind=?2 AND phase='finalized'",
        params![parent.operation_id,MEMBER_REBIND_EFFECT], |row| row.get(0)).optional()
        .map_err(|error| db_error("failed reading completed model assignment",error))?
        .ok_or_else(|| RuntimeError::Conflict("model assignment has no confirmed target".into()))?;
    let evidence: Value =
        serde_json::from_str(&raw).map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
    let command: MemberAgentRebindCommand = serde_json::from_value(evidence["snapshot"].clone())
        .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
    let child = operation_by_id(tx, &command.operation.operation_id)?
        .ok_or_else(|| RuntimeError::Conflict("model rebind child missing".into()))?;
    command_for_record(tx, &child)?;
    if evidence["outcome"]["status"] != "native_rebind_confirmed"
        || child.phase != OperationPhase::Completed
        || child.actor != parent.actor
    {
        return Err(RuntimeError::Conflict(
            "model rebind native outcome is not proven".into(),
        ));
    }
    let response: WorkspaceAgentRebindResponse = serde_json::from_value(
        child
            .exact_terminal_result
            .ok_or_else(|| RuntimeError::Conflict("member rebind receipt missing".into()))?,
    )
    .map_err(|error| RuntimeError::Bootstrap(error.to_string()))?;
    let native = response
        .operation
        .provider_evidence
        .as_ref()
        .ok_or_else(|| RuntimeError::Conflict("member rebind native evidence missing".into()))?;
    let mut expected_agent = command.original_agent.clone();
    expected_agent.recreation_policy.authoritative_cwd = command.destination.worktree_cwd.clone();
    expected_agent.provider_session_ref = native.provider_session_ref.clone();
    expected_agent.canonical_provider_session_ref = native.canonical_provider_session_ref.clone();
    expected_agent.revision = expected_agent
        .revision
        .checked_add(1)
        .ok_or_else(|| RuntimeError::Conflict("rebind revision exhausted".into()))?;
    expected_agent.updated_at = response.operation.updated_at;
    let mut expected_operation = command.operation.clone();
    expected_operation.phase = "completed".into();
    expected_operation.updated_at = response.operation.updated_at;
    expected_operation.previous_cleanup_status = Some("preserved".into());
    expected_operation.provider_evidence = Some(native.clone());
    let expected = json!({"ok":true,"result":{"operation":"assign_worktree","agent_id":command.operation.agent_id,"rebind":response}});
    if response.agent.as_ref() != Some(&expected_agent)
        || response.operation != expected_operation
        || *result != expected
    {
        return Err(RuntimeError::Conflict(
            "model assignment aggregate differs from original child receipt".into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    pub fn unresolved_workspace_agent_rebind_operation(
        &self,
        agent_id: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        connection
            .query_row(
                "SELECT operation_id FROM workspace_agent_rebinds WHERE agent_id=?1
            AND phase IN ('intended','manual_review') ORDER BY created_at,operation_id LIMIT 1",
                [agent_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading original unresolved rebind", error))
    }

    pub fn quarantine_started_workspace_agent_rebinds(
        &self,
        at_ms: i64,
    ) -> Result<Vec<String>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting rebind startup quarantine", error))?;
        let ids = {
            let mut statement = tx.prepare("SELECT operation_id FROM workspace_agent_rebinds WHERE phase='intended' ORDER BY created_at,operation_id")
                .map_err(|error|db_error("failed listing in-flight rebinds",error))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|error| db_error("failed querying in-flight rebinds", error))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| db_error("failed reading in-flight rebinds", error))?
        };
        for id in &ids {
            Self::classify_workspace_agent_rebind_in_tx(
                &tx,
                id,
                "manual_review",
                "startup_rebind_inflight",
                at_ms,
            )?;
        }
        tx.commit()
            .map_err(|error| db_error("failed committing rebind startup quarantine", error))?;
        Ok(ids)
    }

    pub fn plan_member_agent_rebind(
        &self,
        parent_id: &str,
        caller: &str,
        policy: WorkspaceMembershipPolicy,
        destination_id: &str,
        repository: &RepositoryIdentity,
        at_ms: i64,
    ) -> Result<MemberAgentRebindCommand, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting member rebind plan", error))?;
        let parent = operation_by_id(&tx, parent_id)?
            .ok_or_else(|| RuntimeError::NotFound("model assignment parent".into()))?;
        let workspace = parent
            .workspace_id
            .as_deref()
            .ok_or_else(|| RuntimeError::Conflict("model assignment workspace missing".into()))?;
        let args = &parent.normalized_request["args"]["assign_worktree"];
        if parent.kind != "agent_model_tool_mutation"
            || parent.phase != OperationPhase::Requested
            || parent.actor.kind != OperationActorKind::Agent
            || parent.actor.identifier != caller
            || parent.normalized_request["tool_name"] != "gg_team"
            || parent.normalized_request["args"]["mode"] != "assign_worktree"
            || args["preserve_previous_worktree"] == false
            || runtime_core::normalized_json_hash(&parent.normalized_request)?
                != parent.normalized_request_hash
        {
            return Err(RuntimeError::Conflict(
                "invalid model assignment parent".into(),
            ));
        }
        authorize_member(&tx, workspace, caller, policy.non_lead_can_add_members)?;
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_operation_effects WHERE operation_id=?1)",
                [parent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking member rebind plan", error))?;
        if exists {
            return Err(RuntimeError::operation_unresolved(
                parent_id,
                "original assignment plan already exists; no replanning or native retry",
            ));
        }
        let id = args["agent_id"]
            .as_str()
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| RuntimeError::Conflict("assignment target missing".into()))?;
        let agent = tx
            .query_row(
                &format!("{AGENT_SELECT} WHERE a.session_id=?1 AND a.workspace_id=?2"),
                params![id, workspace],
                workspace_agent_from_row,
            )
            .optional()
            .map_err(|error| db_error("failed freezing model rebind agent", error))?
            .ok_or_else(|| RuntimeError::NotFound("assignment target".into()))?;
        let session = restored_session_snapshot(&tx, id)?;
        let (destination, destination_revision) = worktree_snapshot(&tx, destination_id)?;
        let root: String = tx
            .query_row(
                "SELECT canonical_root FROM workspaces WHERE workspace_id=?1",
                [workspace],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking member rebind root", error))?;
        if agent.lifecycle_state != WorkspaceAgentLifecycleState::Active
            || session.status != "ready"
            || session.active_turn_id.is_some()
            || agent.revision >= i64::MAX as u64
            || session
                .provider_session_ref
                .as_deref()
                .is_none_or(str::is_empty)
            || session.cwd.as_deref() != Some(agent.recreation_policy.authoritative_cwd.as_str())
            || session.provider != agent.recreation_policy.provider.as_str()
            || session.model != agent.recreation_policy.model
            || repository.canonical_root != root
            || destination.repo_root != root
            || args["worktree_name"].as_str() != Some(destination.worktree_name.as_str())
            || session.worktree_id.as_deref() == Some(destination.id.as_str())
        {
            return Err(RuntimeError::Conflict(
                "assignment target or destination is not eligible for frozen native admission"
                    .into(),
            ));
        }
        let suffix = runtime_core::normalized_json_hash(&json!([parent_id, id, destination.id]))?;
        let operation = WorkspaceAgentRebindOperation {
            operation_id: format!("rebind_member_{suffix}"),
            workspace_id: workspace.into(),
            agent_id: id.into(),
            idempotency_key: Some(format!("member-rebind:{suffix}")),
            expected_revision: agent.revision,
            previous_worktree_id: session.worktree_id.clone(),
            destination_worktree_id: Some(destination.id.clone()),
            previous_cwd: agent.recreation_policy.authoritative_cwd.clone(),
            destination_cwd: destination.worktree_cwd.clone(),
            cleanup_previous_worktree: false,
            previous_cleanup_status: None,
            previous_cleanup_diagnostic: None,
            phase: "intended".into(),
            provider_evidence: None,
            error_code: None,
            created_at: at_ms,
            updated_at: at_ms,
        };
        let effect_id = format!("fx_member_target_{}", operation.operation_id);
        let request = json!({"model_tool_parent":{"operation_id":parent_id,"effect_id":effect_id},
            "rebind":operation,"original_session":session,"original_agent":agent,"destination":destination,
            "destination_revision":destination_revision,"repository":repository,"non_lead_can_add_members":policy.non_lead_can_add_members});
        let command = MemberAgentRebindCommand {
            parent_operation_id: parent_id.into(),
            parent_effect_id: effect_id.clone(),
            actor: parent.actor,
            operation,
            original_session: session,
            original_agent: agent,
            destination,
            destination_revision,
            repository: repository.clone(),
            non_lead_can_add_members: policy.non_lead_can_add_members,
            normalized_request_hash: runtime_core::normalized_json_hash(&request)?,
            normalized_request: request,
        };
        OperationAuthorityTransaction::new(&tx).insert_effect(&OperationEffectRecord {
            effect_id,
            operation_id: parent_id.into(),
            effect_kind: MEMBER_REBIND_EFFECT.into(),
            target_kind: "workspace_agent".into(),
            target_id: id.into(),
            phase: "intended".into(),
            idempotency_key: "assign:0".into(),
            evidence: Some(json!({"snapshot":command,"outcome":{"status":"not_admitted"}})),
            attempt_count: 0,
            created_at: at_ms,
            updated_at: at_ms,
        })?;
        event(&tx, &command, "planned", at_ms)?;
        tx.commit()
            .map_err(|error| db_error("failed committing member rebind plan", error))?;
        Ok(command)
    }

    pub fn replay_member_agent_rebind(
        &self,
        command: &MemberAgentRebindCommand,
    ) -> Result<Option<MemberAgentRebindAdmission>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction()
            .map_err(|error| db_error("failed reading member rebind receipt", error))?;
        link(&tx, command)?;
        let Some(record) = operation_by_id(&tx, &command.operation.operation_id)? else {
            return Ok(None);
        };
        let original = command_for_record(&tx, &record)?
            .ok_or_else(|| RuntimeError::Conflict("member child has foreign actor".into()))?;
        if original != *command {
            return Err(RuntimeError::Conflict(
                "member child belongs to different plan".into(),
            ));
        }
        Ok(Some(if record.phase == OperationPhase::Completed {
            let response =
                serde_json::from_value(record.exact_terminal_result.ok_or_else(|| {
                    RuntimeError::Conflict("member rebind terminal receipt missing".into())
                })?)
                .map_err(|error| {
                    RuntimeError::Bootstrap(format!("invalid member rebind receipt: {error}"))
                })?;
            MemberAgentRebindAdmission::Replay { response }
        } else {
            MemberAgentRebindAdmission::Unresolved {
                operation_id: record.operation_id,
            }
        }))
    }

    pub fn reject_member_agent_rebind(
        &self,
        command: &MemberAgentRebindCommand,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed rejecting member rebind", error))?;
        let (parent, phase, mut evidence) = link(&tx, command)?;
        if operation_by_id(&tx, &command.operation.operation_id)?.is_some() {
            return Ok(());
        }
        if parent.phase != OperationPhase::Requested
            || phase != "intended"
            || evidence["outcome"]["status"] != "not_admitted"
        {
            return Err(RuntimeError::Conflict(
                "member rebind plan already classified".into(),
            ));
        }
        evidence["outcome"] = json!({"status":"rejected_before_dispatch","reason":reason});
        tx.execute("UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?2,updated_at=?3 WHERE effect_id=?1 AND phase='intended'",
            params![command.parent_effect_id,json_to_string(&evidence)?,at_ms]).map_err(|error|db_error("failed preserving member rebind refusal",error))?;
        event(&tx, command, "rejected_before_dispatch", at_ms)?;
        tx.commit()
            .map_err(|error| db_error("failed committing member rebind refusal", error))
    }
}
