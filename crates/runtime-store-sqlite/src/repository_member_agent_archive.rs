use runtime_core::{
    NewRuntimeEvent, OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase,
    OperationRecord, OperatorAgentArchiveCommand, RuntimeError, RuntimeEventCriticality,
    RuntimeEventScope, WorkspaceMembershipPolicy,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_operator_agent_restore::restore_state;
use crate::repository_workspace::operation_by_id;
use crate::SqliteRuntimeRepository;

pub(crate) const MEMBER_ARCHIVE_EFFECT: &str = "model_member_archive";

fn authorize_member(
    tx: &Transaction<'_>,
    workspace_id: &str,
    caller: &str,
    policy: WorkspaceMembershipPolicy,
) -> Result<(), RuntimeError> {
    let lead: Option<Option<String>> = tx
        .query_row(
            "SELECT w.lead_agent_id FROM workspaces w
         JOIN workspace_agents a ON a.workspace_id=w.workspace_id
         JOIN sessions s ON s.id=a.session_id
         WHERE w.workspace_id=?1 AND w.lifecycle_state='active'
           AND a.session_id=?2 AND a.lifecycle_state='active' AND s.status NOT IN ('closed','failed')",
            params![workspace_id, caller],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("failed checking member archive authority", error))?;
    let lead = lead.ok_or_else(|| {
        RuntimeError::InvalidState(
            "member archive requires an active caller in the same active workspace".into(),
        )
    })?;
    if lead.as_deref().is_some_and(|id| id != caller) && !policy.non_lead_can_remove_members {
        return Err(RuntimeError::InvalidState(
            "caller is not allowed to remove workspace members".into(),
        ));
    }
    Ok(())
}

fn event(
    tx: &Transaction<'_>,
    parent_id: &str,
    workspace_id: &str,
    suffix: &str,
    payload: Value,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    insert_runtime_event_in_transaction(
        tx,
        &NewRuntimeEvent {
            event_id: format!("evt_model_archive_{parent_id}_{suffix}"),
            scope: RuntimeEventScope::Workspace,
            scope_id: workspace_id.into(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "model_tool.archive_target_updated".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload,
            provider: None,
            provider_seq: None,
            created_at: at_ms,
        },
    )?;
    Ok(())
}

struct MemberLink {
    parent_id: String,
    effect_id: String,
    phase: String,
    parent_phase: OperationPhase,
    evidence: Value,
    snapshot: OperatorAgentArchiveCommand,
    policy: WorkspaceMembershipPolicy,
}

fn link(
    tx: &Transaction<'_>,
    operation_id: &str,
    actor: &OperationActor,
    workspace_id: &str,
    request: &Value,
    hash: &str,
) -> Result<MemberLink, RuntimeError> {
    let reference = &request["model_tool_parent"];
    let parent_id = reference["operation_id"]
        .as_str()
        .ok_or_else(|| RuntimeError::Conflict("member archive parent is missing".into()))?;
    let effect_id = reference["effect_id"]
        .as_str()
        .ok_or_else(|| RuntimeError::Conflict("member archive parent effect is missing".into()))?;
    let parent = operation_by_id(tx, parent_id)?
        .ok_or_else(|| RuntimeError::NotFound("member archive parent operation".into()))?;
    if actor.kind != OperationActorKind::Agent
        || parent.kind != "agent_model_tool_mutation"
        || parent.actor != *actor
        || parent.workspace_id.as_deref() != Some(workspace_id)
        || parent.normalized_request["tool_name"] != "gg_team"
        || parent.normalized_request["args"]["mode"] != "remove"
        || runtime_core::normalized_json_hash(&parent.normalized_request)?
            != parent.normalized_request_hash
        || !matches!(
            parent.phase,
            OperationPhase::Requested | OperationPhase::Completed
        )
    {
        return Err(RuntimeError::Conflict(
            "member archive parent authority changed".into(),
        ));
    }
    let (phase, raw): (String, String) = tx
        .query_row(
            "SELECT phase,evidence_json FROM runtime_operation_effects
         WHERE effect_id=?1 AND operation_id=?2 AND effect_kind=?3 AND target_id=?4",
            params![
                effect_id,
                parent_id,
                MEMBER_ARCHIVE_EFFECT,
                request["agent_id"].as_str()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| db_error("failed checking original model archive target", error))?
        .ok_or_else(|| RuntimeError::Conflict("original model archive target is missing".into()))?;
    let evidence: Value = serde_json::from_str(&raw).map_err(|error| {
        RuntimeError::Bootstrap(format!("corrupt model archive snapshot: {error}"))
    })?;
    let snapshot: OperatorAgentArchiveCommand =
        serde_json::from_value(evidence["snapshot"].clone()).map_err(|error| {
            RuntimeError::Bootstrap(format!("corrupt native archive plan: {error}"))
        })?;
    if snapshot.operation_id != operation_id
        || snapshot.actor != *actor
        || snapshot.workspace_id != workspace_id
        || snapshot.normalized_request != *request
        || snapshot.normalized_request_hash != hash
        || runtime_core::normalized_json_hash(request)? != hash
    {
        return Err(RuntimeError::Conflict(
            "member archive differs from its frozen target".into(),
        ));
    }
    let flags = &reference["membership_policy"];
    let policy = WorkspaceMembershipPolicy {
        non_lead_can_add_members: flags["non_lead_can_add_members"].as_bool().ok_or_else(|| {
            RuntimeError::Conflict("member archive policy snapshot missing".into())
        })?,
        non_lead_can_remove_members: flags["non_lead_can_remove_members"].as_bool().ok_or_else(
            || RuntimeError::Conflict("member archive policy snapshot missing".into()),
        )?,
    };
    Ok(MemberLink {
        parent_id: parent_id.into(),
        effect_id: effect_id.into(),
        phase,
        parent_phase: parent.phase,
        evidence,
        snapshot,
        policy,
    })
}

pub(crate) fn validate_member_archive_admission(
    tx: &Transaction<'_>,
    command: &OperatorAgentArchiveCommand,
    new_admission: bool,
) -> Result<(), RuntimeError> {
    let linked = link(
        tx,
        &command.operation_id,
        &command.actor,
        &command.workspace_id,
        &command.normalized_request,
        &command.normalized_request_hash,
    )?;
    if linked.snapshot != *command {
        return Err(RuntimeError::Conflict(
            "member archive native snapshot was substituted".into(),
        ));
    }
    if new_admission {
        if linked.parent_phase != OperationPhase::Requested
            || linked.phase != "intended"
            || linked.evidence["outcome"]["status"] != "not_admitted"
        {
            return Err(RuntimeError::operation_unresolved(
                &linked.parent_id,
                "member archive plan is no longer available for first admission",
            ));
        }
        authorize_member(
            tx,
            &command.workspace_id,
            &command.actor.identifier,
            linked.policy,
        )?;
    }
    Ok(())
}

pub(crate) fn validate_member_archive_record(
    tx: &Transaction<'_>,
    record: &OperationRecord,
) -> Result<(), RuntimeError> {
    link(
        tx,
        &record.operation_id,
        &record.actor,
        record.workspace_id.as_deref().unwrap_or_default(),
        &record.normalized_request,
        &record.normalized_request_hash,
    )?;
    Ok(())
}

pub(crate) fn update_member_archive_parent(
    tx: &Transaction<'_>,
    record: &OperationRecord,
    phase: &str,
    status: &str,
    at_ms: i64,
) -> Result<(), RuntimeError> {
    if record.actor.kind != OperationActorKind::Agent {
        return Ok(());
    }
    let mut linked = link(
        tx,
        &record.operation_id,
        &record.actor,
        record.workspace_id.as_deref().unwrap_or_default(),
        &record.normalized_request,
        &record.normalized_request_hash,
    )?;
    let valid = matches!(
        (linked.phase.as_str(), phase),
        ("intended", "started")
            | ("started", "finalized" | "uncertain")
            | ("uncertain", "finalized")
    );
    if !valid || linked.parent_phase != OperationPhase::Requested {
        return Err(RuntimeError::Conflict(
            "model archive target outcome transition changed".into(),
        ));
    }
    linked.evidence["outcome"] = json!({"status":status,"operation_id":record.operation_id});
    let updated = tx.execute(
        "UPDATE runtime_operation_effects SET phase=?2,evidence_json=?3,attempt_count=1,updated_at=?4
         WHERE effect_id=?1 AND phase=?5",
        params![linked.effect_id,phase,json_to_string(&linked.evidence)?,at_ms,linked.phase],
    ).map_err(|error| db_error("failed linking model archive target outcome", error))?;
    if updated != 1 {
        return Err(RuntimeError::Conflict("model archive outcome raced".into()));
    }
    event(
        tx,
        &linked.parent_id,
        &linked.snapshot.workspace_id,
        &format!("{}_{}", record.operation_id, status),
        json!({"operation_id":linked.parent_id,"child_operation_id":record.operation_id,
            "agent_id":linked.snapshot.agent_id,"status":status}),
        at_ms,
    )
}

/// An aggregate success is justified only by every original target's exact
/// closed-child receipt, not by a caller supplying an envelope with ok=true.
pub(crate) fn verify_model_remove_result(
    tx: &Transaction<'_>,
    parent: &OperationRecord,
    exact_result: &Value,
) -> Result<(), RuntimeError> {
    if parent.normalized_request["args"]["mode"] != "remove" {
        return Ok(());
    }
    let targets = parent.normalized_request["args"]["remove_agent_ids"]
        .as_array()
        .ok_or_else(|| RuntimeError::Conflict("model remove parent has no target set".into()))?;
    let count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_effects WHERE operation_id=?1",
            [&parent.operation_id],
            |row| row.get(0),
        )
        .map_err(|error| db_error("failed checking model remove target count", error))?;
    if targets.is_empty() || count != targets.len() as i64 {
        return Err(RuntimeError::Conflict(
            "model removal requires the complete frozen target set".into(),
        ));
    }
    let mut removed = Vec::with_capacity(targets.len());
    for (index, target) in targets.iter().enumerate() {
        let target = target
            .as_str()
            .map(str::trim)
            .ok_or_else(|| RuntimeError::Conflict("invalid model removal target".into()))?;
        let raw: String = tx
            .query_row(
                "SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1
             AND effect_kind=?2 AND idempotency_key=?3 AND target_id=?4 AND phase='finalized'",
                params![
                    parent.operation_id,
                    MEMBER_ARCHIVE_EFFECT,
                    format!("remove:{index}"),
                    target
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| db_error("failed reading confirmed model removal target", error))?
            .ok_or_else(|| {
                RuntimeError::Conflict("model removal target has no final receipt".into())
            })?;
        let evidence: Value = serde_json::from_str(&raw).map_err(|error| {
            RuntimeError::Bootstrap(format!("invalid model removal evidence: {error}"))
        })?;
        if evidence["outcome"]["status"] != "native_close_confirmed" {
            return Err(RuntimeError::Conflict(
                "model removal native close is not confirmed".into(),
            ));
        }
        let snapshot: OperatorAgentArchiveCommand =
            serde_json::from_value(evidence["snapshot"].clone()).map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid model removal snapshot: {error}"))
            })?;
        let child = operation_by_id(tx, &snapshot.operation_id)?
            .ok_or_else(|| RuntimeError::Conflict("model removal child is missing".into()))?;
        validate_member_archive_record(tx, &child)?;
        if child.kind != crate::repository_operator_agent_archive::OPERATION_KIND
            || child.phase != OperationPhase::Completed
            || child.actor != parent.actor
            || child.normalized_request_hash != snapshot.normalized_request_hash
        {
            return Err(RuntimeError::Conflict(
                "model removal child is not proven completed".into(),
            ));
        }
        let archived: runtime_core::WorkspaceAgentRecord =
            serde_json::from_value(child.exact_terminal_result.ok_or_else(|| {
                RuntimeError::Conflict("model removal child receipt missing".into())
            })?)
            .map_err(|error| {
                RuntimeError::Bootstrap(format!("invalid model removal child receipt: {error}"))
            })?;
        if archived.agent_id != target
            || archived.workspace_id != snapshot.workspace_id
            || archived.lifecycle_state != runtime_core::WorkspaceAgentLifecycleState::Archived
            || archived.revision
                != snapshot.expected_revision.checked_add(1).ok_or_else(|| {
                    RuntimeError::Conflict("model removal revision exhausted".into())
                })?
        {
            return Err(RuntimeError::Conflict(
                "model removal receipt changed target authority".into(),
            ));
        }
        removed.push(json!({"agent_id":archived.agent_id,"alias":archived.alias,
            "lifecycle":archived.lifecycle_state,"operation_id":snapshot.operation_id,"native_close":"confirmed"}));
    }
    let expected = json!({"ok":true,"result":{"operation":"remove","operation_id":parent.operation_id,
        "removed":removed,"preserve_worktrees":true}});
    if *exact_result != expected {
        return Err(RuntimeError::Conflict(
            "model removal aggregate differs from proven children".into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// Planning freezes the complete target set; it does not authorize native
    /// dispatch. Each child still needs fresh transactional member admission.
    pub fn plan_member_agent_archives(
        &self,
        parent_id: &str,
        caller: &str,
        policy: WorkspaceMembershipPolicy,
        at_ms: i64,
    ) -> Result<Vec<OperatorAgentArchiveCommand>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting model archive plan", error))?;
        let parent = operation_by_id(&tx, parent_id)?
            .ok_or_else(|| RuntimeError::NotFound("model tool operation".into()))?;
        let workspace = parent
            .workspace_id
            .as_deref()
            .ok_or_else(|| RuntimeError::InvalidState("model operation has no workspace".into()))?;
        if parent.kind != "agent_model_tool_mutation"
            || parent.phase != OperationPhase::Requested
            || parent.actor.kind != OperationActorKind::Agent
            || parent.actor.identifier != caller
            || parent.normalized_request["tool_name"] != "gg_team"
            || parent.normalized_request["args"]["mode"] != "remove"
            || parent.normalized_request["args"]["preserve_worktrees"] == false
            || runtime_core::normalized_json_hash(&parent.normalized_request)?
                != parent.normalized_request_hash
        {
            return Err(RuntimeError::Conflict(
                "invalid model remove parent authority".into(),
            ));
        }
        authorize_member(&tx, workspace, caller, policy)?;
        let existing: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_operation_effects WHERE operation_id=?1)",
                [parent_id],
                |row| row.get(0),
            )
            .map_err(|error| db_error("failed checking existing model remove plan", error))?;
        if existing {
            return Err(RuntimeError::operation_unresolved(parent_id,
            "original model removal plan already exists; inspect its targets without redispatch"));
        }
        let targets = parent.normalized_request["args"]["remove_agent_ids"]
            .as_array()
            .filter(|ids| !ids.is_empty() && ids.len() <= 100)
            .ok_or_else(|| RuntimeError::InvalidState("invalid model removal target set".into()))?;
        let mut seen = std::collections::HashSet::new();
        let mut commands = Vec::with_capacity(targets.len());
        let authority = OperationAuthorityTransaction::new(&tx);
        for (index, target) in targets.iter().enumerate() {
            let id = target
                .as_str()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| RuntimeError::InvalidState("invalid model removal target".into()))?;
            if !seen.insert(id) {
                return Err(RuntimeError::InvalidState(
                    "duplicate canonical removal target".into(),
                ));
            }
            let state = restore_state(&tx, id)?;
            if state.workspace_id != workspace
                || state.workspace_lifecycle != "active"
                || state.agent_lifecycle != "active"
                || state.session_status != "ready"
                || state.active_turn_id.is_some()
                || state.revision < 0
                || state.revision >= i64::MAX
            {
                return Err(RuntimeError::Conflict(
                    "model removal target is not idle, active or revisionable".into(),
                ));
            }
            let suffix = runtime_core::normalized_json_hash(&json!([parent_id, index, id]))?;
            let child_id = format!("op_member_archive_{suffix}");
            let effect_id = format!("fx_member_target_{child_id}");
            let request = json!({"workspace_id":workspace,"agent_id":id,"action":"archive",
                "reason":"gg_team_remove","model_tool_parent":{"operation_id":parent_id,
                    "effect_id":effect_id,"index":index,"membership_policy":{
                        "non_lead_can_add_members":policy.non_lead_can_add_members,
                        "non_lead_can_remove_members":policy.non_lead_can_remove_members}},
                "original_route":{"cwd":state.cwd,"model":state.model}});
            let command = OperatorAgentArchiveCommand {
                operation_id: child_id,
                actor: parent.actor.clone(),
                idempotency_key: format!("member-archive:{suffix}"),
                workspace_id: workspace.into(),
                agent_id: id.into(),
                reason: Some("gg_team_remove".into()),
                expected_revision: state.revision as u64,
                provider: state.provider,
                provider_session_ref: state
                    .provider_session_ref
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| {
                        RuntimeError::Conflict(
                            "model archive target native identity missing".into(),
                        )
                    })?,
                canonical_provider_session_ref: state.canonical_provider_session_ref,
                session_updated_at: state.session_updated_at,
                normalized_request_hash: runtime_core::normalized_json_hash(&request)?,
                normalized_request: request,
                requested_at: at_ms,
            };
            authority.insert_effect(&OperationEffectRecord {
                effect_id,
                operation_id: parent_id.into(),
                effect_kind: MEMBER_ARCHIVE_EFFECT.into(),
                target_kind: "workspace_agent".into(),
                target_id: id.into(),
                phase: "intended".into(),
                idempotency_key: format!("remove:{index}"),
                evidence: Some(json!({"snapshot":command,
                    "outcome":{"status":"not_admitted"}})),
                attempt_count: 0,
                created_at: at_ms,
                updated_at: at_ms,
            })?;
            commands.push(command);
        }
        event(
            &tx,
            parent_id,
            workspace,
            "planned",
            json!({"operation_id":parent_id,"target_count":commands.len(),"status":"targets_planned"}),
            at_ms,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing model removal plan", error))?;
        Ok(commands)
    }

    pub fn reject_member_agent_archive(
        &self,
        command: &OperatorAgentArchiveCommand,
        reason: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| db_error("failed starting model archive rejection", error))?;
        // Any admitted child owns its native uncertainty. Never overwrite it
        // with a fabricated pre-dispatch rejection based on a returned error.
        if operation_by_id(&tx, &command.operation_id)?.is_some() {
            return Ok(());
        }
        let mut linked = link(
            &tx,
            &command.operation_id,
            &command.actor,
            &command.workspace_id,
            &command.normalized_request,
            &command.normalized_request_hash,
        )?;
        if linked.snapshot != *command || linked.phase != "intended" {
            return Err(RuntimeError::Conflict(
                "model archive rejection changed target".into(),
            ));
        }
        if linked.evidence["outcome"]["status"] == "rejected_before_dispatch" {
            return Ok(());
        }
        linked.evidence["outcome"] = json!({"status":"rejected_before_dispatch",
            "diagnostic":reason.chars().take(240).collect::<String>()});
        tx.execute("UPDATE runtime_operation_effects SET evidence_json=?2,updated_at=?3 WHERE effect_id=?1 AND phase='intended'",
            params![linked.effect_id,json_to_string(&linked.evidence)?,at_ms])
            .map_err(|error|db_error("failed recording model target rejection",error))?;
        event(
            &tx,
            &linked.parent_id,
            &command.workspace_id,
            &format!("{}_rejected", command.operation_id),
            json!({"operation_id":linked.parent_id,"child_operation_id":command.operation_id,
                "agent_id":command.agent_id,"status":"rejected_before_dispatch"}),
            at_ms,
        )?;
        tx.commit()
            .map_err(|error| db_error("failed committing model target rejection", error))
    }
}
