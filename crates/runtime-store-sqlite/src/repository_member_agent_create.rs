use runtime_core::{
    MemberAgentCreateCommand, MemberAgentCreateRoute, NewRuntimeEvent, OperationActorKind,
    OperationEffectRecord, OperationPhase, OperationRecord, OperatorAgentCreateAdmission,
    RuntimeError, RuntimeEventCriticality, RuntimeEventScope, WorkspaceAgentCreateRequest,
    WorkspaceAgentOnboardingRequest, WorkspaceAgentRecord,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};

use crate::db::{db_error, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository::insert_runtime_event_in_transaction;
use crate::repository_operator_agent_create::{classify_create, OPERATION_KIND};
use crate::repository_workspace::{operation_by_id, operation_by_idempotency};
use crate::repository_workspace_agent::{workspace_agent_from_row, AGENT_SELECT};
use crate::SqliteRuntimeRepository;

pub(crate) const EFFECT: &str = "model_member_create";

fn effect_id(command: &MemberAgentCreateCommand) -> String {
    format!("fx_member_target_{}", command.operation.operation_id)
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, RuntimeError> {
    serde_json::from_value(value)
        .map_err(|e| RuntimeError::Bootstrap(format!("corrupt member create plan: {e}")))
}

fn optional_string(value: &Value) -> Result<Option<String>, RuntimeError> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|v| Some(v.to_owned()))
        .ok_or_else(|| RuntimeError::Conflict("member create input is not a string".into()))
}

fn validate_parent(
    command: &MemberAgentCreateCommand,
    parent: &OperationRecord,
) -> Result<(), RuntimeError> {
    let op = &command.operation;
    let intent = &command.intent;
    if op.actor.kind != OperationActorKind::Agent
        || parent.actor != op.actor
        || parent.kind != "agent_model_tool_mutation"
        || parent.operation_id != intent.parent_operation_id
        || parent.workspace_id.as_deref() != Some(op.workspace_id.as_str())
        || parent.normalized_request["tool_name"] != "gg_team"
        || parent.normalized_request["args"]["mode"] != "add"
        || parent.normalized_request_hash != intent.parent_request_hash
        || runtime_core::normalized_json_hash(&parent.normalized_request)?
            != intent.parent_request_hash
        || intent.caller.agent_id != op.actor.identifier
        || intent.caller.workspace_id != op.workspace_id
        || op.normalized_request
            != json!({"workspace_id":op.workspace_id,"action":"create","request":intent.request,"member_intent":intent})
        || runtime_core::normalized_json_hash(&op.normalized_request)? != op.normalized_request_hash
    {
        return Err(RuntimeError::Conflict(
            "member creation lost its exact authenticated parent".into(),
        ));
    }
    let args = &parent.normalized_request["args"];
    let mut policy = intent.caller.recreation_policy.clone();
    match (&intent.preset, optional_string(&args["model_preset"])?) {
        (None, None) => {}
        (Some(preset), Some(name))
            if preset.requested_name == name && !preset.model.trim().is_empty() =>
        {
            policy.provider = preset.provider;
            policy.model = Some(preset.model.clone());
            if let Some(effort) = preset.thinking_effort {
                policy.current_preferences.thinking_effort = Some(effort);
            }
        }
        _ => {
            return Err(RuntimeError::Conflict(
                "member creation preset is not its parent's selection".into(),
            ))
        }
    }
    policy.authoritative_cwd = intent.route.cwd().into();
    let images: Vec<String> = if args["image_paths"].is_null() {
        Vec::new()
    } else {
        decode(args["image_paths"].clone())?
    };
    let expected = WorkspaceAgentCreateRequest {
        provider: policy.provider,
        model: policy.model.clone(),
        permission_intent: policy.permission_intent.clone(),
        setting_sources_intent: policy.setting_sources_intent.clone(),
        current_preferences: policy.current_preferences.clone(),
        system_prompt: policy.system_prompt.clone(),
        allowed_tools: policy.allowed_tools.clone(),
        disallowed_tools: policy.disallowed_tools.clone(),
        cwd: None,
        worktree: Some(intent.route.request_route()),
        harness_version_slot: policy.harness_version_slot.clone(),
        title: optional_string(&args["title"])?,
        metadata: None,
        onboarding: Some(WorkspaceAgentOnboardingRequest {
            prompt: optional_string(&args["prompt"])?,
            image_paths: images,
            skill: None,
            creator_compaction_subscription: optional_string(
                &args["creator_compaction_subscription"],
            )?,
        }),
    };
    if expected != intent.request || policy != intent.recreation_policy {
        return Err(RuntimeError::Conflict(
            "member creation differs from its frozen inherited policy/input".into(),
        ));
    }
    match (&intent.route, optional_string(&args["worktree_name"])?) {
        (MemberAgentCreateRoute::Root { canonical_root }, None)
            if canonical_root == &intent.canonical_root && op.worktree_operation_id.is_none() => {}
        (MemberAgentCreateRoute::Existing { worktree, .. }, Some(name))
            if worktree.worktree_name.trim() == name.trim()
                && op.worktree_operation_id.is_none() => {}
        (
            MemberAgentCreateRoute::New {
                worktree,
                base_commit,
                ..
            },
            Some(name),
        ) if worktree.worktree_name.trim() == name.trim()
            && worktree.deletion_policy == "retain_on_last_claim"
            && worktree.created_by_operation_id.is_some()
            && op.worktree_operation_id == worktree.created_by_operation_id
            && matches!(base_commit.len(), 40 | 64)
            && base_commit.bytes().all(|c| c.is_ascii_hexdigit()) => {}
        _ => {
            return Err(RuntimeError::Conflict(
                "member creation route differs from its parent's selection".into(),
            ))
        }
    }
    Ok(())
}

pub(crate) fn validate_link(
    connection: &Connection,
    command: &MemberAgentCreateCommand,
) -> Result<Value, RuntimeError> {
    let parent = operation_by_id(connection, &command.intent.parent_operation_id)?
        .ok_or_else(|| RuntimeError::NotFound("model create parent".into()))?;
    validate_parent(command, &parent)?;
    let raw: Option<String> = connection.query_row(
        "SELECT evidence_json FROM runtime_operation_effects WHERE effect_id=?1 AND operation_id=?2 AND effect_kind=?3 AND target_id=?4",
        params![effect_id(command), command.intent.parent_operation_id, EFFECT, command.operation.reserved_agent_id], |row| row.get(0),
    ).optional().map_err(|e| db_error("failed reading member create parent link", e))?;
    let evidence: Value =
        serde_json::from_str(&raw.ok_or_else(|| {
            RuntimeError::Conflict("member create parent intent is absent".into())
        })?)
        .map_err(|e| RuntimeError::Bootstrap(format!("invalid member create intent: {e}")))?;
    let snapshot: MemberAgentCreateCommand = decode(evidence["snapshot"].clone())?;
    if snapshot != *command {
        return Err(RuntimeError::Conflict(
            "member create immutable snapshot changed".into(),
        ));
    }
    Ok(evidence)
}

pub(crate) fn command_for_child(
    connection: &Connection,
    child: &OperationRecord,
) -> Result<Option<MemberAgentCreateCommand>, RuntimeError> {
    if child.actor.kind != OperationActorKind::Agent {
        return Ok(None);
    }
    let raw: String = connection.query_row(
        "SELECT evidence_json FROM runtime_operation_effects WHERE effect_id=?1 AND effect_kind=?2",
        params![format!("fx_member_target_{}", child.operation_id), EFFECT], |row| row.get(0),
    ).optional().map_err(|e| db_error("failed reading Agent create origin", e))?
        .ok_or_else(|| RuntimeError::Conflict("Agent create lacks an authenticated parent effect".into()))?;
    let evidence: Value = serde_json::from_str(&raw)
        .map_err(|e| RuntimeError::Bootstrap(format!("invalid member create link: {e}")))?;
    let command: MemberAgentCreateCommand = decode(evidence["snapshot"].clone())?;
    let op = &command.operation;
    if child.kind != OPERATION_KIND
        || child.operation_id != op.operation_id
        || child.actor != op.actor
        || child.workspace_id.as_deref() != Some(op.workspace_id.as_str())
        || child.idempotency_key.as_deref() != Some(op.idempotency_key.as_str())
        || child.normalized_request != op.normalized_request
        || child.normalized_request_hash != op.normalized_request_hash
        || child.created_at != op.requested_at
    {
        return Err(RuntimeError::Conflict(
            "Agent create child identity differs from its parent".into(),
        ));
    }
    validate_link(connection, &command)?;
    Ok(Some(command))
}

pub(crate) fn authorize_stage(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
) -> Result<(), RuntimeError> {
    let evidence = validate_link(tx, command)?;
    let parent = operation_by_id(tx, &command.intent.parent_operation_id)?
        .ok_or_else(|| RuntimeError::Conflict("member parent disappeared".into()))?;
    if parent.phase != OperationPhase::Requested
        || !matches!(
            evidence["outcome"]["status"].as_str(),
            Some("not_admitted" | "native_admitted")
        )
    {
        return Err(RuntimeError::Conflict(
            "member creation parent no longer authorizes a native stage".into(),
        ));
    }
    authorize_current_caller(tx, command)?;
    match &command.intent.route {
        MemberAgentCreateRoute::Existing {
            repository,
            worktree,
            ..
        }
        | MemberAgentCreateRoute::New {
            repository,
            worktree,
            ..
        } => {
            if runtime_core::resolve_repository_identity(std::path::Path::new(
                &command.intent.canonical_root,
            ))? != *repository
                || runtime_core::resolve_repository_identity(std::path::Path::new(
                    &worktree.repo_root,
                ))? != *repository
            {
                return Err(RuntimeError::Conflict(
                    "member workspace no longer belongs to its frozen repository".into(),
                ));
            }
        }
        MemberAgentCreateRoute::Root { .. } => {}
    }
    Ok(())
}

pub(crate) fn authorize_current_caller(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
) -> Result<(), RuntimeError> {
    let op = &command.operation;
    let lead: Option<Option<String>> = tx.query_row(
        "SELECT w.lead_agent_id FROM workspaces w JOIN workspace_agents a ON a.workspace_id=w.workspace_id JOIN sessions s ON s.id=a.session_id
         WHERE w.workspace_id=?1 AND w.canonical_root=?3 AND a.session_id=?2 AND w.lifecycle_state='active' AND a.lifecycle_state='active'
         AND s.status NOT IN ('closed','failed') AND NOT EXISTS(SELECT 1 FROM workspace_retirement_fences f WHERE f.workspace_id=w.workspace_id)",
        params![op.workspace_id, op.actor.identifier, command.intent.canonical_root], |row| row.get(0),
    ).optional().map_err(|e| db_error("failed checking member create stage authority", e))?;
    let lead = lead.ok_or_else(|| {
        RuntimeError::Conflict("member creation requires a current active caller/workspace".into())
    })?;
    if lead.as_deref().is_some_and(|id| id != op.actor.identifier) && !command.intent.allow_non_lead
    {
        return Err(RuntimeError::Conflict(
            "caller can no longer add workspace members".into(),
        ));
    }
    let current = tx
        .query_row(
            &format!("{AGENT_SELECT} WHERE a.session_id=?1"),
            [&op.actor.identifier],
            workspace_agent_from_row,
        )
        .map_err(|e| db_error("failed checking original member create policy", e))?;
    if current.recreation_policy != command.intent.caller.recreation_policy {
        return Err(RuntimeError::Conflict(
            "member creator policy changed before the next native stage".into(),
        ));
    }
    let blocked: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM runtime_operation_resource_claims WHERE resource_id=?1 AND resource_kind IN
         ('workspace_agent_rebind','operator_provider_policy_native','operator_agent_archive_native','operator_agent_restore_native'))",
        [&op.actor.identifier], |row| row.get(0),
    ).map_err(|e| db_error("failed checking creator native policy authority", e))?;
    if blocked {
        return Err(RuntimeError::Conflict(
            "member creator has unresolved native policy/lifecycle authority".into(),
        ));
    }
    Ok(())
}

fn event(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    status: &str,
    at: i64,
) -> Result<(), RuntimeError> {
    insert_runtime_event_in_transaction(
        tx,
        &NewRuntimeEvent {
            event_id: format!(
                "evt_model_create_{}_{}",
                command.operation.operation_id, status
            ),
            scope: RuntimeEventScope::Workspace,
            scope_id: command.operation.workspace_id.clone(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "model_tool.create_target_updated".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({"operation_id":command.intent.parent_operation_id,"child_operation_id":command.operation.operation_id,
            "agent_id":command.operation.reserved_agent_id,"status":status}),
            provider: None,
            provider_seq: None,
            created_at: at,
        },
    )?;
    Ok(())
}

pub(crate) fn record_outcome(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    phase: &str,
    status: &str,
    result: Option<Value>,
    at: i64,
) -> Result<(), RuntimeError> {
    let mut evidence = validate_link(tx, command)?;
    let outcome = json!({"status":status,"model_result":result});
    if evidence["outcome"] == outcome {
        return Ok(());
    }
    if matches!(
        evidence["outcome"]["status"].as_str(),
        Some("native_create_confirmed" | "verified_not_created")
    ) {
        return Err(RuntimeError::Conflict(
            "member creation already has an immutable target outcome".into(),
        ));
    }
    evidence["outcome"] = outcome;
    let changed = tx.execute("UPDATE runtime_operation_effects SET phase=?2,evidence_json=?3,attempt_count=1,updated_at=?4 WHERE effect_id=?1 AND phase IN ('intended','started','uncertain')",
        params![effect_id(command),phase,json_to_string(&evidence)?,at]).map_err(|e| db_error("failed saving member create target outcome",e))?;
    if changed != 1 {
        return Err(RuntimeError::Conflict(
            "member create target outcome raced".into(),
        ));
    }
    event(tx, command, status, at)
}

pub(crate) fn complete_parent(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    agent: &WorkspaceAgentRecord,
    at: i64,
) -> Result<(), RuntimeError> {
    let (notice_id,state): (String,String) = tx.query_row(
        "SELECT notice_id,state FROM workspace_system_notices WHERE notice_id=?1 AND workspace_id=?2 AND recipient_agent_id=?3",
        params![format!("workspace-onboarding:{}", agent.agent_id),agent.workspace_id,agent.agent_id], |row| Ok((row.get(0)?,row.get(1)?)),
    ).map_err(|e| db_error("failed freezing member Add onboarding receipt",e))?;
    let model = json!({"operation":"add","agent_id":agent.agent_id,"alias":agent.alias,"workspace_id":agent.workspace_id,
        "onboarding":{"notice_id":notice_id,"state":state}});
    record_outcome(
        tx,
        command,
        "finalized",
        "native_create_confirmed",
        Some(model),
        at,
    )
}

pub(crate) fn verify_model_add_result(
    tx: &Transaction<'_>,
    parent: &OperationRecord,
    result: &Value,
) -> Result<(), RuntimeError> {
    if parent.normalized_request["tool_name"] != "gg_team"
        || parent.normalized_request["args"]["mode"] != "add"
    {
        return Ok(());
    }
    let mut statement=tx.prepare("SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1 AND effect_kind=?2")
        .map_err(|e|db_error("failed preparing model Add proof",e))?;
    let rows = statement
        .query_map(params![parent.operation_id, EFFECT], |r| {
            r.get::<_, String>(0)
        })
        .map_err(|e| db_error("failed reading model Add proof", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| db_error("failed decoding model Add proof", e))?;
    if rows.len() != 1 {
        return Err(RuntimeError::Conflict(
            "model Add needs one exact native target receipt".into(),
        ));
    }
    let evidence: Value = serde_json::from_str(&rows[0])
        .map_err(|e| RuntimeError::Bootstrap(format!("invalid Add proof: {e}")))?;
    let command: MemberAgentCreateCommand = decode(evidence["snapshot"].clone())?;
    let model = read_result(tx, &command)?;
    if result["result"] != model {
        return Err(RuntimeError::Conflict(
            "model Add result differs from its exact native receipt".into(),
        ));
    }
    Ok(())
}

fn read_result(
    connection: &Connection,
    command: &MemberAgentCreateCommand,
) -> Result<Value, RuntimeError> {
    let evidence = validate_link(connection, command)?;
    let child = operation_by_id(connection, &command.operation.operation_id)?
        .ok_or_else(|| RuntimeError::Conflict("member native create child absent".into()))?;
    command_for_child(connection, &child)?;
    let agent: WorkspaceAgentRecord =
        decode(child.exact_terminal_result.clone().unwrap_or(Value::Null))?;
    let result = &evidence["outcome"]["model_result"];
    if child.phase != OperationPhase::Completed
        || evidence["outcome"]["status"] != "native_create_confirmed"
        || agent.agent_id != command.operation.reserved_agent_id
        || agent.workspace_id != command.operation.workspace_id
        || agent.profile.added_by != command.operation.actor.identifier
        || result["agent_id"] != agent.agent_id
        || result["alias"] != agent.alias
        || result["workspace_id"] != agent.workspace_id
        || result["operation"] != "add"
        || result["onboarding"]["notice_id"] != format!("workspace-onboarding:{}", agent.agent_id)
    {
        return Err(RuntimeError::Conflict(
            "member create has no verified exact model receipt".into(),
        ));
    }
    Ok(result.clone())
}

impl SqliteRuntimeRepository {
    pub fn plan_member_agent_create(
        &self,
        command: &MemberAgentCreateCommand,
    ) -> Result<MemberAgentCreateCommand, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed opening member create plan", e))?;
        let parent = operation_by_id(&tx, &command.intent.parent_operation_id)?
            .ok_or_else(|| RuntimeError::NotFound("member create parent".into()))?;
        // The stored first plan wins even if the preset/roster later changes.
        let existing:Option<String>=tx.query_row("SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1 AND effect_kind=?2",
            params![parent.operation_id,EFFECT],|r|r.get(0)).optional().map_err(|e|db_error("failed reading prior Add plan",e))?;
        if let Some(raw) = existing {
            let evidence: Value = serde_json::from_str(&raw)
                .map_err(|e| RuntimeError::Bootstrap(format!("invalid Add plan: {e}")))?;
            let saved: MemberAgentCreateCommand = decode(evidence["snapshot"].clone())?;
            if saved.operation.actor != command.operation.actor
                || saved.intent.parent_request_hash != command.intent.parent_request_hash
            {
                return Err(RuntimeError::Conflict(
                    "member create parent belongs to different authority".into(),
                ));
            }
            validate_link(&tx, &saved)?;
            return Ok(saved);
        }
        validate_parent(command, &parent)?;
        if parent.phase != OperationPhase::Requested {
            return Err(RuntimeError::Conflict(
                "member create parent is not requested".into(),
            ));
        }
        let actual = tx
            .query_row(
                &format!("{AGENT_SELECT} WHERE a.session_id=?1"),
                [&command.operation.actor.identifier],
                workspace_agent_from_row,
            )
            .map_err(|e| db_error("failed freezing member creator", e))?;
        if actual != command.intent.caller {
            return Err(RuntimeError::Conflict(
                "member creator changed before planning".into(),
            ));
        }
        OperationAuthorityTransaction::new(&tx).insert_effect(&OperationEffectRecord {
            effect_id: effect_id(command),
            operation_id: parent.operation_id,
            effect_kind: EFFECT.into(),
            target_kind: "provider_session".into(),
            target_id: command.operation.reserved_agent_id.clone(),
            phase: "intended".into(),
            idempotency_key: "add:0".into(),
            evidence: Some(
                json!({"snapshot":command,"outcome":{"status":"not_admitted","model_result":null}}),
            ),
            attempt_count: 0,
            created_at: command.operation.requested_at,
            updated_at: command.operation.requested_at,
        })?;
        authorize_stage(&tx, command)?;
        crate::repository_member_agent_create_routes::validate_route(&tx, command, false)?;
        event(&tx, command, "not_admitted", command.operation.requested_at)?;
        tx.commit()
            .map_err(|e| db_error("failed committing member create plan", e))?;
        Ok(command.clone())
    }

    pub fn replay_member_agent_create(
        &self,
        command: &MemberAgentCreateCommand,
    ) -> Result<Option<OperatorAgentCreateAdmission>, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        validate_link(&connection, command)?;
        let op = &command.operation;
        operation_by_idempotency(
            &connection,
            op.actor.kind,
            &op.actor.identifier,
            Some(&op.idempotency_key),
        )?
        .map(|record| {
            command_for_child(&connection, &record)?;
            classify_create(record, &op.workspace_id, &op.normalized_request_hash)
        })
        .transpose()
    }

    pub fn member_agent_create_result(
        &self,
        command: &MemberAgentCreateCommand,
    ) -> Result<Value, RuntimeError> {
        let connection = open_connection(&self.database_path)?;
        read_result(&connection, command)
    }
}
