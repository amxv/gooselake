use runtime_core::{
    MemberAgentCreateCommand, OperationActorKind, OperatorAgentCreateLaunchSnapshot, RuntimeError,
    TeamHookEffectPhase, TeamHookEffectRecord, TeamHookEventName, TeamHookPreAddContext,
    WorkspaceAgentRecord,
};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

use crate::db::db_error;
use crate::repository_member_agent_create::{
    authorize_current_caller, authorize_stage, command_for_child,
};
use crate::repository_operator_agent_create::{verify_requested_create, EFFECT_KIND};
use crate::repository_team_hooks::{effect_grant_is_current, read_team_hook_effect};
use crate::repository_workspace::operation_by_id;

pub(crate) struct PreAddApplication {
    pub actor: Option<String>,
    pub apply: bool,
}

/// Provider absence says nothing about a shell that previously acquired its
/// execution claim. Never release its checkout reservation on that evidence.
/// Startup can quarantine an unclaimed intention too, so attempt_count is
/// required in addition to the unresolved phase.
pub(crate) fn unresolved_execution(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
) -> Result<bool, RuntimeError> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM workspace_hook_effects
         WHERE workspace_id=?1 AND parent_event_id=?2
           AND event_name='pre_teammate_add' AND attempt_count>0
           AND phase IN ('started','manual_review')
           AND json_extract(input_json,'$.subject.member_agent_id')=?3)",
        params![
            command.operation.workspace_id,
            format!(
                "workspace-hook:pre-add:{}",
                command.operation.reserved_agent_id
            ),
            command.operation.reserved_agent_id
        ],
        |row| row.get(0),
    )
    .map_err(|error| db_error("failed checking original member pre-add execution", error))
}

fn frozen_effect(tx: &Transaction<'_>, id: &str) -> Result<Value, RuntimeError> {
    let raw:String=tx.query_row("SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1 AND effect_kind=?2",
        params![id,EFFECT_KIND],|r|r.get(0)).map_err(|e|db_error("failed reading frozen member launch history",e))?;
    serde_json::from_str(&raw)
        .map_err(|e| RuntimeError::Bootstrap(format!("invalid member launch history: {e}")))
}

pub(crate) fn authorize_hook_claim(
    tx: &Transaction<'_>,
    effect: &TeamHookEffectRecord,
) -> Result<(), RuntimeError> {
    if effect.hook.event_name != TeamHookEventName::PreTeammateAdd {
        return Ok(());
    }
    let reserved = effect.input["subject"]["member_agent_id"]
        .as_str()
        .ok_or_else(|| RuntimeError::Conflict("pre-add effect has no reserved target".into()))?;
    let id:Option<String>=tx.query_row("SELECT o.operation_id FROM runtime_operations o JOIN runtime_operation_effects e ON e.operation_id=o.operation_id
        WHERE o.kind='operator_agent_create' AND o.actor_kind='agent' AND e.effect_kind=?1 AND e.target_id=?2",
        params![EFFECT_KIND,reserved],|r|r.get(0)).optional().map_err(|e|db_error("failed locating member pre-add native authority",e))?;
    let Some(id) = id else {
        return Ok(());
    };
    let child = operation_by_id(tx, &id)?
        .ok_or_else(|| RuntimeError::Conflict("member pre-add child disappeared".into()))?;
    let command = command_for_child(tx, &child)?
        .ok_or_else(|| RuntimeError::Conflict("member pre-add has no Agent origin".into()))?;
    verify_requested_create(tx, &command.operation)?;
    authorize_stage(tx, &command)?;
    crate::repository_member_agent_create_routes::validate_route(tx, &command, true)?;
    crate::repository_member_agent_create_routes::validate_native_ready_route(&command)?;
    validate_input(&command, effect)?;
    if frozen_effect(tx, &id)?["stage"] != "reserved_no_provider_launch_snapshot" {
        return Err(RuntimeError::Conflict(
            "pre-add shell cannot start after provider launch admission".into(),
        ));
    }
    Ok(())
}

fn validate_input(
    command: &MemberAgentCreateCommand,
    effect: &TeamHookEffectRecord,
) -> Result<(), RuntimeError> {
    let op = &command.operation;
    let intent = &command.intent;
    let expected_parent = format!("workspace-hook:pre-add:{}", op.reserved_agent_id);
    let expected_subject = json!({"kind":"member","action":"preparing","member_agent_id":op.reserved_agent_id,
        "provider":intent.request.provider.as_str(),"model":intent.request.model,"planned_cwd":intent.route.cwd(),
        "member_title":intent.request.title,"onboarding_prompt":intent.request.onboarding.as_ref().and_then(|v|v.prompt.as_ref())});
    if effect.workspace_id != op.workspace_id
        || effect.parent_event_id != expected_parent
        || effect.input["event_id"] != expected_parent
        || effect.input["hook_event"] != "pre_teammate_add"
        || effect.input["workspace_id"] != op.workspace_id
        || effect.input["actor_agent_id"] != op.actor.identifier
        || effect.input["subject"] != expected_subject
    {
        return Err(RuntimeError::Conflict(
            "pre-add script input does not match its original member intent".into(),
        ));
    }
    Ok(())
}

fn history(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    hooks: Option<&TeamHookPreAddContext>,
) -> Result<Vec<TeamHookEffectRecord>, RuntimeError> {
    let parent = format!(
        "workspace-hook:pre-add:{}",
        command.operation.reserved_agent_id
    );
    let mut statement=tx.prepare("SELECT hook_name FROM workspace_hook_effects WHERE workspace_id=?1 AND parent_event_id=?2 AND event_name='pre_teammate_add' ORDER BY hook_name LIMIT 33")
        .map_err(|e|db_error("failed preparing member pre-add history",e))?;
    let names = statement
        .query_map(params![command.operation.workspace_id, parent], |r| {
            r.get::<_, String>(0)
        })
        .map_err(|e| db_error("failed querying member pre-add history", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| db_error("failed reading member pre-add history", e))?;
    let Some(hooks) = hooks else {
        if !names.is_empty() {
            return Err(RuntimeError::Conflict(
                "member launch omitted its admitted pre-add history".into(),
            ));
        }
        return Ok(Vec::new());
    };
    let mut expected = hooks.completed_hook_names.clone();
    expected.sort();
    if parent != hooks.parent_event_id || names.is_empty() || names.len() > 32 || names != expected
    {
        return Err(RuntimeError::Conflict(
            "member pre-add history is incomplete or substituted".into(),
        ));
    }
    hooks
        .completed_hook_names
        .iter()
        .map(|name| {
            let row = read_team_hook_effect(
                tx,
                &command.operation.workspace_id,
                &parent,
                TeamHookEventName::PreTeammateAdd,
                name,
            )?
            .ok_or_else(|| RuntimeError::Conflict("member hook receipt disappeared".into()))?;
            validate_input(command, &row)?;
            if row.phase != TeamHookEffectPhase::Succeeded
                || row.attempt_count != 1
                || row.application_state != "pending"
                || row.application_error_code.is_some()
                || row.applied_at_ms.is_some()
            {
                return Err(RuntimeError::Conflict(
                    "member hook is not an unapplied successful native receipt".into(),
                ));
            }
            Ok(row)
        })
        .collect()
}

fn append(target: &mut Option<String>, text: &str, limit: usize) -> Result<(), RuntimeError> {
    let next = target
        .as_ref()
        .map_or_else(|| text.to_owned(), |old| format!("{old}\n{text}"));
    if next.len() > limit || next.contains('\0') {
        return Err(RuntimeError::Conflict(
            "member pre-add context exceeds its aggregate bounds".into(),
        ));
    }
    *target = Some(next);
    Ok(())
}

pub(crate) fn validate_launch(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    snapshot: &OperatorAgentCreateLaunchSnapshot,
) -> Result<Value, RuntimeError> {
    crate::repository_member_agent_create_routes::validate_native_ready_route(command)?;
    let intent = &command.intent;
    let op = &command.operation;
    let mut onboarding = intent.request.onboarding.clone().unwrap_or_default();
    onboarding.image_paths = onboarding
        .image_paths
        .into_iter()
        .map(|v| v.trim().to_owned())
        .collect();
    if snapshot.agent.recreation_policy != intent.recreation_policy
        || snapshot.onboarding != onboarding
        || snapshot.agent.profile.added_by != op.actor.identifier
        || snapshot.agent.profile.creator_session_id.as_deref()
            != Some(op.actor.identifier.as_str())
        || snapshot.agent.profile.title
            != intent
                .request
                .title
                .as_deref()
                .map(str::trim)
                .map(str::to_owned)
        || snapshot.agent.profile.creator_compaction_subscription
            != onboarding
                .creator_compaction_subscription
                .as_deref()
                .unwrap_or("auto")
        || snapshot.session.worktree_id.as_deref() != intent.route.worktree().map(|w| w.id.as_str())
        || snapshot.session.cwd.as_deref() != Some(intent.route.cwd())
        || snapshot.session.permission_mode
            != intent.recreation_policy.permission_intent.resolved_mode()
        || snapshot.session.system_prompt != intent.recreation_policy.system_prompt
        || snapshot.session.metadata != json!({})
        || snapshot.agent.metadata != json!({})
        || snapshot.session.active_turn_id.is_some()
        || snapshot.session.closed_at.is_some()
        || snapshot.session.failure_code.is_some()
        || snapshot.session.failure_message.is_some()
    {
        return Err(RuntimeError::Conflict(
            "member provider launch differs from frozen route/policy/creator/onboarding".into(),
        ));
    }
    let effects = history(tx, command, snapshot.hooks.as_ref())?;
    let mut onboard_text = None;
    let mut caller_text = None;
    for effect in &effects {
        if !effect_grant_is_current(tx, &op.workspace_id, &effect.hook)? {
            return Err(RuntimeError::Conflict(
                "member hook approval was revoked before provider admission".into(),
            ));
        }
        let output = effect
            .receipt
            .as_ref()
            .and_then(|r| r.validated_output.as_ref())
            .ok_or_else(|| RuntimeError::Conflict("member hook lacks validated output".into()))?;
        let bytes =
            serde_json::to_vec(output).map_err(|e| RuntimeError::Bootstrap(e.to_string()))?;
        let output = runtime_core::team_hooks::validate_team_hook_script_output(
            TeamHookEventName::PreTeammateAdd,
            &bytes,
        )?;
        if let Some(pre) = output.pre_teammate_add {
            if let Some(route) = pre.spawn_template_mutation {
                let actual = std::fs::canonicalize(&route.cwd)
                    .map_err(|e| RuntimeError::Io(e.to_string()))?;
                if actual.to_string_lossy() != intent.route.cwd() {
                    return Err(RuntimeError::Conflict(
                        "member hook changed its explicit frozen route".into(),
                    ));
                }
            }
            if let Some(text) = pre.onboarding_context_append_text {
                append(&mut onboard_text, &text, 32 * 1024)?;
            }
        }
        if let Some(inject) = output.inject {
            if let Some(caller) = inject.caller {
                append(&mut caller_text, &caller.text, 16 * 1024)?;
            }
            for target in inject.targets {
                if target.agent_id == "__new_member__" || target.agent_id == op.reserved_agent_id {
                    append(&mut onboard_text, &target.text, 32 * 1024)?;
                } else if target.agent_id == op.actor.identifier {
                    append(&mut caller_text, &target.text, 16 * 1024)?;
                } else {
                    return Err(RuntimeError::Conflict(
                        "member hook output targeted an unrelated agent".into(),
                    ));
                }
            }
        }
    }
    if snapshot
        .hooks
        .as_ref()
        .map(|h| (&h.onboarding_context_append_text, &h.caller_context_text))
        != (!effects.is_empty()).then_some((&onboard_text, &caller_text))
    {
        return Err(RuntimeError::Conflict(
            "member hook context differs from its exact shell receipts".into(),
        ));
    }
    serde_json::to_value(effects).map_err(|e| RuntimeError::Bootstrap(e.to_string()))
}

/// Native creation already happened. Revoked delivery authority withholds
/// unconsumed context, while immutable history/attachment still must match.
pub(crate) fn settlement(
    tx: &Transaction<'_>,
    source: Option<&str>,
    agent: &WorkspaceAgentRecord,
    hooks: Option<&TeamHookPreAddContext>,
) -> Result<Option<PreAddApplication>, RuntimeError> {
    let Some(source) = source else {
        return Ok(None);
    };
    let child = operation_by_id(tx, source)?
        .ok_or_else(|| RuntimeError::Conflict("native create source missing".into()))?;
    if child.actor.kind != OperationActorKind::Agent {
        return Ok(None);
    }
    let command = command_for_child(tx, &child)?
        .ok_or_else(|| RuntimeError::Conflict("member source has no parent".into()))?;
    let frozen = frozen_effect(tx, source)?;
    let snapshot: OperatorAgentCreateLaunchSnapshot =
        serde_json::from_value(frozen["launch_snapshot"].clone()).map_err(|e| {
            RuntimeError::Bootstrap(format!("member launch snapshot absent/corrupt: {e}"))
        })?;
    let mut expected = snapshot.agent.clone();
    expected.provider_session_ref = agent.provider_session_ref.clone();
    expected.canonical_provider_session_ref = agent.canonical_provider_session_ref.clone();
    expected.updated_at = agent.updated_at;
    if expected != *agent || snapshot.hooks.as_ref() != hooks {
        return Err(RuntimeError::Conflict(
            "member settlement changed frozen agent/hook context".into(),
        ));
    }
    let effects = history(tx, &command, hooks)?;
    if serde_json::to_value(&effects).map_err(|e| RuntimeError::Bootstrap(e.to_string()))?
        != frozen["member_hook_history"]
    {
        return Err(RuntimeError::Conflict(
            "member pre-add native history changed after provider admission".into(),
        ));
    }
    let mut apply = match authorize_current_caller(tx, &command) {
        Ok(()) => true,
        Err(RuntimeError::Conflict(_)) => false,
        Err(error) => return Err(error),
    };
    for effect in &effects {
        apply &= effect_grant_is_current(tx, &agent.workspace_id, &effect.hook)?;
    }
    Ok(Some(PreAddApplication {
        actor: Some(command.operation.actor.identifier),
        apply,
    }))
}
