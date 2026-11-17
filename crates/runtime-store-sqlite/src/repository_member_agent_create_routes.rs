use runtime_core::{
    MemberAgentCreateCommand, MemberAgentCreateRoute, OperationActorKind, OperationPhase,
    OperationRecord, OperationResourceClaimRecord, RuntimeError, WorktreeCreateResponse,
};
use rusqlite::{params, OptionalExtension, Transaction};

use crate::db::db_error;
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_member_agent_create::{authorize_stage, command_for_child, validate_link};
use crate::repository_member_agent_rebind::worktree_snapshot;
use crate::repository_operator_agent_create::verify_requested_create;
use crate::repository_workspace::operation_by_id;
use crate::repository_workspace_worktree_creation::{
    branch_resource_id, BRANCH_CLAIM_KIND, CLAIM_KIND,
};

fn resources(command: &MemberAgentCreateCommand) -> Vec<(&'static str, String)> {
    command.intent.route.worktree().map_or_else(Vec::new, |w| {
        vec![
            (CLAIM_KIND, w.worktree_cwd.clone()),
            (
                BRANCH_CLAIM_KIND,
                branch_resource_id(&w.repo_root, &w.branch_name),
            ),
        ]
    })
}

/// Recheck the actual path at a new shell/provider stage, never during
/// historical replay or settlement of an already-admitted native effect.
pub(crate) fn validate_native_ready_route(
    command: &MemberAgentCreateCommand,
) -> Result<(), RuntimeError> {
    let cwd = std::path::Path::new(command.intent.route.cwd());
    let canonical =
        std::fs::canonicalize(cwd).map_err(|error| RuntimeError::Io(error.to_string()))?;
    if !canonical.is_dir() || canonical.to_string_lossy() != command.intent.route.canonical_cwd() {
        return Err(RuntimeError::Conflict(
            "member native destination changed before the next stage".into(),
        ));
    }
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
            if runtime_core::resolve_repository_identity(cwd)? != *repository {
                return Err(RuntimeError::Conflict(
                    "member native destination changed repositories".into(),
                ));
            }
            let branch = std::process::Command::new("git")
                .arg("-C")
                .arg(cwd)
                .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
                .output()
                .map_err(|error| RuntimeError::Io(error.to_string()))?;
            if !branch.status.success()
                || String::from_utf8_lossy(&branch.stdout).trim() != worktree.branch_name
            {
                return Err(RuntimeError::Conflict(
                    "member native destination changed branches".into(),
                ));
            }
        }
        MemberAgentCreateRoute::Root { .. } => {}
    }
    Ok(())
}

pub(crate) fn validate_route(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    ready: bool,
) -> Result<(), RuntimeError> {
    match &command.intent.route {
        MemberAgentCreateRoute::Root { canonical_root } => {
            if canonical_root != &command.intent.canonical_root {
                return Err(RuntimeError::Conflict(
                    "member root authority changed".into(),
                ));
            }
        }
        MemberAgentCreateRoute::Existing {
            worktree, revision, ..
        } => {
            let actual = worktree_snapshot(tx, &worktree.id)?;
            if actual != (worktree.clone(), *revision) {
                return Err(RuntimeError::Conflict(
                    "member destination record/revision changed".into(),
                ));
            }
        }
        MemberAgentCreateRoute::New {
            repository,
            worktree,
            configured_root,
            base_commit,
        } => {
            let expected_root =
                std::path::Path::new(configured_root).join(&worktree.unified_workspace_path);
            let expected_path = expected_root.join(worktree.branch_name.replace('/', "--"));
            if worktree.repo_root != repository.canonical_root
                || worktree.worktree_root != expected_root.to_string_lossy()
                || worktree.worktree_cwd != expected_path.to_string_lossy()
                || worktree.branch_name != format!("gg/{}", worktree.worktree_name)
                || worktree.created_by_session_id.as_deref()
                    != Some(command.operation.actor.identifier.as_str())
                || worktree.created_by_operation_id != command.operation.worktree_operation_id
                || !matches!(base_commit.len(), 40 | 64)
                || !base_commit.bytes().all(|c| c.is_ascii_hexdigit())
            {
                return Err(RuntimeError::Conflict(
                    "member Git route is not its frozen plan".into(),
                ));
            }
            if ready {
                let git_id = command
                    .operation
                    .worktree_operation_id
                    .as_deref()
                    .ok_or_else(|| RuntimeError::Conflict("member Git identity missing".into()))?;
                let git = operation_by_id(tx, git_id)?
                    .ok_or_else(|| RuntimeError::Conflict("member Git receipt missing".into()))?;
                let expected = command.worktree_command()?;
                if git.phase != OperationPhase::Completed
                    || git.actor != expected.actor
                    || git.normalized_request != expected.normalized_request
                    || git.normalized_request_hash != expected.normalized_request_hash
                {
                    return Err(RuntimeError::Conflict(
                        "member Git operation has no exact completed receipt".into(),
                    ));
                }
                let response: WorktreeCreateResponse =
                    serde_json::from_value(git.exact_terminal_result.ok_or_else(|| {
                        RuntimeError::Conflict("member Git receipt absent".into())
                    })?)
                    .map_err(|e| {
                        RuntimeError::Bootstrap(format!("invalid member Git receipt: {e}"))
                    })?;
                if response.worktree != *worktree
                    || !response.created
                    || response.init_script_status != "skipped_disabled"
                    || worktree_snapshot(tx, &worktree.id)? != (worktree.clone(), 1)
                {
                    return Err(RuntimeError::Conflict(
                        "member Git receipt/record changed before provider launch".into(),
                    ));
                }
            } else {
                ensure_absent_record(tx, command)?;
            }
        }
    }
    if ready {
        require_pins(tx, command, &command.operation.operation_id)?;
    }
    Ok(())
}

fn ensure_absent_record(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
) -> Result<(), RuntimeError> {
    let w = command
        .intent
        .route
        .worktree()
        .ok_or_else(|| RuntimeError::Conflict("missing member Git route".into()))?;
    let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_worktrees WHERE id=?1 OR (repo_root=?2 AND (worktree_cwd=?3 OR branch_name=?4)))",
        params![w.id,w.repo_root,w.worktree_cwd,w.branch_name],|r|r.get(0)).map_err(|e|db_error("failed checking prospective member checkout identity",e))?;
    if exists {
        return Err(RuntimeError::Conflict(
            "member new checkout cannot adopt an existing managed record".into(),
        ));
    }
    Ok(())
}

pub(crate) fn reserve_initial_route(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
) -> Result<(), RuntimeError> {
    validate_route(tx, command, false)?;
    if matches!(
        command.intent.route,
        MemberAgentCreateRoute::Existing { .. }
    ) {
        for (kind, id) in resources(command) {
            OperationAuthorityTransaction::new(tx).acquire_claim(
                &command.operation.operation_id,
                kind,
                &id,
                command.operation.requested_at,
            )?;
        }
    }
    Ok(())
}

fn claim(
    tx: &Transaction<'_>,
    kind: &str,
    id: &str,
    owner: &str,
) -> Result<Option<OperationResourceClaimRecord>, RuntimeError> {
    let row:Option<(i64,i64)>=tx.query_row("SELECT fence_generation,acquired_at FROM runtime_operation_resource_claims WHERE resource_kind=?1 AND resource_id=?2 AND owner_operation_id=?3 AND claim_mode='exclusive'",
        params![kind,id,owner],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(|e|db_error("failed reading exact member route reservation",e))?;
    row.map(|(generation, at)| {
        Ok(OperationResourceClaimRecord {
            resource_kind: kind.into(),
            resource_id: id.into(),
            claim_mode: "exclusive".into(),
            owner_operation_id: owner.into(),
            fence_generation: u64::try_from(generation)
                .map_err(|_| RuntimeError::Bootstrap("negative member route generation".into()))?,
            acquired_at: at,
        })
    })
    .transpose()
}

fn require_pins(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    owner: &str,
) -> Result<(), RuntimeError> {
    for (kind, id) in resources(command) {
        if claim(tx, kind, &id, owner)?.is_none() {
            return Err(RuntimeError::Conflict(
                "member creation lost its exact route reservation".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn release_create_pins(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    allow_untransferred_git: bool,
) -> Result<(), RuntimeError> {
    if allow_untransferred_git
        && crate::repository_member_agent_create_hooks::unresolved_execution(tx, command)?
    {
        // An independent NotCreated provider review cannot certify that an
        // original shell is quiescent. Keep exact checkout/branch ownership
        // until that separate native outcome can be independently resolved.
        return require_pins(tx, command, &command.operation.operation_id);
    }
    for (kind, id) in resources(command) {
        match claim(tx, kind, &id, &command.operation.operation_id)? {
            Some(owned) => OperationAuthorityTransaction::new(tx).release_claim(&owned)?,
            None if allow_untransferred_git
                && matches!(command.intent.route, MemberAgentCreateRoute::New { .. }) => {}
            None => {
                return Err(RuntimeError::Conflict(
                    "member create route reservation disappeared before settlement".into(),
                ))
            }
        }
    }
    Ok(())
}

pub(crate) fn before_git_admission(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    git: &runtime_core::WorkspaceWorktreeCreationCommand,
) -> Result<(), RuntimeError> {
    validate_link(tx, command)?;
    verify_requested_create(tx, &command.operation)?;
    authorize_stage(tx, command)?;
    if command.worktree_command()? != *git {
        return Err(RuntimeError::Conflict(
            "member Git admission differs from frozen command".into(),
        ));
    }
    validate_route(tx, command, false)?;
    // One generation for Git and one for the atomic transfer to provider
    // creation. Check both before authorizing the first native Git call.
    for (kind, id) in resources(command) {
        let previous:i64=tx.query_row("SELECT last_generation FROM runtime_operation_resource_fences WHERE resource_kind=?1 AND resource_id=?2",
            params![kind,id],|r|r.get(0)).optional().map_err(|e|db_error("failed checking member Git transfer headroom",e))?.unwrap_or(0);
        if previous < 0 || previous.checked_add(2).is_none() {
            return Err(RuntimeError::Conflict(
                "member Git route cannot reserve both required fence generations".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn prepare_git_receipt(
    tx: &Transaction<'_>,
    git: &OperationRecord,
    response: &WorktreeCreateResponse,
) -> Result<Option<MemberAgentCreateCommand>, RuntimeError> {
    if git.actor.kind != OperationActorKind::Agent {
        return Ok(None);
    }
    let child_id = git.normalized_request["member_create_operation_id"]
        .as_str()
        .ok_or_else(|| RuntimeError::Conflict("Agent Git operation lacks create parent".into()))?;
    let child = operation_by_id(tx, child_id)?
        .ok_or_else(|| RuntimeError::Conflict("member create child absent".into()))?;
    let command = command_for_child(tx, &child)?.ok_or_else(|| {
        RuntimeError::Conflict("Agent Git operation belongs to nonmember create".into())
    })?;
    verify_requested_create(tx, &command.operation)?;
    let expected = command.worktree_command()?;
    if expected.operation_id != git.operation_id
        || expected.actor != git.actor
        || expected.normalized_request != git.normalized_request
        || expected.normalized_request_hash != git.normalized_request_hash
        || expected.idempotency_key != git.idempotency_key.clone().unwrap_or_default()
        || command.intent.route.worktree() != Some(&response.worktree)
        || !response.created
        || response.init_script_status != "skipped_disabled"
    {
        return Err(RuntimeError::Conflict(
            "member Git receipt differs from original native authority".into(),
        ));
    }
    require_pins(tx, &command, &git.operation_id)?;
    ensure_absent_record(tx, &command)?;
    crate::repository_worktree_record_events::record_managed_worktree_event_tx(
        tx,
        &response.worktree,
        &response.init_script_status,
        false,
        Some(&command.operation.actor.identifier),
        None,
    )?;
    Ok(Some(command))
}

pub(crate) fn transfer_git_pins(
    tx: &Transaction<'_>,
    command: &MemberAgentCreateCommand,
    at: i64,
) -> Result<(), RuntimeError> {
    // Called after exact Git pins were released, inside that same receipt
    // transaction. There is no independently visible unreserved checkout.
    for (kind, id) in resources(command) {
        OperationAuthorityTransaction::new(tx).acquire_claim(
            &command.operation.operation_id,
            kind,
            &id,
            at,
        )?;
    }
    Ok(())
}
