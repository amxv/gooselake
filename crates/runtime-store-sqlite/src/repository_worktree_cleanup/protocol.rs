//! Exact carriers shared by the two original-action receipt transactions.
use super::*;
use runtime_core::{
    BranchDeletionAck, BranchDeletionGrant, CheckoutRemovalAck, CheckoutRemovalGrant,
    ManagedCleanupBinding, ManagedCleanupTarget,
};
use serde::Serialize;
use serde_json::{json, Value};

pub(super) fn conflict(message: &str) -> RuntimeError {
    RuntimeError::Conflict(format!("native cleanup receipt: {message}"))
}

pub(super) fn value<T: Serialize>(input: &T) -> Result<Value, RuntimeError> {
    serde_json::to_value(input).map_err(|e| RuntimeError::Bootstrap(e.to_string()))
}

pub(super) fn target(worktree: &ManagedWorktreeRecord) -> ManagedCleanupTarget {
    ManagedCleanupTarget {
        worktree_id: worktree.id.clone(),
        worktree_created_at: worktree.created_at,
        repo_root: worktree.repo_root.clone(),
        checkout_path: worktree.worktree_cwd.clone(),
        branch_name: worktree.branch_name.clone(),
        deletion_policy: worktree.deletion_policy.clone(),
    }
}

pub(super) fn input(target: &ManagedCleanupTarget, reason: &Option<String>) -> Value {
    json!({"protocol_version":1,"worktree_id":target.worktree_id,
        "worktree_created_at":target.worktree_created_at,"repo_root":target.repo_root,
        "checkout_path":target.checkout_path,"branch_name":target.branch_name,
        "deletion_policy":target.deletion_policy,"reason":reason})
}

pub(super) fn operation(binding: &ManagedCleanupBinding) -> OperationRecord {
    OperationRecord {
        operation_id: binding.operation_id.clone(),
        workspace_id: None,
        kind: CLEANUP_KIND.into(),
        actor: OperationActor {
            kind: OperationActorKind::System,
            identifier: "managed_worktree_service".into(),
        },
        idempotency_key: Some(format!(
            "{}:{}",
            binding.target.worktree_id, binding.target.worktree_created_at
        )),
        normalized_request: input(&binding.target, &binding.reason),
        normalized_request_hash: binding.normalized_request_hash.clone(),
        phase: OperationPhase::Requested,
        exact_terminal_result: None,
        error_code: None,
        error_message: None,
        created_at: binding.requested_at,
        updated_at: binding.requested_at,
    }
}

/// Verify the original immutable carrier, never substitute current claims.
pub(super) fn bound_operation(
    tx: &Connection,
    binding: &ManagedCleanupBinding,
) -> Result<OperationRecord, RuntimeError> {
    if binding.version != 1
        || binding.operation_id.is_empty()
        || binding.normalized_request_hash
            != normalized_json_hash(&input(&binding.target, &binding.reason))?
    {
        return Err(conflict("invalid original binding"));
    }
    for (claim, kind, key) in [
        (
            &binding.checkout_claim,
            CLAIM_KIND,
            binding.target.checkout_path.clone(),
        ),
        (
            &binding.branch_claim,
            BRANCH_CLAIM_KIND,
            branch_resource_id(&binding.target.repo_root, &binding.target.branch_name),
        ),
    ] {
        if claim.resource_kind != kind
            || claim.resource_id != key
            || claim.claim_mode != "exclusive"
            || claim.owner_operation_id != binding.operation_id
            || claim.acquired_at != binding.requested_at
            || claim.fence_generation == 0
            || claim.fence_generation > i64::MAX as u64
        {
            return Err(conflict("invalid original identity reservation"));
        }
    }
    let current =
        operation_by_id(tx, &binding.operation_id)?.ok_or_else(|| conflict("missing operation"))?;
    let mut expected = operation(binding);
    expected.phase = current.phase;
    expected.exact_terminal_result = current.exact_terminal_result.clone();
    expected.error_code = current.error_code.clone();
    expected.error_message = current.error_message.clone();
    expected.updated_at = current.updated_at;
    if current != expected {
        return Err(conflict("operation binding changed"));
    }
    let actual: (String,i64,String,String,String,String,Option<String>,i64) = tx.query_row(
        "SELECT worktree_id,worktree_created_at,repo_root,checkout_path,branch_name,deletion_policy,reason,requested_at
         FROM managed_worktree_cleanup_receipts WHERE operation_id=?1",[&binding.operation_id],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)),
    ).map_err(|e| db_error("failed reading cleanup binding",e))?;
    let t = &binding.target;
    if actual
        != (
            t.worktree_id.clone(),
            t.worktree_created_at,
            t.repo_root.clone(),
            t.checkout_path.clone(),
            t.branch_name.clone(),
            t.deletion_policy.clone(),
            binding.reason.clone(),
            binding.requested_at,
        )
    {
        return Err(conflict("cleanup receipt binding changed"));
    }
    Ok(current)
}

pub(super) fn verify_claims(
    tx: &Transaction<'_>,
    binding: &ManagedCleanupBinding,
) -> Result<(), RuntimeError> {
    for expected in [&binding.checkout_claim, &binding.branch_claim] {
        if existing_claim(
            tx,
            &binding.operation_id,
            &expected.resource_kind,
            &expected.resource_id,
        )? != *expected
        {
            return Err(conflict("original reservation was superseded"));
        }
        let generation:i64=tx.query_row("SELECT last_generation FROM runtime_operation_resource_fences WHERE resource_kind=?1 AND resource_id=?2",
            params![expected.resource_kind,expected.resource_id],|r|r.get(0)).map_err(|e|db_error("failed verifying cleanup fence generation",e))?;
        if u64::try_from(generation).ok() != Some(expected.fence_generation) {
            return Err(conflict("original reservation counter changed"));
        }
    }
    let count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1",
            [&binding.operation_id],
            |r| r.get(0),
        )
        .map_err(|e| db_error("failed checking cleanup reservations", e))?;
    if count != 2 {
        return Err(conflict("unexpected cleanup reservation set"));
    }
    Ok(())
}

pub(super) fn receipt_state(
    tx: &Connection,
    id: &str,
) -> Result<(String, Option<String>, Option<i64>), RuntimeError> {
    tx.query_row("SELECT phase,response_json,completed_at FROM managed_worktree_cleanup_receipts WHERE operation_id=?1",
        [id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|db_error("failed reading cleanup phase",e))
}

pub(super) fn pending(
    tx: &Transaction<'_>,
    binding: &ManagedCleanupBinding,
) -> Result<(), RuntimeError> {
    if bound_operation(tx, binding)? != operation(binding)
        || receipt_state(tx, &binding.operation_id)? != ("pending".into(), None, None)
    {
        return Err(conflict(
            "operation is no longer an original pending attempt",
        ));
    }
    verify_admission(tx, binding)?;
    verify_claims(tx, binding)
}

fn effect(
    binding: &ManagedCleanupBinding,
    checkout: bool,
    phase: &str,
    evidence: Value,
    attempts: u64,
    at: i64,
) -> OperationEffectRecord {
    let action = if checkout {
        "checkout-remove"
    } else {
        "branch-delete"
    };
    OperationEffectRecord {
        effect_id: format!("{}:{action}", binding.operation_id),
        operation_id: binding.operation_id.clone(),
        effect_kind: if checkout {
            "git_worktree_remove"
        } else {
            "git_branch_delete"
        }
        .into(),
        target_kind: if checkout {
            "git_checkout"
        } else {
            "git_branch"
        }
        .into(),
        target_id: if checkout {
            binding.target.checkout_path.clone()
        } else {
            branch_resource_id(&binding.target.repo_root, &binding.target.branch_name)
        },
        phase: phase.into(),
        idempotency_key: action.into(),
        evidence: Some(evidence),
        attempt_count: attempts,
        created_at: binding.requested_at,
        updated_at: at,
    }
}

pub(super) fn checkout_started(grant: &CheckoutRemovalGrant) -> OperationEffectRecord {
    effect(
        &grant.binding,
        true,
        "started",
        json!({"grant":grant}),
        1,
        grant.binding.requested_at,
    )
}
pub(super) fn branch_intended(binding: &ManagedCleanupBinding) -> OperationEffectRecord {
    effect(
        binding,
        false,
        "intended",
        json!({"binding":binding}),
        0,
        binding.requested_at,
    )
}
pub(super) fn checkout_finalized(ack: &CheckoutRemovalAck) -> OperationEffectRecord {
    effect(
        &ack.grant.binding,
        true,
        "finalized",
        json!({"grant":ack.grant,"ack":ack}),
        1,
        ack.observed_at,
    )
}
pub(super) fn branch_started(grant: &BranchDeletionGrant) -> OperationEffectRecord {
    effect(
        &grant.binding,
        false,
        "started",
        json!({"grant":grant}),
        1,
        grant.granted_at,
    )
}
pub(super) fn branch_finalized(ack: &BranchDeletionAck) -> OperationEffectRecord {
    effect(
        &ack.grant.binding,
        false,
        "finalized",
        json!({"grant":ack.grant,"ack":ack}),
        1,
        ack.observed_at,
    )
}

pub(super) fn effects(
    tx: &Connection,
    id: &str,
) -> Result<Vec<OperationEffectRecord>, RuntimeError> {
    crate::repository_workspace::effects_for_operation(tx, id)
}
pub(super) fn verify_effects(
    tx: &Connection,
    id: &str,
    expected: &[OperationEffectRecord],
) -> Result<(), RuntimeError> {
    let mut actual = effects(tx, id)?;
    let mut expected = expected.to_vec();
    actual.sort_by(|a, b| a.effect_id.cmp(&b.effect_id));
    expected.sort_by(|a, b| a.effect_id.cmp(&b.effect_id));
    if actual != expected {
        return Err(conflict("action carriers changed or are incomplete"));
    }
    Ok(())
}
pub(super) fn update_effect(
    tx: &Transaction<'_>,
    old: &OperationEffectRecord,
    new: &OperationEffectRecord,
) -> Result<(), RuntimeError> {
    let changed=tx.execute("UPDATE runtime_operation_effects SET phase=?2,evidence_json=?3,attempt_count=?4,updated_at=?5
        WHERE effect_id=?1 AND operation_id=?6 AND phase=?7 AND attempt_count=?8 AND evidence_json=?9",
        params![old.effect_id,new.phase,new.evidence.as_ref().map(json_to_string).transpose()?,new.attempt_count,new.updated_at,
            old.operation_id,old.phase,old.attempt_count,old.evidence.as_ref().map(json_to_string).transpose()?])
        .map_err(|e|db_error("failed recording original cleanup action",e))?;
    if changed != 1 {
        return Err(conflict("action receipt write was ignored or superseded"));
    }
    Ok(())
}

pub(super) fn checkout_ack(
    tx: &Connection,
    binding: &ManagedCleanupBinding,
) -> Result<CheckoutRemovalAck, RuntimeError> {
    let rows = effects(tx, &binding.operation_id)?;
    let row = rows
        .iter()
        .find(|e| e.effect_id == format!("{}:checkout-remove", binding.operation_id))
        .ok_or_else(|| conflict("missing checkout action"))?;
    let ack: CheckoutRemovalAck = serde_json::from_value(
        row.evidence
            .as_ref()
            .and_then(|v| v.get("ack"))
            .cloned()
            .ok_or_else(|| conflict("missing original checkout acknowledgement"))?,
    )
    .map_err(|_| conflict("invalid checkout acknowledgement"))?;
    if ack.exit_code != 0 || ack.grant.binding != *binding || *row != checkout_finalized(&ack) {
        return Err(conflict("checkout acknowledgement changed"));
    }
    Ok(ack)
}

pub(super) fn response(binding: &ManagedCleanupBinding) -> WorktreeCleanupResponse {
    WorktreeCleanupResponse {
        worktree_id: binding.target.worktree_id.clone(),
        status: "deleted".into(),
        deletion_policy: binding.target.deletion_policy.clone(),
        active_claim_count: 0,
        worktree_path_deleted: true,
        branch_deleted: true,
        diagnostics: Vec::new(),
    }
}

/// New-protocol replay verifies its aggregate against both original actions.
pub(super) fn verify_completed(
    tx: &Connection,
    ack: &BranchDeletionAck,
) -> Result<WorktreeCleanupResponse, RuntimeError> {
    let binding = &ack.grant.binding;
    let checkout = checkout_ack(tx, binding)?;
    if ack.exit_code != 0
        || ack.grant.checkout_receipt_hash != normalized_json_hash(&value(&checkout)?)?
        || ack.grant.granted_at != checkout.observed_at
    {
        return Err(conflict(
            "branch acknowledgement does not bind checkout receipt",
        ));
    }
    verify_effects(
        tx,
        &binding.operation_id,
        &[checkout_finalized(&checkout), branch_finalized(ack)],
    )?;
    let result = response(binding);
    let exact = value(&result)?;
    let mut expected = operation(binding);
    expected.phase = OperationPhase::Completed;
    expected.updated_at = ack.observed_at;
    expected.exact_terminal_result = Some(exact.clone());
    if bound_operation(tx, binding)? != expected
        || receipt_state(tx, &binding.operation_id)?
            != (
                "completed".into(),
                Some(json_to_string(&exact)?),
                Some(ack.observed_at),
            )
    {
        return Err(conflict("completed cleanup carriers disagree"));
    }
    let count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1",
            [&binding.operation_id],
            |r| r.get(0),
        )
        .map_err(|e| db_error("failed checking completed cleanup reservations", e))?;
    if count != 0 {
        return Err(conflict("completed cleanup retained reservations"));
    }
    verify_admission(tx, binding)?;
    verify_branch_grant(tx, &ack.grant)?;
    verify_transition(tx, &completed_transition(ack))?;
    verify_event(
        tx,
        &worktree_event_record(
            binding,
            "settled",
            "worktree.cleaned_up",
            json!({"operation_id":binding.operation_id,"response":exact,"original_actions":true}),
            ack.observed_at,
        ),
    )?;
    Ok(result)
}

pub(super) fn worktree_event_record(
    binding: &ManagedCleanupBinding,
    suffix: &str,
    kind: &str,
    payload: Value,
    at: i64,
) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: format!("{}:event:worktree:{suffix}", binding.operation_id),
        scope: RuntimeEventScope::Worktree,
        scope_id: binding.target.worktree_id.clone(),
        session_id: None,
        team_id: None,
        turn_id: None,
        kind: kind.into(),
        criticality: RuntimeEventCriticality::Critical,
        payload,
        provider: None,
        provider_seq: None,
        created_at: at,
    }
}

pub(super) fn verify_event(
    tx: &Connection,
    expected: &NewRuntimeEvent,
) -> Result<(), RuntimeError> {
    let actual = crate::db::fetch_runtime_event_by_event_id(tx, &expected.event_id)?
        .ok_or_else(|| conflict("required Critical event is missing"))?;
    let actual = NewRuntimeEvent {
        event_id: actual.event_id,
        scope: actual.scope,
        scope_id: actual.scope_id,
        session_id: actual.session_id,
        team_id: actual.team_id,
        turn_id: actual.turn_id,
        kind: actual.kind,
        criticality: actual.criticality,
        payload: actual.payload,
        provider: actual.provider,
        provider_seq: actual.provider_seq,
        created_at: actual.created_at,
    };
    if actual != *expected {
        return Err(conflict("Critical event carrier changed"));
    }
    Ok(())
}

pub(super) fn admission_transition(binding: &ManagedCleanupBinding) -> OperationTransitionRecord {
    OperationTransitionRecord {
        operation_id: binding.operation_id.clone(),
        sequence: 1,
        from_phase: None,
        to_phase: OperationPhase::Requested,
        evidence: Some(json!({"protocol_version":1,
            "checkout_grant":CheckoutRemovalGrant{binding:binding.clone()}})),
        created_at: binding.requested_at,
    }
}
pub(super) fn branch_transition(grant: &BranchDeletionGrant) -> OperationTransitionRecord {
    OperationTransitionRecord {
        operation_id: grant.binding.operation_id.clone(),
        sequence: 2,
        from_phase: Some(OperationPhase::Requested),
        to_phase: OperationPhase::Requested,
        evidence: Some(json!({"branch_grant":grant})),
        created_at: grant.granted_at,
    }
}
pub(super) fn completed_transition(ack: &BranchDeletionAck) -> OperationTransitionRecord {
    OperationTransitionRecord {
        operation_id: ack.grant.binding.operation_id.clone(),
        sequence: 3,
        from_phase: Some(OperationPhase::Requested),
        to_phase: OperationPhase::Completed,
        evidence: Some(json!({"branch_ack":ack})),
        created_at: ack.observed_at,
    }
}
pub(super) fn verify_transition(
    tx: &Connection,
    expected: &OperationTransitionRecord,
) -> Result<(), RuntimeError> {
    let actual:(Option<String>,String,Option<String>,i64)=tx.query_row(
        "SELECT from_phase,to_phase,evidence_json,created_at FROM runtime_operation_transitions WHERE operation_id=?1 AND sequence=?2",
        params![expected.operation_id,expected.sequence],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))
        .map_err(|e|db_error("failed checking cleanup transition",e))?;
    if actual
        != (
            expected.from_phase.map(|p| p.as_str().to_owned()),
            expected.to_phase.as_str().into(),
            expected.evidence.as_ref().map(json_to_string).transpose()?,
            expected.created_at,
        )
    {
        return Err(conflict("transition carrier changed"));
    }
    verify_event(
        tx,
        &NewRuntimeEvent {
            event_id: format!(
                "{}:event:transition:{}",
                expected.operation_id, expected.sequence
            ),
            scope: RuntimeEventScope::Operation,
            scope_id: expected.operation_id.clone(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "operation.transitioned".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({"operation_id":expected.operation_id,"workspace_id":null,"sequence":expected.sequence,
            "from_phase":expected.from_phase.map(OperationPhase::as_str),"to_phase":expected.to_phase.as_str()}),
            provider: None,
            provider_seq: None,
            created_at: expected.created_at,
        },
    )
}
pub(super) fn verify_admission(
    tx: &Connection,
    binding: &ManagedCleanupBinding,
) -> Result<(), RuntimeError> {
    verify_transition(tx, &admission_transition(binding))?;
    verify_event(
        tx,
        &NewRuntimeEvent {
            event_id: format!("{}:event:admitted", binding.operation_id),
            scope: RuntimeEventScope::Operation,
            scope_id: binding.operation_id.clone(),
            session_id: None,
            team_id: None,
            turn_id: None,
            kind: "operation.admitted".into(),
            criticality: RuntimeEventCriticality::Critical,
            payload: json!({"operation_id":binding.operation_id,"workspace_id":null,"kind":CLEANUP_KIND,"phase":"requested"}),
            provider: None,
            provider_seq: None,
            created_at: binding.requested_at,
        },
    )?;
    verify_event(
        tx,
        &worktree_event_record(
            binding,
            "admitted",
            "worktree.cleanup_admitted",
            json!({"operation_id":binding.operation_id,"checkout_path":binding.target.checkout_path,"branch_name":binding.target.branch_name}),
            binding.requested_at,
        ),
    )
}

pub(super) fn verify_replay(
    tx: &Connection,
    id: &str,
) -> Result<WorktreeCleanupResponse, RuntimeError> {
    let op = operation_by_id(tx, id)?.ok_or_else(|| conflict("completed operation is missing"))?;
    if op.normalized_request.get("protocol_version").is_none() {
        return super::legacy::verify_completed(tx, &op);
    }
    let rows = effects(tx, id)?;
    let ack = rows
        .iter()
        .find(|e| e.effect_id == format!("{id}:branch-delete"))
        .and_then(|e| e.evidence.as_ref())
        .and_then(|e| e.get("ack"))
        .cloned()
        .ok_or_else(|| conflict("completed branch acknowledgement is missing"))?;
    let ack: BranchDeletionAck = serde_json::from_value(ack)
        .map_err(|_| conflict("completed branch acknowledgement is invalid"))?;
    if ack.grant.binding.operation_id != id {
        return Err(conflict(
            "completed acknowledgement belongs to another operation",
        ));
    }
    verify_completed(tx, &ack)
}

pub(super) fn verify_branch_grant(
    tx: &Connection,
    grant: &BranchDeletionGrant,
) -> Result<(), RuntimeError> {
    let ack = checkout_ack(tx, &grant.binding)?;
    if grant.checkout_receipt_hash != normalized_json_hash(&value(&ack)?)?
        || grant.granted_at != ack.observed_at
    {
        return Err(conflict(
            "branch grant is not bound to the original checkout receipt",
        ));
    }
    verify_transition(tx, &branch_transition(grant))?;
    verify_event(
        tx,
        &worktree_event_record(
            &grant.binding,
            "checkout-acknowledged",
            "worktree.checkout_removed",
            json!({"operation_id":grant.binding.operation_id,"checkout_ack":ack,"branch_grant":grant}),
            grant.granted_at,
        ),
    )
}
