//! Recognized historical compound receipts are replay-only. Missing version
//! metadata alone never converts a typed operation into historical authority.
use super::protocol::{conflict, value, verify_effects, verify_event, verify_transition};
use super::*;
use serde_json::{json, Value};

const EFFECT_KIND: &str = "git_worktree_cleanup";

struct ArchivedReceipt {
    worktree_id: String,
    worktree_created_at: i64,
    repo_root: String,
    checkout_path: String,
    branch_name: String,
    deletion_policy: String,
    reason: Option<String>,
    phase: String,
    response_json: Option<String>,
    requested_at: i64,
    completed_at: Option<i64>,
}

fn event(
    id: &str,
    suffix: &str,
    scope: RuntimeEventScope,
    scope_id: &str,
    kind: &str,
    payload: Value,
    at: i64,
) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: format!("{id}:event:{suffix}"),
        scope,
        scope_id: scope_id.into(),
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

fn successful(response: &WorktreeCleanupResponse) -> bool {
    response.status == "deleted"
        && response.worktree_path_deleted
        && response.branch_deleted
        && response.diagnostics.is_empty()
}

pub(super) fn verify_completed(
    tx: &Connection,
    operation: &OperationRecord,
) -> Result<WorktreeCleanupResponse, RuntimeError> {
    let id = &operation.operation_id;
    let receipt = tx
        .query_row(
            "SELECT worktree_id,worktree_created_at,repo_root,checkout_path,branch_name,
         deletion_policy,reason,phase,response_json,requested_at,completed_at
         FROM managed_worktree_cleanup_receipts WHERE operation_id=?1",
            [id],
            |r| {
                Ok(ArchivedReceipt {
                    worktree_id: r.get(0)?,
                    worktree_created_at: r.get(1)?,
                    repo_root: r.get(2)?,
                    checkout_path: r.get(3)?,
                    branch_name: r.get(4)?,
                    deletion_policy: r.get(5)?,
                    reason: r.get(6)?,
                    phase: r.get(7)?,
                    response_json: r.get(8)?,
                    requested_at: r.get(9)?,
                    completed_at: r.get(10)?,
                })
            },
        )
        .map_err(|e| db_error("failed reading historical cleanup receipt", e))?;
    let at = receipt
        .completed_at
        .ok_or_else(|| conflict("historical completion time is missing"))?;
    let exact: Value = serde_json::from_str(
        receipt
            .response_json
            .as_deref()
            .ok_or_else(|| conflict("historical cleanup result is missing"))?,
    )
    .map_err(|_| conflict("historical cleanup result is invalid"))?;
    let response: WorktreeCleanupResponse = serde_json::from_value(exact.clone())
        .map_err(|_| conflict("historical cleanup result is invalid"))?;
    if receipt.phase != "completed"
        || !successful(&response)
        || response.worktree_id != receipt.worktree_id
        || response.deletion_policy != receipt.deletion_policy
        || value(&response)? != exact
    {
        return Err(conflict(
            "historical cleanup result disagrees with its target",
        ));
    }
    let input = json!({"worktree_id":receipt.worktree_id,
        "worktree_created_at":receipt.worktree_created_at,"repo_root":receipt.repo_root,
        "checkout_path":receipt.checkout_path,"branch_name":receipt.branch_name,
        "deletion_policy":receipt.deletion_policy,"reason":receipt.reason});
    let expected = OperationRecord {
        operation_id: id.clone(),
        workspace_id: None,
        kind: CLEANUP_KIND.into(),
        actor: OperationActor {
            kind: OperationActorKind::System,
            identifier: "managed_worktree_service".into(),
        },
        idempotency_key: Some(format!(
            "{}:{}",
            receipt.worktree_id, receipt.worktree_created_at
        )),
        normalized_request_hash: normalized_json_hash(&input)?,
        normalized_request: input,
        phase: OperationPhase::Completed,
        exact_terminal_result: Some(exact.clone()),
        error_code: None,
        error_message: None,
        created_at: receipt.requested_at,
        updated_at: at,
    };
    if *operation != expected {
        return Err(conflict("historical cleanup canonical carriers disagree"));
    }
    // The retired protocol has exactly one compound effect. Typed markers,
    // missing actions, or a stripped version field cannot match this shape.
    verify_effects(
        tx,
        id,
        &[OperationEffectRecord {
            effect_id: format!("fx_{id}"),
            operation_id: id.clone(),
            effect_kind: EFFECT_KIND.into(),
            target_kind: "git_checkout".into(),
            target_id: receipt.checkout_path.clone(),
            phase: "finalized".into(),
            idempotency_key: "native_git_remove_and_branch_delete".into(),
            evidence: Some(json!({"receipt":exact,"native_evidence":"observed"})),
            attempt_count: 1,
            created_at: receipt.requested_at,
            updated_at: at,
        }],
    )?;
    verify_transition(
        tx,
        &OperationTransitionRecord {
            operation_id: id.clone(),
            sequence: 1,
            from_phase: None,
            to_phase: OperationPhase::Requested,
            evidence: Some(json!({"native_effect":EFFECT_KIND})),
            created_at: receipt.requested_at,
        },
    )?;
    verify_event(
        tx,
        &event(
            id,
            "admitted",
            RuntimeEventScope::Operation,
            id,
            "operation.admitted",
            json!({"operation_id":id,"workspace_id":null,"kind":CLEANUP_KIND,"phase":"requested"}),
            receipt.requested_at,
        ),
    )?;
    verify_event(
        tx,
        &event(
            id,
            "worktree:admitted",
            RuntimeEventScope::Worktree,
            &receipt.worktree_id,
            "worktree.cleanup_admitted",
            json!({"operation_id":id,"checkout_path":receipt.checkout_path,"branch_name":receipt.branch_name}),
            receipt.requested_at,
        ),
    )?;
    let (count,last):(u64,u64)=tx.query_row(
        "SELECT COUNT(*),MAX(sequence) FROM runtime_operation_transitions WHERE operation_id=?1",
        [id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|e|db_error("failed checking historical cleanup transitions",e))?;
    let was_review = match (count, last) {
        (2, 2) => false,
        (3, 3) => true,
        _ => return Err(conflict("historical cleanup transition set is invalid")),
    };
    if was_review {
        // Preserve already-completed historical review receipts as archives;
        // this does not grant a current held operation settlement authority.
        let prior = crate::db::fetch_runtime_event_by_event_id(
            tx,
            &format!("{id}:event:worktree:settled"),
        )?
        .ok_or_else(|| conflict("historical review event is missing"))?;
        let prior_exact = prior
            .payload
            .get("response")
            .cloned()
            .ok_or_else(|| conflict("historical review response is missing"))?;
        let prior_response: WorktreeCleanupResponse =
            serde_json::from_value(prior_exact.clone())
                .map_err(|_| conflict("historical review response is invalid"))?;
        if successful(&prior_response)
            || prior_response.worktree_id != receipt.worktree_id
            || value(&prior_response)? != prior_exact
        {
            return Err(conflict("historical review response is inconsistent"));
        }
        verify_transition(
            tx,
            &OperationTransitionRecord {
                operation_id: id.clone(),
                sequence: 2,
                from_phase: Some(OperationPhase::Requested),
                to_phase: OperationPhase::ManualReview,
                evidence: Some(json!({"native_cleanup":"manual_review"})),
                created_at: prior.created_at,
            },
        )?;
        verify_event(
            tx,
            &event(
                id,
                "worktree:settled",
                RuntimeEventScope::Worktree,
                &receipt.worktree_id,
                "worktree.cleanup_failed",
                json!({"operation_id":id,"response":prior_exact,"after_manual_review":false}),
                prior.created_at,
            ),
        )?;
    }
    verify_transition(
        tx,
        &OperationTransitionRecord {
            operation_id: id.clone(),
            sequence: last,
            from_phase: Some(if was_review {
                OperationPhase::ManualReview
            } else {
                OperationPhase::Requested
            }),
            to_phase: OperationPhase::Completed,
            evidence: Some(json!({"native_cleanup":"completed"})),
            created_at: at,
        },
    )?;
    verify_event(
        tx,
        &event(
            id,
            if was_review {
                "worktree:reconciled"
            } else {
                "worktree:settled"
            },
            RuntimeEventScope::Worktree,
            &receipt.worktree_id,
            "worktree.cleaned_up",
            json!({"operation_id":id,"response":exact,"after_manual_review":was_review}),
            at,
        ),
    )?;
    let owned: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_resource_claims WHERE owner_operation_id=?1",
            [id],
            |r| r.get(0),
        )
        .map_err(|e| db_error("failed checking historical cleanup reservations", e))?;
    if owned != 0 {
        return Err(conflict(
            "historical completed cleanup retained reservations",
        ));
    }
    Ok(response)
}
