//! Only acknowledgements of the original invocations can advance cleanup.
use super::protocol::*;
use super::*;
use runtime_core::{
    BranchDeletionAck, BranchDeletionGrant, CheckoutRemovalAck, CheckoutRemovalGrant,
    ManagedCleanupAttempt, ManagedCleanupBinding, ManagedCleanupBranchAdmission,
    ManagedCleanupObservation,
};
use serde_json::json;

fn branch_grant(ack: &CheckoutRemovalAck) -> Result<BranchDeletionGrant, RuntimeError> {
    Ok(BranchDeletionGrant {
        binding: ack.grant.binding.clone(),
        checkout_receipt_hash: normalized_json_hash(&value(ack)?)?,
        granted_at: ack.observed_at,
    })
}

fn hold_transition(
    binding: &ManagedCleanupBinding,
    sequence: u64,
    attempt: &ManagedCleanupAttempt,
    observation: &ManagedCleanupObservation,
    diagnostic: &str,
    at: i64,
    actions: &[OperationEffectRecord],
) -> OperationTransitionRecord {
    OperationTransitionRecord {
        operation_id: binding.operation_id.clone(),
        sequence,
        from_phase: Some(OperationPhase::Requested),
        to_phase: OperationPhase::ManualReview,
        evidence: Some(
            json!({"hold":{"attempt":attempt,"observation":observation,"diagnostic":diagnostic},"actions":actions}),
        ),
        created_at: at,
    }
}

fn verify_hold(
    tx: &Transaction<'_>,
    binding: &ManagedCleanupBinding,
    transition: &OperationTransitionRecord,
    diagnostic: &str,
    actions: &[OperationEffectRecord],
) -> Result<(), RuntimeError> {
    let mut expected = operation(binding);
    expected.phase = OperationPhase::ManualReview;
    expected.error_code = Some("native_cleanup_unresolved".into());
    expected.error_message = Some(diagnostic.into());
    expected.updated_at = transition.created_at;
    if bound_operation(tx, binding)? != expected
        || receipt_state(tx, &binding.operation_id)? != ("manual_review".into(), None, None)
    {
        return Err(conflict("cleanup hold carrier changed"));
    }
    verify_admission(tx, binding)?;
    verify_claims(tx, binding)?;
    verify_effects(tx, &binding.operation_id, actions)?;
    if transition.sequence == 3 {
        // An already-issued branch grant remains part of the held proof,
        // including after the hold event and during exact held replay.
        verify_branch_grant(tx, &branch_grant(&checkout_ack(tx, binding)?)?)?;
    }
    verify_transition(tx, transition)?;
    verify_event(
        tx,
        &worktree_event_record(
            binding,
            "held",
            "worktree.cleanup_held",
            json!({"operation_id":binding.operation_id,"hold":transition.evidence}),
            transition.created_at,
        ),
    )
}

/// Holding preserves known earlier receipts and never releases reservations.
fn hold_tx(
    tx: &Transaction<'_>,
    attempt: &ManagedCleanupAttempt,
    observation: &ManagedCleanupObservation,
    diagnostic: &str,
    at: i64,
) -> Result<(), RuntimeError> {
    let binding = match attempt {
        ManagedCleanupAttempt::Checkout(g) => &g.binding,
        ManagedCleanupAttempt::Branch(g) => &g.binding,
    };
    let current = bound_operation(tx, binding)?;
    verify_admission(tx, binding)?;
    verify_claims(tx, binding)?;
    if current.phase == OperationPhase::ManualReview {
        let (seq,raw,created):(u64,String,i64)=tx.query_row(
            "SELECT sequence,evidence_json,created_at FROM runtime_operation_transitions WHERE operation_id=?1 ORDER BY sequence DESC LIMIT 1",
            [&binding.operation_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|db_error("failed reading cleanup hold",e))?;
        let saved: serde_json::Value =
            serde_json::from_str(&raw).map_err(|_| conflict("invalid cleanup hold"))?;
        let actions: Vec<OperationEffectRecord> = serde_json::from_value(
            saved
                .get("actions")
                .cloned()
                .ok_or_else(|| conflict("missing held action carriers"))?,
        )
        .map_err(|_| conflict("invalid held action carriers"))?;
        let expected =
            hold_transition(binding, seq, attempt, observation, diagnostic, at, &actions);
        if created != at || expected.evidence.as_ref() != Some(&saved) {
            return Err(conflict(
                "cleanup already has different unresolved evidence",
            ));
        }
        return verify_hold(tx, binding, &expected, diagnostic, &actions);
    }
    pending(tx, binding)?;
    let initial = CheckoutRemovalGrant {
        binding: binding.clone(),
    };
    let mut actions = effects(tx, &binding.operation_id)?;
    let initial_pair = [checkout_started(&initial), branch_intended(binding)];
    let (sequence, changed_index) = match attempt {
        ManagedCleanupAttempt::Checkout(grant) => {
            if *grant != initial {
                return Err(conflict("checkout attempt binding mismatch"));
            }
            if verify_effects(tx, &binding.operation_id, &initial_pair).is_ok() {
                (2, Some(format!("{}:checkout-remove", binding.operation_id)))
            } else {
                // A checkout receipt/branch grant may have committed before
                // its response was lost. Preserve both; do not issue it again.
                let ack = checkout_ack(tx, binding)?;
                let branch = branch_grant(&ack)?;
                if verify_effects(
                    tx,
                    &binding.operation_id,
                    &[checkout_finalized(&ack), branch_intended(binding)],
                )
                .is_ok()
                {
                    (2, None)
                } else {
                    verify_effects(
                        tx,
                        &binding.operation_id,
                        &[checkout_finalized(&ack), branch_started(&branch)],
                    )?;
                    verify_branch_grant(tx, &branch)?;
                    (3, None)
                }
            }
        }
        ManagedCleanupAttempt::Branch(grant) => {
            let ack = checkout_ack(tx, binding)?;
            if *grant != branch_grant(&ack)? {
                return Err(conflict(
                    "branch attempt does not bind original checkout receipt",
                ));
            }
            verify_effects(
                tx,
                &binding.operation_id,
                &[checkout_finalized(&ack), branch_started(grant)],
            )?;
            verify_branch_grant(tx, grant)?;
            (3, Some(format!("{}:branch-delete", binding.operation_id)))
        }
    };
    if let Some(id) = changed_index {
        let action = actions
            .iter_mut()
            .find(|e| e.effect_id == id)
            .ok_or_else(|| conflict("missing held action"))?;
        let old = action.clone();
        action.phase = "uncertain".into();
        action.updated_at = at;
        let evidence = action
            .evidence
            .as_mut()
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| conflict("invalid original action evidence"))?;
        evidence.insert(
            "hold".into(),
            json!({"observation":observation,"diagnostic":diagnostic,"observed_at":at}),
        );
        update_effect(tx, &old, action)?;
    }
    let changed=tx.execute("UPDATE runtime_operations SET phase='manual_review',error_code='native_cleanup_unresolved',error_message=?2,updated_at=?3
        WHERE operation_id=?1 AND phase='requested'",params![binding.operation_id,diagnostic,at])
        .map_err(|e|db_error("failed holding unresolved cleanup",e))?;
    let receipt=tx.execute("UPDATE managed_worktree_cleanup_receipts SET phase='manual_review' WHERE operation_id=?1 AND phase='pending'",
        [&binding.operation_id]).map_err(|e|db_error("failed retaining cleanup fence",e))?;
    if changed != 1 || receipt != 1 {
        return Err(conflict("cleanup hold write was ignored"));
    }
    let transition = hold_transition(
        binding,
        sequence,
        attempt,
        observation,
        diagnostic,
        at,
        &actions,
    );
    OperationAuthorityTransaction::new(tx).append_transition(&transition)?;
    let event = worktree_event_record(
        binding,
        "held",
        "worktree.cleanup_held",
        json!({"operation_id":binding.operation_id,"hold":transition.evidence}),
        at,
    );
    insert_runtime_event_in_transaction(tx, &event)?;
    verify_hold(tx, binding, &transition, diagnostic, &actions)
}

impl SqliteRuntimeRepository {
    pub fn acknowledge_managed_checkout_removal(
        &self,
        ack: &CheckoutRemovalAck,
    ) -> Result<ManagedCleanupBranchAdmission, RuntimeError> {
        if ack.exit_code != 0 {
            return Err(conflict(
                "checkout acknowledgement is not a direct zero exit",
            ));
        }
        let mut db = open_connection(&self.database_path)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting checkout receipt", e))?;
        let binding = &ack.grant.binding;
        let current = bound_operation(&tx, binding)?;
        verify_admission(&tx, binding)?;
        if current.phase != OperationPhase::Requested {
            return Ok(ManagedCleanupBranchAdmission::Unresolved {
                operation_id: binding.operation_id.clone(),
            });
        }
        pending(&tx, binding)?;
        let first = [checkout_started(&ack.grant), branch_intended(binding)];
        if verify_effects(&tx, &binding.operation_id, &first).is_err() {
            let known = checkout_ack(&tx, binding)?;
            let grant = branch_grant(&known)?;
            if known != *ack {
                return Err(conflict("checkout already has a different acknowledgement"));
            }
            verify_effects(
                &tx,
                &binding.operation_id,
                &[checkout_finalized(&known), branch_started(&grant)],
            )?;
            verify_branch_grant(&tx, &grant)?;
            // A duplicate never returns a second branch execution grant.
            return Ok(ManagedCleanupBranchAdmission::Unresolved {
                operation_id: binding.operation_id.clone(),
            });
        }
        let checkout = checkout_finalized(ack);
        update_effect(&tx, &first[0], &checkout)?;
        if let Err(blocker) = require_eligible(&tx, &binding.target) {
            hold_tx(
                &tx,
                &ManagedCleanupAttempt::Checkout(ack.grant.clone()),
                &ManagedCleanupObservation::Exited { code: 0 },
                &format!("branch not granted: {blocker}"),
                ack.observed_at,
            )?;
            tx.commit()
                .map_err(|e| db_error("failed preserving checkout receipt and blocker", e))?;
            return Ok(ManagedCleanupBranchAdmission::Unresolved {
                operation_id: binding.operation_id.clone(),
            });
        }
        let grant = branch_grant(ack)?;
        let branch = branch_started(&grant);
        update_effect(&tx, &first[1], &branch)?;
        let transition = branch_transition(&grant);
        OperationAuthorityTransaction::new(&tx).append_transition(&transition)?;
        let event = worktree_event_record(
            binding,
            "checkout-acknowledged",
            "worktree.checkout_removed",
            json!({"operation_id":binding.operation_id,"checkout_ack":ack,"branch_grant":grant}),
            ack.observed_at,
        );
        insert_runtime_event_in_transaction(&tx, &event)?;
        pending(&tx, binding)?;
        verify_effects(&tx, &binding.operation_id, &[checkout, branch])?;
        verify_transition(&tx, &transition)?;
        verify_event(&tx, &event)?;
        tx.commit()
            .map_err(|e| db_error("failed committing checkout receipt and branch grant", e))?;
        Ok(ManagedCleanupBranchAdmission::Execute { grant })
    }

    pub fn acknowledge_managed_branch_deletion(
        &self,
        ack: &BranchDeletionAck,
    ) -> Result<WorktreeCleanupResponse, RuntimeError> {
        if ack.exit_code != 0 {
            return Err(conflict("branch acknowledgement is not a direct zero exit"));
        }
        let mut db = open_connection(&self.database_path)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting branch receipt", e))?;
        let binding = &ack.grant.binding;
        let current = bound_operation(&tx, binding)?;
        if current.phase == OperationPhase::Completed {
            return verify_completed(&tx, ack);
        }
        pending(&tx, binding)?;
        let checkout = checkout_ack(&tx, binding)?;
        if ack.grant != branch_grant(&checkout)? {
            return Err(conflict(
                "branch grant does not bind original checkout receipt",
            ));
        }
        let before = branch_started(&ack.grant);
        verify_effects(
            &tx,
            &binding.operation_id,
            &[checkout_finalized(&checkout), before.clone()],
        )?;
        verify_branch_grant(&tx, &ack.grant)?;
        update_effect(&tx, &before, &branch_finalized(ack))?;
        let result = response(binding);
        let exact = value(&result)?;
        let changed=tx.execute("UPDATE managed_worktree_cleanup_receipts SET phase='completed',response_json=?2,completed_at=?3
            WHERE operation_id=?1 AND phase='pending'",params![binding.operation_id,json_to_string(&exact)?,ack.observed_at])
            .map_err(|e|db_error("failed persisting exact cleanup result",e))?;
        if changed != 1 {
            return Err(conflict("cleanup result write was ignored"));
        }
        let authority = OperationAuthorityTransaction::new(&tx);
        authority.terminalize(
            &binding.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            None,
            &exact,
            None,
            None,
            ack.observed_at,
        )?;
        authority.append_transition(&completed_transition(ack))?;
        authority.release_claim(&binding.checkout_claim)?;
        authority.release_claim(&binding.branch_claim)?;
        let event = worktree_event_record(
            binding,
            "settled",
            "worktree.cleaned_up",
            json!({"operation_id":binding.operation_id,"response":exact,"original_actions":true}),
            ack.observed_at,
        );
        insert_runtime_event_in_transaction(&tx, &event)?;
        verify_completed(&tx, ack)?;
        for claim in [&binding.checkout_claim, &binding.branch_claim] {
            let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM runtime_operation_resource_claims WHERE resource_kind=?1 AND resource_id=?2)",
                params![claim.resource_kind,claim.resource_id],|r|r.get(0)).map_err(|e|db_error("failed checking released cleanup identity",e))?;
            if exists {
                return Err(conflict(
                    "released identity reservation was replaced during settlement",
                ));
            }
            let generation: Option<i64> = tx.query_row(
                "SELECT last_generation FROM runtime_operation_resource_fences WHERE resource_kind=?1 AND resource_id=?2",
                params![claim.resource_kind,claim.resource_id],|r|r.get(0)).optional()
                .map_err(|e|db_error("failed checking settled cleanup fence generation",e))?;
            if generation.and_then(|g| u64::try_from(g).ok()) != Some(claim.fence_generation) {
                return Err(conflict("resource generation changed during settlement"));
            }
        }
        tx.commit()
            .map_err(|e| db_error("failed committing original cleanup acknowledgements", e))?;
        Ok(result)
    }

    pub fn hold_managed_worktree_cleanup(
        &self,
        attempt: &ManagedCleanupAttempt,
        observation: &ManagedCleanupObservation,
        diagnostic: &str,
        at_ms: i64,
    ) -> Result<(), RuntimeError> {
        let mut db = open_connection(&self.database_path)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting cleanup hold", e))?;
        hold_tx(&tx, attempt, observation, diagnostic, at_ms)?;
        tx.commit()
            .map_err(|e| db_error("failed committing cleanup hold", e))
    }
}
