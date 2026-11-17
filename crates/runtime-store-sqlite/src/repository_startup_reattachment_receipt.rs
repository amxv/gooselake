use crate::db::{db_error, fetch_runtime_event_by_event_id, json_to_string, open_connection};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_startup_reattachment::{
    bound_operation, conflict, decode, require_current_claim, StoredEffect,
};
use crate::repository_startup_reattachment_authority::{
    current_authority, guard_startup_projection,
};
use crate::repository_turn_start::read_admission;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;
use runtime_core::{
    OperationPhase, OperationRecord, OperationTransitionRecord, ProviderSession, RuntimeError,
    StartupReattachCompletion, StartupReattachGrant, StartupReattachHold, StartupReattachReceipt,
};
use rusqlite::{params, Transaction, TransactionBehavior};
pub(crate) fn completed_receipt(
    operation: &OperationRecord,
    effect: &StoredEffect,
    grant: &StartupReattachGrant,
) -> Result<StartupReattachReceipt, RuntimeError> {
    if operation.phase != OperationPhase::Completed
        || effect.phase != "finalized"
        || effect.attempts != 1
    {
        return Err(conflict("startup operation has no exact completed receipt"));
    }
    let exact = operation
        .exact_terminal_result
        .clone()
        .ok_or_else(|| conflict("completed startup operation lacks its receipt"))?;
    if effect.evidence["receipt"] != exact {
        return Err(conflict("startup operation and effect receipts disagree"));
    }
    let receipt: StartupReattachReceipt = decode(exact)?;
    if receipt.operation_id != grant.intent.command.operation_id
        || receipt.normalized_request_hash != grant.normalized_request_hash
        || receipt.session_id != grant.intent.command.session_id
        || receipt.provider != grant.intent.authority.provider
        || !runtime_core::startup_ack_identity_matches(&grant.intent, &receipt.acknowledgement)
    {
        return Err(conflict(
            "startup receipt does not match the granted native identity",
        ));
    }
    if let Some(event_id) = receipt.metadata_terminal_event_id.as_deref() {
        let witness = grant
            .intent
            .terminal_metadata_witness
            .as_ref()
            .ok_or_else(|| conflict("startup metadata receipt lacks its original witness"))?;
        if event_id != format!("evt_turn_{}_terminal", witness.turn.id) {
            return Err(conflict(
                "startup metadata receipt names a different terminal event",
            ));
        }
    }
    Ok(receipt)
}
fn metadata_event(
    tx: &Transaction<'_>,
    grant: &StartupReattachGrant,
    authority: &runtime_core::StartupAttachmentAuthority,
    current: &runtime_core::SessionRecord,
) -> Result<Option<String>, RuntimeError> {
    if authority.request.metadata == grant.intent.authority.request.metadata {
        return runtime_core::startup_completion_metadata_event(grant, authority, current, None);
    }
    let witness = grant
        .intent
        .terminal_metadata_witness
        .as_ref()
        .ok_or_else(|| conflict("startup metadata changed without a frozen terminal witness"))?;
    let turn = read_turn(tx, &witness.turn.id)?;
    let has_admission: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM turn_admissions WHERE turn_id=?1)",
            [&witness.turn.id],
            |r| r.get(0),
        )
        .map_err(|e| db_error("failed reading completed startup metadata admission", e))?;
    let admission = has_admission
        .then(|| read_admission(tx, &witness.turn.id))
        .transpose()?;
    let event =
        fetch_runtime_event_by_event_id(tx, &format!("evt_turn_{}_terminal", witness.turn.id))?
            .ok_or_else(|| conflict("startup metadata lacks its committed terminal receipt"))?;
    runtime_core::startup_completion_metadata_event(
        grant,
        authority,
        current,
        Some(runtime_core::StartupTerminalMetadataEvidence {
            turn: &turn,
            admission: admission.as_ref(),
            event: &event,
        }),
    )
}
impl SqliteRuntimeRepository {
    pub fn complete_startup_reattach(
        &self,
        grant: &StartupReattachGrant,
        ack: &ProviderSession,
        at_ms: i64,
    ) -> Result<StartupReattachCompletion, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting startup acknowledgement receipt", e))?;
        let (operation, effect) = bound_operation(&tx, grant)?;
        if operation.phase == OperationPhase::Completed {
            let receipt = completed_receipt(&operation, &effect, grant)?;
            if !runtime_core::startup_ack_is_identical(&receipt.acknowledgement, ack) {
                return Err(conflict(
                    "completed startup receipt has a different acknowledgement",
                ));
            }
            let session = read_session(&tx, &receipt.session_id)?;
            return Ok(StartupReattachCompletion { receipt, session });
        }
        if operation.phase != OperationPhase::Requested || effect.phase != "started" {
            return Err(conflict(
                "startup effect is no longer the original granted attempt",
            ));
        }
        require_current_claim(&tx, grant)?;
        if !runtime_core::startup_ack_identity_matches(&grant.intent, ack) {
            return Err(RuntimeError::ProtocolViolation(
                "provider startup acknowledgement changed the granted attachment".into(),
            ));
        }
        let (current, authority) = current_authority(&tx, &grant.intent.command.session_id)?;
        guard_startup_projection(&tx, &current)?;
        let metadata_terminal_event_id = metadata_event(&tx, grant, &authority, &current)?;
        let at_ms = at_ms.max(operation.updated_at);
        if current.canonical_provider_session_ref.is_none()
            && ack.canonical_provider_session_ref.is_some()
        {
            let changed=tx.execute("UPDATE sessions SET canonical_provider_session_ref=?2,updated_at=MAX(updated_at,?3) WHERE id=?1 AND canonical_provider_session_ref IS NULL",params![current.id,ack.canonical_provider_session_ref,at_ms]).map_err(|e|db_error("failed applying narrow startup canonical identity",e))?;
            if changed != 1 {
                return Err(conflict(
                    "startup canonical identity changed before receipt",
                ));
            }
        }
        let receipt = StartupReattachReceipt {
            operation_id: operation.operation_id.clone(),
            normalized_request_hash: grant.normalized_request_hash.clone(),
            session_id: current.id,
            provider: grant.intent.authority.provider,
            acknowledgement: ack.clone(),
            acknowledged_at: at_ms,
            metadata_terminal_event_id,
        };
        let exact = crate::repository_startup_reattachment::value(&receipt)?;
        let evidence = serde_json::json!({"grant":grant,"receipt":&receipt});
        let changed=tx.execute("UPDATE runtime_operation_effects SET phase='finalized',evidence_json=?3,updated_at=?4 WHERE effect_id=?1 AND operation_id=?2 AND phase='started' AND attempt_count=1",params![effect.id,operation.operation_id,json_to_string(&evidence)?,at_ms]).map_err(|e|db_error("failed recording startup native acknowledgement",e))?;
        if changed != 1 {
            return Err(conflict("startup native effect changed before receipt"));
        }
        let authority_tx = OperationAuthorityTransaction::new(&tx);
        authority_tx.terminalize(
            &operation.operation_id,
            OperationPhase::Requested,
            OperationPhase::Completed,
            operation.workspace_id.as_deref(),
            &exact,
            None,
            None,
            at_ms,
        )?;
        authority_tx.append_transition(&OperationTransitionRecord {
            operation_id: operation.operation_id,
            sequence: 2,
            from_phase: Some(OperationPhase::Requested),
            to_phase: OperationPhase::Completed,
            evidence: Some(
                serde_json::json!({"native_resume":"acknowledged","session_id":receipt.session_id}),
            ),
            created_at: at_ms,
        })?;
        authority_tx.release_claim(&grant.claim)?;
        let session = read_session(&tx, &receipt.session_id)?;
        tx.commit()
            .map_err(|e| db_error("failed committing startup native receipt", e))?;
        Ok(StartupReattachCompletion { receipt, session })
    }
    pub fn hold_startup_reattach(
        &self,
        grant: &StartupReattachGrant,
        ack: Option<&ProviderSession>,
        reason: &str,
        at_ms: i64,
    ) -> Result<StartupReattachHold, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting startup native hold", e))?;
        let (operation, effect) = bound_operation(&tx, grant)?;
        if operation.phase == OperationPhase::Completed {
            let receipt = completed_receipt(&operation, &effect, grant)?;
            let session = read_session(&tx, &receipt.session_id)?;
            return Ok(StartupReattachHold::Completed(StartupReattachCompletion {
                receipt,
                session,
            }));
        }
        require_current_claim(&tx, grant)?;
        if operation.phase == OperationPhase::ManualReview {
            if !matches!(effect.phase.as_str(), "observed" | "uncertain") {
                return Err(conflict(
                    "startup held operation has inconsistent effect evidence",
                ));
            }
            return Ok(StartupReattachHold::Held {
                operation_id: operation.operation_id,
            });
        }
        if operation.phase != OperationPhase::Requested || effect.phase != "started" {
            return Err(conflict(
                "startup native effect cannot be held from this phase",
            ));
        }
        let identity_matches =
            ack.is_some_and(|ack| runtime_core::startup_ack_identity_matches(&grant.intent, ack));
        let phase = if identity_matches {
            "observed"
        } else {
            "uncertain"
        };
        let diagnostic: String = reason.chars().take(400).collect();
        let evidence = serde_json::json!({"grant":grant,"acknowledgement":ack,"identity_matches":identity_matches,"diagnostic":diagnostic});
        let at_ms = at_ms.max(operation.updated_at);
        let changed=tx.execute("UPDATE runtime_operation_effects SET phase=?3,evidence_json=?4,updated_at=?5 WHERE effect_id=?1 AND operation_id=?2 AND phase='started' AND attempt_count=1",params![effect.id,operation.operation_id,phase,json_to_string(&evidence)?,at_ms]).map_err(|e|db_error("failed retaining startup native evidence",e))?;
        if changed != 1 {
            return Err(conflict("startup granted effect changed before hold"));
        }
        let changed=tx.execute("UPDATE runtime_operations SET phase='manual_review',error_code='outcome_unresolved',error_message=?2,updated_at=?3 WHERE operation_id=?1 AND phase='requested'",params![operation.operation_id,diagnostic,at_ms]).map_err(|e|db_error("failed retaining startup operation hold",e))?;
        if changed != 1 {
            return Err(conflict("startup operation changed before hold"));
        }
        OperationAuthorityTransaction::new(&tx).append_transition(&OperationTransitionRecord{operation_id:operation.operation_id.clone(),sequence:2,from_phase:Some(OperationPhase::Requested),to_phase:OperationPhase::ManualReview,evidence:Some(serde_json::json!({"native_resume":phase,"claim_retained":true,"session_id":grant.intent.command.session_id})),created_at:at_ms})?;
        tx.commit()
            .map_err(|e| db_error("failed committing retained startup native hold", e))?;
        Ok(StartupReattachHold::Held {
            operation_id: operation.operation_id,
        })
    }
}
