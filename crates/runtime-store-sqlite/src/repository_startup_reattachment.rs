use crate::db::{db_error, open_connection, string_to_json};
use crate::operation_tx::OperationAuthorityTransaction;
use crate::repository_startup_operator_turn::startup_operator_turn_tx;
use crate::repository_startup_reattachment_authority::{
    current_authority, guard_startup_native, terminal_metadata_witness,
};
use crate::repository_startup_reattachment_receipt::completed_receipt;
use crate::repository_turn_terminal::read_session;
use crate::repository_workspace::{operation_by_id, operation_by_idempotency};
use crate::SqliteRuntimeRepository;
use runtime_core::{
    OperationActor, OperationActorKind, OperationEffectRecord, OperationPhase, OperationRecord,
    OperationResourceClaimRecord, OperationTransitionRecord, RuntimeError,
    StartupPendingOperatorTurn, StartupReattachAdmission, StartupReattachCommand,
    StartupReattachGrant, StartupReattachIntent,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
pub(crate) const RESUME_CLAIM: &str = crate::repository_operator_agent_restore::CLAIM_KIND;
pub(crate) const STARTUP_KIND: &str = "startup_session_reattach";
pub(crate) const STARTUP_EFFECT: &str = "provider_session_reattach";
pub(crate) const STARTUP_ACTOR: &str = "runtime-startup";
pub(crate) const STARTUP_EFFECT_KEY: &str = "startup_resume";
pub(crate) fn conflict(message: &str) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}
pub(crate) fn value<T: Serialize>(input: &T) -> Result<Value, RuntimeError> {
    serde_json::to_value(input)
        .map_err(|e| RuntimeError::Bootstrap(format!("invalid startup evidence: {e}")))
}
pub(crate) fn decode<T: DeserializeOwned>(input: Value) -> Result<T, RuntimeError> {
    serde_json::from_value(input)
        .map_err(|e| RuntimeError::Bootstrap(format!("corrupt startup evidence: {e}")))
}
pub(crate) fn claim_for(
    db: &Connection,
    kind: &str,
    id: &str,
) -> Result<Option<OperationResourceClaimRecord>, RuntimeError> {
    let row:Option<(String,i64,i64)>=db.query_row("SELECT owner_operation_id,fence_generation,acquired_at FROM runtime_operation_resource_claims WHERE resource_kind=?1 AND resource_id=?2 AND claim_mode='exclusive'",params![kind,id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(|e|db_error("failed reading exact startup resource claim",e))?;
    row.map(|(owner, generation, acquired_at)| {
        if generation <= 0 {
            return Err(conflict("invalid startup resource fence generation"));
        }
        Ok(OperationResourceClaimRecord {
            resource_kind: kind.into(),
            resource_id: id.into(),
            claim_mode: "exclusive".into(),
            owner_operation_id: owner,
            fence_generation: generation as u64,
            acquired_at,
        })
    })
    .transpose()
}
pub(crate) struct StoredEffect {
    pub id: String,
    pub kind: String,
    pub target_kind: String,
    pub target_id: String,
    pub phase: String,
    pub key: String,
    pub evidence: Value,
    pub attempts: i64,
    pub created_at: i64,
}
pub(crate) fn sole_effect(
    db: &Connection,
    operation_id: &str,
) -> Result<StoredEffect, RuntimeError> {
    let count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM runtime_operation_effects WHERE operation_id=?1",
            [operation_id],
            |r| r.get(0),
        )
        .map_err(|e| db_error("failed counting exact startup operation effects", e))?;
    if count != 1 {
        return Err(conflict(
            "operation does not own exactly one expected native effect",
        ));
    }
    db.query_row("SELECT effect_id,effect_kind,target_kind,target_id,phase,idempotency_key,evidence_json,attempt_count,created_at FROM runtime_operation_effects WHERE operation_id=?1",[operation_id],|r|{let evidence:Option<String>=r.get(6)?;Ok(StoredEffect{id:r.get(0)?,kind:r.get(1)?,target_kind:r.get(2)?,target_id:r.get(3)?,phase:r.get(4)?,key:r.get(5)?,evidence:evidence.map(string_to_json).transpose()?.unwrap_or(Value::Null),attempts:r.get(7)?,created_at:r.get(8)?})}).map_err(|e|db_error("failed reading exact startup operation effect",e))
}
pub(crate) fn bound_operation(
    tx: &Transaction<'_>,
    grant: &StartupReattachGrant,
) -> Result<(OperationRecord, StoredEffect), RuntimeError> {
    let intent = &grant.intent;
    let command = &intent.command;
    let operation = operation_by_id(tx, &command.operation_id)?
        .ok_or_else(|| conflict("startup grant has no operation"))?;
    let exact = value(intent)?;
    if intent.version != 1
        || command.session_id != intent.authority.request.runtime_session_id
        || command.provider != intent.authority.provider
        || command.request != intent.authority.request
        || operation.kind != STARTUP_KIND
        || operation.actor.kind != OperationActorKind::System
        || operation.actor.identifier != STARTUP_ACTOR
        || operation.workspace_id.as_deref() != intent.authority.scope.workspace_id()
        || operation.normalized_request != exact
        || operation.normalized_request_hash != grant.normalized_request_hash
        || runtime_core::normalized_json_hash(&exact)? != grant.normalized_request_hash
        || grant.claim.resource_kind != RESUME_CLAIM
        || grant.claim.resource_id != command.session_id
        || grant.claim.claim_mode != "exclusive"
        || grant.claim.owner_operation_id != command.operation_id
        || grant.claim.fence_generation == 0
        || operation.created_at != grant.claim.acquired_at
    {
        return Err(conflict(
            "startup operation differs from the frozen first-attempt grant",
        ));
    }
    let effect = sole_effect(tx, &operation.operation_id)?;
    if effect.id != format!("{}:resume", operation.operation_id)
        || effect.kind != STARTUP_EFFECT
        || effect.target_kind != "provider_session"
        || effect.target_id != command.session_id
        || effect.key != STARTUP_EFFECT_KEY
        || effect.attempts != 1
        || effect.created_at != grant.claim.acquired_at
        || effect.evidence["grant"] != value(grant)?
    {
        return Err(conflict(
            "startup native effect differs from the original granted attempt",
        ));
    }
    Ok((operation, effect))
}
pub(crate) fn require_current_claim(
    tx: &Transaction<'_>,
    grant: &StartupReattachGrant,
) -> Result<(), RuntimeError> {
    if claim_for(tx, RESUME_CLAIM, &grant.intent.command.session_id)?.as_ref() != Some(&grant.claim)
    {
        return Err(conflict(
            "startup operation no longer owns its exact native resume claim",
        ));
    }
    Ok(())
}
pub(crate) fn guard_resume_claim(
    tx: &Transaction<'_>,
    session_id: &str,
) -> Result<(), RuntimeError> {
    if let Some(claim) = claim_for(tx, RESUME_CLAIM, session_id)? {
        return Err(RuntimeError::operation_unresolved(
            claim.owner_operation_id,
            "native attachment resume owns this session; another native effect is withheld",
        ));
    }
    Ok(())
}
impl SqliteRuntimeRepository {
    pub fn settle_startup_operator_turn(
        &self,
        session_id: &str,
        at_ms: i64,
    ) -> Result<Option<StartupPendingOperatorTurn>, RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting startup parent settlement", e))?;
        let session = read_session(&tx, session_id)?;
        let pending = startup_operator_turn_tx(&tx, &session, at_ms, true)?;
        tx.commit()
            .map_err(|e| db_error("failed committing startup parent settlement", e))?;
        Ok(pending)
    }
    pub fn admit_startup_reattach(
        &self,
        command: &StartupReattachCommand,
        at_ms: i64,
    ) -> Result<StartupReattachAdmission, RuntimeError> {
        if command.operation_id.trim().is_empty()
            || command.operation_id.trim() != command.operation_id
            || command.startup_instance_id.trim().is_empty()
            || command.startup_instance_id.trim() != command.startup_instance_id
            || command.session_id.trim().is_empty()
            || command.request.runtime_session_id != command.session_id
        {
            return Err(RuntimeError::InvalidState(
                "invalid startup attempt identity".into(),
            ));
        }
        let key = runtime_core::normalized_json_hash(
            &serde_json::json!({"startup_instance_id":command.startup_instance_id,"session_id":command.session_id}),
        )?;
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting startup native admission", e))?;
        if let Some(existing) =
            operation_by_idempotency(&tx, OperationActorKind::System, STARTUP_ACTOR, Some(&key))?
        {
            let intent: StartupReattachIntent = decode(existing.normalized_request.clone())?;
            if intent.command != *command {
                return Err(conflict(
                    "startup attempt key belongs to different frozen input",
                ));
            }
            let effect = sole_effect(&tx, &existing.operation_id)?;
            let grant: StartupReattachGrant = decode(effect.evidence["grant"].clone())?;
            if existing.operation_id != command.operation_id || grant.intent != intent {
                return Err(conflict(
                    "startup replay grant belongs to different original authority",
                ));
            }
            let (operation, effect) = bound_operation(&tx, &grant)?;
            return if operation.phase == OperationPhase::Completed {
                Ok(StartupReattachAdmission::Replay(completed_receipt(
                    &operation, &effect, &grant,
                )?))
            } else {
                Ok(StartupReattachAdmission::Unresolved {
                    operation_id: operation.operation_id,
                })
            };
        }
        if operation_by_id(&tx, &command.operation_id)?.is_some() {
            return Err(conflict(
                "startup operation ID already belongs to another attempt",
            ));
        }
        let (session, authority) = current_authority(&tx, &command.session_id)?;
        if authority.provider != command.provider || authority.request != command.request {
            return Err(conflict(
                "startup request changed before native authority was granted",
            ));
        }
        let pending_operator_turn = startup_operator_turn_tx(&tx, &session, at_ms, false)?;
        guard_startup_native(&tx, &session, &authority)?;
        let terminal_metadata_witness = terminal_metadata_witness(&tx, &session)?;
        let intent = StartupReattachIntent {
            version: 1,
            command: command.clone(),
            authority,
            pending_operator_turn,
            terminal_metadata_witness,
        };
        let normalized_request = value(&intent)?;
        let normalized_request_hash = runtime_core::normalized_json_hash(&normalized_request)?;
        let authority_tx = OperationAuthorityTransaction::new(&tx);
        authority_tx.insert_operation(&OperationRecord {
            operation_id: command.operation_id.clone(),
            workspace_id: intent.authority.scope.workspace_id().map(str::to_owned),
            kind: STARTUP_KIND.into(),
            actor: OperationActor {
                kind: OperationActorKind::System,
                identifier: STARTUP_ACTOR.into(),
            },
            idempotency_key: Some(key),
            normalized_request_hash: normalized_request_hash.clone(),
            normalized_request,
            phase: OperationPhase::Requested,
            exact_terminal_result: None,
            error_code: None,
            error_message: None,
            created_at: at_ms,
            updated_at: at_ms,
        })?;
        let claim = authority_tx.acquire_claim(
            &command.operation_id,
            RESUME_CLAIM,
            &command.session_id,
            at_ms,
        )?;
        let grant = StartupReattachGrant {
            intent,
            normalized_request_hash,
            claim,
        };
        authority_tx.insert_effect(&OperationEffectRecord {
            effect_id: format!("{}:resume", command.operation_id),
            operation_id: command.operation_id.clone(),
            effect_kind: STARTUP_EFFECT.into(),
            target_kind: "provider_session".into(),
            target_id: command.session_id.clone(),
            phase: "started".into(),
            idempotency_key: STARTUP_EFFECT_KEY.into(),
            evidence: Some(serde_json::json!({"grant":&grant})),
            attempt_count: 1,
            created_at: at_ms,
            updated_at: at_ms,
        })?;
        authority_tx.append_transition(&OperationTransitionRecord{operation_id:command.operation_id.clone(),sequence:1,from_phase:None,to_phase:OperationPhase::Requested,evidence:Some(serde_json::json!({"native_resume":"first_attempt_granted","session_id":command.session_id})),created_at:at_ms})?;
        tx.commit()
            .map_err(|e| db_error("failed committing first native startup grant", e))?;
        Ok(StartupReattachAdmission::Execute(grant))
    }
}
