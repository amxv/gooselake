//! Existing delivery claims are pre-native authority. Startup cannot resume
//! over an unresolved claim, and only the fresh claim winner may interrupt.
use runtime_core::{
    AgentDeliveryRecord, RuntimeError, SessionRecord, TeamDeliveryRecord, TurnRecord,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{db_error, open_connection};
use crate::repository_agent_comms::{decode_delivery, DELIVERY_COLUMNS};
use crate::repository_agent_delivery_result::{
    require_claim_receipt as require_agent_claim, require_correlated_admitted_turn,
};
use crate::repository_legacy_team_delivery_result::{
    require_claim_receipt as require_legacy_claim, verify_native_turn_admission,
};
use crate::repository_legacy_team_messages::message_by_id;
use crate::repository_startup_reattachment::guard_resume_claim;
use crate::repository_turn_start::read_admission;
use crate::repository_turn_terminal::{read_session, read_turn};
use crate::SqliteRuntimeRepository;

fn legacy_delivery(tx: &Transaction<'_>, id: &str) -> Result<TeamDeliveryRecord, RuntimeError> {
    tx.query_row(
        "SELECT id,message_id,team_id,recipient_agent_id,provider,status,
          effective_policy,injection_strategy,injected_turn_id,last_error_code,
          last_error_message,created_at,updated_at FROM team_deliveries WHERE id=?1",
        [id],
        |row| {
            Ok(TeamDeliveryRecord {
                id: row.get(0)?,
                message_id: row.get(1)?,
                team_id: row.get(2)?,
                recipient_agent_id: row.get(3)?,
                provider: row.get(4)?,
                status: row.get(5)?,
                effective_policy: row.get(6)?,
                injection_strategy: row.get(7)?,
                injected_turn_id: row.get(8)?,
                last_error_code: row.get(9)?,
                last_error_message: row.get(10)?,
                created_at: row.get(11)?,
                updated_at: row.get(12)?,
            })
        },
    )
    .optional()
    .map_err(|e| db_error("failed reading legacy delivery native owner", e))?
    .ok_or_else(|| RuntimeError::Conflict(format!("legacy delivery {id} vanished")))
}

// Include unknown/pending children in the uniqueness check. An additional
// correlated attempt is not permission to select a convenient positive one.
fn correlated_children(
    tx: &Transaction<'_>,
    session_id: &str,
    message_id: &str,
) -> Result<Vec<String>, RuntimeError> {
    let mut statement = tx
        .prepare(
            "SELECT a.turn_id FROM turn_admissions a
         WHERE a.session_id=?1
           AND json_extract(a.correlation_json,'$.correlation_id')=?2
         ORDER BY a.turn_id LIMIT 2",
        )
        .map_err(|e| db_error("failed preparing delivery child authority", e))?;
    let rows = statement
        .query_map(params![session_id, message_id], |row| row.get(0))
        .map_err(|e| db_error("failed querying delivery child authority", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| db_error("failed reading delivery child authority", e))?;
    Ok(rows)
}

fn agent_unresolved(row: &AgentDeliveryRecord, detail: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Conflict(format!(
        "delivery {} for message {} and recipient {} retains unresolved native injection: {detail}; inspect /v2/messages/{}/deliveries",
        row.id, row.message_id, row.recipient_agent_id, row.message_id
    ))
}

fn legacy_unresolved(row: &TeamDeliveryRecord, detail: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Conflict(format!(
        "legacy delivery {} for message {}, team {} and recipient {} retains unresolved native injection: {detail}; inspect /v1/teams/{}/deliveries",
        row.id, row.message_id, row.team_id, row.recipient_agent_id, row.team_id
    ))
}

fn require_legacy_message(
    tx: &Transaction<'_>,
    row: &TeamDeliveryRecord,
) -> Result<runtime_core::TeamMessageRecord, RuntimeError> {
    let message = message_by_id(tx, &row.message_id)?
        .ok_or_else(|| legacy_unresolved(row, "original message vanished"))?;
    if message.team_id != row.team_id
        || !message.recipient_agent_ids.as_array().is_some_and(|ids| {
            ids.iter()
                .any(|id| id.as_str() == Some(row.recipient_agent_id.as_str()))
        })
    {
        return Err(legacy_unresolved(row, "original message identity changed"));
    }
    Ok(message)
}

/// Called only inside startup's fresh IMMEDIATE admission transaction.
/// Receipt replay and known delivery result commits deliberately bypass it.
pub(crate) fn guard_startup_delivery_claims(
    tx: &Transaction<'_>,
    session_id: &str,
) -> Result<(), RuntimeError> {
    let rows = {
        let mut statement = tx
            .prepare(&format!(
                "SELECT {DELIVERY_COLUMNS} FROM agent_deliveries
             WHERE recipient_agent_id=?1 AND status='injecting' ORDER BY id"
            ))
            .map_err(|e| db_error("failed preparing startup delivery claims", e))?;
        let rows = statement
            .query_map([session_id], decode_delivery)
            .map_err(|e| db_error("failed querying startup delivery claims", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| db_error("failed reading startup delivery claims", e))?;
        rows
    };
    for row in rows {
        if row.injected_turn_id.is_some() {
            return Err(agent_unresolved(
                &row,
                "injecting row already carries a result",
            ));
        }
        require_agent_claim(tx, &row).map_err(|e| agent_unresolved(&row, e))?;
        let children = correlated_children(tx, session_id, &row.message_id)?;
        let [turn_id] = children.as_slice() else {
            return Err(agent_unresolved(
                &row,
                "no unique correlated accepted child",
            ));
        };
        require_correlated_admitted_turn(tx, &row, turn_id)
            .map_err(|e| agent_unresolved(&row, e))?;
    }
    let legacy_ids = {
        let mut statement = tx
            .prepare(
                "SELECT d.id FROM team_deliveries d
             WHERE d.recipient_agent_id=?1 AND d.status='injecting'
             ORDER BY d.id",
            )
            .map_err(|e| db_error("failed preparing startup legacy delivery claims", e))?;
        let rows = statement
            .query_map([session_id], |row| row.get::<_, String>(0))
            .map_err(|e| db_error("failed querying startup legacy delivery claims", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| db_error("failed reading startup legacy delivery claims", e))?;
        rows
    };
    for id in legacy_ids {
        let row = legacy_delivery(tx, &id)?;
        let canonical = tx
            .query_row(
                &format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE id=?1"),
                [&id],
                decode_delivery,
            )
            .optional()
            .map_err(|e| db_error("failed checking startup legacy mirror", e))?;
        if let Some(canonical) = canonical {
            let mirror = AgentDeliveryRecord {
                id: row.id.clone(),
                message_id: row.message_id.clone(),
                recipient_agent_id: row.recipient_agent_id.clone(),
                provider: row.provider.clone(),
                status: row.status.clone(),
                effective_policy: row.effective_policy.clone(),
                injection_strategy: row.injection_strategy.clone(),
                injected_turn_id: row.injected_turn_id.clone(),
                last_error_code: row.last_error_code.clone(),
                last_error_message: row.last_error_message.clone(),
                created_at: row.created_at,
                updated_at: row.updated_at,
            };
            if canonical != mirror {
                return Err(legacy_unresolved(
                    &row,
                    "canonical same-ID row is not its exact native claim mirror",
                ));
            }
            // The earlier canonical loop proved this identical injecting row
            // for the same recipient. It does not represent a second attempt.
            continue;
        }
        if row.injected_turn_id.is_some() {
            return Err(legacy_unresolved(
                &row,
                "injecting row already carries a result",
            ));
        }
        require_legacy_message(tx, &row)?;
        require_legacy_claim(tx, &row).map_err(|e| legacy_unresolved(&row, e))?;
        let children = correlated_children(tx, session_id, &row.message_id)?;
        let [turn_id] = children.as_slice() else {
            return Err(legacy_unresolved(
                &row,
                "no unique correlated accepted child",
            ));
        };
        verify_native_turn_admission(tx, &row, turn_id).map_err(|e| legacy_unresolved(&row, e))?;
    }
    Ok(())
}

fn require_interrupt_target(
    tx: &Transaction<'_>,
    recipient: &str,
    provider: &str,
    message_id: &str,
    policy: &str,
    session: &SessionRecord,
    turn: &TurnRecord,
) -> Result<(), RuntimeError> {
    let stored_session = read_session(tx, recipient)?;
    let stored_turn = read_turn(tx, &turn.id)?;
    if session.id != recipient
        || session.provider != provider
        || stored_session.provider != provider
        || stored_session.provider_session_ref != session.provider_session_ref
        || stored_session.canonical_provider_session_ref != session.canonical_provider_session_ref
        || session.active_turn_id.as_deref() != Some(turn.id.as_str())
        || stored_session.active_turn_id.as_deref() != Some(turn.id.as_str())
        || stored_session.closed_at.is_some()
        || matches!(stored_session.status.as_str(), "closed" | "failed")
        || turn.session_id != recipient
        || turn.completed_at.is_some()
        || stored_turn != *turn
        || !matches!(
            policy.trim().to_ascii_lowercase().as_str(),
            "immediate_interrupt" | "interrupt_after_tool_boundary"
        )
    {
        return Err(RuntimeError::Conflict(
            "delivery interruption no longer owns its exact active recipient, provider, turn and policy".into(),
        ));
    }
    // A delivery claim proves who owns this interrupt attempt, not whether
    // an old active pointer proves native dispatch. Historical/no-admission
    // and unresolved first-dispatch states cannot authorize an interrupt.
    let admitted: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM turn_admissions WHERE turn_id=?1)",
            [&turn.id],
            |row| row.get(0),
        )
        .map_err(|e| db_error("failed checking delivery interrupt dispatch authority", e))?;
    if !admitted {
        return Err(RuntimeError::Conflict(
            "legacy active turn lacks its original dispatched admission; native delivery interrupt is unsupported".into(),
        ));
    }
    let admission = read_admission(tx, &turn.id)?;
    if admission.session_id != recipient
        || admission.provider != provider
        || admission.dispatch_state != runtime_core::TurnDispatchState::Dispatched
    {
        return Err(RuntimeError::Conflict(
            "delivery interrupt requires the original known Dispatched native turn".into(),
        ));
    }
    let decision_pending: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM approvals WHERE session_id=?1 AND status='decision_pending')",
        [recipient], |row| row.get(0),
    ).map_err(|e| db_error("failed checking delivery interrupt approval authority", e))?;
    if let Some(reason) = runtime_core::startup_projection_refusal(
        &stored_session,
        Some(&stored_turn),
        Some(&admission),
        decision_pending,
    ) {
        return Err(RuntimeError::Conflict(format!(
            "native delivery interrupt withheld: {reason}"
        )));
    }
    guard_resume_claim(tx, recipient)?;
    crate::repository_legacy_spawn_obligations::guard_legacy_native_session(tx, recipient)?;
    if !correlated_children(tx, recipient, message_id)?.is_empty() {
        return Err(RuntimeError::Conflict(
            "delivery already has a correlated child; only receipt settlement is authorized".into(),
        ));
    }
    Ok(())
}

impl SqliteRuntimeRepository {
    /// Validation is not a grant to replay. Only the caller which just won
    /// the original injecting CAS may invoke the internal interrupt helper.
    pub fn verify_agent_delivery_interrupt_owner(
        &self,
        claim: &AgentDeliveryRecord,
        session: &SessionRecord,
        turn: &TurnRecord,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting delivery interrupt owner verification", e))?;
        let current = tx
            .query_row(
                &format!("SELECT {DELIVERY_COLUMNS} FROM agent_deliveries WHERE id=?1"),
                [&claim.id],
                decode_delivery,
            )
            .optional()
            .map_err(|e| db_error("failed reading delivery interrupt owner", e))?;
        if current.as_ref() != Some(claim)
            || claim.status != "injecting"
            || claim.injected_turn_id.is_some()
        {
            return Err(agent_unresolved(claim, "current native claim differs"));
        }
        require_agent_claim(&tx, claim).map_err(|e| agent_unresolved(claim, e))?;
        let policy: Option<String> = tx
            .query_row(
                "SELECT COALESCE(?2,policy) FROM agent_messages WHERE id=?1",
                params![claim.message_id, claim.effective_policy],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| db_error("failed reading delivery interrupt policy", e))?;
        let policy = policy.ok_or_else(|| agent_unresolved(claim, "original message vanished"))?;
        require_interrupt_target(
            &tx,
            &claim.recipient_agent_id,
            &claim.provider,
            &claim.message_id,
            &policy,
            session,
            turn,
        )
        .map_err(|e| {
            if e.legacy_native_recovery().is_some() {
                e
            } else {
                agent_unresolved(claim, e)
            }
        })?;
        tx.commit()
            .map_err(|e| db_error("failed finishing delivery interrupt verification", e))
    }

    pub fn verify_legacy_delivery_interrupt_owner(
        &self,
        claim: &TeamDeliveryRecord,
        session: &SessionRecord,
        turn: &TurnRecord,
    ) -> Result<(), RuntimeError> {
        let mut connection = open_connection(&self.database_path)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| db_error("failed starting legacy interrupt owner verification", e))?;
        let canonical: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM agent_deliveries WHERE id=?1)",
                [&claim.id],
                |row| row.get(0),
            )
            .map_err(|e| db_error("failed checking canonical delivery interrupt owner", e))?;
        if canonical
            || legacy_delivery(&tx, &claim.id)? != *claim
            || claim.status != "injecting"
            || claim.injected_turn_id.is_some()
        {
            return Err(legacy_unresolved(claim, "current native claim differs"));
        }
        let message = require_legacy_message(&tx, claim)?;
        require_legacy_claim(&tx, claim).map_err(|e| legacy_unresolved(claim, e))?;
        let policy = claim.effective_policy.as_deref().unwrap_or(&message.policy);
        require_interrupt_target(
            &tx,
            &claim.recipient_agent_id,
            &claim.provider,
            &claim.message_id,
            policy,
            session,
            turn,
        )
        .map_err(|e| {
            if e.legacy_native_recovery().is_some() {
                e
            } else {
                legacy_unresolved(claim, e)
            }
        })?;
        tx.commit()
            .map_err(|e| db_error("failed finishing legacy interrupt verification", e))
    }
}
