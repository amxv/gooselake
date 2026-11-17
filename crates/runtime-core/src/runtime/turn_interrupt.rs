use crate::{
    AgentDeliveryRecord, ProviderInterruptTurnRequest, ProviderKind, RuntimeError,
    RuntimeEventCriticality, RuntimeEventScope, SessionRecord, TeamDeliveryRecord,
};

use super::RuntimeSessionManager;

#[derive(Clone, Copy)]
enum DeliveryInterrupt<'a> {
    Agent(&'a AgentDeliveryRecord),
    Legacy(&'a TeamDeliveryRecord),
}

impl RuntimeSessionManager {
    pub async fn interrupt_turn(
        &self,
        _session_id: &str,
        _turn_id: &str,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "direct session interruption has no durable owner; use a journaled workspace interrupt"
                .into(),
        ))
    }

    pub(super) async fn interrupt_turn_for_operation(
        &self,
        session_id: &str,
        turn_id: &str,
        operation_id: &str,
    ) -> Result<(), RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        self.require_delivery_interrupt_not_retiring(session_id)?;
        let session = self.get_session(session_id).await?;
        self.interrupt_turn_with_receipt_under_guard(
            &session,
            turn_id,
            serde_json::json!({"operation_id":operation_id}),
        )
        .await
    }

    pub(super) async fn interrupt_retirement_turn_under_policy_guard(
        &self,
        session_id: &str,
        turn_id: &str,
        operation_id: &str,
    ) -> Result<(), RuntimeError> {
        let session = self.get_session(session_id).await?;
        self.interrupt_turn_with_receipt_under_guard(
            &session,
            turn_id,
            serde_json::json!({"operation_id":operation_id}),
        )
        .await
    }

    // These entry points are called only in the fresh durable claim-winner
    // branches. An existing injecting record is never a retry token.
    pub(crate) async fn interrupt_agent_delivery_turn(
        &self,
        claim: &AgentDeliveryRecord,
        turn_id: &str,
    ) -> Result<(), RuntimeError> {
        self.interrupt_claimed_delivery(DeliveryInterrupt::Agent(claim), turn_id)
            .await
    }

    pub(crate) async fn interrupt_legacy_delivery_turn(
        &self,
        claim: &TeamDeliveryRecord,
        turn_id: &str,
    ) -> Result<(), RuntimeError> {
        self.interrupt_claimed_delivery(DeliveryInterrupt::Legacy(claim), turn_id)
            .await
    }

    async fn interrupt_claimed_delivery(
        &self,
        claim: DeliveryInterrupt<'_>,
        turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let _mutation = self.session_policy_mutation_lock.lock().await;
        let session_id = match claim {
            DeliveryInterrupt::Agent(row) => &row.recipient_agent_id,
            DeliveryInterrupt::Legacy(row) => &row.recipient_agent_id,
        };
        self.require_delivery_interrupt_not_retiring(session_id)?;
        let session = self.get_session(session_id).await?;
        let turn = self
            .turns
            .read()
            .await
            .get(turn_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(format!("turn {turn_id}")))?;
        let payload = match claim {
            DeliveryInterrupt::Agent(row) => {
                self.store
                    .verify_agent_delivery_interrupt_owner(row, &session, &turn)?;
                serde_json::json!({"message_id":row.message_id,"delivery_id":row.id})
            }
            DeliveryInterrupt::Legacy(row) => {
                self.store
                    .verify_legacy_delivery_interrupt_owner(row, &session, &turn)?;
                serde_json::json!({
                    "message_id":row.message_id,"delivery_id":row.id,"team_id":row.team_id
                })
            }
        };
        self.interrupt_turn_with_receipt_under_guard(&session, turn_id, payload)
            .await
    }

    fn require_delivery_interrupt_not_retiring(
        &self,
        session_id: &str,
    ) -> Result<(), RuntimeError> {
        if let Some(operation_id) = self.store.retiring_operation_for_session(session_id)? {
            return Err(RuntimeError::Conflict(format!(
                "workspace retirement {operation_id} owns native turn interruption"
            )));
        }
        Ok(())
    }

    async fn interrupt_turn_with_receipt_under_guard(
        &self,
        session: &SessionRecord,
        turn_id: &str,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        if session.active_turn_id.as_deref() != Some(turn_id) {
            return Err(RuntimeError::InvalidState(format!(
                "turn {turn_id} is not active for session {}",
                session.id
            )));
        }
        let provider_kind = ProviderKind::from_str(&session.provider).ok_or_else(|| {
            RuntimeError::ProtocolViolation(format!("unknown provider {}", session.provider))
        })?;
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            RuntimeError::ProviderNotRegistered(provider_kind.as_str().to_string())
        })?;
        provider
            .interrupt_turn(ProviderInterruptTurnRequest {
                runtime_session_id: session.id.clone(),
                turn_id: turn_id.to_string(),
            })
            .await
            .map_err(interrupt_outcome_unknown)?;
        let _ = self
            .append_event(
                RuntimeEventScope::Session,
                &session.id,
                Some(&session.id),
                Some(turn_id),
                "turn.interrupt_requested",
                RuntimeEventCriticality::Critical,
                payload,
            )
            .await
            .map_err(interrupt_outcome_unknown)?;
        Ok(())
    }
}

fn interrupt_outcome_unknown(error: RuntimeError) -> RuntimeError {
    RuntimeError::provider_dispatch_unknown(
        "interrupt_outcome_unknown",
        format!("native interrupt acknowledgement or durable receipt is unavailable: {error}"),
    )
}
