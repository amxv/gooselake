use runtime_core::{
    ProviderCloseSessionRequest, ProviderRetirementCloseEvidence, ProviderRetirementCloseProofKind,
    RuntimeError,
};

use crate::CodexProvider;

impl CodexProvider {
    pub(super) async fn close_retiring_session(
        &self,
        req: ProviderCloseSessionRequest,
    ) -> Result<ProviderRetirementCloseEvidence, RuntimeError> {
        // Keep the attachment stable and prevent a new turn from being admitted
        // between the idle check and confirmed app-server process termination.
        let _admission = self.inner.admission_lock.lock().await;
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions.get(&req.runtime_session_id).ok_or_else(|| {
            RuntimeError::Conflict(
                "Codex native attachment missing; retirement requires reconciliation".into(),
            )
        })?;
        if session.active_turn_id.is_some() || !session.pending_approvals.is_empty() {
            return Err(RuntimeError::Conflict(
                "Codex native work remains active during retirement".into(),
            ));
        }
        let native_ref = session.provider_session_ref.clone();
        if native_ref.trim().is_empty() {
            return Err(RuntimeError::Conflict(
                "Codex native attachment identity is missing during retirement".into(),
            ));
        }
        session.transport.shutdown_verified().await?;
        sessions.remove(&req.runtime_session_id);
        Ok(ProviderRetirementCloseEvidence {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: native_ref,
            proof_kind: ProviderRetirementCloseProofKind::NativeProcessTerminated,
        })
    }
}
