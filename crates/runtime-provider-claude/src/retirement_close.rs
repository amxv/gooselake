use runtime_core::{
    ProviderCloseSessionRequest, ProviderRetirementCloseEvidence, ProviderRetirementCloseProofKind,
    RuntimeError,
};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::{bridge::send_bridge_request, ClaudeProvider};

impl ClaudeProvider {
    pub(crate) async fn close_retiring_session(
        &self,
        req: ProviderCloseSessionRequest,
    ) -> Result<ProviderRetirementCloseEvidence, RuntimeError> {
        let session = self.get_session(&req.runtime_session_id).await?;
        let _operation = session.operation_lock.lock().await;
        crate::advanced::ensure_idle(&session).await?;
        let native_ref = session.provider_session_ref.read().await.clone();
        if native_ref.trim().is_empty() {
            return Err(RuntimeError::Conflict(
                "Claude native attachment identity is missing during retirement".into(),
            ));
        }

        // This distinct RPC fails closed with old bridges. Ordinary session.close
        // may ignore interrupt errors and cannot prove absence of native work.
        let response = send_bridge_request(
            &self.inner,
            &session.bridge,
            "session.close_for_retirement",
            json!({"sessionId": session.bridge_session_id, "reason": req.reason}),
            self.inner.config.request_timeout_ms,
        )
        .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                session.quarantined.store(true, Ordering::SeqCst);
                return Err(error);
            }
        };
        if response.get("ok").and_then(Value::as_bool) != Some(true)
            || response.get("sessionId").and_then(Value::as_str)
                != Some(session.bridge_session_id.as_str())
            || response.get("providerSessionRef").and_then(Value::as_str)
                != Some(native_ref.as_str())
        {
            session.quarantined.store(true, Ordering::SeqCst);
            return Err(RuntimeError::ProtocolViolation(
                "Claude bridge did not confirm closure of the admitted native attachment".into(),
            ));
        }
        {
            let mut sessions = self.inner.sessions.write().await;
            if !sessions
                .get(&req.runtime_session_id)
                .is_some_and(|current| Arc::ptr_eq(current, &session))
            {
                session.quarantined.store(true, Ordering::SeqCst);
                return Err(RuntimeError::Conflict(
                    "Claude native attachment changed during retirement close".into(),
                ));
            }
            session.quarantined.store(true, Ordering::SeqCst);
            sessions.remove(&req.runtime_session_id);
            self.inner.sessions_by_bridge_key.write().await.remove(
                &crate::auth::bridge_session_key(
                    session.bridge.instance_id,
                    &session.bridge_session_id,
                ),
            );
        }
        self.shutdown_bridges_if_idle().await;
        Ok(ProviderRetirementCloseEvidence {
            runtime_session_id: req.runtime_session_id,
            provider_session_ref: native_ref,
            proof_kind: ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged,
        })
    }
}
