use super::*;
use std::sync::atomic::Ordering;

impl TestProvider {
    pub(crate) fn set_edit_mode(&self, mode: u8) {
        self.edit_mode.store(mode, Ordering::SeqCst);
    }

    pub(crate) fn edit_calls(&self) -> usize {
        self.edit_calls.load(Ordering::SeqCst)
    }

    pub(crate) async fn captured_edit_inputs(&self) -> Vec<Vec<serde_json::Value>> {
        self.edit_inputs.lock().await.clone()
    }

    pub(crate) async fn fake_observe_session(
        &self,
        agent_id: &str,
    ) -> Result<Option<ProviderSession>, RuntimeError> {
        if self.edit_mode.load(Ordering::SeqCst) == 0 {
            return Ok(None);
        }
        let state = self.state.lock().await;
        let session = state
            .sessions
            .get(agent_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("fake provider session {agent_id}")))?;
        Ok(Some(ProviderSession {
            runtime_session_id: agent_id.into(),
            provider_session_ref: session.provider_session_ref.clone(),
            canonical_provider_session_ref: Some(session.provider_session_ref.clone()),
        }))
    }

    pub(crate) async fn fake_restore_mapping(
        &self,
        agent_id: &str,
        logical_turn_id: &str,
        native_turn_id: &str,
    ) -> Result<(), RuntimeError> {
        let state = self.state.lock().await;
        let session = state
            .sessions
            .get(agent_id)
            .ok_or_else(|| RuntimeError::NotFound(format!("fake provider session {agent_id}")))?;
        if self.edit_mode.load(Ordering::SeqCst) == 0
            || native_turn_id != format!("native-{logical_turn_id}")
            || !session.completed.contains_key(logical_turn_id)
        {
            return Err(RuntimeError::ProtocolViolation(
                "historical native turn mapping mismatch".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn fake_hard_fork(
        &self,
        req: runtime_core::ProviderHardForkEditRerunRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        self.edit_calls.fetch_add(1, Ordering::SeqCst);
        if req.edited_input.is_empty() {
            return Err(RuntimeError::InvalidState(
                "edited provider input is empty".into(),
            ));
        }
        self.edit_inputs.lock().await.push(req.edited_input);
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(&req.runtime_session_id)
            .ok_or_else(|| RuntimeError::NotFound("provider session missing".into()))?;
        if !session.completed.contains_key(&req.target_turn_id) {
            return Err(RuntimeError::NotFound(
                "historical native target is missing".into(),
            ));
        }
        match self.edit_mode.load(Ordering::SeqCst) {
            1 => {
                session.provider_session_ref = format!(
                    "child-of-{}-{}",
                    session.provider_session_ref, req.target_turn_id
                );
                session.history.clear();
                Ok(ProviderSession {
                    runtime_session_id: req.runtime_session_id,
                    provider_session_ref: session.provider_session_ref.clone(),
                    canonical_provider_session_ref: Some(session.provider_session_ref.clone()),
                })
            }
            2 => {
                session.provider_session_ref = format!("unknown-child-{}", req.target_turn_id);
                Err(RuntimeError::provider_dispatch_unknown(
                    "fake_fork_outcome_unknown",
                    "fake native fork may have happened",
                ))
            }
            _ => Err(RuntimeError::Unsupported(
                "fake native fork disabled".into(),
            )),
        }
    }
}

impl TestProvider {
    pub(crate) fn enable_thinking_mutation(&self) {
        self.supports_thinking
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) async fn observed_thinking_efforts(
        &self,
        session_id: &str,
    ) -> Vec<Option<runtime_core::ProviderThinkingEffort>> {
        self.state
            .lock()
            .await
            .sessions
            .get(session_id)
            .map(|session| session.seen_thinking_efforts.clone())
            .unwrap_or_default()
    }

    pub(crate) async fn set_rebind_mode(&self, mode: Option<&str>) {
        *self.rebind_mode.lock().await = mode.map(str::to_string);
    }

    pub(crate) async fn rebind_calls(&self) -> usize {
        self.rebind_calls.lock().await.len()
    }

    pub(crate) fn extract_text(input: &[serde_json::Value]) -> String {
        for item in input {
            if let Some(text) = item.get("text").and_then(serde_json::Value::as_str) {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }
        "empty".to_string()
    }

    pub(crate) async fn resumed_requests(&self) -> Vec<ProviderResumeSessionRequest> {
        self.resumed_requests.lock().await.clone()
    }
}
