use super::*;

impl TestClaudeProvider {
    pub(crate) fn permission_mutation_calls(&self) -> usize {
        self.permission_mutation_calls
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn set_permission_mutation_mode(&self, mode: u8) {
        self.permission_mutation_mode
            .store(mode, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) async fn fake_claude_mutate_permission(
        &self,
        request: runtime_core::ProviderPermissionMutationRequest,
    ) -> Result<runtime_core::ProviderPermissionMutationResult, RuntimeError> {
        self.permission_mutation_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut state = self.state.lock().await;
        let session = state
            .sessions
            .get_mut(request.runtime_session_id.as_str())
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("session {}", request.runtime_session_id))
            })?;
        session.current_permission_intent = request.permission_intent.clone();
        if self
            .permission_mutation_mode
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
        {
            return Err(RuntimeError::provider_dispatch_unknown(
                "test_claude_permission_ack_lost",
                "fake Claude native permission changed before losing acknowledgement",
            ));
        }
        Ok(runtime_core::ProviderPermissionMutationResult {
            revision: 0,
            permission_intent: request.permission_intent,
        })
    }
}
