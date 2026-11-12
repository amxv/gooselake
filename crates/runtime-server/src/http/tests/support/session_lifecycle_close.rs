use super::*;

impl TestProvider {
    pub(crate) fn allow_lifecycle_close_for_test(&self) {
        self.allow_lifecycle_close
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) async fn perform_test_session_close(
        &self,
        req: runtime_core::ProviderCloseSessionRequest,
    ) -> Result<(), RuntimeError> {
        if !self
            .allow_lifecycle_close
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(RuntimeError::Unsupported(
                "provider close_session is not supported".into(),
            ));
        }
        self.state
            .lock()
            .await
            .sessions
            .remove(&req.runtime_session_id);
        Ok(())
    }
}
