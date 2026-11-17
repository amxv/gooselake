use super::*;

impl TestProvider {
    pub(crate) fn hold_next_create(&self) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.create_barrier.lock().unwrap() = Some((entered.clone(), release.clone()));
        (entered, release)
    }

    pub(super) async fn perform_test_session_create(
        &self,
        req: ProviderCreateSessionRequest,
    ) -> Result<ProviderSession, RuntimeError> {
        use std::sync::atomic::Ordering;
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        let mode = self.create_failure_mode.load(Ordering::SeqCst);
        if mode == 4 {
            return Err(RuntimeError::provider_not_dispatched(
                "test_create_proven_absent",
                "provider refused native session creation",
            ));
        }
        let native_ref = format!("test-thread-{}", req.runtime_session_id);
        let canonical_ref = (self.edit_mode.load(std::sync::atomic::Ordering::SeqCst) != 0)
            .then(|| native_ref.clone());
        let mut state = self.state.lock().await;
        state.sessions.insert(
            req.runtime_session_id.clone(),
            TestProviderSession {
                provider_session_ref: native_ref.clone(),
                cwd: req.cwd,
                ..Default::default()
            },
        );
        drop(state);
        let barrier = self.create_barrier.lock().unwrap().take();
        if let Some((entered, release)) = barrier {
            entered.notify_one();
            release.notified().await;
        }
        if mode == 2 {
            return Err(RuntimeError::provider_dispatch_unknown(
                "test_create_ack_lost",
                "provider created the native session but lost its acknowledgement",
            ));
        }
        Ok(ProviderSession {
            runtime_session_id: if mode == 5 {
                format!("{}-wrong-reserved-runtime-id", req.runtime_session_id)
            } else {
                req.runtime_session_id.clone()
            },
            provider_session_ref: if mode == 3 {
                format!("{native_ref}-wrong-native-attachment")
            } else {
                native_ref
            },
            canonical_provider_session_ref: canonical_ref,
        })
    }
}
