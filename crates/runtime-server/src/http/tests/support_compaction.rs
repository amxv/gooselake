use super::*;
use std::sync::atomic::Ordering;
use tokio::sync::broadcast;

impl TestProvider {
    pub(crate) fn set_compaction_mode(&self, mode: u8) {
        self.compaction_mode.store(mode, Ordering::SeqCst);
    }

    pub(crate) fn compaction_calls(&self) -> usize {
        self.compaction_calls.load(Ordering::SeqCst)
    }

    pub(super) fn compaction_event_receiver(
        &self,
    ) -> broadcast::Receiver<runtime_core::ProviderRuntimeEvent> {
        let mut slot = self.compaction_events.lock().expect("event mutex");
        slot.get_or_insert_with(|| broadcast::channel(32).0)
            .subscribe()
    }

    pub(crate) fn emit_compaction_event(&self, session_id: &str, phase: &str) {
        let slot = self.compaction_events.lock().expect("event mutex");
        if let Some(sender) = slot.as_ref() {
            let _ = sender.send(
                runtime_core::ProviderRuntimeEvent::ContextCompactionObserved {
                    runtime_session_id: session_id.into(),
                    turn_id: None,
                    phase: phase.into(),
                    trigger: Some("manual".into()),
                    pre_tokens: None,
                    post_tokens: None,
                    context_window_size: None,
                },
            );
        }
    }

    pub(super) fn run_test_compaction(
        &self,
        _req: runtime_core::ProviderCompactSessionRequest,
    ) -> Result<runtime_core::ProviderCompactSessionOutcome, RuntimeError> {
        self.compaction_calls.fetch_add(1, Ordering::SeqCst);
        match self.compaction_mode.load(Ordering::SeqCst) {
            1 => Ok(runtime_core::ProviderCompactSessionOutcome::Accepted),
            2 => Ok(runtime_core::ProviderCompactSessionOutcome::NotPerformed),
            3 => Err(RuntimeError::provider_dispatch_unknown(
                "fake_compaction_unknown",
                "test provider outcome is unknown",
            )),
            _ => Err(RuntimeError::Unsupported(
                "test provider manual compaction is disabled".into(),
            )),
        }
    }
}
