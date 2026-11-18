// Keep the operator history/read-only extension alongside RuntimeStore's
// existing event APIs without expanding the oversized services.rs trait file.
macro_rules! event_history_methods {
    () => {
        /// Fallible generation-bound observation. Consumers await readiness,
        /// retain health, and replay only through the subscription's reader.
        fn subscribe_event_commits(
            &self,
        ) -> Result<crate::EventCommitSubscription, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable event observation is unavailable in this store".into(),
            ))
        }
        fn list_operator_events_page(
            &self,
            _scope: Option<(crate::RuntimeEventScope, &str)>,
            _after_id: Option<i64>,
            _limit: usize,
        ) -> Result<Vec<crate::OperatorEventSummary>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "indexed operator event history unavailable".into(),
            ))
        }

        fn get_operator_event(
            &self,
            _event_id: &str,
        ) -> Result<Option<crate::RuntimeEventRecord>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "operator event inspection unavailable".into(),
            ))
        }
    };
}

pub(super) use event_history_methods;
