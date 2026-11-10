// Keep the SQLite context-status projection's trait delegates outside the
// 1000-line RuntimeStore implementation, matching other store delegate groups.
macro_rules! delegate_context_limit_methods {
    () => {
        fn record_session_context_limit(
            &self,
            snapshot: &runtime_core::SessionContextLimitSnapshot,
            expected_session_updated_at: i64,
        ) -> Result<bool, runtime_core::RuntimeError> {
            self.repository
                .record_session_context_limit(snapshot, expected_session_updated_at)
        }

        fn get_session_context_limit(
            &self,
            agent_id: &str,
        ) -> Result<Option<runtime_core::SessionContextLimitSnapshot>, runtime_core::RuntimeError> {
            self.repository.get_session_context_limit(agent_id)
        }

        fn list_session_context_limits(
            &self,
            agent_ids: &[String],
        ) -> Result<Vec<runtime_core::SessionContextLimitSnapshot>, runtime_core::RuntimeError> {
            self.repository.list_session_context_limits(agent_ids)
        }
    };
}

pub(crate) use delegate_context_limit_methods;
