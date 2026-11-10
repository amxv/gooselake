// Keep the store's context-snapshot default methods separate from the large
// trait declaration. Backends without batch support retain point-query parity.
macro_rules! default_context_limit_methods {
    () => {
        fn record_session_context_limit(
            &self,
            _snapshot: &crate::SessionContextLimitSnapshot,
            _expected_session_updated_at: i64,
        ) -> Result<bool, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "durable provider context snapshots are not implemented by this runtime store"
                    .into(),
            ))
        }

        fn get_session_context_limit(
            &self,
            _agent_id: &str,
        ) -> Result<Option<crate::SessionContextLimitSnapshot>, crate::RuntimeError> {
            Ok(None)
        }

        /// Only current provider-attachment-fenced context observations;
        /// SQLite overrides this fallback with one bounded indexed query.
        fn list_session_context_limits(
            &self,
            agent_ids: &[String],
        ) -> Result<Vec<crate::SessionContextLimitSnapshot>, crate::RuntimeError> {
            agent_ids
                .iter()
                .map(|id| self.get_session_context_limit(id))
                .filter_map(|result| result.transpose())
                .collect()
        }
    };
}

pub(crate) use default_context_limit_methods;
