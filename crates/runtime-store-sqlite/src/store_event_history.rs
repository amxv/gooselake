macro_rules! delegate_event_history {
    () => {
        fn list_operator_events_page(
            &self,
            scope: Option<(runtime_core::RuntimeEventScope, &str)>,
            after_id: Option<i64>,
            limit: usize,
        ) -> Result<Vec<runtime_core::OperatorEventSummary>, runtime_core::RuntimeError> {
            self.repository
                .list_operator_events_page(scope, after_id, limit)
        }

        fn get_operator_event(
            &self,
            event_id: &str,
        ) -> Result<Option<runtime_core::RuntimeEventRecord>, runtime_core::RuntimeError> {
            self.repository.get_operator_event(event_id)
        }
    };
}

pub(crate) use delegate_event_history;
