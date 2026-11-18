macro_rules! delegate_event_history {
    () => {
        fn subscribe_event_commits(
            &self,
        ) -> Result<runtime_core::EventCommitSubscription, runtime_core::RuntimeError> {
            self.event_commits.subscribe(self.database_path())
        }
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

        fn list_operator_events_page_bound(
            &self,
            collection: &runtime_core::EventCollection,
            after: Option<&runtime_core::EventCursor>,
            limit: usize,
        ) -> Result<
            runtime_core::BoundEventPage<runtime_core::OperatorEventSummary>,
            runtime_core::EventReadError,
        > {
            self.repository
                .list_operator_events_page_bound(collection, after, limit)
        }
    };
}

pub(crate) use delegate_event_history;
