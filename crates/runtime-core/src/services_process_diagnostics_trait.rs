// Optional SQLite-backed read projections; a legacy/runtime-memory store can
// retain its existing full-list behavior. Unsupported is not an empty count.
macro_rules! default_managed_process_reads {
    () => {
        fn list_managed_processes(
            &self,
        ) -> Result<Vec<crate::ManagedProcessRecord>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "managed-process authority is not implemented by this runtime store".into(),
            ))
        }

        fn managed_process_status_counts(
            &self,
        ) -> Result<Option<std::collections::BTreeMap<String, usize>>, crate::RuntimeError> {
            Ok(None)
        }

        fn list_managed_processes_page(
            &self,
            _workspace_id: Option<&str>,
            _cursor: Option<(i64, String)>,
            _limit: usize,
        ) -> Result<Vec<crate::OperatorProcessSummary>, crate::RuntimeError> {
            Err(crate::RuntimeError::Unsupported(
                "indexed operator process history unavailable".into(),
            ))
        }
    };
}

pub(super) use default_managed_process_reads;
