// Optional read-only anomaly projection. Non-SQLite stores retain the exact
// prior hydrate-and-inspect behavior; None never means "no anomalies".
macro_rules! default_recovery_diagnostic_reads {
    () => {
        fn recovery_diagnostic_anomalies(
            &self,
        ) -> Result<Option<Vec<String>>, crate::RuntimeError> {
            Ok(None)
        }
    };
}

pub(super) use default_recovery_diagnostic_reads;
