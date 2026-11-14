macro_rules! delegate_recovery_diagnostic_reads {
    () => {
        fn recovery_diagnostic_anomalies(&self) -> Result<Option<Vec<String>>, RuntimeError> {
            self.repository.recovery_diagnostic_anomalies().map(Some)
        }
    };
}

pub(crate) use delegate_recovery_diagnostic_reads;
