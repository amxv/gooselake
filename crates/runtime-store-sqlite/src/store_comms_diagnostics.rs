macro_rules! delegate_comms_diagnostic_reads {
    () => {
        fn legacy_comms_diagnostic_counts(
            &self,
        ) -> Result<Option<runtime_core::LegacyCommsDiagnosticCounts>, RuntimeError> {
            self.repository.legacy_comms_diagnostic_counts().map(Some)
        }
    };
}

pub(crate) use delegate_comms_diagnostic_reads;
