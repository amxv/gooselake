// Optional counts-only legacy team view. In-memory/other stores retain the
// existing hydrated semantics; unsupported is never interpreted as zero.
macro_rules! default_comms_diagnostic_reads {
    () => {
        fn legacy_comms_diagnostic_counts(
            &self,
        ) -> Result<Option<crate::LegacyCommsDiagnosticCounts>, crate::RuntimeError> {
            Ok(None)
        }
    };
}

pub(super) use default_comms_diagnostic_reads;
