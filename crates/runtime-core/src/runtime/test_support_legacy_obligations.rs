//! Provider test fixture has no persisted historical spawn authority.
macro_rules! mock_legacy_obligation_methods {
    () => {
        fn classify_legacy_spawn_obligations(
            &self,
            targets: &[crate::legacy_spawn_obligations::LegacyNativeTarget],
            _budget: crate::legacy_spawn_obligations::LegacySpawnScanBudget,
        ) -> Result<crate::legacy_spawn_obligations::LegacySpawnScan, crate::RuntimeError> {
            use crate::legacy_spawn_obligations::*;
            // This provider-only fixture has no historical spawn persistence.
            // Real SQLite tests own all legacy history/classifier authority.
            Ok(LegacySpawnScan {
                decisions: vec![LegacyTargetDecision::Clear; targets.len()],
                termination: LegacyScanTermination::ExhaustedBothStreams,
                stats: LegacySpawnScanStats::default(),
            })
        }

        fn upsert_team_operation_journal(
            &self,
            _record: &crate::TeamOperationJournalRecord,
        ) -> Result<(), crate::RuntimeError> {
            Ok(())
        }

        fn append_team_operation_diagnostic(
            &self,
            _operation_id: Option<&str>,
            _team_id: Option<&str>,
            _code: &str,
            _message: &str,
            _payload: &Value,
            _created_at: i64,
        ) -> Result<crate::TeamOperationDiagnosticRecord, crate::RuntimeError> {
            Ok(crate::TeamOperationDiagnosticRecord {
                id: 1,
                operation_id: None,
                team_id: None,
                code: "stub".to_string(),
                message: "stub".to_string(),
                payload: serde_json::json!({}),
                created_at: 0,
            })
        }

        fn list_team_operation_journal(
            &self,
            _team_id: Option<&str>,
        ) -> Result<Vec<crate::TeamOperationJournalRecord>, crate::RuntimeError> {
            Ok(Vec::new())
        }

        fn list_team_operation_diagnostics(
            &self,
            _team_id: Option<&str>,
            _operation_id: Option<&str>,
        ) -> Result<Vec<crate::TeamOperationDiagnosticRecord>, crate::RuntimeError> {
            Ok(Vec::new())
        }
    };
}
pub(super) use mock_legacy_obligation_methods;
