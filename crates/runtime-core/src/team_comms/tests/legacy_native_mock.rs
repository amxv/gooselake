//! This communication fixture persists journals but has no spawn admission
//! implementation. Only genuinely empty history can stand in for clearance.
macro_rules! empty_legacy_history_methods {
    () => {
        fn classify_legacy_spawn_obligations(
            &self,
            targets: &[crate::legacy_spawn_obligations::LegacyNativeTarget],
            _budget: crate::legacy_spawn_obligations::LegacySpawnScanBudget,
        ) -> Result<crate::legacy_spawn_obligations::LegacySpawnScan, crate::RuntimeError> {
            use crate::legacy_spawn_obligations::*;
            if !self.hydrated.lock().expect("hydrated lock").team_operation_journal.is_empty() {
                return Err(crate::RuntimeError::Unsupported(
                    "nonempty historical spawn evidence requires the real SQLite fixture classifier".into(),
                ));
            }
            Ok(LegacySpawnScan {
                decisions: vec![LegacyTargetDecision::Clear; targets.len()],
                termination: LegacyScanTermination::ExhaustedBothStreams,
                stats: LegacySpawnScanStats::default(),
            })
        }
    };
}
pub(super) use empty_legacy_history_methods;
