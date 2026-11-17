macro_rules! session_control_store_methods {
    () => {
        fn begin_session_edit_rerun(
            &self,
            _operation: &SessionEditRerunOperation,
        ) -> Result<crate::EditRerunTransitionReceipt, RuntimeError> {
            unsupported_store("edit-and-rerun persistence unavailable")
        }
        fn get_session_edit_rerun(
            &self,
            _operation_id: &str,
        ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
            Ok(None)
        }
        fn get_session_edit_rerun_by_key(
            &self,
            _agent_id: &str,
            _idempotency_key: &str,
        ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
            Ok(None)
        }
        fn unresolved_session_edit_rerun(&self, _agent_id: &str) -> Result<bool, RuntimeError> {
            Ok(false)
        }
        fn advance_session_edit_rerun(
            &self,
            _operation_id: &str,
            _phase: &str,
            _child: Option<&crate::ProviderSession>,
            _replacement_turn_id: Option<&str>,
            _error_code: Option<&str>,
            _now: i64,
        ) -> Result<crate::EditRerunTransitionReceipt, RuntimeError> {
            unsupported_store("edit-and-rerun transition unavailable")
        }
        fn recover_session_edit_reruns(
            &self,
            _now: i64,
        ) -> Result<Vec<crate::EditRerunTransitionReceipt>, RuntimeError> {
            Ok(Vec::new())
        }
        fn resolve_session_edit_rerun_review(
            &self,
            _operation_id: &str,
            _evidence: &str,
            _now: i64,
        ) -> Result<crate::EditRerunTransitionReceipt, RuntimeError> {
            unsupported_store("edit-and-rerun review unavailable")
        }
        fn begin_session_compaction(
            &self,
            _run: &SessionCompactionRun,
        ) -> Result<CompactionTransitionReceipt, RuntimeError> {
            unsupported_store("session compaction persistence unavailable")
        }
        fn transition_session_compaction(
            &self,
            _run_id: &str,
            _phase: &str,
            _outcome: Option<crate::ProviderCompactSessionOutcome>,
            _error_code: Option<&str>,
            _updated_at_ms: i64,
        ) -> Result<CompactionTransitionReceipt, RuntimeError> {
            unsupported_store("session compaction persistence unavailable")
        }
        fn get_session_compaction(
            &self,
            _run_id: &str,
        ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
            Ok(None)
        }
        fn observe_provider_compaction(
            &self,
            _observation: &ProviderCompactionObservation,
        ) -> Result<CompactionObservationReceipt, RuntimeError> {
            unsupported_store("provider compaction observation persistence unavailable")
        }
        fn get_session_compaction_by_key(
            &self,
            _agent_id: &str,
            _key: &str,
        ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
            Ok(None)
        }
        fn unresolved_session_compaction(&self, _agent_id: &str) -> Result<bool, RuntimeError> {
            Ok(false)
        }
        fn recover_session_compactions(
            &self,
            _now_ms: i64,
        ) -> Result<Vec<CompactionTransitionReceipt>, RuntimeError> {
            Ok(Vec::new())
        }
        fn resolve_session_compaction_review(
            &self,
            _run_id: &str,
            _phase: &str,
            _evidence: &str,
            _now_ms: i64,
        ) -> Result<CompactionTransitionReceipt, RuntimeError> {
            unsupported_store("compaction manual review unavailable")
        }
        fn record_session_control_legacy_recovery(
            &self,
            _owner: &crate::SessionControlCoordinate,
            _recovery: &crate::legacy_spawn_obligations::LegacyNativeRecovery,
            _at_ms: i64,
        ) -> Result<Option<RuntimeEventRecord>, RuntimeError> {
            unsupported_store("durable session-control recovery observation unavailable")
        }
        fn read_session_control_legacy_recovery(
            &self,
            _owner: &crate::SessionControlCoordinate,
        ) -> Result<Option<crate::legacy_spawn_obligations::LegacyNativeRecovery>, RuntimeError> {
            unsupported_store("verified session-control recovery observation unavailable")
        }
    };
}
pub(crate) use session_control_store_methods;
