macro_rules! delegate_session_control_methods {
    () => {
        fn begin_session_edit_rerun(
            &self,
            operation: &SessionEditRerunOperation,
        ) -> Result<SessionEditRerunOperation, RuntimeError> {
            self.repository.begin_session_edit_rerun(operation)
        }
        fn get_session_edit_rerun(
            &self,
            operation_id: &str,
        ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
            self.repository.get_session_edit_rerun(operation_id)
        }
        fn get_session_edit_rerun_by_key(
            &self,
            agent_id: &str,
            key: &str,
        ) -> Result<Option<SessionEditRerunOperation>, RuntimeError> {
            self.repository.get_session_edit_rerun_by_key(agent_id, key)
        }
        fn unresolved_session_edit_rerun(&self, agent_id: &str) -> Result<bool, RuntimeError> {
            self.repository.unresolved_session_edit_rerun(agent_id)
        }
        fn advance_session_edit_rerun(
            &self,
            operation_id: &str,
            phase: &str,
            child: Option<&runtime_core::ProviderSession>,
            replacement_turn_id: Option<&str>,
            error_code: Option<&str>,
            now: i64,
        ) -> Result<SessionEditRerunOperation, RuntimeError> {
            self.repository.advance_session_edit_rerun(
                operation_id,
                phase,
                child,
                replacement_turn_id,
                error_code,
                now,
            )
        }
        fn recover_session_edit_reruns(
            &self,
            now: i64,
        ) -> Result<Vec<SessionEditRerunOperation>, RuntimeError> {
            self.repository.recover_session_edit_reruns(now)
        }
        fn resolve_session_edit_rerun_review(
            &self,
            operation_id: &str,
            evidence: &str,
            now: i64,
        ) -> Result<SessionEditRerunOperation, RuntimeError> {
            self.repository
                .resolve_session_edit_rerun_review(operation_id, evidence, now)
        }
        fn begin_session_compaction(
            &self,
            run: &SessionCompactionRun,
        ) -> Result<CompactionTransitionReceipt, RuntimeError> {
            self.repository.begin_session_compaction_with_receipt(run)
        }
        fn transition_session_compaction(
            &self,
            run_id: &str,
            phase: &str,
            outcome: Option<runtime_core::ProviderCompactSessionOutcome>,
            error_code: Option<&str>,
            updated_at_ms: i64,
        ) -> Result<CompactionTransitionReceipt, RuntimeError> {
            self.repository.transition_session_compaction_with_receipt(
                run_id,
                phase,
                outcome,
                error_code,
                updated_at_ms,
            )
        }
        fn get_session_compaction(
            &self,
            run_id: &str,
        ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
            self.repository.get_session_compaction(run_id)
        }
        fn observe_provider_compaction(
            &self,
            observation: &ProviderCompactionObservation,
        ) -> Result<CompactionObservationReceipt, RuntimeError> {
            self.repository.observe_provider_compaction(observation)
        }
        fn get_session_compaction_by_key(
            &self,
            agent_id: &str,
            key: &str,
        ) -> Result<Option<SessionCompactionRun>, RuntimeError> {
            self.repository.get_session_compaction_by_key(agent_id, key)
        }
        fn unresolved_session_compaction(&self, agent_id: &str) -> Result<bool, RuntimeError> {
            self.repository.unresolved_session_compaction(agent_id)
        }
        fn recover_session_compactions(
            &self,
            now_ms: i64,
        ) -> Result<Vec<CompactionTransitionReceipt>, RuntimeError> {
            self.repository
                .recover_session_compactions_with_receipts(now_ms)
        }
        fn resolve_session_compaction_review(
            &self,
            run_id: &str,
            phase: &str,
            evidence: &str,
            now_ms: i64,
        ) -> Result<CompactionTransitionReceipt, RuntimeError> {
            self.repository
                .resolve_session_compaction_review_with_receipt(run_id, phase, evidence, now_ms)
        }
        fn record_session_control_legacy_recovery(
            &self,
            owner: &runtime_core::SessionControlCoordinate,
            recovery: &runtime_core::legacy_spawn_obligations::LegacyNativeRecovery,
            at_ms: i64,
        ) -> Result<Option<RuntimeEventRecord>, RuntimeError> {
            self.repository
                .record_session_control_legacy_recovery(owner, recovery, at_ms)
        }
        fn read_session_control_legacy_recovery(
            &self,
            owner: &runtime_core::SessionControlCoordinate,
        ) -> Result<
            Option<runtime_core::legacy_spawn_obligations::LegacyNativeRecovery>,
            RuntimeError,
        > {
            self.repository.read_session_control_legacy_recovery(owner)
        }
    };
}
pub(crate) use delegate_session_control_methods;
