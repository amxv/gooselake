// Synchronous retirement forwards remain part of the single RuntimeStore implementation.
macro_rules! delegate_workspace_retirement {
    () => {
        fn retire_empty_workspace(
            &self,
            command: &runtime_core::WorkspaceRetirementCommand,
        ) -> Result<runtime_core::WorkspaceRetirementResponse, runtime_core::RuntimeError> {
            self.repository.retire_empty_workspace(command)
        }
        fn admit_workspace_retirement(
            &self,
            command: &runtime_core::WorkspaceRetirementCommand,
        ) -> Result<runtime_core::WorkspaceRetirementAdmission, runtime_core::RuntimeError> {
            self.repository.admit_workspace_retirement(command)
        }
        fn reconcile_workspace_retirement(
            &self,
            command: &runtime_core::WorkspaceRetirementReviewCommand,
        ) -> Result<runtime_core::WorkspaceRetirementReviewOutcome, runtime_core::RuntimeError> {
            self.repository.reconcile_workspace_retirement(command)
        }
        fn claim_workspace_retirement_native_close(
            &self,
            operation_id: &str,
            agent_id: &str,
            at_ms: i64,
        ) -> Result<runtime_core::WorkspaceRetirementCloseAttempt, runtime_core::RuntimeError> {
            self.repository
                .claim_workspace_retirement_native_close(operation_id, agent_id, at_ms)
        }
        fn finalize_workspace_retirement_native_close(
            &self,
            attempt: &runtime_core::WorkspaceRetirementCloseAttempt,
            evidence: &runtime_core::ProviderRetirementCloseEvidence,
            at_ms: i64,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository
                .finalize_workspace_retirement_native_close(attempt, evidence, at_ms)
        }
        fn quarantine_workspace_retirement_native_close(
            &self,
            attempt: &runtime_core::WorkspaceRetirementCloseAttempt,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository
                .quarantine_workspace_retirement_native_close(attempt, reason, at_ms)
        }
        fn settle_passive_workspace_retirement(
            &self,
            operation_id: &str,
            at_ms: i64,
        ) -> Result<runtime_core::WorkspaceRetirementPlan, runtime_core::RuntimeError> {
            self.repository
                .settle_passive_workspace_retirement(operation_id, at_ms)
        }
        fn finalize_settled_workspace_retirement(
            &self,
            operation_id: &str,
            at_ms: i64,
        ) -> Result<Option<runtime_core::WorkspaceRetirementResponse>, runtime_core::RuntimeError> {
            self.repository
                .finalize_settled_workspace_retirement(operation_id, at_ms)
        }
        fn progress_workspace_retirement_turn(
            &self,
            operation_id: &str,
            turn_id: &str,
            at_ms: i64,
        ) -> Result<runtime_core::WorkspaceRetirementTurnStep, runtime_core::RuntimeError> {
            self.repository
                .progress_workspace_retirement_turn(operation_id, turn_id, at_ms)
        }
        fn acknowledge_workspace_retirement_interrupt(
            &self,
            operation_id: &str,
            turn_id: &str,
            at_ms: i64,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository
                .acknowledge_workspace_retirement_interrupt(operation_id, turn_id, at_ms)
        }
        fn quarantine_workspace_retirement_interrupt(
            &self,
            operation_id: &str,
            turn_id: &str,
            reason: &str,
            at_ms: i64,
        ) -> Result<(), runtime_core::RuntimeError> {
            self.repository.quarantine_workspace_retirement_interrupt(
                operation_id,
                turn_id,
                reason,
                at_ms,
            )
        }
        fn retiring_operation_for_session(
            &self,
            session_id: &str,
        ) -> Result<Option<String>, runtime_core::RuntimeError> {
            self.repository.retiring_operation_for_session(session_id)
        }
        fn workspace_retirement_preflight(
            &self,
            workspace_id: &str,
        ) -> Result<runtime_core::WorkspaceRetirementPreflight, runtime_core::RuntimeError> {
            self.repository.workspace_retirement_preflight(workspace_id)
        }
    };
}

pub(crate) use delegate_workspace_retirement;
