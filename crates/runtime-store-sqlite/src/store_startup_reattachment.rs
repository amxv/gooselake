macro_rules! delegate_startup_reattachment {
    () => {
        fn settle_startup_operator_turn(
            &self,
            session_id: &str,
            at_ms: i64,
        ) -> Result<Option<runtime_core::StartupPendingOperatorTurn>, runtime_core::RuntimeError> {
            self.repository
                .settle_startup_operator_turn(session_id, at_ms)
        }
        fn admit_startup_reattach(
            &self,
            command: &runtime_core::StartupReattachCommand,
            at_ms: i64,
        ) -> Result<runtime_core::StartupReattachAdmission, runtime_core::RuntimeError> {
            self.repository.admit_startup_reattach(command, at_ms)
        }
        fn complete_startup_reattach(
            &self,
            grant: &runtime_core::StartupReattachGrant,
            ack: &runtime_core::ProviderSession,
            at_ms: i64,
        ) -> Result<runtime_core::StartupReattachCompletion, runtime_core::RuntimeError> {
            self.repository.complete_startup_reattach(grant, ack, at_ms)
        }
        fn hold_startup_reattach(
            &self,
            grant: &runtime_core::StartupReattachGrant,
            ack: Option<&runtime_core::ProviderSession>,
            reason: &str,
            at_ms: i64,
        ) -> Result<runtime_core::StartupReattachHold, runtime_core::RuntimeError> {
            self.repository
                .hold_startup_reattach(grant, ack, reason, at_ms)
        }
    };
}
pub(super) use delegate_startup_reattachment;
