macro_rules! startup_reattachment_store_methods {
    () => {
        fn settle_startup_operator_turn(
            &self,
            _session_id: &str,
            _at_ms: i64,
        ) -> Result<Option<crate::StartupPendingOperatorTurn>, crate::RuntimeError> {
            unsupported_store("durable startup parent settlement unavailable")
        }
        fn admit_startup_reattach(
            &self,
            _command: &crate::StartupReattachCommand,
            _at_ms: i64,
        ) -> Result<crate::StartupReattachAdmission, crate::RuntimeError> {
            unsupported_store("durable startup native admission unavailable")
        }
        fn complete_startup_reattach(
            &self,
            _grant: &crate::StartupReattachGrant,
            _ack: &crate::ProviderSession,
            _at_ms: i64,
        ) -> Result<crate::StartupReattachCompletion, crate::RuntimeError> {
            unsupported_store("durable startup native receipt unavailable")
        }
        fn hold_startup_reattach(
            &self,
            _grant: &crate::StartupReattachGrant,
            _ack: Option<&crate::ProviderSession>,
            _reason: &str,
            _at_ms: i64,
        ) -> Result<crate::StartupReattachHold, crate::RuntimeError> {
            unsupported_store("durable startup native hold unavailable")
        }
    };
}
pub(super) use startup_reattachment_store_methods;
