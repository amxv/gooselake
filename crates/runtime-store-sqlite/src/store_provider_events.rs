macro_rules! delegate_provider_event_methods {
    () => {
        fn commit_provider_turn_result(
            &self,
            previous_turn: &runtime_core::TurnRecord,
            updated_turn: &runtime_core::TurnRecord,
            previous_session: Option<&runtime_core::SessionRecord>,
            updated_session: Option<&runtime_core::SessionRecord>,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::ProviderTerminalCommit, runtime_core::RuntimeError> {
            self.repository.commit_provider_turn_result(
                previous_turn,
                updated_turn,
                previous_session,
                updated_session,
                event,
            )
        }

        fn admit_provider_approval(
            &self,
            previous_turn: &runtime_core::TurnRecord,
            updated_turn: &runtime_core::TurnRecord,
            previous_session: &runtime_core::SessionRecord,
            updated_session: &runtime_core::SessionRecord,
            approval: &runtime_core::ApprovalRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::ProviderApprovalAdmission, runtime_core::RuntimeError> {
            self.repository.admit_provider_approval(
                previous_turn,
                updated_turn,
                previous_session,
                updated_session,
                approval,
                event,
            )
        }

        fn claim_provider_approval_decision(
            &self,
            pending: &runtime_core::ApprovalRecord,
            reserved: &runtime_core::ApprovalRecord,
            turn: &runtime_core::TurnRecord,
            session: &runtime_core::SessionRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::ProviderApprovalDecisionReceipt, runtime_core::RuntimeError> {
            self.repository
                .claim_provider_approval_decision(pending, reserved, turn, session, event)
        }

        fn release_unexecuted_provider_approval_decision(
            &self,
            reserved: &runtime_core::ApprovalRecord,
            pending: &runtime_core::ApprovalRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::ProviderApprovalDecisionReceipt, runtime_core::RuntimeError> {
            self.repository
                .release_unexecuted_provider_approval_decision(reserved, pending, event)
        }
    };
}
pub(super) use delegate_provider_event_methods;
