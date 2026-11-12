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

        fn commit_provider_failure_result(
            &self,
            previous_turn: &runtime_core::TurnRecord,
            updated_turn: &runtime_core::TurnRecord,
            previous_session: &runtime_core::SessionRecord,
            updated_session: &runtime_core::SessionRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::ProviderTerminalCommit, runtime_core::RuntimeError> {
            self.repository.commit_provider_failure_result(
                previous_turn,
                updated_turn,
                previous_session,
                updated_session,
                event,
            )
        }

        fn commit_session_lifecycle(
            &self,
            before: Option<&runtime_core::SessionRecord>,
            after: &runtime_core::SessionRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<
            (
                runtime_core::SessionRecord,
                Option<runtime_core::RuntimeEventRecord>,
            ),
            runtime_core::RuntimeError,
        > {
            self.repository
                .commit_session_lifecycle(before, after, event)
        }

        fn commit_legacy_team_mutation(
            &self,
            mutation: &runtime_core::LegacyTeamMutation,
        ) -> Result<runtime_core::RuntimeEventRecord, runtime_core::RuntimeError> {
            self.repository.commit_legacy_team_mutation(mutation)
        }

        fn commit_legacy_team_deletion(
            &self,
            before: &runtime_core::TeamRecord,
            after: &runtime_core::TeamRecord,
            all_deliveries: &[runtime_core::TeamDeliveryRecord],
            cancelled_deliveries: &[runtime_core::TeamDeliveryRecord],
            events: &[runtime_core::NewRuntimeEvent],
        ) -> Result<runtime_core::TeamRecord, runtime_core::RuntimeError> {
            self.repository.commit_legacy_team_deletion(
                before,
                after,
                all_deliveries,
                cancelled_deliveries,
                events,
            )
        }

        fn commit_legacy_team_message_admission(
            &self,
            message: &runtime_core::TeamMessageRecord,
            deliveries: &[runtime_core::TeamDeliveryRecord],
            events: &[runtime_core::NewRuntimeEvent],
        ) -> Result<runtime_core::TeamMessageAck, runtime_core::RuntimeError> {
            self.repository
                .commit_legacy_team_message_admission(message, deliveries, events)
        }

        fn commit_legacy_team_message_cancellation(
            &self,
            message: &runtime_core::TeamMessageRecord,
            before: &[runtime_core::TeamDeliveryRecord],
            after: &[runtime_core::TeamDeliveryRecord],
            events: &[runtime_core::NewRuntimeEvent],
        ) -> Result<Vec<runtime_core::TeamDeliveryRecord>, runtime_core::RuntimeError> {
            self.repository
                .commit_legacy_team_message_cancellation(message, before, after, events)
        }

        fn get_legacy_team_message_cancellation_receipt(
            &self,
            message: &runtime_core::TeamMessageRecord,
        ) -> Result<Option<Vec<runtime_core::TeamDeliveryRecord>>, runtime_core::RuntimeError> {
            self.repository
                .get_legacy_team_message_cancellation_receipt(message)
        }

        fn claim_legacy_team_delivery_injection(
            &self,
            previous: &runtime_core::TeamDeliveryRecord,
            injecting: &runtime_core::TeamDeliveryRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<bool, runtime_core::RuntimeError> {
            self.repository
                .claim_legacy_team_delivery_injection(previous, injecting, event)
        }

        fn commit_legacy_team_delivery_pre_native_transition(
            &self,
            before: &runtime_core::TeamDeliveryRecord,
            after: &runtime_core::TeamDeliveryRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::TeamDeliveryRecord, runtime_core::RuntimeError> {
            self.repository
                .commit_legacy_team_delivery_pre_native_transition(before, after, event)
        }

        fn commit_legacy_team_delivery_injected(
            &self,
            before: &runtime_core::TeamDeliveryRecord,
            injected: &runtime_core::TeamDeliveryRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::TeamDeliveryRecord, runtime_core::RuntimeError> {
            self.repository
                .commit_legacy_team_delivery_injected(before, injected, event)
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

        fn finalize_provider_approval_decision(
            &self,
            reserved: &runtime_core::ApprovalRecord,
            resolved: &runtime_core::ApprovalRecord,
            before_turn: &runtime_core::TurnRecord,
            after_turn: &runtime_core::TurnRecord,
            before_session: &runtime_core::SessionRecord,
            after_session: &runtime_core::SessionRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::ProviderApprovalFinalization, runtime_core::RuntimeError> {
            self.repository.finalize_provider_approval_decision(
                reserved,
                resolved,
                before_turn,
                after_turn,
                before_session,
                after_session,
                event,
            )
        }

        fn quarantine_provider_approval_decision(
            &self,
            reserved: &runtime_core::ApprovalRecord,
            before_turn: &runtime_core::TurnRecord,
            after_turn: &runtime_core::TurnRecord,
            before_session: &runtime_core::SessionRecord,
            after_session: &runtime_core::SessionRecord,
            event: &runtime_core::NewRuntimeEvent,
        ) -> Result<runtime_core::ProviderApprovalFinalization, runtime_core::RuntimeError> {
            self.repository.quarantine_provider_approval_decision(
                reserved,
                before_turn,
                after_turn,
                before_session,
                after_session,
                event,
            )
        }
    };
}
pub(super) use delegate_provider_event_methods;
