use super::{helpers::now_ms, RuntimeSessionManager};
use crate::legacy_spawn_obligations::LegacyNativeRecovery;
use crate::{RuntimeError, SessionControlCoordinate};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ProviderDispatchOutcome;
    use crate::legacy_spawn_obligations::{LegacySpawnCoordinate, LegacyTargetBlockReason};
    use crate::runtime::test_support::manager_with_provider;

    #[test]
    fn family_error_preserves_only_explicit_original_dispatch_evidence() {
        let manager = manager_with_provider(0);
        let owner = SessionControlCoordinate::EditRerun {
            agent_id: "agent".into(),
            operation_id: "edit".into(),
        };
        for (error, expected) in [
            (
                RuntimeError::provider_not_dispatched("before", "declined"),
                Some(ProviderDispatchOutcome::NotDispatched),
            ),
            (
                RuntimeError::provider_dispatch_unknown("lost", "unconfirmed"),
                Some(ProviderDispatchOutcome::Unknown),
            ),
            (RuntimeError::Bootstrap("receipt unavailable".into()), None),
        ] {
            let wrapped = manager.session_control_error(&owner, "family_code", error, None);
            assert_eq!(
                wrapped.provider_dispatch_outcome(),
                expected.unwrap_or(ProviderDispatchOutcome::Unknown)
            );
            assert_eq!(wrapped.provider_dispatch_code(), Some("family_code"));
            assert!(
                matches!(wrapped, RuntimeError::SessionControlUnresolved { dispatch_outcome, .. } if dispatch_outcome == expected)
            );
        }
    }

    #[test]
    fn rewrapping_and_quarantine_failure_preserve_owner_cause_and_actual_turn() {
        let manager = manager_with_provider(0);
        let owner = SessionControlCoordinate::EditRerun {
            agent_id: "agent".into(),
            operation_id: "edit".into(),
        };
        let cause = LegacyNativeRecovery::Blocked {
            coordinate: LegacySpawnCoordinate {
                operation_id: "original-history".into(),
                team_id: "original-team".into(),
                stage: Some("rolled_back".into()),
                evidence_problem: None,
            },
            reason: LegacyTargetBlockReason::ExactSession,
        };
        let wrapped = manager.session_control_error_with_recovery(
            &owner,
            "original_code",
            RuntimeError::provider_dispatch_unknown("original_code", "native outcome unavailable"),
            Some(cause.clone()),
            Some("real-admitted-turn".into()),
        );
        let wrapped = manager.session_control_error_with_recovery(
            &owner,
            "original_code",
            wrapped,
            None,
            None,
        );
        let wrapped = manager.session_control_quarantine_failed(
            wrapped,
            RuntimeError::Bootstrap("quarantine receipt fault".into()),
        );
        assert_eq!(wrapped.legacy_native_recovery(), Some(cause));
        let RuntimeError::SessionControlUnresolved {
            coordinate,
            code,
            message,
            replacement_turn_id,
            dispatch_outcome,
            ..
        } = wrapped
        else {
            panic!("family owner lost");
        };
        assert_eq!(coordinate, owner);
        assert_eq!(code, "original_code");
        assert_eq!(replacement_turn_id.as_deref(), Some("real-admitted-turn"));
        assert_eq!(dispatch_outcome, Some(ProviderDispatchOutcome::Unknown));
        assert!(message.contains("quarantine receipt fault"));
        assert!(message.contains("could not be persisted"));
    }
}

impl RuntimeSessionManager {
    pub(super) fn session_control_error(
        &self,
        owner: &SessionControlCoordinate,
        code: &str,
        error: RuntimeError,
        replacement_turn_id: Option<String>,
    ) -> RuntimeError {
        let recovery = error.legacy_native_recovery();
        self.session_control_error_with_recovery(owner, code, error, recovery, replacement_turn_id)
    }

    /// The owner must already be admitted. Current typed evidence survives
    /// observation failure; an absent saved pair never authorizes retry.
    pub(super) fn session_control_error_with_recovery(
        &self,
        owner: &SessionControlCoordinate,
        code: &str,
        error: RuntimeError,
        mut recovery: Option<LegacyNativeRecovery>,
        replacement_turn_id: Option<String>,
    ) -> RuntimeError {
        recovery = recovery.or_else(|| error.legacy_native_recovery());
        let dispatch_outcome = match &error {
            RuntimeError::ProviderDispatch { .. }
            | RuntimeError::OperatorTurnNotDispatched { .. } => {
                Some(error.provider_dispatch_outcome())
            }
            RuntimeError::SessionControlUnresolved {
                dispatch_outcome, ..
            } => *dispatch_outcome,
            _ => None,
        };
        let replacement_turn_id = replacement_turn_id.or_else(|| match &error {
            RuntimeError::SessionControlUnresolved {
                replacement_turn_id,
                ..
            } => replacement_turn_id.clone(),
            _ => None,
        });
        let mut message = error.to_string();
        if let Some(current) = recovery.as_ref() {
            if let Err(failure) = self.persist_session_control_legacy_recovery(
                owner,
                current,
                replacement_turn_id.clone(),
            ) {
                message.push_str(&format!("; current legacy cause could not be persisted ({failure}); absence of saved evidence is not clearance"));
            }
        } else {
            match self.store.read_session_control_legacy_recovery(owner) {
                Ok(saved) => recovery = saved,
                Err(failure) => message.push_str(&format!(
                    "; original legacy observation inspection unavailable ({failure})"
                )),
            }
        }
        RuntimeError::SessionControlUnresolved {
            coordinate: owner.clone(),
            code: code.into(),
            message,
            legacy_recovery: recovery.map(Box::new),
            replacement_turn_id,
            dispatch_outcome,
        }
    }

    /// Completion cannot discard the current typed cause merely because the
    /// replacement turn already has durable admission authority.
    pub(super) fn persist_session_control_legacy_recovery(
        &self,
        owner: &SessionControlCoordinate,
        recovery: &LegacyNativeRecovery,
        replacement_turn_id: Option<String>,
    ) -> Result<(), RuntimeError> {
        match self.store.record_session_control_legacy_recovery(owner, recovery, now_ms()) {
            Ok(Some(event)) => { let _ = self.event_tx.send(event); Ok(()) }
            Ok(None) => Ok(()),
            Err(error) => Err(RuntimeError::SessionControlUnresolved {
                coordinate: owner.clone(),
                code: "session_control_legacy_receipt_unavailable".into(),
                message: format!("current legacy cause could not be persisted ({error}); native retry is not authorized"),
                legacy_recovery: Some(Box::new(recovery.clone())),
                replacement_turn_id,
                dispatch_outcome: None,
            }),
        }
    }

    pub(super) fn session_control_quarantine_failed(
        &self,
        mut error: RuntimeError,
        failure: RuntimeError,
    ) -> RuntimeError {
        if let RuntimeError::SessionControlUnresolved { message, .. } = &mut error {
            message.push_str(&format!("; unresolved state could not be persisted ({failure}); native retry is not authorized"));
        }
        error
    }
}

impl RuntimeSessionManager {
    /// A bounded reread of this exact run may recover a committed terminal
    /// receipt. A different run or an absent row cannot prove nonexecution.
    pub(super) fn verified_terminal_compaction_receipt(
        &self,
        expected: &crate::SessionCompactionRun,
    ) -> Result<Option<crate::SessionCompactionRun>, RuntimeError> {
        let Some(saved) = self.store.get_session_compaction(&expected.run_id)? else {
            return Ok(None);
        };
        if saved.run_id != expected.run_id
            || saved.agent_id != expected.agent_id
            || saved.provider != expected.provider
            || saved.provider_session_ref != expected.provider_session_ref
            || saved.canonical_provider_session_ref != expected.canonical_provider_session_ref
            || saved.agent_revision != expected.agent_revision
            || saved.expected_session_updated_at != expected.expected_session_updated_at
            || saved.idempotency_key != expected.idempotency_key
            || saved.source != expected.source
            || saved.created_at_ms != expected.created_at_ms
        {
            return Err(RuntimeError::ProtocolViolation(
                "compaction receipt changed its original admission identity".into(),
            ));
        }
        Ok(matches!(
            saved.phase.as_str(),
            "completed" | "failed" | "cancelled" | "not_performed"
        )
        .then_some(saved))
    }
}
