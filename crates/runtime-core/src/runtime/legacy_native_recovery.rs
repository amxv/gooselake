use super::{helpers::now_ms, RuntimeSessionManager};
use crate::legacy_spawn_obligations::LegacyNativeRecovery;
use crate::RuntimeError;

impl RuntimeSessionManager {
    /// Replaying an admitted canonical operation can recover its original
    /// typed observation without rescanning or reopening any native action.
    pub(super) fn restore_legacy_native_recovery(&self, error: RuntimeError) -> RuntimeError {
        let RuntimeError::OperationUnresolved { operation_id, .. } = &error else {
            return error;
        };
        if error.legacy_native_recovery().is_some() {
            return error;
        }
        let observed = self.store.get_operation(operation_id).and_then(|details| {
            let details = details
                .ok_or_else(|| RuntimeError::NotFound("original canonical operation".into()))?;
            for effect in details.effects {
                if let Some(saved) = self.store.read_operation_effect_legacy_recovery(
                    operation_id,
                    &effect.effect_id,
                    &effect.effect_kind,
                    &effect.target_id,
                )? {
                    return Ok(Some(saved));
                }
            }
            Ok(None)
        });
        match observed {
            Ok(recovery) => error.with_legacy_native_recovery(recovery),
            Err(failure) => RuntimeError::operation_unresolved(operation_id,
                format!("{error}; original recovery observation inspection unavailable ({failure}); native retry is not authorized")),
        }
    }

    pub(super) fn admitted_legacy_guard_error(
        &self,
        operation_id: &str,
        effect_kind: &str,
        target_id: &str,
        error: RuntimeError,
    ) -> RuntimeError {
        let Some(recovery) = error.legacy_native_recovery() else {
            return error;
        };
        if let Err(persistence) = self.preserve_legacy_native_recovery(
            operation_id,
            effect_kind,
            target_id,
            Some(recovery.clone()),
        ) {
            return persistence;
        }
        RuntimeError::operation_unresolved_with_legacy_recovery(operation_id,
            format!("The original admitted operation remains withheld by historical native evidence: {error}"),
            Some(recovery))
    }

    /// Select the existing carrier by immutable owner/kind/target, then let
    /// the store verify its exact effect ID again under its writer lock.
    pub(super) fn preserve_legacy_native_recovery(
        &self,
        operation_id: &str,
        effect_kind: &str,
        target_id: &str,
        recovery: Option<LegacyNativeRecovery>,
    ) -> Result<(), RuntimeError> {
        let Some(recovery) = recovery else {
            return Ok(());
        };
        let result = (|| {
            let operation = self
                .store
                .get_operation(operation_id)?
                .ok_or_else(|| RuntimeError::NotFound("admitted legacy recovery owner".into()))?;
            let mut effects = operation.effects.iter().filter(|effect| {
                effect.operation_id == operation_id
                    && effect.effect_kind == effect_kind
                    && effect.target_id == target_id
            });
            let effect = effects.next().ok_or_else(|| {
                RuntimeError::Conflict(
                    "admitted operation has no exact legacy recovery carrier".into(),
                )
            })?;
            if effects.next().is_some() {
                return Err(RuntimeError::Conflict(
                    "legacy recovery carrier is ambiguous".into(),
                ));
            }
            self.store.record_operation_effect_legacy_recovery(
                operation_id,
                &effect.effect_id,
                effect_kind,
                target_id,
                &recovery,
                now_ms(),
            )
        })();
        result.map_err(|error| RuntimeError::operation_unresolved_with_legacy_recovery(
            operation_id,
            format!("Native work remains withheld; the typed legacy recovery observation could not be persisted ({error}). Inspect the original canonical operation; absence of the observation is not clearance."),
            Some(recovery),
        ))
    }
}
