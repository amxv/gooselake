//! Typed inspection metadata: no ID-prefix inference or native retry authority.
use super::RuntimeError;
use crate::legacy_spawn_obligations::LegacyNativeRecovery;

pub(crate) fn path_segment(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut result = String::with_capacity(value.len());
    for byte in value.bytes() {
        // Encode dot-only segments too; keep the original ID in the DTO.
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'~')
            || (byte == b'.' && value != "." && value != "..")
        {
            result.push(char::from(byte));
        } else {
            result.push('%');
            result.push(char::from(HEX[usize::from(byte >> 4)]));
            result.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    result
}

pub fn legacy_team_operation_inspection_path(operation_id: &str) -> String {
    format!(
        "/v1/diagnostics/team-operations/{}",
        path_segment(operation_id)
    )
}

pub fn canonical_operation_inspection_path(operation_id: &str) -> String {
    format!("/v2/operations/{}", path_segment(operation_id))
}

impl LegacyNativeRecovery {
    /// Existing native receipts preserve older evidence under prior_evidence.
    /// Decode that bounded known schema, never infer a family from text/IDs.
    pub fn from_effect_evidence(
        evidence: &serde_json::Value,
    ) -> Result<Option<Self>, RuntimeError> {
        let mut current = evidence;
        let mut saved: Option<Self> = None;
        for _ in 0..32 {
            if current.is_null() {
                return Ok(saved);
            }
            if !current.is_object() {
                return Err(RuntimeError::Bootstrap(
                    "invalid recovery evidence object".into(),
                ));
            }
            if let Some(value) = current.get("legacy_native_recovery") {
                let observed: Self = serde_json::from_value(value.clone()).map_err(|error| {
                    RuntimeError::Bootstrap(format!(
                        "invalid saved legacy recovery observation: {error}"
                    ))
                })?;
                if saved.as_ref().is_some_and(|original| original != &observed) {
                    return Err(RuntimeError::Conflict(
                        "original legacy recovery observation changed".into(),
                    ));
                }
                saved = Some(observed);
            }
            match current.get("prior_evidence") {
                Some(prior) => current = prior,
                None => return Ok(saved),
            }
        }
        Err(RuntimeError::Bootstrap(
            "legacy recovery evidence nesting exceeds inspection limit".into(),
        ))
    }

    pub fn error_details(&self) -> serde_json::Value {
        match self {
            Self::Blocked { coordinate, reason } => serde_json::json!({
                "kind":"blocked", "coordinate":coordinate, "reason":reason,
                "inspection_path":legacy_team_operation_inspection_path(&coordinate.operation_id),
                "native_retry_authorized":false,
            }),
            Self::ScanIncomplete { incomplete } => serde_json::json!({
                "kind":"scan_incomplete", "incomplete":incomplete,
                "native_retry_authorized":false,
            }),
        }
    }
}

impl RuntimeError {
    pub fn legacy_native_recovery(&self) -> Option<LegacyNativeRecovery> {
        match self {
            Self::LegacyNativeObligationBlocked { coordinate, reason } => {
                Some(LegacyNativeRecovery::Blocked {
                    coordinate: coordinate.clone(),
                    reason: *reason,
                })
            }
            Self::LegacyNativeScanIncomplete { incomplete } => {
                Some(LegacyNativeRecovery::ScanIncomplete {
                    incomplete: incomplete.clone(),
                })
            }
            Self::OperationUnresolved {
                legacy_recovery, ..
            }
            | Self::SessionControlUnresolved {
                legacy_recovery, ..
            } => legacy_recovery.as_deref().cloned(),
            _ => None,
        }
    }

    pub fn with_legacy_native_recovery(mut self, recovery: Option<LegacyNativeRecovery>) -> Self {
        if let Self::OperationUnresolved {
            legacy_recovery, ..
        }
        | Self::SessionControlUnresolved {
            legacy_recovery, ..
        } = &mut self
        {
            if legacy_recovery.is_none() {
                *legacy_recovery = recovery.map(Box::new);
            }
        }
        self
    }

    /// The canonical operation must already exist. The legacy descriptor is
    /// a separate cause; it cannot be substituted for the canonical owner.
    pub fn operation_unresolved_with_legacy_recovery(
        operation_id: impl Into<String>,
        message: impl Into<String>,
        recovery: Option<LegacyNativeRecovery>,
    ) -> Self {
        Self::OperationUnresolved {
            operation_id: operation_id.into(),
            message: message.into(),
            legacy_recovery: recovery.map(Box::new),
        }
    }
}

#[cfg(test)]
mod tests;
