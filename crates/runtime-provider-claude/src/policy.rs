use runtime_core::{
    claude_model_catalog, ProviderPermissionIntent, ProviderSessionLaunchPolicy,
    ProviderSessionPreferences, RuntimeError, HARNESS_VERSION,
};

pub(crate) fn legacy_policy(
    permission: Option<String>,
    sources: Vec<String>,
    prompt: Option<String>,
    allowed: Vec<String>,
    disallowed: Vec<String>,
    harness: Option<String>,
) -> Result<ProviderSessionLaunchPolicy, RuntimeError> {
    Ok(ProviderSessionLaunchPolicy {
        permission_intent: match permission {
            Some(mode) => ProviderPermissionIntent::explicit(mode)?,
            None => ProviderPermissionIntent::InheritProviderConfiguration,
        },
        setting_sources_intent: serde_json::from_value(serde_json::json!(sources))
            .map_err(|e| RuntimeError::InvalidState(e.to_string()))?,
        system_prompt: prompt,
        allowed_tools: allowed,
        disallowed_tools: disallowed,
        harness_version_slot: harness,
    })
}

pub(crate) fn validate_policy(
    policy: &ProviderSessionLaunchPolicy,
    cwd: Option<&str>,
    preferences: &ProviderSessionPreferences,
    model: Option<&str>,
) -> Result<(), RuntimeError> {
    policy.setting_sources_intent.resolved_wire_values(cwd)?;
    match &policy.permission_intent {
        ProviderPermissionIntent::InheritProviderConfiguration => {}
        ProviderPermissionIntent::Explicit { mode }
            if ["default", "acceptEdits", "bypassPermissions", "plan", "dontAsk"].contains(&mode.as_str()) => {}
        _ => return Err(RuntimeError::InvalidState("invalid Claude permission intent; use inherit_provider_configuration or an explicit Claude mode".into())),
    }
    if policy
        .harness_version_slot
        .as_deref()
        .is_some_and(|v| v != HARNESS_VERSION)
    {
        return Err(RuntimeError::InvalidState(
            "unsupported Claude harness version".into(),
        ));
    }
    if let Some(effort) = preferences.thinking_effort {
        let models = claude_model_catalog();
        let supported = models
            .iter()
            .find(|m| Some(m.id.as_str()) == model)
            .or_else(|| models.first().filter(|_| model.is_none()))
            .is_some_and(|m| {
                m.reasoning_levels
                    .iter()
                    .any(|level| level == effort.as_str())
            });
        if !supported {
            return Err(RuntimeError::InvalidState(
                "thinking effort is unsupported for the selected Claude model".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_resume_identity(
    provider: &str,
    canonical: Option<&str>,
) -> Result<(), RuntimeError> {
    if provider.trim().is_empty() || canonical.is_none_or(|v| v.trim().is_empty()) {
        return Err(RuntimeError::InvalidState(
            "Claude resume requires canonical native identity".into(),
        ));
    }
    Ok(())
}
