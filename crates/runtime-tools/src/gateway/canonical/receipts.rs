use super::*;

pub(super) fn attach_operation_details(
    gateway: &RuntimeToolGateway,
    operation_id: &str,
    mut envelope: Value,
) -> Value {
    let mut targets = Vec::new();
    let mut projected = envelope["error"]["details"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    projected.insert("operation_id".into(), json!(operation_id));
    projected.insert(
        "inspection_path".into(),
        json!(runtime_core::error::canonical_operation_inspection_path(
            operation_id
        )),
    );
    projected.insert("native_retry_authorized".into(), json!(false));
    let details = match runtime_core_store(gateway).get_operation(operation_id) {
        Ok(Some(details)) => details,
        _ => {
            envelope["error"]["code"] = json!("outcome_unresolved");
            projected.insert("targets".into(), json!([]));
            projected.insert("inspection".into(), json!("unavailable"));
            envelope["error"]["details"] = Value::Object(projected);
            return envelope;
        }
    };
    let mut saved_recoveries = Vec::new();
    let mut unverified_recoveries = Vec::new();
    for effect in &details.effects {
        match runtime_core_store(gateway).read_operation_effect_legacy_recovery(
            operation_id,
            &effect.effect_id,
            &effect.effect_kind,
            &effect.target_id,
        ) {
            Ok(Some(recovery)) => {
                let value = recovery.error_details();
                if !saved_recoveries.contains(&value) {
                    saved_recoveries.push(value);
                }
            }
            Ok(None) => {}
            Err(_) => {
                projected.insert("legacy_recovery_inspection".into(), json!("unavailable"));
                if let Some(evidence) = effect.evidence.as_ref() {
                    if let Ok(Some(raw)) = runtime_core::legacy_spawn_obligations::LegacyNativeRecovery::from_effect_evidence(evidence) {
                        let value = raw.error_details();
                        if !unverified_recoveries.contains(&value) { unverified_recoveries.push(value); }
                    }
                }
            }
        }
    }
    if !unverified_recoveries.is_empty() {
        projected.insert(
            "unverified_legacy_recoveries".into(),
            json!(unverified_recoveries),
        );
    }
    if !projected.contains_key("legacy_recovery") {
        if let Some(first) = saved_recoveries.first() {
            projected.insert("legacy_recovery".into(), first.clone());
        }
    }
    if !saved_recoveries.is_empty() {
        projected.insert("recorded_legacy_recoveries".into(), json!(saved_recoveries));
    } else if !projected.contains_key("legacy_recovery_inspection") {
        projected.insert("legacy_recovery_inspection".into(), json!("not_recorded"));
    }
    {
        let mut effects = details
            .effects
            .into_iter()
            .filter(|effect| {
                matches!(
                    effect.effect_kind.as_str(),
                    "model_member_archive" | "model_member_rebind" | "model_member_create"
                )
            })
            .collect::<Vec<_>>();
        effects.sort_by_key(|effect| {
            effect
                .evidence
                .as_ref()
                .and_then(|value| {
                    value["snapshot"]["normalized_request"]["model_tool_parent"]["index"].as_u64()
                })
                .unwrap_or(u64::MAX)
        });
        for effect in effects {
            let evidence = effect.evidence.unwrap_or(Value::Null);
            let status = evidence["outcome"]["status"]
                .as_str()
                .unwrap_or("outcome_unresolved");
            let child_id = evidence["snapshot"]["operation_id"]
                .as_str()
                .or_else(|| evidence["snapshot"]["operation"]["operation_id"].as_str());
            targets.push(json!({"agent_id":effect.target_id,
                "operation_id":child_id,"status":status}));
        }
    }
    if targets.iter().any(|target| {
        !matches!(
            target["status"].as_str(),
            Some("not_admitted" | "rejected_before_dispatch")
        )
    }) {
        envelope["error"]["code"] = json!("outcome_unresolved");
    }
    projected.insert("targets".into(), json!(targets));
    envelope["error"]["details"] = Value::Object(projected);
    envelope
}

pub(super) fn unresolved_team_operation(gateway: &RuntimeToolGateway, operation_id: &str) -> Value {
    attach_operation_details(
        gateway,
        operation_id,
        json!({"ok":false,"error":{
            "code":"outcome_unresolved",
            "message":"The workspace mutation has no complete durable tool receipt. Inspect the original operation and target outcomes; do not repeat native work.",
            "retryable":false,
        }}),
    )
}
