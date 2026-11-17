use super::*;

pub(super) fn attach_operation_details(
    gateway: &RuntimeToolGateway,
    operation_id: &str,
    mut envelope: Value,
) -> Value {
    let mut targets = Vec::new();
    let details = match runtime_core_store(gateway).get_operation(operation_id) {
        Ok(Some(details)) => details,
        _ => {
            envelope["error"]["code"] = json!("outcome_unresolved");
            envelope["error"]["details"] = json!({"operation_id":operation_id,
                "targets":[],"inspection":"unavailable"});
            return envelope;
        }
    };
    {
        let mut effects = details
            .effects
            .into_iter()
            .filter(|effect| {
                matches!(
                    effect.effect_kind.as_str(),
                    "model_member_archive" | "model_member_rebind"
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
    envelope["error"]["details"] = json!({"operation_id":operation_id,"targets":targets});
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
