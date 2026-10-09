use super::*;

#[tokio::test]
async fn codex_current_thinking_preference_is_revisioned_and_inherited_by_next_turn() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, log_path) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_thinking"))
        .await
        .expect("create provider session");

    let changed = provider
        .mutate_session_preferences(ProviderSessionPreferencesMutationRequest {
            runtime_session_id: "sess_thinking".into(),
            expected_revision: Some(0),
            current_preferences: ProviderSessionPreferences {
                thinking_effort: Some(ProviderThinkingEffort::Xhigh),
            },
        })
        .await
        .expect("set current preference");
    assert_eq!(changed.revision, 1);
    assert_eq!(
        changed.current_preferences.thinking_effort,
        Some(ProviderThinkingEffort::Xhigh)
    );
    let stale = provider
        .mutate_session_preferences(ProviderSessionPreferencesMutationRequest {
            runtime_session_id: "sess_thinking".into(),
            expected_revision: Some(0),
            current_preferences: ProviderSessionPreferences::default(),
        })
        .await
        .expect_err("stale preference revision must not change live state");
    assert!(matches!(stale, RuntimeError::Conflict(_)));

    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "sess_thinking".into(),
            turn_id: "logical-thinking".into(),
            input: vec![json!({"type":"text","text":"hello"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
        })
        .await
        .expect("start turn with current effort");
    provider
        .wait_for_turn(ProviderWaitTurnRequest {
            runtime_session_id: "sess_thinking".into(),
            turn_id: "logical-thinking".into(),
            timeout_ms: Some(2_000),
        })
        .await
        .expect("turn result");
    let turns = read_log(&log_path)
        .into_iter()
        .filter(|record| record["method"] == "turn/start")
        .collect::<Vec<_>>();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["params"]["effort"], "xhigh");
}
