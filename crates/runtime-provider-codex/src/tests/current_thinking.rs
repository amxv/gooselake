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
            turn_settings: Default::default(),
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

#[tokio::test]
async fn codex_per_turn_thinking_and_collaboration_do_not_mutate_session_defaults() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (provider, log_path) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("sess_turn_options"))
        .await
        .expect("create provider session");
    provider
        .mutate_session_preferences(ProviderSessionPreferencesMutationRequest {
            runtime_session_id: "sess_turn_options".into(),
            expected_revision: Some(0),
            current_preferences: ProviderSessionPreferences {
                thinking_effort: Some(ProviderThinkingEffort::Xhigh),
            },
        })
        .await
        .expect("set current effort");

    for (turn_id, settings) in [
        (
            "override",
            runtime_core::ProviderTurnSettings {
                thinking_effort: Some(ProviderThinkingEffort::High),
                codex_collaboration_mode: Some(runtime_core::CodexCollaborationMode::Plan),
            },
        ),
        ("inherited", Default::default()),
    ] {
        provider
            .send_turn(ProviderSendTurnRequest {
                runtime_session_id: "sess_turn_options".into(),
                turn_id: turn_id.into(),
                input: vec![json!({"type":"text","text":"hello"})],
                expected_turn_id: None,
                permission_mode: None,
                approval_id: None,
                turn_settings: settings,
            })
            .await
            .expect("send turn");
        provider
            .wait_for_turn(ProviderWaitTurnRequest {
                runtime_session_id: "sess_turn_options".into(),
                turn_id: turn_id.into(),
                timeout_ms: Some(2_000),
            })
            .await
            .expect("turn result");
    }
    let turns = read_log(&log_path)
        .into_iter()
        .filter(|record| record["method"] == "turn/start")
        .collect::<Vec<_>>();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["params"]["effort"], "high");
    assert_eq!(turns[0]["params"]["collaborationMode"]["mode"], "plan");
    assert_eq!(
        turns[0]["params"]["collaborationMode"]["settings"]["reasoning_effort"],
        "high"
    );
    assert_eq!(turns[1]["params"]["effort"], "xhigh");
    assert!(turns[1]["params"].get("collaborationMode").is_none());
}
