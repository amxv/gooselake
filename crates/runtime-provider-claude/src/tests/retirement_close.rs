use super::*;
use runtime_core::ProviderRetirementCloseProofKind;

fn create_request(id: &str) -> ProviderCreateSessionRequest {
    ProviderCreateSessionRequest {
        runtime_session_id: id.into(),
        model: Some("claude-sonnet-5-5".into()),
        cwd: Some("/tmp".into()),
        permission_mode: None,
        setting_sources: Vec::new(),
        system_prompt: None,
        allowed_tools: Vec::new(),
        disallowed_tools: Vec::new(),
        harness_version_slot: None,
        metadata: None,
    }
}

fn close_request(id: &str) -> ProviderCloseSessionRequest {
    ProviderCloseSessionRequest {
        runtime_session_id: id.into(),
        reason: Some("workspace_retirement".into()),
    }
}

#[tokio::test]
async fn retirement_close_requires_strict_ack_and_preserves_shared_bridge_peer() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    let created = provider
        .create_session(create_request("retiring"))
        .await
        .unwrap();
    provider
        .create_session(create_request("peer"))
        .await
        .unwrap();
    let evidence = provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap();
    assert_eq!(evidence.runtime_session_id, "retiring");
    assert_eq!(evidence.provider_session_ref, created.provider_session_ref);
    assert_eq!(evidence.provider_session_ref, "provider-session-1");
    assert_eq!(
        evidence.proof_kind,
        ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged
    );
    assert!(provider.get_session("retiring").await.is_err());
    assert!(provider.get_session("peer").await.is_ok());
    assert_eq!(provider.inner.bridges.read().await.len(), 1);
    let requests = harness.read_requests();
    let strict = requests_for_method(&requests, "session.close_for_retirement");
    assert_eq!(strict.len(), 1);
    assert_eq!(strict[0]["params"]["sessionId"], "bridge-session-1");
    assert!(requests_for_method(&requests, "session.close").is_empty());
    assert!(provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .is_err());
    provider
        .close_session_for_retirement(close_request("peer"))
        .await
        .unwrap();
    assert!(provider.inner.bridges.read().await.is_empty());
}

#[tokio::test]
async fn retirement_close_unknown_or_invalid_ack_preserves_quarantined_attachment() {
    for scenario in [
        "retirement_close_error",
        "retirement_close_bad_identity",
        "retirement_close_old_bridge",
    ] {
        let harness = FakeClaudeBridgeHarness::new(scenario);
        let provider = harness.provider(ClaudeGgMcpConfig::default());
        provider
            .create_session(create_request("retiring"))
            .await
            .unwrap();
        assert!(
            provider
                .close_session_for_retirement(close_request("retiring"))
                .await
                .is_err(),
            "{scenario}"
        );
        let session = provider
            .inner
            .sessions
            .read()
            .await
            .get("retiring")
            .cloned()
            .unwrap();
        assert!(session
            .quarantined
            .load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            *session.provider_session_ref.read().await,
            "provider-session-1"
        );
        assert!(provider
            .close_session_for_retirement(close_request("retiring"))
            .await
            .is_err());
        assert_eq!(
            requests_for_method(&harness.read_requests(), "session.close_for_retirement").len(),
            1
        );
        provider
            .close_session(close_request("retiring"))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn retirement_close_timeout_cannot_become_native_success() {
    let harness = FakeClaudeBridgeHarness::new("retirement_close_hang");
    let mut config = harness
        .provider(ClaudeGgMcpConfig::default())
        .inner
        .config
        .clone();
    config.request_timeout_ms = 5_000;
    let provider = ClaudeProvider::new(config);
    provider
        .create_session(create_request("retiring"))
        .await
        .unwrap();
    let error = provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().to_lowercase().contains("timed out"),
        "{error}"
    );
    assert!(provider
        .inner
        .sessions
        .read()
        .await
        .contains_key("retiring"));
    provider
        .close_session(close_request("retiring"))
        .await
        .unwrap();
}

#[tokio::test]
async fn retirement_close_refuses_busy_attachment_before_native_dispatch() {
    let harness = FakeClaudeBridgeHarness::new("normal");
    let provider = harness.provider(ClaudeGgMcpConfig::default());
    provider
        .create_session(create_request("retiring"))
        .await
        .unwrap();
    let session = provider.get_session("retiring").await.unwrap();
    *session.pending_runtime_turn_id.write().await = Some("pending-turn".into());
    assert!(provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .is_err());
    assert!(
        requests_for_method(&harness.read_requests(), "session.close_for_retirement").is_empty()
    );
    *session.pending_runtime_turn_id.write().await = None;
    provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap();
}
