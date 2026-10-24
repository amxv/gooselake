use runtime_core::{
    ProviderCloseSessionRequest, ProviderCreateSessionRequest, ProviderRetirementCloseProofKind,
    RuntimeError, RuntimeProvider,
};

use super::support::FakeAgentHarness;
use crate::{AcpProvider, AcpProviderConfig};

fn create_request(id: &str) -> ProviderCreateSessionRequest {
    ProviderCreateSessionRequest {
        runtime_session_id: id.into(),
        model: None,
        cwd: None,
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
async fn retirement_close_requires_native_ack_and_preserves_shared_connection_peer() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
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
    assert_eq!(
        evidence.proof_kind,
        ProviderRetirementCloseProofKind::NativeSessionCloseAcknowledged
    );
    assert!(!provider
        .inner
        .sessions
        .read()
        .await
        .contains_key("retiring"));
    assert!(provider.inner.sessions.read().await.contains_key("peer"));
    assert!(provider.inner.connection.lock().await.is_some());
    assert!(provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .is_err());
    provider
        .close_session_for_retirement(close_request("peer"))
        .await
        .unwrap();
    assert!(provider.inner.connection.lock().await.is_none());
}

#[tokio::test]
async fn retirement_close_error_or_malformed_ack_quarantines_existing_attachment() {
    for scenario in ["error_close", "invalid_close"] {
        let harness = FakeAgentHarness::new(scenario);
        let provider = harness.provider();
        provider
            .create_session(create_request("retiring"))
            .await
            .unwrap();
        let error = provider
            .close_session_for_retirement(close_request("retiring"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, RuntimeError::ProtocolViolation(_)),
            "{scenario}: {error}"
        );
        assert!(provider.inner.sessions.read().await["retiring"].resuming);
        assert!(provider
            .close_session_for_retirement(close_request("retiring"))
            .await
            .is_err());
        // Dispose the isolated fake child without replaying an unknown native
        // close. Ordinary close intentionally refuses a quarantined attachment.
        provider.inner.sessions.write().await.clear();
        provider.shutdown_connection_if_idle().await;
    }
}

#[tokio::test]
async fn retirement_close_timeout_preserves_ambiguous_native_attachment() {
    let harness = FakeAgentHarness::new("hang_close");
    let provider = AcpProvider::new(AcpProviderConfig {
        enabled: true,
        provider_dir: harness.provider_dir.clone(),
        command: Some("python3".into()),
        args: vec![harness.script_path.display().to_string()],
        request_timeout_secs: 5,
        ..AcpProviderConfig::default()
    });
    provider
        .create_session(create_request("retiring"))
        .await
        .unwrap();
    let error = provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(provider.inner.sessions.read().await["retiring"].resuming);
    assert!(provider.inner.connection.lock().await.is_some());
    provider.inner.sessions.write().await.clear();
    provider.shutdown_connection_if_idle().await;
}

#[tokio::test]
async fn retirement_close_requires_advertised_capability_and_same_native_connection() {
    let harness = FakeAgentHarness::new("no_close");
    let provider = harness.provider();
    provider
        .create_session(create_request("retiring"))
        .await
        .unwrap();
    let error = provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Unsupported(_)));
    assert!(!provider.inner.sessions.read().await["retiring"].resuming);
    provider
        .close_session(close_request("retiring"))
        .await
        .unwrap();

    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    provider
        .create_session(create_request("retiring"))
        .await
        .unwrap();
    provider
        .inner
        .sessions
        .write()
        .await
        .get_mut("retiring")
        .unwrap()
        .connection_id = Some(u64::MAX);
    let error = provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Conflict(_)));
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
async fn retirement_close_refuses_busy_and_missing_native_attachments() {
    let harness = FakeAgentHarness::new("normal");
    let provider = harness.provider();
    assert!(provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .is_err());
    assert!(provider.inner.connection.lock().await.is_none());
    provider
        .create_session(create_request("retiring"))
        .await
        .unwrap();
    provider
        .inner
        .sessions
        .write()
        .await
        .get_mut("retiring")
        .unwrap()
        .resuming = true;
    assert!(provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .is_err());
    provider
        .inner
        .sessions
        .write()
        .await
        .get_mut("retiring")
        .unwrap()
        .resuming = false;
    provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap();
}
