use super::*;
use runtime_core::{ProviderCloseSessionRequest, ProviderRetirementCloseProofKind};

fn close_request(id: &str) -> ProviderCloseSessionRequest {
    ProviderCloseSessionRequest {
        runtime_session_id: id.into(),
        reason: Some("workspace_retirement".into()),
    }
}

#[tokio::test]
async fn retirement_close_reaps_exact_native_process_and_preserves_peer() {
    let temp = tempfile::tempdir().unwrap();
    let (provider, _) = fake_provider(&temp, false);
    let created = provider
        .create_session_with_policy(legacy_create("retiring"))
        .await
        .unwrap();
    provider
        .create_session_with_policy(legacy_create("peer"))
        .await
        .unwrap();
    let transport = {
        let sessions = provider.inner.sessions.read().await;
        std::sync::Arc::clone(&sessions["retiring"].transport)
    };
    let native = transport.request("initialize", json!({})).await.unwrap();
    let evidence = provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .unwrap();
    assert_eq!(evidence.runtime_session_id, "retiring");
    assert_eq!(evidence.provider_session_ref, created.provider_session_ref);
    assert_eq!(
        evidence.proof_kind,
        ProviderRetirementCloseProofKind::NativeProcessTerminated
    );
    assert!(!provider
        .inner
        .sessions
        .read()
        .await
        .contains_key("retiring"));
    assert!(provider.inner.sessions.read().await.contains_key("peer"));
    #[cfg(unix)]
    {
        let pid = native["processId"].as_u64().unwrap();
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success();
        assert!(
            !alive,
            "retirement evidence requires the native child to be reaped"
        );
    }
    #[cfg(not(unix))]
    let _ = native;
    let peer_transport = {
        let sessions = provider.inner.sessions.read().await;
        std::sync::Arc::clone(&sessions["peer"].transport)
    };
    assert!(peer_transport
        .request("initialize", json!({}))
        .await
        .is_ok());
    assert!(provider
        .close_session_for_retirement(close_request("retiring"))
        .await
        .is_err());
    provider
        .close_session_for_retirement(close_request("peer"))
        .await
        .unwrap();
}

#[tokio::test]
async fn retirement_close_refuses_active_or_unknown_native_turn() {
    let temp = tempfile::tempdir().unwrap();
    let (provider, _) = fake_provider(&temp, false);
    provider
        .create_session_with_policy(legacy_create("retiring"))
        .await
        .unwrap();
    provider
        .send_turn(ProviderSendTurnRequest {
            runtime_session_id: "retiring".into(),
            turn_id: "pending-native-turn".into(),
            input: vec![json!({"type":"text", "text":"interrupt this turn"})],
            expected_turn_id: None,
            permission_mode: None,
            approval_id: None,
            turn_settings: Default::default(),
        })
        .await
        .unwrap();
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
async fn retirement_close_never_infers_evidence_from_missing_attachment() {
    let temp = tempfile::tempdir().unwrap();
    let (provider, _) = fake_provider(&temp, false);
    let error = provider
        .close_session_for_retirement(close_request("unknown"))
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::Conflict(_)));
    assert!(provider.inner.sessions.read().await.is_empty());
}
