use super::*;

async fn body_json(response: Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body"),
    )
    .expect("response json")
}

#[tokio::test]
async fn v2_provider_contract_reports_capabilities_harness_and_discovery_modes() {
    let (router, token, _temp_dir, _acp) = build_mixed_provider_test_router().await;

    let capabilities = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/providers/acp/capabilities")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("capabilities request"),
        )
        .await
        .expect("capabilities response");
    assert_eq!(capabilities.status(), StatusCode::OK);
    let capabilities = body_json(capabilities).await;
    assert_eq!(capabilities["provider"], "acp");
    assert_eq!(
        capabilities["capabilities"]["model_discovery"],
        "agent_managed"
    );
    assert_eq!(
        capabilities["capabilities"]["skill_discovery"],
        "unsupported"
    );
    assert_eq!(
        capabilities["capabilities"]["permission_mutation"],
        "unsupported"
    );
    assert_eq!(
        capabilities["capabilities"]["session_preferences"],
        "unsupported"
    );
    assert_eq!(capabilities["capabilities"]["approvals"], "supported");
    assert_eq!(capabilities["capabilities"]["tools"], "agent_managed");
    assert_eq!(capabilities["capabilities"]["images"], "agent_managed");
    assert_eq!(capabilities["harness"]["version"], "gooselake-harness-v1");
    assert_eq!(capabilities["harness"]["injection_mode"], "scoped_mcp_only");
    assert!(capabilities["harness"]["content_hash"]
        .as_str()
        .is_some_and(|value| value.starts_with("harness_v1_")));
    assert!(capabilities["harness"]["tool_manifest_hash"]
        .as_str()
        .is_some_and(|value| value.starts_with("tools_v1_")));

    let acp_models = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/providers/acp/models/discover")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"force_refresh":false,"startup_mode":"start_runtime"}"#,
                ))
                .expect("model discovery request"),
        )
        .await
        .expect("model discovery response");
    assert_eq!(acp_models.status(), StatusCode::OK);
    let acp_models = body_json(acp_models).await;
    assert_eq!(acp_models["provider"], "acp");
    assert_eq!(acp_models["mode"], "agent_managed");
    assert_eq!(acp_models["models"], serde_json::json!([]));

    let codex_models = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/providers/codex/models/discover")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(r#"{}"#))
                .expect("codex model discovery request"),
        )
        .await
        .expect("codex model discovery response");
    assert_eq!(codex_models.status(), StatusCode::OK);
    let codex_models = body_json(codex_models).await;
    assert_eq!(codex_models["mode"], "catalog");
    assert_eq!(codex_models["models"][0]["model_key"], "test-model");

    let acp_skills = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/providers/acp/skills/discover")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"setting_sources_intent":{"kind":"isolated"}}"#,
                ))
                .expect("skill discovery request"),
        )
        .await
        .expect("skill discovery response");
    assert_eq!(acp_skills.status(), StatusCode::OK);
    let acp_skills = body_json(acp_skills).await;
    assert_eq!(acp_skills["mode"], "unsupported");
    assert_eq!(acp_skills["skills"], serde_json::json!([]));

    let invalid_sources = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/providers/acp/models/discover")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    r#"{"setting_sources_intent":{"kind":"explicit","sources":["project"]}}"#,
                ))
                .expect("invalid source request"),
        )
        .await
        .expect("invalid source response");
    assert_eq!(invalid_sources.status(), StatusCode::BAD_REQUEST);
    let invalid_sources = body_json(invalid_sources).await;
    assert!(invalid_sources["error"]
        .as_str()
        .is_some_and(|message| message.contains("working directory")));
}
