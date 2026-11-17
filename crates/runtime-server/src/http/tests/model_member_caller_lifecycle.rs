use super::workspace_retirement_support::{retirement_fixture, RetirementProvider};
use super::*;
use serde_json::json;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn running_model_caller_can_remove_idle_peer_but_never_its_own_busy_target() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("running-model-caller");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let caller = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let peer = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, started) = fixture
        .call(
            "POST",
            &format!("/v2/agents/{}/turns", caller.agent_id),
            Some(json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let original = fixture.runtime.get_session(&caller.agent_id).await.unwrap();
    assert_eq!(original.status, "turn_running");
    assert_eq!(
        original.active_turn_id.as_deref(),
        started["turn_id"].as_str()
    );
    let scoped = runtime_core::mint_scoped_mcp_token(&fixture.token, &caller.agent_id);
    let closes = provider.closes.load(Ordering::SeqCst);
    for (target, key, expected_ok) in [
        (&caller.agent_id, "running-self-removal", false),
        (&peer.agent_id, "running-peer-removal", true),
    ] {
        let response = fixture
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v2/mcp/invoke")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {scoped}"))
                    .header("x-gg-tool-invocation-id", key)
                    .body(Body::from(
                        json!({"tool_name":"gg_team","args":{
                            "mode":"remove","remove_agent_ids":[target]
                        }})
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let result: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(result["ok"], expected_ok, "{result}");
        assert_eq!(
            provider.closes.load(Ordering::SeqCst),
            closes + usize::from(expected_ok)
        );
        assert_eq!(
            fixture.runtime.get_session(&caller.agent_id).await.unwrap(),
            original
        );
        if expected_ok {
            let child_id = result["result"]["removed"][0]["operation_id"]
                .as_str()
                .unwrap();
            let child = fixture.store.get_operation(child_id).unwrap().unwrap();
            assert_eq!(
                child.operation.actor.kind,
                runtime_core::OperationActorKind::Agent
            );
            assert_eq!(child.operation.actor.identifier, caller.agent_id);
            assert_eq!(
                child.operation.phase,
                runtime_core::OperationPhase::Completed
            );
        }
    }
    assert_eq!(
        provider
            .turns_containing("retirement_wait_for_interrupt")
            .await,
        1
    );
    assert_eq!(
        fixture
            .runtime
            .get_workspace_agent(&workspace.workspace_id, &peer.agent_id)
            .unwrap()
            .lifecycle_state,
        runtime_core::WorkspaceAgentLifecycleState::Archived
    );
}
