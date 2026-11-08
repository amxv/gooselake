use super::workspace_retirement_support::*;
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn frozen_retirement_202_exposes_exact_operation_without_claiming_native_completion() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("operation-header-retirement");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    provider.fail_close.store(true, Ordering::SeqCst);
    let fixture = retirement_fixture(temp.path(), &[provider]).await;
    let workspace = fixture.register(&root).await;
    fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let request = || {
        Request::builder()
            .method("POST")
            .uri(format!(
                "/v2/workspaces/{}/retirement/advance",
                workspace.workspace_id
            ))
            .header(header::AUTHORIZATION, format!("Bearer {}", fixture.token))
            .header(header::CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", "retirement-header-replay")
            .body(Body::from(
                serde_json::json!({"expected_revision":revision}).to_string(),
            ))
            .unwrap()
    };
    let response = fixture.router.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let operation_id = response.headers()["x-operation-id"]
        .to_str()
        .unwrap()
        .to_string();
    let details: runtime_core::OperationDetails =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(details.operation.operation_id, operation_id);
    assert_ne!(
        details.operation.phase,
        runtime_core::OperationPhase::Completed
    );

    // Lost HTTP response / repeat request: return the *original* durable
    // identity, not a newly fabricated status or a new provider close.
    let replay = fixture.router.clone().oneshot(request()).await.unwrap();
    assert!(matches!(
        replay.status(),
        StatusCode::ACCEPTED | StatusCode::OK
    ));
    assert_eq!(replay.headers()["x-operation-id"], operation_id.as_str());
    let replayed: runtime_core::OperationDetails =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(replayed.operation.operation_id, operation_id);
}
