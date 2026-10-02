use std::process::Command;

use super::*;

#[tokio::test]
async fn workspace_migration_routes_preview_apply_resolve_and_preserve_v1_sessions() {
    let (router, token, temp_dir) = build_test_router().await;
    let git_root = temp_dir.path().join("migration-repo");
    init_git_repo(&git_root);

    let mapped_id =
        create_session(&router, &token, Some(git_root.to_string_lossy().as_ref())).await;
    let orphan_id = create_session(&router, &token, None).await;

    let preview_response = router
        .clone()
        .oneshot(auth_request(
            "POST",
            "/v2/migrations/workspaces/preview",
            &token,
            None,
            None,
        ))
        .await
        .expect("preview response");
    assert_eq!(preview_response.status(), StatusCode::OK);
    let preview: runtime_core::LegacyWorkspaceMigrationStatus = decode(preview_response).await;
    assert_eq!(preview.mapped_subjects, 1);
    assert_eq!(preview.unresolved_subjects, 1);
    assert!(preview.cutover_blocked);

    let apply_response = router
        .clone()
        .oneshot(auth_request(
            "POST",
            "/v2/migrations/workspaces/apply",
            &token,
            None,
            Some("migration-http-apply"),
        ))
        .await
        .expect("apply response");
    assert_eq!(apply_response.status(), StatusCode::OK);
    let applied: runtime_core::LegacyWorkspaceMigrationApplyResponse = decode(apply_response).await;
    assert_eq!(applied.created_workspaces, 1);
    assert_eq!(applied.owned_sessions_created, 1);
    assert_eq!(applied.unresolved_subjects, 1);
    assert!(applied.cutover_blocked);

    let workspaces_response = router
        .clone()
        .oneshot(auth_request("GET", "/v2/workspaces", &token, None, None))
        .await
        .expect("workspaces response");
    assert_eq!(workspaces_response.status(), StatusCode::OK);
    let workspaces: Vec<runtime_core::WorkspaceRecord> = decode(workspaces_response).await;
    assert_eq!(workspaces.len(), 1);

    let resolution_body = serde_json::json!({
        "action": "map",
        "workspace_id": workspaces[0].workspace_id,
    });
    let resolution_response = router
        .clone()
        .oneshot(auth_request(
            "POST",
            &format!("/v2/migrations/workspaces/resolutions/session/{orphan_id}"),
            &token,
            Some(resolution_body),
            Some("migration-http-resolve"),
        ))
        .await
        .expect("resolution response");
    assert_eq!(resolution_response.status(), StatusCode::OK);
    let resolution: runtime_core::LegacyWorkspaceMigrationResolutionResponse =
        decode(resolution_response).await;
    assert_eq!(
        resolution.subject.classification,
        runtime_core::LegacyWorkspaceMigrationClassification::Mapped
    );

    let status_response = router
        .clone()
        .oneshot(auth_request(
            "GET",
            "/v2/migrations/workspaces",
            &token,
            None,
            None,
        ))
        .await
        .expect("status response");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status: runtime_core::LegacyWorkspaceMigrationStatus = decode(status_response).await;
    assert_eq!(status.unresolved_subjects, 0);
    assert!(!status.cutover_blocked);

    for session_id in [&mapped_id, &orphan_id] {
        let v1_response = router
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/v1/sessions/{session_id}"),
                &token,
                None,
                None,
            ))
            .await
            .expect("v1 session response");
        assert_eq!(v1_response.status(), StatusCode::OK);
        let session: runtime_core::SessionRecord = decode(v1_response).await;
        assert_eq!(session.id, *session_id);
    }

    let operation_response = router
        .clone()
        .oneshot(auth_request(
            "GET",
            &format!("/v2/operations/{}", applied.operation_id),
            &token,
            None,
            None,
        ))
        .await
        .expect("operation response");
    assert_eq!(operation_response.status(), StatusCode::OK);
    let operation: runtime_core::OperationDetails = decode(operation_response).await;
    assert_eq!(operation.operation.kind, "legacy_workspace_migration_apply");
    assert_eq!(
        operation.operation.phase,
        runtime_core::OperationPhase::Completed
    );
}

#[tokio::test]
async fn workspace_migration_routes_require_bearer_auth() {
    let (router, _token, _temp_dir) = build_test_router().await;
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/migrations/workspaces/preview")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

async fn create_session(router: &Router, token: &str, cwd: Option<&str>) -> String {
    let body = serde_json::json!({
        "provider": "codex",
        "model": "test-model",
        "cwd": cwd,
        "permission_mode": null,
        "metadata": {}
    });
    let response = router
        .clone()
        .oneshot(auth_request(
            "POST",
            "/v1/sessions",
            token,
            Some(body),
            None,
        ))
        .await
        .expect("create session response");
    assert_eq!(response.status(), StatusCode::OK);
    let session: runtime_core::SessionRecord = decode(response).await;
    session.id
}

fn auth_request(
    method: &str,
    uri: &str,
    token: &str,
    body: Option<serde_json::Value>,
    idempotency_key: Option<&str>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    if let Some(key) = idempotency_key {
        builder = builder.header("Idempotency-Key", key);
    }
    builder
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .expect("request")
}

async fn decode<T: serde::de::DeserializeOwned>(response: Response) -> T {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("response json")
}

fn init_git_repo(root: &Path) {
    std::fs::create_dir_all(root).expect("repo dir");
    run_git(root, &["init"]);
    run_git(root, &["config", "user.email", "runtime@example.invalid"]);
    run_git(root, &["config", "user.name", "Runtime Test"]);
    std::fs::write(root.join("README.md"), "http migration fixture\n").expect("seed");
    run_git(root, &["add", "."]);
    run_git(root, &["commit", "-m", "seed"]);
}

fn run_git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}
