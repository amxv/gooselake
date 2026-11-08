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

    let unsupported_key = router
        .clone()
        .oneshot(auth_request(
            "POST",
            "/v2/migrations/workspaces/preview",
            &token,
            None,
            Some("must-not-claim-preview-replay"),
        ))
        .await
        .unwrap();
    assert_eq!(unsupported_key.status(), StatusCode::BAD_REQUEST);
    let keyed: serde_json::Value = decode(unsupported_key).await;
    assert_eq!(keyed["error"]["code"], "idempotency_not_supported");

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

    // Page only the diagnostic subjects, never the global migration totals.
    // Both preview and persisted-status routes use the same stable ordering.
    for route in [
        ("POST", "/v2/migrations/workspaces/preview"),
        ("GET", "/v2/migrations/workspaces"),
    ] {
        let first_response = router
            .clone()
            .oneshot(auth_request(
                route.0,
                &format!("{}?limit=1", route.1),
                &token,
                None,
                None,
            ))
            .await
            .expect("first migration page");
        assert_eq!(first_response.status(), StatusCode::OK);
        let cursor = first_response.headers()["x-next-cursor"]
            .to_str()
            .unwrap()
            .to_string();
        let first: runtime_core::LegacyWorkspaceMigrationStatus = decode(first_response).await;
        assert_eq!(first.subjects.len(), 1);
        assert_eq!(first.mapped_subjects, 1);
        assert_eq!(first.unresolved_subjects, 1);

        let second_response = router
            .clone()
            .oneshot(auth_request(
                route.0,
                &format!("{}?limit=1&cursor={cursor}", route.1),
                &token,
                None,
                None,
            ))
            .await
            .expect("second migration page");
        assert_eq!(second_response.status(), StatusCode::OK);
        assert!(!second_response.headers().contains_key("x-next-cursor"));
        let second: runtime_core::LegacyWorkspaceMigrationStatus = decode(second_response).await;
        assert_eq!(second.subjects.len(), 1);
        assert_eq!(second.mapped_subjects, 1);
        assert_eq!(second.unresolved_subjects, 1);
        assert_ne!(first.subjects[0].subject_id, second.subjects[0].subject_id);

        // A well-formed opaque continuation must refer to a real subject in
        // the *current* migration snapshot. It is not a free-form seek key.
        let forged = encode_opaque_pair_cursor("0", "not_a_current_migration_subject");
        let response = router
            .clone()
            .oneshot(auth_request(
                route.0,
                &format!("{}?limit=1&cursor={forged}", route.1),
                &token,
                None,
                None,
            ))
            .await
            .expect("forged opaque migration cursor response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let invalid: serde_json::Value = decode(response).await;
        assert_eq!(invalid["error"]["code"], "invalid_request");

        for bad_query in [
            "limit=0",
            "limit=101",
            "cursor=zz",
            "cursor=5b5d",
            "cursor=abc",
        ] {
            let invalid = router
                .clone()
                .oneshot(auth_request(
                    route.0,
                    &format!("{}?{bad_query}", route.1),
                    &token,
                    None,
                    None,
                ))
                .await
                .expect("invalid migration page");
            assert_eq!(invalid.status(), StatusCode::BAD_REQUEST, "{bad_query}");
        }
    }

    // Neither preview nor apply has a JSON input contract. An optimistic
    // `dry_run` or `force` flag must not be ignored while rewriting durable
    // migration state or admitting a migration operation.
    let db = rusqlite::Connection::open(temp_dir.path().join("runtime.sqlite3")).unwrap();
    let before_unexpected_bodies: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_operations", [], |row| {
            row.get(0)
        })
        .unwrap();
    for path in [
        "/v2/migrations/workspaces/preview",
        "/v2/migrations/workspaces/apply",
    ] {
        for unexpected in [
            serde_json::json!({"dry_run":true}),
            serde_json::json!({"force":true}),
        ] {
            let response = router
                .clone()
                .oneshot(auth_request(
                    "POST",
                    path,
                    &token,
                    Some(unexpected),
                    Some("discarded-options-must-not-admit"),
                ))
                .await
                .expect("unsupported mutation body response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            let error: serde_json::Value = decode(response).await;
            assert_eq!(error["error"]["code"], "invalid_request");
            assert_eq!(error["error"]["retryable"], false);
        }
    }
    let after_unexpected_bodies: i64 = db
        .query_row("SELECT COUNT(*) FROM runtime_operations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(after_unexpected_bodies, before_unexpected_bodies);
    let before_apply = router
        .clone()
        .oneshot(auth_request("GET", "/v2/workspaces", &token, None, None))
        .await
        .unwrap();
    let unchanged_workspaces: Vec<runtime_core::WorkspaceRecord> = decode(before_apply).await;
    assert!(
        unchanged_workspaces.is_empty(),
        "JSON options must not apply migration"
    );

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
    let apply_operation_id = apply_response.headers()["x-operation-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let applied: runtime_core::LegacyWorkspaceMigrationApplyResponse = decode(apply_response).await;
    assert_eq!(applied.operation_id, apply_operation_id);
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

    // Rejected options must not silently authorize an authority rewrite or
    // allocate an operation, even if the other resolution fields are valid.
    let db = rusqlite::Connection::open(temp_dir.path().join("runtime.sqlite3")).unwrap();
    let operation_count = || -> i64 {
        db.query_row("SELECT COUNT(*) FROM runtime_operations", [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    let before_invalid_resolution = operation_count();
    for invalid in [
        serde_json::json!({
            "action":"map", "workspace_id":workspaces[0].workspace_id,
            "force":true
        }),
        serde_json::json!({
            "action":"archive", "workspace_id":null,
            "dry_run":true
        }),
    ] {
        let response = router
            .clone()
            .oneshot(auth_request(
                "POST",
                &format!("/v2/migrations/workspaces/resolutions/session/{orphan_id}"),
                &token,
                Some(invalid),
                Some("invalid-migration-options"),
            ))
            .await
            .expect("invalid migration resolution response");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value = decode(response).await;
        assert_eq!(body["error"]["code"], "invalid_request");
    }
    assert_eq!(operation_count(), before_invalid_resolution);

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
    let resolution_operation_id = resolution_response.headers()["x-operation-id"]
        .to_str()
        .unwrap()
        .to_string();
    let resolution: runtime_core::LegacyWorkspaceMigrationResolutionResponse =
        decode(resolution_response).await;
    assert_eq!(resolution.operation_id, resolution_operation_id);
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
