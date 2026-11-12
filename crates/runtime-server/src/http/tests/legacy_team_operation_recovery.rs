use super::*;

#[tokio::test]
async fn exact_spawn_recovery_stage_is_authenticated_and_survives_independent_sqlite_reads() {
    let (router, token, temp) = build_test_router().await;
    let db = temp.path().join("runtime.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: db.clone(),
    });
    let op_id = "op_spawn_test-worker_17";
    store
        .upsert_team_operation_journal(&runtime_core::TeamOperationJournalRecord {
            operation_id: op_id.into(),
            team_id: "team_disposable".into(),
            kind: "spawn_member_with_worktree".into(),
            stage: "rollback_pending".into(),
            payload: serde_json::json!({
                "spawned_session_id":"session_disposable",
                "created_worktree_id":"wt_disposable",
            }),
            created_at: 10,
            updated_at: 20,
        })
        .unwrap();
    store
        .append_team_operation_diagnostic(
            Some(op_id),
            Some("team_disposable"),
            "native_close_unknown",
            "native close requires independent review",
            &serde_json::json!({"native_retry":false}),
            30,
        )
        .unwrap();

    let unauthenticated = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations/{op_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let wrong_bearer = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations/{op_id}"))
                .header(header::AUTHORIZATION, "Bearer wrong-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_bearer.status(), StatusCode::UNAUTHORIZED);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations/{op_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["operation"]["operation_id"], op_id);
    assert_eq!(body["operation"]["stage"], "rollback_pending");
    assert_eq!(body["operation"]["created_at"], 10);
    assert_eq!(
        body["operation"]["payload"]["created_worktree_id"],
        "wt_disposable"
    );
    assert_eq!(body["diagnostics"][0]["code"], "native_close_unknown");
    assert_eq!(body["native_retry_authorized"], false);
    assert_eq!(body["requires_manual_review"], true);

    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig { database_path: db });
    let row = reopened.get_team_operation_journal(op_id).unwrap().unwrap();
    assert_eq!(row.stage, "rollback_pending");
    assert_eq!(row.created_at, 10);
    assert_eq!(
        reopened
            .get_team_operation_journal("unknown-operation")
            .unwrap(),
        None
    );

    let not_found = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/team-operations/does-not-exist")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(not_found.status(), StatusCode::NOT_FOUND);

    let error = ApiError::from(RuntimeError::operation_unresolved(
        op_id,
        "ambiguous native effect",
    ));
    assert_eq!(error.clone().into_response().status(), StatusCode::CONFLICT);
    assert_eq!(error.code, "outcome_unresolved");
    assert!(error
        .message
        .contains(&format!("/v1/diagnostics/team-operations/{op_id}")));

    let canonical_error = ApiError::from(RuntimeError::operation_unresolved(
        "op_native_test",
        "unresolved",
    ));
    assert!(canonical_error
        .message
        .contains("/v2/operations/op_native_test"));
}
