use super::*;

#[tokio::test]
async fn process_diagnostics_aggregate_preserves_json_and_operator_auth_at_scale() {
    let (router, token, temp) = build_test_router().await;
    let mut db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let tx = db.transaction().unwrap();
    let command = serde_json::json!({"private_command": "s".repeat(1024)}).to_string();
    for i in 0..960i64 {
        tx.execute(
            "INSERT INTO managed_processes (
                process_id, command_json, status, admission_order, queue_order,
                capture_limit_bytes, stdout_path, stderr_path, admitted_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?4, 1024, '/private/out', '/private/err', ?4, ?4)",
            rusqlite::params![
                format!("diagnostic_{i:04}"),
                command,
                if i % 4 == 0 { "queued" } else { "completed" },
                i + 1,
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let forbidden = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/processes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::UNAUTHORIZED);

    let standalone = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/processes")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(standalone.status(), StatusCode::OK);
    let body = to_bytes(standalone.into_body(), usize::MAX).await.unwrap();
    let counts: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(counts.as_object().unwrap().len(), 2);
    assert_eq!(counts["process_total"], 960);
    assert_eq!(counts["process_status_counts"]["queued"], 240);
    assert_eq!(counts["process_status_counts"]["completed"], 720);
    assert!(!body
        .windows(b"private_command".len())
        .any(|window| window == b"private_command"));

    let combined = router
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(combined.status(), StatusCode::OK);
    let combined_body = to_bytes(combined.into_body(), usize::MAX).await.unwrap();
    let combined: Value = serde_json::from_slice(&combined_body).unwrap();
    assert_eq!(combined["processes"], counts);
    assert!(combined["recovery"]["active_anomalies"].is_array());
}
