use super::*;

#[tokio::test]
async fn legacy_comms_aggregate_preserves_auth_historical_json_and_private_inputs() {
    let (router, token, temp) = build_test_router().await;
    let mut db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let transaction = db.transaction().unwrap();
    let private_input = serde_json::json!({
        "secret_fixture_input": "p".repeat(1024)
    })
    .to_string();
    let statuses = ["pending", "deferred", "failed", "historical_unknown"];
    for i in 0..960i64 {
        let team_id = format!("team_{:04}", i % 480);
        let session_id = format!("session_{:04}", i % 480);
        let message_id = format!("message_{i:04}");
        if i < 480 {
            transaction
                .execute(
                    "INSERT INTO sessions(id,provider,status,created_at,updated_at)
                 VALUES(?1,'inert','ready',?2,?2)",
                    rusqlite::params![session_id, i + 1],
                )
                .unwrap();
            transaction.execute(
                "INSERT INTO teams(id,name,lead_agent_id,created_by,created_at,updated_at,deleted_at)
                 VALUES(?1,'legacy',?2,'fixture',?3,?3,?4)",
                rusqlite::params![
                    team_id,
                    session_id,
                    i + 1,
                    (i % 3 == 0).then_some(i + 2)
                ],
            ).unwrap();
            transaction
                .execute(
                    "INSERT INTO team_members(team_id,agent_id,joined_at,added_by)
                 VALUES(?1,?2,?3,'fixture')",
                    rusqlite::params![team_id, session_id, i + 1],
                )
                .unwrap();
        }
        transaction
            .execute(
                "INSERT INTO team_messages(id,team_id,scope,sender_agent_id,
                  recipient_agent_ids_json,input_json,priority,policy,created_at)
             VALUES(?1,?2,'direct',?3,'[]',?4,'normal','auto',?5)",
                rusqlite::params![message_id, team_id, session_id, private_input, i + 1],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO team_deliveries(id,message_id,team_id,recipient_agent_id,
                  provider,status,created_at,updated_at)
             VALUES(?1,?2,?3,?4,'inert',?5,?6,?6)",
                rusqlite::params![
                    format!("delivery_{i:04}"),
                    message_id,
                    team_id,
                    session_id,
                    statuses[i as usize % 4],
                    i + 1
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let unauthenticated = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/comms")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let standalone = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/comms")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(standalone.status(), StatusCode::OK);
    let body = to_bytes(standalone.into_body(), usize::MAX).await.unwrap();
    let counts: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(counts.as_object().unwrap().len(), 5);
    assert_eq!(counts["team_count"], 480);
    assert_eq!(counts["member_count"], 480);
    assert_eq!(counts["message_count"], 960);
    assert_eq!(counts["delivery_total"], 960);
    for status in statuses {
        assert_eq!(counts["delivery_status_counts"][status], 240);
    }
    assert!(!body
        .windows(b"secret_fixture_input".len())
        .any(|window| window == b"secret_fixture_input"));

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
    let combined: Value =
        serde_json::from_slice(&to_bytes(combined.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(combined["comms"], counts);
    assert!(combined["recovery"]["active_anomalies"].is_array());
}
