use super::*;
use tokio::sync::Notify;

#[tokio::test]
async fn targeted_command_merge_cannot_overwrite_newer_sse_record() {
    let fetched = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runtime_addr = spawn_blocked_broadcast_runtime(fetched.clone(), release.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = Arc::new(live_gateway_with_session_version(config, 1).await);
    {
        let mut materialized = gateway.materialized.write().await;
        let state = materialized.get_mut("local").unwrap();
        state.upsert_team(team_record("team_1"));
        state
            .source_health
            .transition(SourceHealthState::Live, Some(10), None);
    }

    let command_gateway = gateway.clone();
    let command = tokio::spawn(async move {
        let mut conn = test_connection(&command_gateway);
        command_gateway
            .admit_and_route_command(
                &mut conn,
                broadcast_team_message_command("cmd_race", "team_1", "stale response"),
            )
            .await
    });
    fetched.notified().await;

    gateway
        .ingest_source_event(SourceEvent::from_runtime_event(
            "local",
            "static-0",
            RuntimeEventRecord {
                row_id: 11,
                event_id: "evt_message_11".into(),
                scope: RuntimeEventScope::Team,
                scope_id: "team_1".into(),
                session_id: None,
                team_id: Some("team_1".into()),
                turn_id: None,
                seq: 1,
                kind: "team_message.created".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: json!({ "message": team_message_value("newer event", 11) }),
                provider: None,
                provider_seq: None,
                created_at: 11,
            },
        ))
        .await;
    release.notify_one();
    let response = command.await.expect("command task");
    assert!(matches!(
        response.payload,
        Some(Payload::CommandAccepted(_))
    ));

    let materialized = gateway.materialized.read().await;
    let message = &materialized["local"].messages_by_team["team_1"][0];
    assert_eq!(
        message.input,
        json!([{ "type": "text", "text": "newer event" }])
    );
    assert_eq!(
        materialized["local"].source_health.last_source_seq,
        Some(11)
    );
}

async fn spawn_blocked_broadcast_runtime(fetched: Arc<Notify>, release: Arc<Notify>) -> SocketAddr {
    let handler = move || {
        let (fetched, release) = (fetched.clone(), release.clone());
        async move {
            fetched.notify_one();
            release.notified().await;
            Json(json!({
                "message": team_message_value("stale response", 10),
                "deliveries": [],
                "disposition": "accepted"
            }))
        }
    };
    let app = Router::new().route(
        "/v1/teams/{team_id}/broadcasts",
        axum::routing::post(handler),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

fn team_message_value(text: &str, created_at: i64) -> Value {
    json!({
        "id": "msg_race",
        "team_id": "team_1",
        "scope": "broadcast",
        "sender_agent_id": "session_1",
        "recipient_agent_ids": [],
        "input": [{ "type": "text", "text": text }],
        "image_paths": [],
        "priority": "normal",
        "policy": "non_interrupting",
        "correlation_id": "cmd_race",
        "reply_to_message_id": null,
        "idempotency_key": "cmd_race",
        "created_at": created_at
    })
}
