use super::*;
use tokio::sync::Notify;

#[tokio::test]
async fn post_command_bootstrap_cannot_overwrite_event_reduced_while_fetch_is_in_flight() {
    let fetched = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runtime_addr = spawn_blocked_bootstrap_runtime(fetched.clone(), release.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = Arc::new(live_gateway_with_session_version(config, 1).await);
    gateway
        .materialized
        .write()
        .await
        .get_mut("local")
        .unwrap()
        .source_health
        .transition(SourceHealthState::Live, Some(10), None);
    let refresh_gateway = gateway.clone();
    let command = send_turn_command("cmd_refresh_race");
    let refresh = tokio::spawn(async move {
        refresh_gateway.refresh_source_after_command(&command).await;
    });
    fetched.notified().await;
    gateway
        .ingest_source_event(SourceEvent::from_runtime_event(
            "local",
            "static-0",
            RuntimeEventRecord {
                row_id: 11,
                event_id: "evt_session_2".into(),
                scope: RuntimeEventScope::Session,
                scope_id: "session_2".into(),
                session_id: Some("session_2".into()),
                team_id: None,
                turn_id: None,
                seq: 1,
                kind: "session.created".into(),
                criticality: RuntimeEventCriticality::Critical,
                payload: json!({ "provider": "codex" }),
                provider: Some("codex".into()),
                provider_seq: Some(1),
                created_at: 11,
            },
        ))
        .await;
    release.notify_one();
    refresh.await.expect("refresh task");
    let states = gateway.materialized.read().await;
    let state = states.get("local").unwrap();
    assert_eq!(state.source_health.last_source_seq, Some(11));
    assert!(state.sessions.contains_key("session_2"));
}

async fn spawn_blocked_bootstrap_runtime(fetched: Arc<Notify>, release: Arc<Notify>) -> SocketAddr {
    let bootstrap = move || {
        let (fetched, release) = (fetched.clone(), release.clone());
        async move {
            fetched.notify_one();
            release.notified().await;
            Json(json!({
                "source_epoch": "static-0", "high_watermark": 10,
                "records": {
                    "sessions": [session_record()], "approvals": [], "teams": [],
                    "team_members": [], "team_messages": [], "team_deliveries": [],
                    "managed_worktrees": [], "managed_worktree_claims": [], "processes": []
                }
            }))
        }
    };
    let app = Router::new()
        .route("/v1/bootstrap", get(bootstrap))
        .route(
            "/v1/providers",
            get(|| async { Json(json!({ "providers": [] })) }),
        )
        .route(
            "/v1/diagnostics",
            get(|| async {
                Json(json!({
                    "providers": {}, "comms": {}, "processes": {}, "worktrees": {}, "recovery": {}
                }))
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}
