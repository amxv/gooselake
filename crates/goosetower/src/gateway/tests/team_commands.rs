use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use super::*;

#[tokio::test]
async fn team_scoped_join_member_routes_to_team_source() {
    let hits = Arc::new(StdMutex::new(Vec::new()));
    let runtime_addr = spawn_accepting_create_runtime("local", hits.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    {
        let mut materialized = gateway.materialized.write().await;
        materialized
            .get_mut("local")
            .expect("local source")
            .upsert_team(team_record("team_1"));
    }
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(&mut conn, join_team_member_command("cmd_join", "team_1"))
        .await;

    assert!(
        matches!(response.payload, Some(Payload::CommandAccepted(_))),
        "expected join-team-member command to be accepted, got {:?}",
        response.payload
    );
    assert_eq!(
        hits.lock().unwrap().as_slice(),
        ["local:join_team_member:team_1:session_2"]
    );
}

#[tokio::test]
async fn team_broadcast_refreshes_existing_team_messages() {
    let sent = Arc::new(AtomicBool::new(false));
    let runtime_addr = spawn_team_broadcast_refresh_runtime(sent.clone()).await;
    let mut config = GoosetowerConfig::default();
    config.runtimes.sources[0].base_url = format!("http://{runtime_addr}");
    let gateway = live_gateway_with_session_version(config, 1).await;
    {
        let mut materialized = gateway.materialized.write().await;
        materialized
            .get_mut("local")
            .expect("local source")
            .upsert_team(team_record("team_1"));
    }
    let mut conn = test_connection(&gateway);

    let response = gateway
        .admit_and_route_command(
            &mut conn,
            broadcast_team_message_command("cmd_broadcast", "team_1", "Visible team message"),
        )
        .await;

    assert!(
        matches!(response.payload, Some(Payload::CommandAccepted(_))),
        "expected broadcast command to be accepted, got {:?}",
        response.payload
    );
    assert!(
        sent.load(Ordering::SeqCst),
        "runtime broadcast endpoint hit"
    );
    let materialized = gateway.materialized.read().await;
    let messages = materialized
        .get("local")
        .expect("local source")
        .messages_by_team
        .get("team_1")
        .expect("team messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, "msg_1");
    assert_eq!(
        messages[0].input,
        json!([{ "type": "text", "text": "Visible team message" }])
    );
}

async fn spawn_team_broadcast_refresh_runtime(sent: Arc<AtomicBool>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind broadcast refresh runtime");
    let addr = listener.local_addr().expect("runtime addr");
    tokio::spawn(async move {
        let post_sent = sent.clone();
        let post_broadcast = move |axum::extract::Path(team_id): axum::extract::Path<String>,
                                   Json(input): Json<Value>| {
            let sent = post_sent.clone();
            async move {
                sent.store(true, Ordering::SeqCst);
                Json(team_message_ack(
                    &team_id,
                    input["text"].as_str().unwrap_or_default(),
                ))
            }
        };
        let view_sent = sent.clone();
        let team_view = move |axum::extract::Path(team_id): axum::extract::Path<String>| {
            let sent = view_sent.clone();
            async move {
                let messages = if sent.load(Ordering::SeqCst) {
                    vec![team_message_json(&team_id, "Visible team message")]
                } else {
                    Vec::new()
                };
                Json(json!({
                    "team": team_with_members_json(&team_id),
                    "messages": messages,
                    "deliveries_by_message_id": {},
                    "next_message_cursor": null,
                    "snapshot_at": 2
                }))
            }
        };
        let app = Router::new()
            .route(
                "/v1/teams/{team_id}/broadcasts",
                axum::routing::post(post_broadcast),
            )
            .route("/v1/events", get(|| async { Json(Vec::<Value>::new()) }))
            .route(
                "/v1/sessions",
                get(|| async { Json(vec![session_record()]) }),
            )
            .route(
                "/v1/teams",
                get(|| async { Json(vec![team_with_members_json("team_1")]) }),
            )
            .route("/v1/teams/{team_id}/view", get(team_view))
            .route("/v1/processes", get(|| async { Json(Vec::<Value>::new()) }))
            .route("/v1/worktrees", get(|| async { Json(Vec::<Value>::new()) }))
            .route(
                "/v1/providers",
                get(|| async { Json(json!({ "providers": [] })) }),
            )
            .route(
                "/v1/diagnostics",
                get(|| async {
                    Json(json!({
                        "providers": {},
                        "comms": {},
                        "processes": {},
                        "worktrees": {},
                        "recovery": {},
                    }))
                }),
            );
        axum::serve(listener, app)
            .await
            .expect("serve broadcast refresh runtime");
    });
    addr
}

fn team_with_members_json(team_id: &str) -> Value {
    json!({
        "team": {
            "id": team_id,
            "name": "Live Team",
            "lead_agent_id": "session_1",
            "created_by": "session_1",
            "created_at": 1,
            "updated_at": 2,
            "deleted_at": null
        },
        "members": [{
            "team_id": team_id,
            "agent_id": "session_1",
            "title": "Lead",
            "joined_at": 1,
            "added_by": "session_1",
            "creator_agent_id": null,
            "creator_compaction_subscription": "auto",
            "worktree_id": null
        }]
    })
}

fn team_message_ack(team_id: &str, text: &str) -> Value {
    json!({
        "message": team_message_json(team_id, text),
        "deliveries": [],
        "disposition": "accepted"
    })
}

fn team_message_json(team_id: &str, text: &str) -> Value {
    json!({
        "id": "msg_1",
        "team_id": team_id,
        "scope": "broadcast",
        "sender_agent_id": "session_1",
        "recipient_agent_ids": ["session_1"],
        "input": [{ "type": "text", "text": text }],
        "image_paths": [],
        "priority": "normal",
        "policy": "default",
        "correlation_id": null,
        "reply_to_message_id": null,
        "idempotency_key": null,
        "created_at": 2
    })
}
