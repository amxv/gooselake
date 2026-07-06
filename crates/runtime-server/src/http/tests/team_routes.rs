use super::*;

#[tokio::test]
async fn team_routes_lifecycle_and_controls() {
    let (router, token, _temp_dir) = build_test_router().await;

    let leader_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
    let member_one_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
    let member_two_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "Lifecycle Team",
                        "lead_agent_id": leader_session_id,
                        "member_agent_ids": [member_one_session_id]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team response");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("create team body"),
    )
    .expect("create team json");
    let team_id = create_team_json["team"]["id"]
        .as_str()
        .expect("team id")
        .to_string();

    let list_teams_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/teams")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("list teams response");
    assert_eq!(list_teams_response.status(), StatusCode::OK);
    let list_teams_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(list_teams_response.into_body(), usize::MAX)
            .await
            .expect("list teams body"),
    )
    .expect("list teams json");
    assert_eq!(list_teams_json.as_array().map(Vec::len), Some(1));

    let get_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get team response");
    assert_eq!(get_team_response.status(), StatusCode::OK);
    let get_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(get_team_response.into_body(), usize::MAX)
            .await
            .expect("get team body"),
    )
    .expect("get team json");
    assert_eq!(
        get_team_json["members"].as_array().map(Vec::len),
        Some(2),
        "team should include lead plus one member"
    );

    let join_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/members"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "agent_id": member_two_session_id,
                        "title": "Worker"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("join response");
    assert_eq!(join_response.status(), StatusCode::OK);

    let set_lead_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/lead"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "lead_agent_id": member_one_session_id
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("set lead response");
    assert_eq!(set_lead_response.status(), StatusCode::OK);
    let set_lead_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(set_lead_response.into_body(), usize::MAX)
            .await
            .expect("set lead body"),
    )
    .expect("set lead json");
    assert_eq!(
        set_lead_json["team"]["lead_agent_id"].as_str(),
        Some(member_one_session_id.as_str())
    );

    let remove_member_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/teams/{team_id}/members/{leader_session_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("remove member response");
    assert_eq!(remove_member_response.status(), StatusCode::OK);
    let remove_member_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(remove_member_response.into_body(), usize::MAX)
            .await
            .expect("remove member body"),
    )
    .expect("remove member json");
    assert_eq!(
        remove_member_json["members"].as_array().map(Vec::len),
        Some(2)
    );

    let direct_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/messages"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": member_one_session_id,
                        "recipient_agent_id": member_two_session_id,
                        "input": [{"type":"text","text":"phase5 lifecycle direct"}],
                        "policy": "non_interrupting",
                        "priority": "normal",
                        "idempotency_key": "phase5_lifecycle_direct"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("direct response");
    assert_eq!(direct_response.status(), StatusCode::OK);
    let direct_ack: serde_json::Value = serde_json::from_slice(
        &to_bytes(direct_response.into_body(), usize::MAX)
            .await
            .expect("direct body"),
    )
    .expect("direct ack");
    let message_id = direct_ack["message"]["id"]
        .as_str()
        .expect("message id")
        .to_string();
    let delivery_id = direct_ack["deliveries"][0]["id"]
        .as_str()
        .expect("delivery id")
        .to_string();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let list_messages_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/messages?limit=10"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("list messages response");
    assert_eq!(list_messages_response.status(), StatusCode::OK);
    let list_messages_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(list_messages_response.into_body(), usize::MAX)
            .await
            .expect("list messages body"),
    )
    .expect("list messages json");
    assert!(
        list_messages_json["messages"].as_array().map(|messages| {
            messages
                .iter()
                .any(|message| message["id"].as_str() == Some(message_id.as_str()))
        }) == Some(true),
        "expected direct message in list"
    );

    let list_deliveries_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/teams/{team_id}/deliveries?message_id={message_id}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("list deliveries response");
    assert_eq!(list_deliveries_response.status(), StatusCode::OK);
    let list_deliveries_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(list_deliveries_response.into_body(), usize::MAX)
            .await
            .expect("list deliveries body"),
    )
    .expect("list deliveries json");
    assert!(
        list_deliveries_json.as_array().map(|rows| !rows.is_empty()) == Some(true),
        "expected at least one delivery for direct message"
    );

    let retry_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v1/teams/{team_id}/deliveries/{delivery_id}/retry"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("retry response");
    assert!(
        retry_response.status() == StatusCode::OK
            || retry_response.status() == StatusCode::BAD_REQUEST,
        "retry route should be wired and return a domain status"
    );

    let cancel_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/messages/{message_id}/cancel"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("cancel response");
    assert!(
        cancel_response.status() == StatusCode::OK
            || cancel_response.status() == StatusCode::BAD_REQUEST,
        "cancel route should be wired and return a domain status"
    );

    let snapshot_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/view"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("snapshot response");
    assert_eq!(snapshot_response.status(), StatusCode::OK);

    let interrupt_all_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/interrupt-all"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("interrupt-all response");
    assert_eq!(interrupt_all_response.status(), StatusCode::OK);
    let interrupt_all_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(interrupt_all_response.into_body(), usize::MAX)
            .await
            .expect("interrupt-all body"),
    )
    .expect("interrupt-all json");
    assert_eq!(
        interrupt_all_json["team_id"].as_str(),
        Some(team_id.as_str())
    );

    let delete_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/teams/{team_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("delete team response");
    assert_eq!(delete_team_response.status(), StatusCode::NO_CONTENT);

    let get_deleted_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("get deleted team response");
    assert_eq!(get_deleted_team_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn team_and_global_events_stream_replay_then_live() {
    let (router, token, _temp_dir) = build_test_router().await;

    let lead_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
    let member_one_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
    let member_two_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "SSE Team",
                        "lead_agent_id": lead_session_id,
                        "member_agent_ids": [member_one_session_id]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team response");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("create team body"),
    )
    .expect("create team json");
    let team_id = create_team_json["team"]["id"]
        .as_str()
        .expect("team id")
        .to_string();

    let direct_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/messages"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": lead_session_id,
                        "recipient_agent_id": member_one_session_id,
                        "input": [{"type":"text","text":"phase5 team stream seed"}],
                        "policy": "non_interrupting",
                        "priority": "normal",
                        "idempotency_key": "phase5_stream_direct_1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("direct response");
    assert_eq!(direct_response.status(), StatusCode::OK);

    let team_events_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/events"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("team events response");
    assert_eq!(team_events_response.status(), StatusCode::OK);
    let team_events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
        &to_bytes(team_events_response.into_body(), usize::MAX)
            .await
            .expect("team events body"),
    )
    .expect("team events json");
    assert!(
        team_events.len() >= 2,
        "expected at least two team events for replay"
    );
    let team_cursor = team_events[0].seq;
    let replay_team_ids = team_events
        .iter()
        .filter(|event| event.seq > team_cursor)
        .map(|event| event.seq.to_string())
        .collect::<Vec<_>>();

    let team_stream_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/teams/{team_id}/events/stream?after_seq={team_cursor}"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("team stream response");
    assert_eq!(team_stream_response.status(), StatusCode::OK);
    let mut team_stream = team_stream_response.into_body().into_data_stream();

    let join_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/members"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "agent_id": member_two_session_id
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("join response");
    assert_eq!(join_response.status(), StatusCode::OK);

    let mut team_sse_payload = String::new();
    let mut observed_team_live = false;
    for _ in 0..20 {
        let next = timeout(Duration::from_secs(1), team_stream.next()).await;
        if let Ok(Some(Ok(chunk))) = next {
            team_sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
            if replay_team_ids
                .iter()
                .all(|seq| team_sse_payload.contains(format!("id: {seq}").as_str()))
                && team_sse_payload.contains("event: team.member_joined")
            {
                observed_team_live = true;
                break;
            }
        }
    }
    assert!(
            observed_team_live,
            "expected team stream replay ids and team.member_joined live event in payload: {team_sse_payload}"
        );

    let global_events_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("global events response");
    assert_eq!(global_events_response.status(), StatusCode::OK);
    let global_events: Vec<runtime_core::RuntimeEventRecord> = serde_json::from_slice(
        &to_bytes(global_events_response.into_body(), usize::MAX)
            .await
            .expect("global events body"),
    )
    .expect("global events json");
    assert!(
        global_events.len() >= 2,
        "expected at least two global events for replay"
    );
    let global_cursor = global_events[0].row_id;
    let replay_global_ids = global_events
        .iter()
        .filter(|event| event.row_id > global_cursor)
        .map(|event| event.row_id.to_string())
        .collect::<Vec<_>>();

    let global_stream_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/events/stream?after_seq={global_cursor}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("global stream response");
    assert_eq!(global_stream_response.status(), StatusCode::OK);
    let mut global_stream = global_stream_response.into_body().into_data_stream();

    let set_lead_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/lead"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "lead_agent_id": member_one_session_id
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("set lead response");
    assert_eq!(set_lead_response.status(), StatusCode::OK);

    let mut global_sse_payload = String::new();
    let mut observed_global_live = false;
    for _ in 0..20 {
        let next = timeout(Duration::from_secs(1), global_stream.next()).await;
        if let Ok(Some(Ok(chunk))) = next {
            global_sse_payload.push_str(String::from_utf8_lossy(chunk.as_ref()).as_ref());
            if replay_global_ids
                .iter()
                .all(|seq| global_sse_payload.contains(format!("id: {seq}").as_str()))
                && global_sse_payload.contains("event: team.lead_changed")
            {
                observed_global_live = true;
                break;
            }
        }
    }
    assert!(
            observed_global_live,
            "expected global stream replay ids and team.lead_changed live event in payload: {global_sse_payload}"
        );
}

#[tokio::test]
async fn team_routes_direct_and_broadcast_create_deliveries_and_snapshot() {
    let (router, token, _temp_dir) = build_test_router().await;

    let leader_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;
    let teammate_session_id = create_test_session(router.clone(), token.as_str(), "phase5").await;

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "Phase5 Team",
                        "lead_agent_id": leader_session_id,
                        "member_agent_ids": [teammate_session_id]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team response");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("create team body"),
    )
    .expect("create team json");
    let team_id = create_team_json
        .pointer("/team/id")
        .and_then(serde_json::Value::as_str)
        .expect("team id")
        .to_string();

    let direct_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/messages"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": leader_session_id,
                        "recipient_agent_id": teammate_session_id,
                        "input": [{"type":"text","text":"phase5 direct hello"}],
                        "policy": "non_interrupting",
                        "priority": "normal",
                        "idempotency_key": "phase5_direct_1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("direct send response");
    assert_eq!(direct_response.status(), StatusCode::OK);

    let broadcast_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/broadcasts"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": leader_session_id,
                        "input": [{"type":"text","text":"phase5 broadcast hello"}],
                        "policy": "start_new_turn_only",
                        "priority": "normal",
                        "include_sender": false,
                        "idempotency_key": "phase5_broadcast_1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("broadcast response");
    assert_eq!(broadcast_response.status(), StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let deliveries_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/deliveries"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("deliveries response");
    assert_eq!(deliveries_response.status(), StatusCode::OK);
    let deliveries: Vec<runtime_core::TeamDeliveryRecord> = serde_json::from_slice(
        &to_bytes(deliveries_response.into_body(), usize::MAX)
            .await
            .expect("deliveries body"),
    )
    .expect("deliveries json");
    assert!(deliveries.len() >= 2, "expected at least two deliveries");

    let snapshot_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/view"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("snapshot response");
    assert_eq!(snapshot_response.status(), StatusCode::OK);
    let snapshot_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(snapshot_response.into_body(), usize::MAX)
            .await
            .expect("snapshot body"),
    )
    .expect("snapshot json");
    assert!(
        snapshot_json["messages"]
            .as_array()
            .map(|rows| !rows.is_empty())
            == Some(true),
        "expected snapshot messages"
    );
}

#[tokio::test]
#[ignore = "real Codex smoke test: requires local ~/.gg/codex/auth.json"]
async fn smoke_real_codex_phase5_team_comms_slice() {
    let home_dir = std::env::var("HOME").expect("HOME must be set");
    let source_auth = std::path::PathBuf::from(home_dir)
        .join(".gg")
        .join("codex")
        .join("auth.json");
    assert!(source_auth.exists(), "missing {}", source_auth.display());

    let temp_dir = tempfile::tempdir().expect("temp dir");
    let mut config = RuntimeServerConfig::default();
    config.data.root_dir = temp_dir.path().to_path_buf();
    config.providers.claude.enabled = false;
    config.providers.codex.enabled = true;

    let bootstrapped = bootstrap_runtime(config.clone()).await.expect("bootstrap");
    let token = bootstrapped.auth.bearer_token.clone();
    let router = build_router(AppState {
        app: bootstrapped.app,
        runtime: bootstrapped.runtime,
        bearer_token: token.clone(),
        public_base_url: bootstrapped.public_base_url,
        startup_recovery: Arc::new(runtime_core::StartupRecoverySummary::default()),
    });

    let create_session = |router: Router, token: String, cwd: String| async move {
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::json!({
                            "provider": "codex",
                            "model": codex_test_model(),
                            "cwd": cwd,
                            "metadata": {"smoke":"phase5_team_comms"}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .expect("create session response");
        assert_eq!(response.status(), StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("create body"),
        )
        .expect("create json");
        json["id"].as_str().expect("session id").to_string()
    };

    let cwd = temp_dir.path().display().to_string();
    let lead_session_id = create_session(router.clone(), token.clone(), cwd.clone()).await;
    let member_session_id = create_session(router.clone(), token.clone(), cwd.clone()).await;

    let create_team_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/teams")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "name": "Real Codex Team",
                        "lead_agent_id": lead_session_id,
                        "member_agent_ids": [member_session_id]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("create team");
    assert_eq!(create_team_response.status(), StatusCode::OK);
    let create_team_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(create_team_response.into_body(), usize::MAX)
            .await
            .expect("team body"),
    )
    .expect("team json");
    let team_id = create_team_json["team"]["id"]
        .as_str()
        .expect("team id")
        .to_string();

    let direct_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/messages"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": lead_session_id,
                        "recipient_agent_id": member_session_id,
                        "input": [{"type":"text","text":"Reply only with phase5teamtoken_89321"}],
                        "policy": "immediate_interrupt",
                        "priority": "high",
                        "idempotency_key": "phase5_smoke_direct_1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("direct response");
    assert_eq!(direct_response.status(), StatusCode::OK);

    let broadcast_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/teams/{team_id}/broadcasts"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(
                    serde_json::json!({
                        "sender_agent_id": lead_session_id,
                        "input": [{"type":"text","text":"Broadcast ack phase5teamtoken_89321"}],
                        "policy": "start_new_turn_only",
                        "priority": "normal",
                        "include_sender": true,
                        "idempotency_key": "phase5_smoke_broadcast_1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("broadcast response");
    assert_eq!(broadcast_response.status(), StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let deliveries_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/deliveries"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("deliveries response");
    assert_eq!(deliveries_response.status(), StatusCode::OK);
    let deliveries_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(deliveries_response.into_body(), usize::MAX)
            .await
            .expect("deliveries body"),
    )
    .expect("deliveries json");
    assert!(
        deliveries_json.as_array().map(|rows| !rows.is_empty()) == Some(true),
        "expected non-empty deliveries"
    );

    let events_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/teams/{team_id}/events"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("events response");
    assert_eq!(events_response.status(), StatusCode::OK);
    let events_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(events_response.into_body(), usize::MAX)
            .await
            .expect("events body"),
    )
    .expect("events json");
    let kinds = events_json
        .as_array()
        .into_iter()
        .flat_map(|events| events.iter())
        .filter_map(|event| event.get("kind"))
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    assert!(
        kinds.iter().any(|kind| *kind == "team_message.created"),
        "expected team_message.created in team events"
    );
}
