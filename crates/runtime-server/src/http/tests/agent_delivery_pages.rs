use super::agent_message_routes::{create_agent, create_message, register_workspace};
use super::*;

#[tokio::test]
async fn delivery_page_cursor_must_identify_a_row_in_the_selected_message_and_recipient() {
    let (router, token, temp) = build_test_router().await;
    let root = temp.path().join("delivery-page-anchors");
    fs::create_dir_all(&root).unwrap();
    let workspace = register_workspace(&router, &token, &root, "Delivery Anchors").await;
    let sender = create_agent(&router, &token, &workspace.workspace_id, &root, "Sender").await;
    let receiver_one = create_agent(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Receiver One",
    )
    .await;
    let receiver_two = create_agent(
        &router,
        &token,
        &workspace.workspace_id,
        &root,
        "Receiver Two",
    )
    .await;
    let message_body = serde_json::json!({
        "mode":"broadcast", "sender_agent_id":sender.agent_id,
        "input":[{"type":"text","text":"page the snapshot"}],
    });
    let first = create_message(&router, &token, "delivery-page-first", message_body.clone()).await;
    let other = create_message(&router, &token, "delivery-page-other", message_body).await;
    assert_eq!(first.deliveries.len(), 2);
    assert_eq!(other.deliveries.len(), 2);

    let get = |uri: &str| {
        Request::builder()
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let base = format!("/v2/messages/{}/deliveries", first.message.id);
    let page_one = router
        .clone()
        .oneshot(get(&format!("{base}?limit=1")))
        .await
        .unwrap();
    assert_eq!(page_one.status(), StatusCode::OK);
    let cursor = page_one
        .headers()
        .get("x-next-cursor")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let first_page: Vec<runtime_core::AgentDeliveryRecord> =
        serde_json::from_slice(&to_bytes(page_one.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(first_page.len(), 1);
    let page_two = router
        .clone()
        .oneshot(get(&format!("{base}?limit=1&cursor={cursor}")))
        .await
        .unwrap();
    assert_eq!(page_two.status(), StatusCode::OK);
    assert!(page_two.headers().get("x-next-cursor").is_none());
    let second_page: Vec<runtime_core::AgentDeliveryRecord> =
        serde_json::from_slice(&to_bytes(page_two.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(second_page.len(), 1);
    assert_ne!(first_page[0].id, second_page[0].id);

    let selected = &first.deliveries[0];
    let foreign = &other.deliveries[0];
    let wrong_recipient = if selected.recipient_agent_id == receiver_one.agent_id {
        receiver_two.agent_id.as_str()
    } else {
        receiver_one.agent_id.as_str()
    };
    for invalid in [
        format!("{base}?cursor={}:{}", foreign.created_at, foreign.id),
        format!("{base}?cursor={}:{}", selected.created_at + 1, selected.id),
        format!("{base}?cursor=100:delivery_nonexistent"),
        format!(
            "{base}?recipient_agent_id={wrong_recipient}&cursor={}:{}",
            selected.created_at, selected.id
        ),
    ] {
        let response = router.clone().oneshot(get(&invalid)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{invalid}");
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["error"]["code"], "invalid_state", "{invalid}");
    }
}
