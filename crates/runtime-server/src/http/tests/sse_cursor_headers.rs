use super::*;

#[tokio::test]
async fn sse_rejects_duplicate_last_event_id_headers_before_replaying_history() {
    let (router, token, _temp) = build_test_router().await;
    for uri in ["/v1/events/stream", "/v1/events/stream?after_seq=0"] {
        for (first, second) in [
            ("0", "10"),
            ("10", "0"),
            ("0", "0"),
            ("not-an-integer", "0"),
        ] {
            let request = Request::builder()
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header("last-event-id", first)
                .header("last-event-id", second)
                .body(Body::empty())
                .unwrap();
            assert_eq!(request.headers().get_all("last-event-id").iter().count(), 2,);
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let value: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                    .unwrap();
            assert_eq!(
                value["error"],
                "exactly one Last-Event-ID header is allowed",
            );
        }
    }
    for uri in ["/v1/events/stream", "/v1/events/stream?after_seq=0"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header("last-event-id", "0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "single valid SSE cursor");
        drop(response);
    }
}
