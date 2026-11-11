use super::*;
use runtime_core::RuntimeEventCriticality;
use runtime_store_sqlite::SqliteRuntimeRepository;

use super::durable_event_stream_routes::{observed_sse_ids, stream};

#[tokio::test]
async fn native_process_lifecycle_is_one_durable_journal_visible_in_operator_sse_after_restart() {
    let (router, token, temp) = build_test_router().await;
    let repository = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let marker = temp.path().join("executed-once");
    let script = temp.path().join("native-process.sh");
    std::fs::write(&script, format!("printf 'once' >> {}\n", marker.display())).unwrap();
    let start = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/processes")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "command": format!("sh {}", script.display()),
                        "cwd": temp.path().display().to_string()
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(start.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(start.into_body(), usize::MAX).await.unwrap()).unwrap();
    let process_id = body["process"]["process_id"].as_str().unwrap().to_string();

    let mut terminal = false;
    for _ in 0..200 {
        if repository
            .get_managed_process(&process_id)
            .unwrap()
            .is_some_and(|record| record.status == "completed")
        {
            terminal = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(terminal, "native process must persist completion");
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "once");

    let reopened = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let events = reopened
        .list_runtime_events(Some((RuntimeEventScope::Process, &process_id)), None, 10)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        ["process.queued", "process.started", "process.completed"]
    );
    assert_eq!(
        events.iter().map(|event| event.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    for event in &events {
        assert_eq!(event.criticality, RuntimeEventCriticality::Critical);
        assert_eq!(
            event.event_id,
            format!("evt_proc_{process_id}_{}", event.kind)
        );
    }

    let uri = format!("/v2/processes/{process_id}/events/stream?after_seq=0&limit=1");
    observed_sse_ids(stream(&router, &token, &uri, None, None).await, &[1, 2, 3]).await;
    let uri = format!("/v2/processes/{process_id}/events/stream?limit=1");
    observed_sse_ids(stream(&router, &token, &uri, None, Some(2)).await, &[3]).await;

    let denied = router
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
}
