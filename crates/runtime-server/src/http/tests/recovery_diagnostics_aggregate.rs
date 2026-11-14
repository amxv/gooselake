use super::*;
use runtime_store_sqlite::SqliteRuntimeRepository;

#[tokio::test]
async fn recovery_diagnostics_indexed_snapshot_preserves_auth_order_and_combined_parity() {
    let (router, token, temp) = build_test_router().await;
    let path = temp.path().join("runtime.sqlite3");
    let mut db = rusqlite::Connection::open(&path).unwrap();
    // This fixture must exercise historical broken references, not authorize
    // their insertion by the live, FK-enforced runtime.
    db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
    let tx = db.transaction().unwrap();
    let private = serde_json::json!({"secret_fixture": "s".repeat(1024)}).to_string();
    for i in 0..480i64 {
        tx.execute(
            "INSERT INTO sessions(id,provider,status,metadata_json,active_turn_id,created_at,updated_at)
             VALUES(?1,'inert','ready',?2,?3,?4,?4)",
            rusqlite::params![format!("s_{i:04}"), private,
                              if i % 3 == 0 { format!("missing_{i}") } else { format!("t_{i:04}") },
                              i % 13],
        ).unwrap();
    }
    for i in 0..480i64 {
        if i % 3 == 0 {
            continue;
        }
        tx.execute(
            "INSERT INTO turns(id,session_id,status,input_json) VALUES(?1,?2,?3,?4)",
            rusqlite::params![
                format!("t_{i:04}"),
                if i % 3 == 1 {
                    "s_0000".to_string()
                } else {
                    format!("s_{i:04}")
                },
                if i % 3 == 2 { "failed" } else { "running" },
                private
            ],
        )
        .unwrap();
    }
    for i in 0..480i64 {
        tx.execute(
            "INSERT INTO approvals(id,session_id,turn_id,status,request_json,created_at)
             VALUES(?1,?2,?3,?4,?5,?6)",
            rusqlite::params![
                format!("a_{i:04}"),
                format!("s_{i:04}"),
                if i % 3 == 0 {
                    format!("missing_approval_{i}")
                } else {
                    format!("t_{i:04}")
                },
                if i % 5 == 0 { "resolved" } else { "pending" },
                private,
                i % 17
            ],
        )
        .unwrap();
    }
    for i in 0..240i64 {
        let worktree = format!("wt_{i:04}");
        tx.execute(
            "INSERT INTO managed_worktrees(id,repo_root,worktree_root,worktree_cwd,
                                           branch_name,worktree_name,unified_workspace_path,
                                           deletion_policy,created_at,updated_at)
             VALUES(?1,'/fixture','/fixture/w','/fixture/w',?1,?1,'/fixture/w','retain',?2,?2)",
            rusqlite::params![worktree, i],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO managed_worktree_claims(worktree_id,session_id,claim_role,created_at,released_at)
             VALUES(?1,?2,'primary',?3,?4)",
            rusqlite::params![worktree,
                if i % 2 == 0 { "s_0000".to_string() } else { format!("missing_session_{i}") },
                i % 11, if i % 5 == 0 { Some(i) } else { None }],
        ).unwrap();
    }
    tx.commit().unwrap();

    let repository = SqliteRuntimeRepository::new(path);
    let hydrated = repository.hydrate_runtime_state().unwrap();
    let historical = collect_recovery_anomalies(&hydrated, false);
    assert!(historical.len() > 500);

    for uri in ["/v1/diagnostics/recovery", "/v1/diagnostics"] {
        let unauthorized = router
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED, "{uri}");
    }

    let standalone = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/recovery")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(standalone.status(), StatusCode::OK);
    let body = to_bytes(standalone.into_body(), usize::MAX).await.unwrap();
    let recovery: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(recovery.as_object().unwrap().len(), 2);
    assert_eq!(recovery["active_anomalies"], serde_json::json!(historical));
    assert!(recovery["startup"].is_object());
    assert!(!body
        .windows(b"secret_fixture".len())
        .any(|w| w == b"secret_fixture"));

    let combined = router
        .clone()
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
    assert_eq!(combined["recovery"], recovery);
    assert!(combined["comms"].is_object());
    assert!(combined["processes"].is_object());
    assert!(combined["worktrees"].is_object());
    assert!(!combined_body
        .windows(b"secret_fixture".len())
        .any(|w| w == b"secret_fixture"));
}
