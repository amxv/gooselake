use super::*;

#[tokio::test]
async fn worktree_diagnostics_aggregate_preserves_auth_exact_json_and_orphan_counts() {
    let (router, token, temp) = build_test_router().await;
    // Fixture-only database: insert one orphan that a live FK-enforced writer
    // would reject, to assert historical anomaly parity without mutation.
    let mut db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
    let tx = db.transaction().unwrap();
    let private_path = format!("/private/{}", "p".repeat(1024));
    for i in 0..960i64 {
        tx.execute(
            "INSERT INTO managed_worktrees (
               id, repo_root, worktree_root, worktree_cwd, branch_name,
               worktree_name, unified_workspace_path, deletion_policy, created_at, updated_at
             ) VALUES (?1, '/fixture', ?2, ?2, ?1, ?1, ?2, ?3, ?4, ?4)",
            rusqlite::params![
                format!("wt_{i:04}"),
                private_path,
                if i % 2 == 0 { "retain" } else { "cleanup" },
                i + 1
            ],
        )
        .unwrap();
    }
    for i in 0..480i64 {
        tx.execute(
            "INSERT INTO sessions (id, provider, status, created_at, updated_at)
            VALUES (?1, 'inert', 'ready', ?2, ?2)",
            rusqlite::params![format!("session_{i:04}"), i + 1],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO managed_worktree_claims (
            worktree_id, session_id, claim_role, created_at, released_at
            ) VALUES (?1, ?2, 'primary', ?3, ?4)",
            rusqlite::params![
                format!("wt_{i:04}"),
                format!("session_{i:04}"),
                i + 1,
                if i % 4 == 0 { Some(i + 2) } else { None }
            ],
        )
        .unwrap();
    }
    tx.execute(
        "INSERT INTO managed_worktree_claims (
        worktree_id, session_id, claim_role, created_at
        ) VALUES ('ghost_worktree', 'session_0000', 'primary', 1000)",
        [],
    )
    .unwrap();
    tx.commit().unwrap();

    let forbidden = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/worktrees")
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
                .uri("/v1/diagnostics/worktrees")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(standalone.status(), StatusCode::OK);
    let body = to_bytes(standalone.into_body(), usize::MAX).await.unwrap();
    let counts: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(counts.as_object().unwrap().len(), 4);
    assert_eq!(counts["worktree_total"], 960);
    assert_eq!(counts["active_claim_total"], 361);
    assert_eq!(counts["orphaned_claim_total"], 1);
    assert_eq!(counts["deletion_policy_counts"]["retain"], 480);
    assert_eq!(counts["deletion_policy_counts"]["cleanup"], 480);
    assert!(!body.windows(b"/private/".len()).any(|w| w == b"/private/"));

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
    assert_eq!(combined["worktrees"], counts);
    assert!(combined["recovery"]["active_anomalies"].is_array());
}
