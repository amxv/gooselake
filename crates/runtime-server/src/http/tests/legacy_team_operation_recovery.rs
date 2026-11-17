use super::*;

#[tokio::test]
async fn exact_spawn_recovery_stage_is_authenticated_and_survives_independent_sqlite_reads() {
    let (router, token, temp) = build_test_router().await;
    let db = temp.path().join("runtime.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: db.clone(),
    });
    let op_id = "op_spawn_test-worker_17";
    store
        .upsert_team_operation_journal(&runtime_core::TeamOperationJournalRecord {
            operation_id: op_id.into(),
            team_id: "team_disposable".into(),
            kind: "spawn_member_with_worktree".into(),
            stage: "rollback_pending".into(),
            payload: serde_json::json!({
                "spawned_session_id":"session_disposable",
                "created_worktree_id":"wt_disposable",
            }),
            created_at: 10,
            updated_at: 20,
        })
        .unwrap();
    store
        .append_team_operation_diagnostic(
            Some(op_id),
            Some("team_disposable"),
            "native_close_unknown",
            "native close requires independent review",
            &serde_json::json!({"native_retry":false}),
            30,
        )
        .unwrap();

    let unauthenticated = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations/{op_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let wrong_bearer = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations/{op_id}"))
                .header(header::AUTHORIZATION, "Bearer wrong-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_bearer.status(), StatusCode::UNAUTHORIZED);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations/{op_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["operation"]["operation_id"], op_id);
    assert_eq!(body["operation"]["stage"], "rollback_pending");
    assert_eq!(body["operation"]["created_at"], 10);
    assert_eq!(
        body["operation"]["payload"]["created_worktree_id"],
        "wt_disposable"
    );
    assert_eq!(body["diagnostics"][0]["code"], "native_close_unknown");
    assert_eq!(body["native_retry_authorized"], false);
    assert_eq!(body["requires_manual_review"], true);

    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig { database_path: db });
    let row = reopened.get_team_operation_journal(op_id).unwrap().unwrap();
    assert_eq!(row.stage, "rollback_pending");
    assert_eq!(row.created_at, 10);
    assert_eq!(
        reopened
            .get_team_operation_journal("unknown-operation")
            .unwrap(),
        None
    );

    let not_found = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/diagnostics/team-operations/does-not-exist")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(not_found.status(), StatusCode::NOT_FOUND);

    let error = ApiError::from_legacy_spawn(RuntimeError::operation_unresolved(
        op_id,
        "ambiguous native effect",
    ));
    assert_eq!(error.clone().into_response().status(), StatusCode::CONFLICT);
    assert_eq!(error.code, "outcome_unresolved");
    assert!(error
        .message
        .contains(&format!("/v1/diagnostics/team-operations/{op_id}")));

    let canonical_error = ApiError::from(RuntimeError::operation_unresolved(
        "op_native_test",
        "unresolved",
    ));
    assert!(canonical_error
        .message
        .contains("/v2/operations/op_native_test"));
}

async fn inspect_history(router: &Router, token: &str, op_id: &str) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/diagnostics/team-operations/{op_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned())),
    )
}

fn seed_inspection_journal(db: &rusqlite::Connection, operation: &str, stage: &str) {
    db.execute("INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
        VALUES(?1,'historical-team','spawn_member_with_worktree',?2,'{}',10,20)",
        rusqlite::params![operation, stage]).unwrap();
}

#[tokio::test]
async fn legacy_operation_inspection_keeps_identity_when_journal_and_diagnostics_json_are_damaged()
{
    let (router, token, temp) = build_test_router().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let operation = "op_spawn_damaged_inspection";
    seed_inspection_journal(&db, operation, "session_create_review_required");
    db.execute(
        "UPDATE team_operation_journal SET payload_json='{broken-journal' WHERE operation_id=?1",
        [operation],
    )
    .unwrap();
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    store
        .append_team_operation_diagnostic(
            Some(operation),
            Some("historical-team"),
            "native_unknown",
            "retain original coordinate",
            &serde_json::json!({}),
            30,
        )
        .unwrap();
    db.execute("UPDATE team_operation_diagnostics SET payload_json='{broken-diagnostic' WHERE operation_id=?1", [operation]).unwrap();
    let (status, body) = inspect_history(&router, &token, operation).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["operation"]["operation_id"], operation);
    assert_eq!(body["operation"]["team_id"], "historical-team");
    assert_eq!(body["operation"]["stage"], "session_create_review_required");
    assert_eq!(body["operation"]["created_at"], 10);
    assert!(body["operation"]["payload"].is_null());
    assert!(body["operation"]["payload_decode_error"].is_string());
    assert_eq!(body["diagnostics"][0]["code"], "native_unknown");
    assert!(body["diagnostics"][0]["payload"].is_null());
    assert!(body["diagnostics"][0]["payload_decode_error"].is_string());
    assert_eq!(body["requires_manual_review"], true);
    assert_eq!(body["native_retry_authorized"], false);
    assert!(
        store.get_team_operation_journal(operation).is_err(),
        "authority reader remains strict"
    );
    let raw: String = db
        .query_row(
            "SELECT payload_json FROM team_operation_journal WHERE operation_id=?1",
            [operation],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        raw, "{broken-journal",
        "inspection never repairs or replaces evidence"
    );
}

#[tokio::test]
async fn legacy_operation_inspection_finds_original_admission_when_its_journal_is_missing() {
    let (router, token, temp) = build_test_router().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let operation = "op_spawn_missing_journal";
    db.execute("INSERT INTO team_spawn_admissions(team_id,source_session_id,idempotency_key,request_hash,operation_id,created_at,updated_at)
        VALUES('historical-team','historical-source','original-key','fixture-original-hash',?1,10,20)", [operation]).unwrap();
    let (status, body) = inspect_history(&router, &token, operation).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["operation"].is_null());
    assert_eq!(body["admission"]["operation_id"], operation);
    assert_eq!(body["admission"]["source_session_id"], "historical-source");
    assert_eq!(body["requires_manual_review"], true);
    assert_eq!(body["native_retry_authorized"], false);
    let (status, _) = inspect_history(&router, &token, "absent-from-both-authorities").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn legacy_operation_inspection_never_clears_review_from_weak_terminal_stage_labels() {
    let (router, token, temp) = build_test_router().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    for stage in ["completed", "rolled_back", "rollback_review_required"] {
        let operation = format!("op_spawn_weak_{stage}");
        seed_inspection_journal(&db, &operation, stage);
        let (status, body) = inspect_history(&router, &token, &operation).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["operation"]["stage"], stage);
        assert_eq!(body["requires_manual_review"], true, "{body}");
        assert_eq!(body["native_retry_authorized"], false);
    }
}

#[tokio::test]
async fn legacy_operation_inspection_requires_original_typed_completion_not_mutable_roster() {
    let (router, token, temp, provider, runtime, app) =
        build_test_router_with_services(TeamMcpPolicy::default()).await;
    let (status, source) = legacy_session_fixture_json(
        &runtime,
        serde_json::json!({
            "provider":"codex","model":"test-model","cwd":temp.path(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let source = source["id"].as_str().unwrap();
    let team = app
        .services
        .team_comms
        .create_team(runtime_core::TeamCreateRequest {
            name: "inspection receipt history".into(),
            lead_agent_id: source.into(),
            member_agent_ids: vec![],
            created_by: None,
        })
        .await
        .unwrap();
    let (status, saved) = legacy_spawn_fixture_json(
        &app,
        &team.team.id,
        Some("inspection-saved"),
        serde_json::json!({"source_session_id":source,"title":"original member"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    let operation = saved["operation_id"].as_str().unwrap();
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute("DELETE FROM team_members WHERE team_id=?1", [&team.team.id])
        .unwrap();
    db.execute(
        "UPDATE teams SET deleted_at=123 WHERE id=?1",
        [&team.team.id],
    )
    .unwrap();
    db.execute(
        "UPDATE sessions SET status='closed',closed_at=123 WHERE id=?1",
        [source],
    )
    .unwrap();
    let creates = provider.create_call_count();
    let (status, body) = inspect_history(&router, &token, operation).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["requires_manual_review"], false,
        "typed saved history remains independently inspectable"
    );
    assert_eq!(body["admission"]["completion_certified"], true);
    assert_eq!(body["admission"]["source_session_id"], source);
    assert_eq!(body["native_retry_authorized"], false);
    let mut wrong_operation = saved.clone();
    wrong_operation["operation_id"] = serde_json::json!("op_spawn_foreign");
    let mut wrong_team = saved.clone();
    wrong_team["team"]["team"]["id"] = serde_json::json!("foreign-team");
    let mut wrong_stage = saved.clone();
    wrong_stage["journal_stage"] = serde_json::json!("onboarding_sent");
    let bad_responses: Vec<Option<String>> = vec![
        None,
        Some("{damaged".into()),
        Some("{}".into()),
        Some(wrong_operation.to_string()),
        Some(wrong_team.to_string()),
        Some(wrong_stage.to_string()),
        Some(format!("{}{}", saved, " ".repeat(70 * 1024))),
    ];
    for bad in bad_responses {
        db.execute(
            "UPDATE team_spawn_admissions SET completed_response_json=?2 WHERE operation_id=?1",
            rusqlite::params![operation, bad],
        )
        .unwrap();
        let (status, body) = inspect_history(&router, &token, operation).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["operation"]["operation_id"], operation);
        assert_eq!(body["admission"]["operation_id"], operation);
        assert_eq!(body["requires_manual_review"], true, "{body}");
        assert_eq!(body["native_retry_authorized"], false);
    }
    db.execute(
        "UPDATE team_spawn_admissions SET completed_response_json=?2 WHERE operation_id=?1",
        rusqlite::params![operation, saved.to_string()],
    )
    .unwrap();
    for (team_id, kind, stage) in [
        (&team.team.id[..], "wrong-kind", "completed"),
        ("foreign-team", "spawn_member_with_worktree", "completed"),
        (
            &team.team.id[..],
            "spawn_member_with_worktree",
            "rolled_back",
        ),
    ] {
        db.execute(
            "UPDATE team_operation_journal SET team_id=?2,kind=?3,stage=?4 WHERE operation_id=?1",
            rusqlite::params![operation, team_id, kind, stage],
        )
        .unwrap();
        let (status, body) = inspect_history(&router, &token, operation).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["requires_manual_review"], true, "{body}");
        assert_eq!(body["native_retry_authorized"], false);
    }
    assert_eq!(
        provider.create_call_count(),
        creates,
        "inspection never creates another provider session"
    );
}

#[tokio::test]
async fn legacy_operation_inspection_pages_across_oversized_rows_and_preserves_raw_evidence() {
    let (router, token, temp) = build_test_router().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let operation = "op_spawn_paged_evidence";
    seed_inspection_journal(&db, operation, "planned");
    let huge = serde_json::json!("j".repeat(70 * 1024)).to_string();
    db.execute(
        "UPDATE team_operation_journal SET payload_json=?2 WHERE operation_id=?1",
        rusqlite::params![operation, huge],
    )
    .unwrap();
    let payloads = vec![
        serde_json::json!("x".repeat(70 * 1024)).to_string(),
        "null".into(),
        "{broken".into(),
        serde_json::json!({"last":true}).to_string(),
    ];
    let mut ids = Vec::new();
    for (i, payload) in payloads.iter().enumerate() {
        let message = if i == 0 {
            format!("a{}", "é".repeat(3000))
        } else {
            format!("message-{i}")
        };
        db.execute("INSERT INTO team_operation_diagnostics(operation_id,team_id,code,message,payload_json,created_at)
            VALUES(?1,'historical-team',?2,?3,?4,30)",rusqlite::params![operation,format!("diag-{i}"),message,payload]).unwrap();
        ids.push(db.last_insert_rowid());
    }
    db.execute("INSERT INTO team_operation_diagnostics(operation_id,team_id,code,message,payload_json,created_at)
        VALUES('unrelated','foreign','must-not-appear','foreign','{broken-unrelated',40)",[]).unwrap();
    let counts = inspection_counts(&db);
    let mut cursor = 0;
    let mut seen = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let (status, body) = inspect_history(
            &router,
            &token,
            &format!("{operation}?limit=1&after_diagnostic_id={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["operation"]["operation_id"], operation);
        assert!(body["operation"]["payload"].is_null());
        assert!(body["operation"]["payload_decode_error"].is_string());
        let rows = body["diagnostics"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row["id"], *id);
        seen.push(row["id"].as_i64().unwrap());
        assert_eq!(row["code"], format!("diag-{index}"));
        if index == 0 {
            assert!(row["payload"].is_null());
            assert!(row["payload_decode_error"].is_string());
            assert_eq!(row["message_bytes"], 6001);
            assert_eq!(row["message_truncated"], true);
            assert!(row["message"].as_str().unwrap().len() <= 4096);
        } else if index == 1 {
            assert!(row["payload"].is_null());
            assert!(
                row["payload_decode_error"].is_null(),
                "valid JSON null differs from damaged evidence"
            );
        } else if index == 2 {
            assert!(row["payload_decode_error"].is_string());
        } else {
            assert_eq!(row["payload"]["last"], true);
        }
        if index + 1 < ids.len() {
            cursor = body["next_diagnostic_id"].as_i64().unwrap();
            assert_eq!(cursor, *id);
        } else {
            assert!(body["next_diagnostic_id"].is_null());
            assert_eq!(body["diagnostics_truncated"], false);
        }
    }
    assert_eq!(seen, ids);
    for suffix in [
        "?limit=0",
        "?limit=101",
        "?after_diagnostic_id=-1",
        "?limit=-1",
        "?unknown=true",
    ] {
        let (status, _) = inspect_history(&router, &token, &format!("{operation}{suffix}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{suffix}");
    }
    assert_eq!(
        inspection_counts(&db),
        counts,
        "GET never writes authority or event rows"
    );
    let retained: String = db
        .query_row(
            "SELECT payload_json FROM team_operation_journal WHERE operation_id=?1",
            [operation],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(retained, huge);
    let retained: Vec<String> = db
        .prepare(
            "SELECT payload_json FROM team_operation_diagnostics WHERE operation_id=?1 ORDER BY id",
        )
        .unwrap()
        .query_map([operation], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(retained, payloads);
}

fn inspection_counts(db: &rusqlite::Connection) -> Vec<i64> {
    [
        "team_operation_journal",
        "team_operation_diagnostics",
        "team_spawn_admissions",
        "sessions",
        "team_members",
        "runtime_events",
    ]
    .iter()
    .map(|table| {
        db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    })
    .collect()
}

#[tokio::test]
async fn legacy_operation_inspection_limits_aggregate_payload_and_rejects_nontext_json() {
    let (router, token, temp) = build_test_router().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let operation = "op_spawn_payload_budget";
    seed_inspection_journal(&db, operation, "planned");
    db.execute(
        "UPDATE team_operation_journal SET payload_json=?2 WHERE operation_id=?1",
        rusqlite::params![operation, vec![b'{', b'}']],
    )
    .unwrap();
    let payload = serde_json::json!("p".repeat(60 * 1024)).to_string();
    for _ in 0..10 {
        db.execute("INSERT INTO team_operation_diagnostics(operation_id,code,message,payload_json,created_at)
            VALUES(?1,'budget','evidence',?2,30)",rusqlite::params![operation,payload]).unwrap();
    }
    db.execute(
        "INSERT INTO team_operation_diagnostics(operation_id,code,message,payload_json,created_at)
        VALUES(?1,'small-later','evidence','{\"later\":true}',30)",
        [operation],
    )
    .unwrap();
    let (status, body) = inspect_history(&router, &token, &format!("{operation}?limit=100")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["operation"]["payload_decode_error"]
        .as_str()
        .unwrap()
        .contains("blob"));
    assert_eq!(body["max_payload_bytes"], 64 * 1024);
    assert_eq!(body["total_payload_budget_bytes"], 256 * 1024);
    assert!(body["payload_bytes_loaded"].as_u64().unwrap() <= 256 * 1024);
    let rows = body["diagnostics"].as_array().unwrap();
    assert_eq!(rows.len(), 11);
    assert_eq!(
        rows.iter()
            .filter(|r| r["payload_decode_error"].is_string())
            .count(),
        6
    );
    assert_eq!(
        rows[10]["payload"]["later"], true,
        "a skipped oversized aggregate row cannot starve later small evidence"
    );
    assert_eq!(body["native_retry_authorized"], false);
}

#[tokio::test]
async fn legacy_operation_inspection_retains_invalid_utf8_text_in_all_json_evidence_columns() {
    let (router, token, temp) = build_test_router().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let operation = "op_spawn_invalid_utf8";
    seed_inspection_journal(&db, operation, "completed");
    db.execute(
        "UPDATE team_operation_journal SET payload_json=CAST(X'FF' AS TEXT) WHERE operation_id=?1",
        [operation],
    )
    .unwrap();
    db.execute("INSERT INTO team_spawn_admissions(team_id,source_session_id,idempotency_key,request_hash,operation_id,completed_response_json,created_at,updated_at)
        VALUES('historical-team','historical-source','utf8-original-key','original-hash',?1,CAST(X'FF' AS TEXT),10,20)",[operation]).unwrap();
    db.execute(
        "INSERT INTO team_operation_diagnostics(operation_id,code,message,payload_json,created_at)
        VALUES(?1,'invalid-utf8',CAST(X'FF' AS TEXT),CAST(X'FF' AS TEXT),30)",
        [operation],
    )
    .unwrap();
    let counts = inspection_counts(&db);
    let (status, body) = inspect_history(&router, &token, operation).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["operation"]["operation_id"], operation);
    assert_eq!(body["operation"]["payload_bytes"], 1);
    assert!(body["operation"]["payload_decode_error"]
        .as_str()
        .unwrap()
        .contains("UTF-8"));
    assert_eq!(body["admission"]["completion_response_bytes"], 1);
    assert!(body["admission"]["completion_decode_error"]
        .as_str()
        .unwrap()
        .contains("UTF-8"));
    assert_eq!(body["diagnostics"][0]["payload_bytes"], 1);
    assert!(body["diagnostics"][0]["payload_decode_error"]
        .as_str()
        .unwrap()
        .contains("UTF-8"));
    assert_eq!(body["diagnostics"][0]["message_utf8_replaced"], true);
    assert_eq!(body["requires_manual_review"], true);
    assert_eq!(body["native_retry_authorized"], false);
    assert_eq!(inspection_counts(&db), counts);
    for (table, column) in [
        ("team_operation_journal", "payload_json"),
        ("team_spawn_admissions", "completed_response_json"),
        ("team_operation_diagnostics", "payload_json"),
    ] {
        let raw:(String,String)=db.query_row(&format!("SELECT typeof({column}),hex(CAST({column} AS BLOB)) FROM {table} WHERE operation_id=?1"),[operation],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
        assert_eq!(raw, ("text".into(), "FF".into()));
    }
}
