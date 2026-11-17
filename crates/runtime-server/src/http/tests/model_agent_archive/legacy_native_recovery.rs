//! Actual authenticated HTTP/MCP error paths. Only disposable historical rows
//! are injected; the fake provider's native-close counter must remain unchanged.
use super::*;
use runtime_core::legacy_spawn_obligations::LegacyNativeRecovery;

fn seed_history(fixture: &Fixture, operation_id: &str, payload: Value) {
    fixture.db().execute(
        "INSERT INTO team_operation_journal(operation_id,team_id,kind,stage,payload_json,created_at,updated_at)
         VALUES(?1,'historical-team','spawn_member_with_worktree','session_created',?2,10,20)",
        rusqlite::params![operation_id,payload.to_string()],
    ).unwrap();
}

fn authority_counts(fixture: &Fixture) -> Vec<i64> {
    let db = fixture.db();
    [
        "runtime_operations",
        "runtime_operation_effects",
        "runtime_operation_resource_claims",
        "runtime_events",
    ]
    .into_iter()
    .map(|table| {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    })
    .collect()
}

async fn get_json(fixture: &Fixture, path: &str) -> (StatusCode, Value) {
    let response = fixture
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {}", fixture.token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, body)
}

async fn archive(fixture: &Fixture, key: &str) -> (StatusCode, Value) {
    call(
        &fixture.router,
        &fixture.token,
        "POST",
        &format!(
            "/v2/workspaces/{}/agents/{}/archive",
            fixture.workspace, fixture.targets[0].agent_id
        ),
        Some(json!({"reason":"historical obligation ingress regression"})),
        Some(key),
    )
    .await
}

async fn reopen(fixture: &mut Fixture) {
    let store: Arc<dyn RuntimeStore> = Arc::new(fixture.store());
    let mut providers = runtime_core::ProviderRegistry::new();
    providers.register(fixture.provider.clone()).unwrap();
    let (router, runtime, _app) = assemble_test_router(
        fixture.temp.path(),
        store,
        Arc::new(providers),
        &fixture.token,
        TeamMcpPolicy::default(),
    )
    .await;
    fixture.router = router;
    fixture.runtime = runtime;
}

fn assert_original_cause(details: &Value, original_id: &str) {
    let recovery = &details["legacy_recovery"];
    assert_eq!(recovery["kind"], "blocked", "{details}");
    assert_eq!(
        recovery["coordinate"]["operation_id"], original_id,
        "{details}"
    );
    assert_eq!(
        recovery["coordinate"]["team_id"], "historical-team",
        "{details}"
    );
    assert_eq!(
        recovery["coordinate"]["stage"], "session_created",
        "{details}"
    );
    assert_eq!(recovery["reason"], "exact_session", "{details}");
    assert_eq!(
        recovery["inspection_path"],
        runtime_core::error::legacy_team_operation_inspection_path(original_id)
    );
    assert_eq!(recovery["native_retry_authorized"], false);
    assert_eq!(details["native_retry_authorized"], false);
}

#[tokio::test]
async fn legacy_guard_http_preserves_arbitrary_original_ids_without_inventing_v2_owners() {
    let fixture = Fixture::new(1).await;
    let native_before = fixture.provider.archive_close_count();
    let authority_before = authority_counts(&fixture);
    // These are actual Axum requests carrying the emitted encoded URI. This
    // does not claim that every external URL normalizer preserves dot segments.
    for (index, original_id) in ["history/custom ?#%é", ".", "..", "op_looks_canonical"]
        .into_iter()
        .enumerate()
    {
        seed_history(
            &fixture,
            original_id,
            json!({"spawned_session_id":fixture.targets[0].agent_id}),
        );
        let (status, body) = archive(&fixture, &format!("fresh-legacy-guard-{index}")).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"]["code"], "outcome_unresolved", "{body}");
        let details = &body["error"]["details"];
        assert_eq!(details["operation_id"], original_id, "{body}");
        assert_eq!(
            details["inspection_path"],
            runtime_core::error::legacy_team_operation_inspection_path(original_id)
        );
        assert_original_cause(details, original_id);
        let (lookup_status, lookup) =
            get_json(&fixture, details["inspection_path"].as_str().unwrap()).await;
        assert_eq!(
            lookup_status,
            StatusCode::OK,
            "original coordinate did not route: {lookup}"
        );
        assert_eq!(lookup["operation"]["operation_id"], original_id, "{lookup}");
        assert_eq!(lookup["operation"]["team_id"], "historical-team");
        assert_eq!(lookup["operation"]["stage"], "session_created");
        assert_eq!(lookup["native_retry_authorized"], false);
        assert_eq!(authority_counts(&fixture), authority_before);
        assert_eq!(fixture.provider.archive_close_count(), native_before);
        fixture
            .db()
            .execute(
                "DELETE FROM team_operation_journal WHERE operation_id=?1",
                [original_id],
            )
            .unwrap();
    }
}

#[tokio::test]
async fn legacy_guard_http_incomplete_scan_has_no_primary_operation_or_lookup_url() {
    let fixture = Fixture::new(1).await;
    seed_history(
        &fixture,
        "opaque-budget-row",
        json!({"padding":"x".repeat(65_537)}),
    );
    let native_before = fixture.provider.archive_close_count();
    let before = authority_counts(&fixture);
    let (status, body) = archive(&fixture, "incomplete-before-admission").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"], "legacy_native_scan_incomplete",
        "{body}"
    );
    let details = &body["error"]["details"];
    assert!(details.get("operation_id").is_none(), "{body}");
    assert!(details.get("inspection_path").is_none(), "{body}");
    assert_eq!(details["native_retry_authorized"], false);
    let recovery = &details["legacy_recovery"];
    assert_eq!(recovery["kind"], "scan_incomplete");
    assert_eq!(recovery["incomplete"]["reason"], "payload_cell_limit");
    assert!(recovery.get("coordinate").is_none());
    assert!(recovery.get("operation_id").is_none());
    assert!(recovery.get("inspection_path").is_none());
    assert_eq!(authority_counts(&fixture), before);
    assert_eq!(fixture.provider.archive_close_count(), native_before);
}

#[tokio::test]
async fn legacy_guard_mcp_keeps_real_parent_and_saved_original_cause_across_reopen_and_replay() {
    let mut fixture = Fixture::new(1).await;
    let original_id = "historical/member ?#%é";
    let target = fixture.targets[0].agent_id.clone();
    seed_history(&fixture, original_id, json!({"spawned_session_id":target}));
    let native_before = fixture.provider.archive_close_count();
    let before = authority_counts(&fixture);
    let first = fixture
        .remove("legacy-parent-replay", std::slice::from_ref(&target))
        .await;
    assert_eq!(first["ok"], false, "{first}");
    let details = &first["error"]["details"];
    let parent_id = details["operation_id"].as_str().unwrap().to_owned();
    assert_ne!(parent_id, original_id);
    assert_eq!(
        details["inspection_path"],
        runtime_core::error::canonical_operation_inspection_path(&parent_id)
    );
    assert_original_cause(details, original_id);
    assert_eq!(details["targets"][0]["agent_id"], target);
    assert_eq!(details["targets"][0]["status"], "rejected_before_dispatch");
    let saved = fixture.store().get_operation(&parent_id).unwrap().unwrap();
    assert_eq!(
        saved.operation.actor.kind,
        runtime_core::OperationActorKind::Agent
    );
    assert_eq!(saved.operation.actor.identifier, fixture.caller);
    assert_eq!(
        saved.operation.phase,
        runtime_core::OperationPhase::Requested
    );
    assert!(saved.operation.exact_terminal_result.is_none());
    assert_eq!(saved.effects.len(), 1);
    assert_eq!(saved.effects[0].attempt_count, 0);
    assert_eq!(
        authority_counts(&fixture)[0],
        before[0] + 1,
        "a rejected child must not be admitted"
    );
    assert_eq!(fixture.provider.archive_close_count(), native_before);
    let child_id = details["targets"][0]["operation_id"].as_str().unwrap();
    assert!(fixture.store().get_operation(child_id).unwrap().is_none());
    let (parent_status, parent_body) =
        get_json(&fixture, details["inspection_path"].as_str().unwrap()).await;
    assert_eq!(parent_status, StatusCode::OK, "{parent_body}");
    assert_eq!(parent_body["operation"]["operation_id"], parent_id);
    let (legacy_status, legacy_body) = get_json(
        &fixture,
        details["legacy_recovery"]["inspection_path"]
            .as_str()
            .unwrap(),
    )
    .await;
    assert_eq!(legacy_status, StatusCode::OK, "{legacy_body}");
    assert_eq!(legacy_body["operation"]["operation_id"], original_id);

    // Removing the current historical row is not a capability to rerun this
    // already-admitted host invocation. Its original observation is durable.
    fixture
        .db()
        .execute(
            "DELETE FROM team_operation_journal WHERE operation_id=?1",
            [original_id],
        )
        .unwrap();
    reopen(&mut fixture).await;
    let after_reopen = authority_counts(&fixture);
    let replay = fixture
        .remove("legacy-parent-replay", std::slice::from_ref(&target))
        .await;
    assert_eq!(replay["ok"], false, "{replay}");
    assert_eq!(replay["error"]["code"], "outcome_unresolved");
    let replay_details = &replay["error"]["details"];
    assert_eq!(replay_details["operation_id"], parent_id);
    assert_original_cause(replay_details, original_id);
    assert_eq!(
        replay_details["recorded_legacy_recoveries"][0],
        replay_details["legacy_recovery"]
    );
    assert_eq!(
        fixture.store().get_operation(&parent_id).unwrap().unwrap(),
        saved
    );
    assert_eq!(authority_counts(&fixture), after_reopen);
    assert_eq!(fixture.provider.archive_close_count(), native_before);
}

#[tokio::test]
async fn legacy_guard_mcp_incomplete_scan_preserves_only_its_real_parent_across_replay() {
    let fixture = Fixture::new(1).await;
    let target = fixture.targets[0].agent_id.clone();
    seed_history(
        &fixture,
        "oversized-historical-row",
        json!({"padding":"x".repeat(65_537)}),
    );
    let native_before = fixture.provider.archive_close_count();
    let first = fixture
        .remove("parent-scan-incomplete", std::slice::from_ref(&target))
        .await;
    assert_eq!(first["ok"], false, "{first}");
    let details = &first["error"]["details"];
    let parent_id = details["operation_id"].as_str().unwrap();
    assert_ne!(parent_id, "oversized-historical-row");
    assert_eq!(
        details["inspection_path"],
        runtime_core::error::canonical_operation_inspection_path(parent_id)
    );
    let recovery = &details["legacy_recovery"];
    assert_eq!(recovery["kind"], "scan_incomplete");
    assert!(recovery.get("coordinate").is_none());
    assert!(recovery.get("inspection_path").is_none());
    let before_replay = authority_counts(&fixture);
    let replay = fixture
        .remove("parent-scan-incomplete", std::slice::from_ref(&target))
        .await;
    assert_eq!(replay["ok"], false);
    assert_eq!(replay["error"]["details"]["operation_id"], parent_id);
    assert_eq!(replay["error"]["details"]["legacy_recovery"], *recovery);
    assert_eq!(authority_counts(&fixture), before_replay);
    assert_eq!(fixture.provider.archive_close_count(), native_before);
}

#[tokio::test]
async fn legacy_guard_current_cause_survives_critical_observation_failure_without_false_receipt() {
    let fixture = Fixture::new(1).await;
    let target = fixture.targets[0].agent_id.clone();
    let original_id = "current/error-must-survive";
    seed_history(&fixture, original_id, json!({"spawned_session_id":target}));
    fixture
        .db()
        .execute_batch(
            "CREATE TRIGGER refuse_legacy_observation BEFORE INSERT ON runtime_events
        WHEN NEW.kind='operation.legacy_native_recovery_observed'
        BEGIN SELECT RAISE(ABORT,'injected Critical observation fault'); END;",
        )
        .unwrap();
    let native_before = fixture.provider.archive_close_count();
    let first = fixture
        .remove("parent-observation-fault", std::slice::from_ref(&target))
        .await;
    assert_eq!(first["ok"], false, "{first}");
    assert!(
        first["error"]["message"]
            .as_str()
            .unwrap()
            .contains("could not be persisted"),
        "{first}"
    );
    let details = &first["error"]["details"];
    let parent_id = details["operation_id"].as_str().unwrap();
    assert_original_cause(details, original_id);
    assert_eq!(
        details["inspection_path"],
        runtime_core::error::canonical_operation_inspection_path(parent_id)
    );
    assert_eq!(details["legacy_recovery_inspection"], "not_recorded");
    let saved = fixture.store().get_operation(parent_id).unwrap().unwrap();
    assert_eq!(
        saved.operation.phase,
        runtime_core::OperationPhase::Requested
    );
    assert!(saved.operation.exact_terminal_result.is_none());
    assert_eq!(saved.effects.len(), 1);
    assert_eq!(saved.effects[0].attempt_count, 0);
    assert_eq!(
        LegacyNativeRecovery::from_effect_evidence(saved.effects[0].evidence.as_ref().unwrap())
            .unwrap(),
        None
    );
    let event_count: i64 = fixture.db().query_row("SELECT COUNT(*) FROM runtime_events WHERE kind='operation.legacy_native_recovery_observed'",[],|row|row.get(0)).unwrap();
    assert_eq!(event_count, 0);
    let before_replay = authority_counts(&fixture);
    let replay = fixture
        .remove("parent-observation-fault", std::slice::from_ref(&target))
        .await;
    assert_eq!(replay["ok"], false);
    assert_eq!(replay["error"]["details"]["operation_id"], parent_id);
    assert_eq!(
        replay["error"]["details"]["legacy_recovery_inspection"],
        "not_recorded"
    );
    assert_eq!(
        fixture.store().get_operation(parent_id).unwrap().unwrap(),
        saved
    );
    assert_eq!(authority_counts(&fixture), before_replay);
    assert_eq!(fixture.provider.archive_close_count(), native_before);
}

#[tokio::test]
async fn legacy_guard_mcp_damaged_pair_exposes_raw_cause_only_as_unverified_without_repair() {
    let fixture = Fixture::new(1).await;
    let target = fixture.targets[0].agent_id.clone();
    let original_id = "saved/unverified-original";
    seed_history(&fixture, original_id, json!({"spawned_session_id":target}));
    let first = fixture
        .remove("parent-damaged-observation", std::slice::from_ref(&target))
        .await;
    assert_eq!(first["ok"], false, "{first}");
    let parent_id = first["error"]["details"]["operation_id"].as_str().unwrap();
    assert_original_cause(&first["error"]["details"], original_id);
    fixture.db().execute("DELETE FROM runtime_events WHERE kind='operation.legacy_native_recovery_observed' AND json_extract(payload_json,'$.operation_id')=?1",[parent_id]).unwrap();
    let damaged = fixture.store().get_operation(parent_id).unwrap().unwrap();
    let before = authority_counts(&fixture);
    let native_before = fixture.provider.archive_close_count();
    let replay = fixture
        .remove("parent-damaged-observation", std::slice::from_ref(&target))
        .await;
    assert_eq!(replay["ok"], false, "{replay}");
    let details = &replay["error"]["details"];
    assert_eq!(details["operation_id"], parent_id);
    assert_eq!(details["legacy_recovery_inspection"], "unavailable");
    assert!(
        details.get("legacy_recovery").is_none(),
        "damaged pair was restored as verified: {replay}"
    );
    assert!(details.get("recorded_legacy_recoveries").is_none());
    assert_eq!(
        details["unverified_legacy_recoveries"][0]["coordinate"]["operation_id"],
        original_id
    );
    assert_eq!(details["native_retry_authorized"], false);
    assert_eq!(
        fixture.store().get_operation(parent_id).unwrap().unwrap(),
        damaged
    );
    assert_eq!(authority_counts(&fixture), before);
    assert_eq!(fixture.provider.archive_close_count(), native_before);
}
