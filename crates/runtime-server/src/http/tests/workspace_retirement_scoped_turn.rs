use super::workspace_retirement_support::*;
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn settled_native_retirement_turn_refresh_ignores_unrelated_corrupt_session() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("settled-turn-scoped-target");
    let other_root = temp.path().join("settled-turn-foreign-workspace");
    fs::create_dir_all(&root).unwrap();
    fs::create_dir_all(&other_root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let target = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, admitted) = fixture
        .call(
            "POST",
            &format!("/v2/agents/{}/turns", target.agent_id),
            Some(serde_json::json!({"input":[{"type":"text","text":"retirement_wait_for_interrupt"}]})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{admitted}");
    let turn_id = admitted["turn_id"].as_str().unwrap().to_string();
    let foreign_workspace = fixture.register(&other_root).await;
    let foreign = fixture
        .agent(&foreign_workspace.workspace_id, ProviderKind::Codex)
        .await;

    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE sessions SET metadata_json='{invalid-json' WHERE id=?1",
        [&foreign.agent_id],
    )
    .unwrap();
    assert!(fixture.store.hydrate_runtime_state().is_err());

    let revision = fixture.workspace(&workspace.workspace_id).await.revision;
    let uri = format!(
        "/v2/workspaces/{}/retirement/advance",
        workspace.workspace_id
    );
    let payload = serde_json::json!({"expected_revision":revision});
    // Native interrupt admission is not terminal evidence: safely resume the
    // same keyed retirement until its durable turn actually settles.
    let (status, result) = timeout(Duration::from_secs(10), async {
        loop {
            let (status, result) = fixture
                .call(
                    "POST",
                    &uri,
                    Some(payload.clone()),
                    Some("settled-turn-foreign-corruption"),
                )
                .await;
            if status == StatusCode::INTERNAL_SERVER_ERROR
                || result["operation"]["phase"] == "completed"
            {
                return (status, result);
            }
            assert!(
                matches!(status, StatusCode::OK | StatusCode::ACCEPTED),
                "unexpected intermediate retirement status {status}: {result}"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disposable native turn did not settle for retirement");
    let persisted_status: String = db
        .query_row("SELECT status FROM turns WHERE id=?1", [&turn_id], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        persisted_status,
        "interrupted",
        "native turn must be durably settled; status={status}, result={result}, native_interrupts={}, native_closes={}",
        provider.interrupts.load(Ordering::SeqCst),
        provider.closes.load(Ordering::SeqCst)
    );
    assert_eq!(provider.interrupts.load(Ordering::SeqCst), 1);
    let effect_phase: String = db
        .query_row(
            "SELECT phase FROM runtime_operation_effects
             WHERE effect_kind='workspace_turn_interrupt' AND target_id=?1",
            [&turn_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        effect_phase, "finalized",
        "turn receipt must already be durably final"
    );
    assert!(
        matches!(status, StatusCode::OK | StatusCode::ACCEPTED),
        "committed retirement turn returned {status}: {result}"
    );
    let (status, replay) = fixture
        .call(
            "POST",
            &uri,
            Some(payload),
            Some("settled-turn-foreign-corruption"),
        )
        .await;
    assert!(
        matches!(status, StatusCode::OK | StatusCode::ACCEPTED),
        "{replay}"
    );
    assert_eq!(provider.interrupts.load(Ordering::SeqCst), 1);
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        replay["operation"]["operation_id"],
        result["operation"]["operation_id"]
    );
    assert!(fixture.store.hydrate_runtime_state().is_err());
}
