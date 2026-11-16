use super::workspace_retirement_support::*;
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn populated_retirement_signed_workspace_ceiling_rejects_before_provider_close() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-lead-revision-ceiling");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, lead) = fixture
        .call(
            "POST",
            &format!("/v2/workspaces/{}/lead", workspace.workspace_id),
            Some(serde_json::json!({
                "lead_agent_id":agent.agent_id,
                "expected_revision":fixture.workspace(&workspace.workspace_id).await.revision,
            })),
            Some("retirement-ceiling-lead-setup"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{lead:?}");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspaces SET revision=?1 WHERE workspace_id=?2",
        rusqlite::params![i64::MAX, workspace.workspace_id],
    )
    .unwrap();

    let (status, response) = fixture
        .call(
            "POST",
            &format!(
                "/v2/workspaces/{}/retirement/advance",
                workspace.workspace_id
            ),
            Some(serde_json::json!({"expected_revision":i64::MAX})),
            Some("retirement-signed-ceiling"),
        )
        .await;
    assert_eq!(
        provider.closes.load(Ordering::SeqCst),
        0,
        "retirement must not invoke fake native close at signed ceiling: {status} {response:?}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response:?}");
    assert_eq!(response["error"]["code"], "invalid_state");
    let durable: (i64, Option<String>, i64) = db
        .query_row(
            "SELECT revision,lead_agent_id,
             (SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_retirement')
             FROM workspaces WHERE workspace_id=?1",
            [&workspace.workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(durable, (i64::MAX, Some(agent.agent_id), 0));
}

#[tokio::test]
async fn populated_retirement_near_signed_ceiling_completes_and_replays_at_max() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-near-revision-ceiling");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, lead) = fixture
        .call(
            "POST",
            &format!("/v2/workspaces/{}/lead", workspace.workspace_id),
            Some(serde_json::json!({
                "lead_agent_id":agent.agent_id,
                "expected_revision":fixture.workspace(&workspace.workspace_id).await.revision,
            })),
            Some("retirement-near-ceiling-lead-setup"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{lead:?}");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspaces SET revision=?1 WHERE workspace_id=?2",
        rusqlite::params![i64::MAX - 2, workspace.workspace_id],
    )
    .unwrap();
    let initial_revision = (i64::MAX - 2) as u64;
    let retired = fixture
        .finish_retirement(
            &workspace.workspace_id,
            initial_revision,
            "retirement-near-ceiling",
        )
        .await;
    assert_eq!(
        retired.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.workspace(&workspace.workspace_id).await.revision,
        i64::MAX as u64
    );
    let replay = fixture
        .advance(
            &workspace.workspace_id,
            initial_revision,
            "retirement-near-ceiling",
        )
        .await;
    assert_eq!(
        replay.operation.operation_id,
        retired.operation.operation_id
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn populated_retirement_lead_needs_two_signed_workspace_successors() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-one-revision-left");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let (status, lead) = fixture
        .call(
            "POST",
            &format!("/v2/workspaces/{}/lead", workspace.workspace_id),
            Some(serde_json::json!({
                "lead_agent_id":agent.agent_id,
                "expected_revision":fixture.workspace(&workspace.workspace_id).await.revision,
            })),
            Some("retirement-one-revision-lead-setup"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{lead:?}");
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspaces SET revision=?1 WHERE workspace_id=?2",
        rusqlite::params![i64::MAX - 1, workspace.workspace_id],
    )
    .unwrap();
    let (status, response) = fixture
        .call(
            "POST",
            &format!(
                "/v2/workspaces/{}/retirement/advance",
                workspace.workspace_id
            ),
            Some(serde_json::json!({"expected_revision":i64::MAX - 1})),
            Some("retirement-lead-two-revisions-required"),
        )
        .await;
    assert_eq!(
        provider.closes.load(Ordering::SeqCst),
        0,
        "retirement requires both lead-clear and retire revisions: {status} {response:?}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response:?}");
    assert_eq!(response["error"]["code"], "invalid_state");
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_retirement'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn populated_retirement_agent_signed_revision_ceiling_blocks_native_close() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-agent-revision-ceiling");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX, agent.agent_id],
    )
    .unwrap();
    let (status, response) = fixture
        .call(
            "POST",
            &format!("/v2/workspaces/{}/retirement/advance", workspace.workspace_id),
            Some(serde_json::json!({"expected_revision":fixture.workspace(&workspace.workspace_id).await.revision})),
            Some("retirement-agent-signed-ceiling"),
        )
        .await;
    assert_eq!(
        provider.closes.load(Ordering::SeqCst),
        0,
        "agent revision cannot advance after native retirement close: {status} {response:?}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response:?}");
    assert_eq!(response["error"]["code"], "invalid_state");
    let effects: (i64, i64) = db
        .query_row(
            "SELECT COUNT(*),COALESCE(SUM(CASE WHEN phase='started' THEN 1 ELSE 0 END),0)
             FROM runtime_operation_effects WHERE effect_kind='workspace_provider_close'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(effects.1, 0, "no native close claim may be dispatched");
}

#[tokio::test]
async fn populated_retirement_without_lead_uses_exactly_one_remaining_signed_revision() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-one-revision-nonlead");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    assert!(fixture
        .workspace(&workspace.workspace_id)
        .await
        .lead_agent_id
        .is_none());
    db.execute(
        "UPDATE workspaces SET revision=?1 WHERE workspace_id=?2",
        rusqlite::params![i64::MAX - 1, workspace.workspace_id],
    )
    .unwrap();
    let receipt = fixture
        .finish_retirement(
            &workspace.workspace_id,
            (i64::MAX - 1) as u64,
            "nonlead-last-revision",
        )
        .await;
    assert_eq!(
        receipt.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(
        fixture.workspace(&workspace.workspace_id).await.revision,
        i64::MAX as u64
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .runtime
            .get_session(&agent.agent_id)
            .await
            .unwrap()
            .status,
        "closed"
    );
}

#[tokio::test]
async fn previously_archived_agent_at_signed_ceiling_does_not_need_another_roster_revision() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-legacy-archived-agent-ceiling");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    // Simulate legacy state that marked the roster archived without the newer
    // exact native-close evidence. The provider still has one open attachment;
    // retiring it must close that attachment but must NOT bump the already
    // archived (max-revision) roster row again.
    db.execute(
        "UPDATE workspace_agents SET lifecycle_state='archived',archived_at=10,
         archive_reason='legacy_unverified',revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX, agent.agent_id],
    )
    .unwrap();
    assert_eq!(provider.native_session_count().await, 1);
    let details = fixture
        .finish_retirement(
            &workspace.workspace_id,
            workspace.revision,
            "retirement-already-archived-agent-ceiling",
        )
        .await;
    assert_eq!(
        details.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    let revision: i64 = db
        .query_row(
            "SELECT revision FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(revision, i64::MAX);
    assert_eq!(
        fixture.workspace(&workspace.workspace_id).await.revision,
        workspace.revision + 1
    );
}

#[tokio::test]
async fn active_agent_with_one_signed_roster_revision_left_can_retire_and_replay() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retirement-active-agent-near-ceiling");
    fs::create_dir_all(&root).unwrap();
    let provider = RetirementProvider::new(ProviderKind::Codex);
    let fixture = retirement_fixture(temp.path(), &[provider.clone()]).await;
    let workspace = fixture.register(&root).await;
    let agent = fixture
        .agent(&workspace.workspace_id, ProviderKind::Codex)
        .await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX - 1, agent.agent_id],
    )
    .unwrap();
    let receipt = fixture
        .finish_retirement(
            &workspace.workspace_id,
            workspace.revision,
            "retirement-active-agent-last-revision",
        )
        .await;
    assert_eq!(
        receipt.operation.phase,
        runtime_core::OperationPhase::Completed
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
    assert_eq!(
        db.query_row(
            "SELECT revision FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        i64::MAX
    );
    let replay = fixture
        .advance(
            &workspace.workspace_id,
            workspace.revision,
            "retirement-active-agent-last-revision",
        )
        .await;
    assert_eq!(
        replay.operation.operation_id,
        receipt.operation.operation_id
    );
    assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
}
