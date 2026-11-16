use super::operator_native_policy::operator_policy_fixture;
use super::workspace_worktree_routes::call;
use super::*;

#[tokio::test]
async fn native_archive_lead_workspace_sqlite_revision_ceiling_rejects_before_close() {
    let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let workspace_id = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let revision: i64 = db
        .query_row(
            "SELECT revision FROM workspaces WHERE workspace_id=?1",
            [&workspace_id],
            |row| row.get(0),
        )
        .unwrap();
    let (status, lead) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/lead"),
        Some(serde_json::json!({
            "lead_agent_id":agent_id,"expected_revision":revision,
        })),
        Some("archive-lead-ceiling-setup"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lead:?}");
    // Agent revision is normal; only the separate workspace revision has no
    // signed successor when native archive clears the current lead.
    db.execute(
        "UPDATE workspaces SET revision=?1 WHERE workspace_id=?2",
        rusqlite::params![i64::MAX, workspace_id],
    )
    .unwrap();
    let before = provider.archive_close_count();
    let (status, response) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"),
        Some(serde_json::json!({"reason":"lead signed SQLite ceiling"})),
        Some("archive-lead-workspace-signed-ceiling"),
    )
    .await;
    assert_eq!(
        provider.archive_close_count(),
        before,
        "lead workspace revision must be fenced BEFORE fake native close: {status} {response:?}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response:?}");
    assert_eq!(response["error"]["code"], "invalid_state");
    let durable: (i64, Option<String>, i64) = db
        .query_row(
            "SELECT w.revision,w.lead_agent_id,(SELECT COUNT(*) FROM runtime_operations
             WHERE idempotency_key='archive-lead-workspace-signed-ceiling')
             FROM workspaces w WHERE w.workspace_id=?1",
            [&workspace_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(durable, (i64::MAX, Some(agent_id.clone()), 0));
    // Trusted direct repository callers are fenced under SQLite IMMEDIATE,
    // independently of the HTTP manager's earlier workspace snapshot.
    let session = runtime.get_session(&agent_id).await.unwrap();
    let reason = "lead signed SQLite ceiling";
    let request = serde_json::json!({
        "workspace_id":workspace_id,"agent_id":agent_id,
        "action":"archive","reason":reason,
    });
    let command = runtime_core::OperatorAgentArchiveCommand {
        operation_id: "op_archive_direct_lead_ceiling".into(),
        actor: runtime_core::OperationActor::operator("runtime_operator"),
        idempotency_key: "archive-direct-lead-ceiling".into(),
        workspace_id: workspace_id.clone(),
        agent_id: agent_id.clone(),
        reason: Some(reason.into()),
        expected_revision: runtime
            .get_active_workspace_agent_by_id(&agent_id)
            .unwrap()
            .revision,
        provider: session.provider.clone(),
        provider_session_ref: session.provider_session_ref.clone().unwrap(),
        canonical_provider_session_ref: session.canonical_provider_session_ref.clone(),
        session_updated_at: session.updated_at,
        normalized_request_hash: runtime_core::normalized_json_hash(&request).unwrap(),
        normalized_request: request,
        requested_at: session.updated_at + 1,
    };
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: temp.path().join("runtime.sqlite3"),
    });
    assert!(matches!(
        store.repository().admit_operator_agent_archive(&command),
        Err(runtime_core::RuntimeError::InvalidState(_))
    ));
    assert!(store
        .repository()
        .get_operation(&command.operation_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn native_archive_nonlead_allows_exhausted_workspace_revision_without_increment() {
    let (router, token, temp, provider, runtime, agent_id, _) = operator_policy_fixture().await;
    let workspace_id = runtime
        .get_active_workspace_agent_by_id(&agent_id)
        .unwrap()
        .workspace_id;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    let lead: Option<String> = db
        .query_row(
            "SELECT lead_agent_id FROM workspaces WHERE workspace_id=?1",
            [&workspace_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(lead.is_none(), "fixture agent must not hold lead authority");
    db.execute(
        "UPDATE workspaces SET revision=?1 WHERE workspace_id=?2",
        rusqlite::params![i64::MAX, workspace_id],
    )
    .unwrap();
    let before = provider.archive_close_count();
    let (status, archived) = call(
        &router,
        &token,
        "POST",
        &format!("/v2/workspaces/{workspace_id}/agents/{agent_id}/archive"),
        Some(serde_json::json!({"reason":"nonlead safe archive"})),
        Some("archive-nonlead-workspace-max"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{archived:?}");
    assert_eq!(archived["lifecycle_state"], "archived");
    assert_eq!(provider.archive_close_count(), before + 1);
    let revision: i64 = db
        .query_row(
            "SELECT revision FROM workspaces WHERE workspace_id=?1",
            [&workspace_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        revision,
        i64::MAX,
        "nonlead archive must not change workspace revision"
    );
}
