use super::workspace_worktree_routes::{call, fixture};
use super::*;

#[tokio::test]
async fn signed_agent_revision_ceiling_rejects_before_fake_provider_rebind() {
    let (router, token, temp, provider, workspace, agent, _linked) = fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX, agent.agent_id],
    )
    .unwrap();
    let (status, response) = call(
        &router,
        &token,
        "POST",
        &format!(
            "/v2/workspaces/{}/agents/{}/worktree",
            workspace.workspace_id, agent.agent_id
        ),
        Some(serde_json::json!({"worktree_id":"wt_linked","expected_revision":i64::MAX})),
        Some("worktree-rebind-signed-ceiling"),
    )
    .await;
    assert_eq!(
        provider.rebind_calls().await,
        0,
        "signed revision ceiling must not rebind a native attachment: {status} {response:?}"
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response:?}");
    assert_eq!(response["error"]["code"], "invalid_state");
    let rows: (i64, i64, i64, String) = db
        .query_row(
            "SELECT (SELECT COUNT(*) FROM workspace_agent_rebinds),
                    (SELECT COUNT(*) FROM runtime_operations WHERE kind='workspace_agent_rebind'),
                    a.revision,typeof(a.revision)
             FROM workspace_agents a WHERE a.session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(rows, (0, 0, i64::MAX, "integer".into()));
}

#[tokio::test]
async fn rebind_last_signed_successor_commits_and_exact_key_replays_at_limit() {
    let (router, token, temp, provider, workspace, agent, _linked) = fixture().await;
    let db = rusqlite::Connection::open(temp.path().join("runtime.sqlite3")).unwrap();
    db.execute(
        "UPDATE workspace_agents SET revision=?1 WHERE session_id=?2",
        rusqlite::params![i64::MAX - 1, agent.agent_id],
    )
    .unwrap();
    let url = format!(
        "/v2/workspaces/{}/agents/{}/worktree",
        workspace.workspace_id, agent.agent_id
    );
    let input = serde_json::json!({"worktree_id":"wt_linked","expected_revision":i64::MAX - 1});
    let (status, first) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(input.clone()),
        Some("rebind-last-successor"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first:?}");
    assert_eq!(first["agent"]["revision"], i64::MAX);
    assert_eq!(provider.rebind_calls().await, 1);
    let (status, replay) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(input),
        Some("rebind-last-successor"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replay:?}");
    assert_eq!(replay["operation"], first["operation"]);
    assert_eq!(provider.rebind_calls().await, 1);
    let (status, rejected) = call(
        &router,
        &token,
        "POST",
        &url,
        Some(serde_json::json!({"worktree_id":null,"expected_revision":i64::MAX})),
        Some("rebind-no-more-signed-successors"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert_eq!(rejected["error"]["code"], "invalid_state");
    assert_eq!(provider.rebind_calls().await, 1);
    let (revision, kind): (i64, String) = db
        .query_row(
            "SELECT revision,typeof(revision) FROM workspace_agents WHERE session_id=?1",
            [&agent.agent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((revision, kind), (i64::MAX, "integer".into()));
}
