use super::workspace_agent::{registered_workspace, workspace_agent_fixture};
use super::*;
use runtime_core::ProviderKind;

#[test]
fn current_context_status_batch_is_bounded_and_does_not_scan_other_agents() {
    use runtime_core::{ProviderContextLimitObservation, SessionContextLimitSnapshot};

    let temp_dir = tempfile::tempdir().unwrap();
    let root = temp_dir.path().join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    let repository = repo(&temp_dir);
    repository.initialize_schema().unwrap();
    let workspace = registered_workspace(&repository, &root);

    let mut expected_ids = Vec::new();
    for index in 0..120 {
        let id = format!("sess_context_batch_{index:03}");
        let (session, agent) =
            workspace_agent_fixture(&workspace, &id, &format!("steady-otter-context-{index:03}"));
        repository.create_workspace_agent(&session, &agent).unwrap();
        let snapshot = SessionContextLimitSnapshot {
            agent_id: id.clone(),
            provider: ProviderKind::Codex,
            provider_session_ref: session.provider_session_ref.clone().unwrap(),
            canonical_provider_session_ref: None,
            agent_revision: 0,
            observation: ProviderContextLimitObservation {
                model_context_window: 200_000,
                last_total_tokens: index * 100,
                remaining_percentage: 100 - u8::try_from(index % 99).unwrap(),
            },
            observed_at_ms: 200,
            observed_turn_id: None,
        };
        assert!(repository
            .record_session_context_limit(&snapshot, session.updated_at)
            .unwrap());
        if index % 12 == 0 {
            expected_ids.push(id);
        }
    }
    let output = repository
        .list_session_context_limits(&expected_ids)
        .unwrap();
    assert_eq!(output.len(), expected_ids.len());
    for snapshot in &output {
        assert!(expected_ids.contains(&snapshot.agent_id));
        assert_eq!(
            repository
                .get_session_context_limit(&snapshot.agent_id)
                .unwrap(),
            Some(snapshot.clone()),
            "batch status must preserve the point-query provider/revision fences"
        );
    }
    assert!(repository
        .list_session_context_limits(&[])
        .unwrap()
        .is_empty());
    assert!(repository
        .list_session_context_limits(&vec!["x".into(); 257])
        .is_err());
    let restarted = SqliteRuntimeRepository::new(temp_dir.path().join("runtime.sqlite3"));
    assert_eq!(
        restarted
            .list_session_context_limits(&expected_ids)
            .unwrap(),
        output
    );

    let db = rusqlite::Connection::open(temp_dir.path().join("runtime.sqlite3")).unwrap();
    let mut statement = db
        .prepare(
            "EXPLAIN QUERY PLAN SELECT c.session_id FROM session_context_limit_snapshots c
             JOIN sessions s ON s.id=c.session_id
             JOIN workspace_agents a ON a.session_id=s.id
             JOIN workspaces w ON w.workspace_id=a.workspace_id
             WHERE c.session_id IN ('sess_context_batch_000','sess_context_batch_012')
               AND c.provider=s.provider AND a.lifecycle_state='active'
               AND w.lifecycle_state='active'",
        )
        .unwrap();
    let plan = statement
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ");
    assert!(
        plan.contains("SEARCH"),
        "status lookup must use an index: {plan}"
    );
    assert!(
        !plan.contains("SCAN c"),
        "context status query must not scan all prior observations: {plan}"
    );
}
