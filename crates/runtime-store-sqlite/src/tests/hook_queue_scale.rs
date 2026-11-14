use super::*;
use runtime_core::{TeamHookEventName, TeamHookResolvedEntry, TeamHookScope};

const EVENTS: [TeamHookEventName; 4] = [
    TeamHookEventName::PostTeammateAdd,
    TeamHookEventName::PostTeammateRemove,
    TeamHookEventName::PostDirectMessageSend,
    TeamHookEventName::PostBroadcastMessageSend,
];

// These predicates intentionally match the two private worker queries. Keep
// the EXPLAIN and old-query differential checks adjacent to the real SQL.
const DISPATCH_FILTER: &str = "e.phase='intended' AND e.event_name IN
    ('post_teammate_add','post_teammate_remove',
     'post_direct_message_send','post_broadcast_message_send')
    AND (e.event_name NOT IN ('post_direct_message_send','post_broadcast_message_send')
      OR NOT EXISTS(SELECT 1 FROM agent_deliveries pending
        WHERE pending.message_id=json_extract(e.input_json,'$.subject.message_id')
          AND pending.status IN ('pending','deferred','injecting')))
    AND EXISTS(SELECT 1 FROM runtime_events parent
      WHERE parent.event_id=e.parent_event_id AND parent.kind='workspace.hook_parent'
        AND parent.scope='workspace' AND parent.scope_id=e.workspace_id)
    AND EXISTS(SELECT 1 FROM workspaces w JOIN workspace_hook_approvals a
      ON a.workspace_id=w.workspace_id, json_each(a.approved_hooks_json) approved
      WHERE w.workspace_id=e.workspace_id AND w.lifecycle_state='active'
        AND approved.value=e.hook_json)";

const APPLY_FILTER: &str = "e.application_state='pending'
    AND e.phase IN ('succeeded','failed','timed_out')
    AND e.event_name IN
      ('post_teammate_add','post_teammate_remove',
       'post_direct_message_send','post_broadcast_message_send')";

fn queue_fixture() -> (tempfile::TempDir, SqliteRuntimeRepository) {
    let temp = tempfile::tempdir().unwrap();
    let repository = SqliteRuntimeRepository::new(temp.path().join("hooks.sqlite"));
    repository.initialize_schema().unwrap();
    let mut db = open_connection(&repository.database_path).unwrap();
    let hooks: Vec<TeamHookResolvedEntry> = EVENTS
        .iter()
        .copied()
        .map(|event_name| TeamHookResolvedEntry {
            event_name,
            name: "bootstrap".into(),
            command: "printf inert".into(),
            timeout_ms: 500,
            scope: TeamHookScope::Project,
        })
        .collect();
    let tx = db.transaction().unwrap();
    tx.execute_batch(
        "INSERT INTO workspaces(workspace_id,canonical_root,display_name,lifecycle_state,revision,created_at,updated_at)
         VALUES('ws_hook_queue','/tmp/inert_hook_queue','Queue','active',0,1,1);",
    )
    .unwrap();
    tx.execute(
        "INSERT INTO workspace_hook_approvals(workspace_id,revision,approved_hooks_json,approved_at_ms)
         VALUES('ws_hook_queue',1,?1,1)",
        [serde_json::to_string(&hooks).unwrap()],
    )
    .unwrap();
    // Large terminal history plus pending work, creation ties and several
    // undelivered broadcasts. These are disposable inert SQLite rows only;
    // no native hook script/provider is ever invoked.
    for i in 0..3_600usize {
        let parent = format!("hook_{i:05}");
        let hook = &hooks[i % EVENTS.len()];
        let (phase, application_state) = match i % 8 {
            0..=3 => ("intended", "not_ready"),
            4 => ("succeeded", "pending"),
            5 => ("failed", "pending"),
            6 => ("timed_out", "pending"),
            _ => ("succeeded", "applied"),
        };
        let message = format!("message_{i:05}");
        let input = serde_json::json!({
            "event_id": parent,
            "hook_event": hook.event_name.as_str(),
            "subject": {"message_id": message},
            "private": "x".repeat(384),
        });
        tx.execute(
            "INSERT INTO runtime_events(event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
             VALUES(?1,'workspace','ws_hook_queue',?2,'workspace.hook_parent',0,
                    '{\"workspace_id\":\"ws_hook_queue\"}',?3)",
            params![parent, i as i64 + 1, i as i64],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO workspace_hook_effects(
                workspace_id,parent_event_id,event_name,hook_name,hook_json,input_json,
                phase,attempt_count,receipt_json,created_at_ms,updated_at_ms,application_state)
             VALUES('ws_hook_queue',?1,?2,'bootstrap',?3,?4,?5,1,NULL,?6,?6,?7)",
            params![
                parent,
                hook.event_name.as_str(),
                serde_json::to_string(hook).unwrap(),
                input.to_string(),
                phase,
                (3_600 - i / 7) as i64,
                application_state,
            ],
        )
        .unwrap();
        if i % 8 == 3 {
            tx.execute(
                "INSERT INTO agent_messages(
                   id,scope,context_kind,workspace_id,sender_agent_id,
                   recipient_agent_ids_json,input_json,priority,policy,created_at)
                 VALUES(?1,'broadcast','workspace_team','ws_hook_queue','sender',
                        '[\"recipient\"]','{}','normal','immediate',1)",
                [&message],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO agent_deliveries(
                  id,message_id,recipient_agent_id,provider,status,created_at,updated_at)
                 VALUES(?1,?2,'recipient','codex','pending',1,1)",
                params![format!("delivery_{i:05}"), message],
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
    (temp, repository)
}

fn old_reference(repository: &SqliteRuntimeRepository, filter: &str, limit: usize) -> Vec<String> {
    let db = open_connection(&repository.database_path).unwrap();
    let query = format!(
        "SELECT e.parent_event_id FROM workspace_hook_effects e NOT INDEXED WHERE {filter}
         ORDER BY e.created_at_ms,e.parent_event_id,e.hook_name LIMIT ?1"
    );
    let result = db
        .prepare(&query)
        .unwrap()
        .query_map([limit.min(256) as i64], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    result
}

fn assert_queue_parity(repository: &SqliteRuntimeRepository) {
    for limit in [1, 17, 64, 256, 500] {
        let actual = repository
            .list_dispatchable_team_hook_effects(limit)
            .unwrap()
            .into_iter()
            .map(|effect| effect.parent_event_id)
            .collect::<Vec<_>>();
        assert_eq!(actual, old_reference(repository, DISPATCH_FILTER, limit));
        let actual = repository
            .list_unapplied_team_hook_effects(limit)
            .unwrap()
            .into_iter()
            .map(|effect| effect.parent_event_id)
            .collect::<Vec<_>>();
        assert_eq!(actual, old_reference(repository, APPLY_FILTER, limit));
    }
}

#[test]
fn hook_workers_use_small_partial_order_indexes_without_temporary_sort() {
    let (_temp, repository) = queue_fixture();
    let db = open_connection(&repository.database_path).unwrap();
    for (index, filter) in [
        ("idx_workspace_hook_effects_dispatch_order", DISPATCH_FILTER),
        ("idx_workspace_hook_effects_unapplied_order", APPLY_FILTER),
    ] {
        let sql = format!(
            "EXPLAIN QUERY PLAN SELECT e.workspace_id,e.parent_event_id,e.event_name,e.hook_name
             FROM workspace_hook_effects e INDEXED BY {index} WHERE {filter}
             ORDER BY e.created_at_ms,e.parent_event_id,e.hook_name LIMIT 64"
        );
        let details = db
            .prepare(&sql)
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" | ");
        assert!(details.contains(index), "missing ordered index: {details}");
        assert!(
            !details.contains("USE TEMP B-TREE"),
            "hook worker must not sort historical effects: {details}"
        );
    }
    assert_queue_parity(&repository);
    assert_eq!(
        repository
            .list_dispatchable_team_hook_effects(0)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        repository
            .list_unapplied_team_hook_effects(0)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn hook_queue_read_indexes_upgrade_and_observe_only_committed_wal_changes() {
    let (_temp, repository) = queue_fixture();
    let original_dispatch = old_reference(&repository, DISPATCH_FILTER, 256);
    let original_apply = old_reference(&repository, APPLY_FILTER, 256);
    let db = open_connection(&repository.database_path).unwrap();
    db.execute_batch(
        "DROP INDEX idx_workspace_hook_effects_dispatch_order;
         DROP INDEX idx_workspace_hook_effects_unapplied_order;
         DELETE FROM schema_migrations WHERE version=42;",
    )
    .unwrap();
    drop(db);
    // Simulate a populated source-schema-41 database; this upgrade creates
    // only its read indexes and never rewrites canonical hook effect rows.
    repository.initialize_schema().unwrap();
    let reopened = SqliteRuntimeRepository::new(repository.database_path.clone());
    assert_eq!(
        old_reference(&reopened, DISPATCH_FILTER, 256),
        original_dispatch
    );
    assert_eq!(old_reference(&reopened, APPLY_FILTER, 256), original_apply);
    assert_queue_parity(&reopened);

    let mut writer = open_connection(&reopened.database_path).unwrap();
    let tx = writer.transaction().unwrap();
    tx.execute(
        "UPDATE workspace_hook_effects
         SET phase='succeeded',application_state='pending'
         WHERE parent_event_id='hook_03592'",
        [],
    )
    .unwrap();
    assert_eq!(
        old_reference(&reopened, DISPATCH_FILTER, 256),
        original_dispatch
    );
    assert_eq!(old_reference(&reopened, APPLY_FILTER, 256), original_apply);
    tx.commit().unwrap();
    assert_ne!(
        old_reference(&reopened, DISPATCH_FILTER, 256),
        original_dispatch
    );
    assert_ne!(old_reference(&reopened, APPLY_FILTER, 256), original_apply);
    assert_queue_parity(&reopened);
}
