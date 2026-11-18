use super::*;
use rusqlite::TransactionBehavior;
use tokio::sync::broadcast::error::TryRecvError;

fn sample_event(id: &str, at: i64) -> NewRuntimeEvent {
    NewRuntimeEvent {
        event_id: id.to_owned(),
        scope: RuntimeEventScope::Team,
        scope_id: "team_sse".into(),
        session_id: None,
        team_id: Some("team_sse".into()),
        turn_id: None,
        kind: "team.member_joined".into(),
        criticality: RuntimeEventCriticality::Critical,
        payload: serde_json::json!({"event":id}),
        provider: None,
        provider_seq: None,
        created_at: at,
    }
}

#[tokio::test]
async fn committed_event_inserts_wake_all_store_instances_but_reads_and_replays_do_not() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    store.initialize().await.unwrap();
    let another_instance = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    let other_temp = tempfile::tempdir().unwrap();
    let unrelated = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: other_temp.path().join("runtime.sqlite3"),
    });
    unrelated.initialize().await.unwrap();
    let mut a = observe_commits(&store);
    let mut b = observe_commits(&another_instance);
    let mut unrelated_receiver = observe_commits(&unrelated);

    let repository = SqliteRuntimeRepository::new(path);
    repository.list_runtime_events(None, None, 10).unwrap();
    assert!(matches!(a.settled_try_recv(), Err(TryRecvError::Empty)));
    repository
        .append_runtime_event(&sample_event("evt_first", 1))
        .unwrap();
    a.wait().expect("first store observes committed event");
    b.wait().expect("second store observes committed event");
    assert!(matches!(
        unrelated_receiver.settled_try_recv(),
        Err(TryRecvError::Empty)
    ));

    // Idempotent append has no new durable event and emits no wakeup.
    repository
        .append_runtime_event(&sample_event("evt_first", 1))
        .unwrap();
    assert!(matches!(a.settled_try_recv(), Err(TryRecvError::Empty)));
    unrelated
        .append_runtime_event(&sample_event("evt_foreign", 2))
        .unwrap();
    unrelated_receiver.wait().expect("other database wakeup");
    assert!(matches!(a.settled_try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn rollback_never_wakes_and_transactional_multi_event_commit_wakes_once() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    store.initialize().await.unwrap();
    let mut receiver = observe_commits(&store);
    let sql = "INSERT INTO runtime_events
      (event_id,scope,scope_id,seq,kind,critical,payload_json,created_at)
      VALUES (?1,'team','team_sse',?2,'team.member_joined',1,'{}',?3)";

    {
        let mut db = open_connection(&path).unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        tx.execute(sql, params!["evt_rollback", 1, 1]).unwrap();
        tx.rollback().unwrap();
    }
    assert!(matches!(
        receiver.settled_try_recv(),
        Err(TryRecvError::Empty)
    ));
    assert!(store
        .list_runtime_events(None, None, 10)
        .unwrap()
        .is_empty());

    {
        let mut db = open_connection(&path).unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        tx.execute(sql, params!["evt_committed_one", 1, 2]).unwrap();
        tx.execute(sql, params!["evt_committed_two", 2, 3]).unwrap();
        tx.commit().unwrap();
        receiver
            .wait()
            .expect("committed batch wakes while its SQLite connection remains open");
    }
    assert!(matches!(
        receiver.settled_try_recv(),
        Err(TryRecvError::Empty)
    ));
    assert_eq!(
        store
            .list_runtime_events(Some((RuntimeEventScope::Team, "team_sse")), None, 10)
            .unwrap()
            .len(),
        2
    );
}
