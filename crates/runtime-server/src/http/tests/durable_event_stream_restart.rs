use super::*;
use axum::response::IntoResponse;
use runtime_core::RuntimeStore;
use runtime_store_sqlite::{SqliteRuntimeRepository, SqliteRuntimeStore, SqliteStoreConfig};

use super::durable_event_stream_routes::{new_event, observed_sse_ids};

#[tokio::test]
async fn reopened_sqlite_streams_resume_all_four_cursors_and_catch_new_commits_once() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runtime.sqlite3");
    let original = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    original.initialize().await.unwrap();
    let repository = SqliteRuntimeRepository::new(path.clone());

    let seed_workspace = repository
        .append_runtime_event(&new_event(
            "restart_workspace_seed",
            RuntimeEventScope::Workspace,
            "ws_restart",
        ))
        .unwrap();
    let seed_agent = repository
        .append_runtime_event(&new_event(
            "restart_agent_seed",
            RuntimeEventScope::Session,
            "agent_restart",
        ))
        .unwrap();
    let seed_process = repository
        .append_runtime_event(&new_event(
            "restart_process_seed",
            RuntimeEventScope::Process,
            "proc_restart",
        ))
        .unwrap();
    let seed_global = repository
        .append_runtime_event(&new_event(
            "restart_global_seed",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    drop(original);

    // New runtime/store instance must first replay from durable SQLite and
    // then receive new commits, without relying on a prior in-memory bus.
    let reopened = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    reopened.initialize().await.unwrap();
    let store: Arc<dyn RuntimeStore> = Arc::new(reopened);
    let headers = HeaderMap::new();
    let global = durable_event_stream(store.clone(), None, Some(seed_global.row_id), 1, &headers)
        .await
        .unwrap()
        .into_response();
    let workspace = durable_event_stream(
        store.clone(),
        Some((RuntimeEventScope::Workspace, "ws_restart".into())),
        Some(seed_workspace.seq),
        1,
        &headers,
    )
    .await
    .unwrap()
    .into_response();
    let agent = durable_event_stream(
        store.clone(),
        Some((RuntimeEventScope::Session, "agent_restart".into())),
        Some(seed_agent.seq),
        1,
        &headers,
    )
    .await
    .unwrap()
    .into_response();
    let process = durable_event_stream(
        store.clone(),
        Some((RuntimeEventScope::Process, "proc_restart".into())),
        Some(seed_process.seq),
        1,
        &headers,
    )
    .await
    .unwrap()
    .into_response();

    let w1 = repository
        .append_runtime_event(&new_event(
            "restart_w1",
            RuntimeEventScope::Workspace,
            "ws_restart",
        ))
        .unwrap();
    let a1 = repository
        .append_runtime_event(&new_event(
            "restart_a1",
            RuntimeEventScope::Session,
            "agent_restart",
        ))
        .unwrap();
    let p1 = repository
        .append_runtime_event(&new_event(
            "restart_p1",
            RuntimeEventScope::Process,
            "proc_restart",
        ))
        .unwrap();
    let w2 = repository
        .append_runtime_event(&new_event(
            "restart_w2",
            RuntimeEventScope::Workspace,
            "ws_restart",
        ))
        .unwrap();
    let a2 = repository
        .append_runtime_event(&new_event(
            "restart_a2",
            RuntimeEventScope::Session,
            "agent_restart",
        ))
        .unwrap();
    let p2 = repository
        .append_runtime_event(&new_event(
            "restart_p2",
            RuntimeEventScope::Process,
            "proc_restart",
        ))
        .unwrap();

    observed_sse_ids(
        global,
        &[
            w1.row_id, a1.row_id, p1.row_id, w2.row_id, a2.row_id, p2.row_id,
        ],
    )
    .await;
    observed_sse_ids(workspace, &[w1.seq, w2.seq]).await;
    observed_sse_ids(agent, &[a1.seq, a2.seq]).await;
    observed_sse_ids(process, &[p1.seq, p2.seq]).await;
    drop(store);

    // Reconnect again on a third fresh store. Each scope resumes from the
    // correct domain: global row ID, resource-local sequence otherwise.
    let next: Arc<dyn RuntimeStore> = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path,
    }));
    observed_sse_ids(
        durable_event_stream(next.clone(), None, Some(a2.row_id), 1, &headers)
            .await
            .unwrap()
            .into_response(),
        &[p2.row_id],
    )
    .await;
    for (scope, cursor, last) in [
        (RuntimeEventScope::Workspace, ("ws_restart", w1.seq), w2.seq),
        (
            RuntimeEventScope::Session,
            ("agent_restart", a1.seq),
            a2.seq,
        ),
        (RuntimeEventScope::Process, ("proc_restart", p1.seq), p2.seq),
    ] {
        observed_sse_ids(
            durable_event_stream(
                next.clone(),
                Some((scope, cursor.0.into())),
                Some(cursor.1),
                1,
                &headers,
            )
            .await
            .unwrap()
            .into_response(),
            &[last],
        )
        .await;
    }
}
