//! Bounded characterization of the current cross-OS-process SSE wake gap.
//! The SQLite journal is shared, but its notification registry is not. This
//! fixture must be revisited when cross-process fan-out gains an explicit design.

use super::*;
use axum::response::IntoResponse;
use runtime_core::RuntimeEventScope;
use runtime_store_sqlite::SqliteRuntimeRepository;
use std::process::Command;
use tokio_stream::StreamExt;

const CHILD_DB: &str = "GOOSELAKE_SSE_EXTERNAL_COMMIT_FIXTURE_DB";
const WORKSPACE_ID: &str = "ws_cross_process_sse";

// The child is an independently executing copy of the Rust test binary, with
// an ordinary SQLite repository and a separate event notifier registry. The
// missing env var makes the normal top-level test suite a harmless no-op.
#[test]
fn child_commits_cross_process_sse_event() {
    let Some(path) = std::env::var_os(CHILD_DB) else {
        return;
    };
    let repository = SqliteRuntimeRepository::new(PathBuf::from(path));
    repository
        .append_runtime_event(&super::durable_event_stream_routes::new_event(
            "external_sse_commit",
            RuntimeEventScope::Workspace,
            WORKSPACE_ID,
        ))
        .expect("child OS process must commit the durable event");
}

#[tokio::test]
async fn separate_os_process_commit_is_readable_but_does_not_wake_idle_sse() {
    let temp = tempfile::tempdir().expect("disposable SQLite fixture");
    let path = temp.path().join("runtime.sqlite3");
    let sqlite = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    }));
    sqlite.initialize().await.expect("initialize SQLite");
    // Force global row IDs to differ from this workspace's scoped seq IDs.
    // Otherwise a mistaken SSE cursor type can pass an empty-database test.
    let seed = SqliteRuntimeRepository::new(path.clone())
        .append_runtime_event(&super::durable_event_stream_routes::new_event(
            "unrelated_global_seed",
            RuntimeEventScope::System,
            "global",
        ))
        .expect("unrelated global seed");

    // Both scopes subscribe before the external OS process writes. An empty
    // first page rules out initial replay being mistaken for live fan-out.
    let global = durable_event_stream(
        sqlite.clone(),
        None,
        Some(seed.row_id),
        1,
        &HeaderMap::new(),
    )
    .await
    .expect("global SSE");
    let workspace = durable_event_stream(
        sqlite,
        Some((RuntimeEventScope::Workspace, WORKSPACE_ID.to_string())),
        Some(0),
        1,
        &HeaderMap::new(),
    )
    .await
    .expect("workspace SSE");
    let mut global_chunks = global.into_response().into_body().into_data_stream();
    let mut workspace_chunks = workspace.into_response().into_body().into_data_stream();

    let child = Command::new(std::env::current_exe().expect("current test executable"))
        .args([
            "--exact",
            "http::tests::durable_event_stream_cross_process::child_commits_cross_process_sse_event",
        ])
        .env(CHILD_DB, &path)
        .output()
        .expect("start independent Rust test process");
    assert!(
        child.status.success(),
        "child process failed: stdout={} stderr={}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );

    // This observation is read-side only. It does NOT imply that the SSE
    // subscriber in the parent process received any wakeup notification.
    let reader = SqliteRuntimeRepository::new(path.clone());
    let external = reader
        .list_runtime_events(None, Some(seed.row_id), 8)
        .expect("external committed event is readable");
    assert_eq!(external.len(), 1, "child must execute, not be filtered out");
    assert_eq!(external[0].event_id, "external_sse_commit");
    assert_eq!(external[0].seq, 1);
    assert_eq!(external[0].row_id, seed.row_id + 1);

    // A timeout here is intentionally a *known limitation*, not acceptable
    // delivery semantics. It differentiates actual idle cross-process SSE
    // from the separately proven WAL/cursor read and same-process wake tests.
    let (global_result, workspace_result) = tokio::join!(
        timeout(Duration::from_millis(450), global_chunks.next()),
        timeout(Duration::from_millis(450), workspace_chunks.next()),
    );
    assert!(
        global_result.is_err(),
        "external commit unexpectedly woke global SSE; update this characterization"
    );
    assert!(
        workspace_result.is_err(),
        "external commit unexpectedly woke workspace SSE; update this characterization"
    );

    // A subsequent local SQLite commit generates an in-process wakeup. Both
    // streams must then replay the previously stranded external event first,
    // without loss or duplication, followed by the local event.
    let local = reader
        .append_runtime_event(&super::durable_event_stream_routes::new_event(
            "local_sse_commit",
            RuntimeEventScope::Workspace,
            WORKSPACE_ID,
        ))
        .expect("local committed wakeup");
    let global_expected = [external[0].row_id, local.row_id];
    let scoped_expected = [external[0].seq, local.seq];
    for (chunks, expected) in [
        (&mut global_chunks, global_expected),
        (&mut workspace_chunks, scoped_expected),
    ] {
        let mut observed = Vec::new();
        for _ in 0..2 {
            let bytes = timeout(Duration::from_secs(3), chunks.next())
                .await
                .expect("local commit must wake SSE")
                .expect("SSE stream must remain live")
                .expect("SSE body bytes");
            let data = String::from_utf8_lossy(&bytes);
            observed.extend(
                data.lines()
                    .filter_map(|line| line.strip_prefix("id: "))
                    .map(|value| value.parse::<i64>().expect("numeric SSE cursor")),
            );
        }
        assert_eq!(
            observed, expected,
            "cross-process event must be recovered in order"
        );
    }

    // A reopened reader may resume from the external row directly, without
    // needing either process-local notification channel.
    let reopened = SqliteRuntimeRepository::new(path);
    assert_eq!(
        reopened
            .list_runtime_events(None, Some(external[0].row_id), 1)
            .unwrap()[0]
            .row_id,
        local.row_id,
    );
}
