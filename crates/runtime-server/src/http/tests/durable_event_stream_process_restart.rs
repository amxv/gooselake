//! An owned reader OS process dies after consuming a cursor. A different
//! reader process replays the absent interval and then receives a live commit
//! from another OS process. Only disposable, marker-bound fixture data is used.
use super::*;
use axum::response::IntoResponse;
use runtime_core::{EventCollection, EventCursor};
use runtime_store_sqlite::SqliteRuntimeRepository;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};

use super::durable_event_stream_routes::new_event;

const ROOT_ENV: &str = "GOOSELAKE_N05_READER_PROCESS_FIXTURE_ROOT";
const ROLE_ENV: &str = "GOOSELAKE_N05_READER_PROCESS_FIXTURE_ROLE";
const GLOBAL_ENV: &str = "GOOSELAKE_N05_READER_PROCESS_GLOBAL_AFTER";
const SCOPED_ENV: &str = "GOOSELAKE_N05_READER_PROCESS_SCOPED_AFTER";
const MARKER: &str = "gooselake-n05-owned-reader-restart-fixture-v1\n";
const WORKSPACE: &str = "ws_reader_process_restart";
const READER: &str = "http::tests::durable_event_stream_process_restart::reader_child";
const WRITER: &str = "http::tests::durable_event_stream_process_restart::writer_child";

fn child_root() -> Option<PathBuf> {
    let root = PathBuf::from(std::env::var_os(ROOT_ENV)?);
    assert_eq!(
        std::fs::read_to_string(root.join("fixture-marker")).unwrap(),
        MARKER
    );
    assert!(root.join("data/runtime.sqlite3").is_file());
    Some(root)
}

fn publish(root: &Path, name: &str, value: &serde_json::Value) {
    // Control traffic lives outside the database's watched parent directory.
    let pending = root.join("control").join(format!("{name}.pending"));
    let complete = root.join("control").join(format!("{name}.json"));
    assert!(!complete.exists());
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)
        .unwrap();
    file.write_all(&serde_json::to_vec(value).unwrap()).unwrap();
    drop(file);
    std::fs::rename(pending, complete).unwrap();
}

#[test]
fn writer_child() {
    let Some(root) = child_root() else {
        return;
    };
    let name = std::env::var(ROLE_ENV).unwrap();
    assert!(["a", "b", "c"].contains(&name.as_str()));
    let record = SqliteRuntimeRepository::new(root.join("data/runtime.sqlite3"))
        .append_runtime_event(&new_event(&name, RuntimeEventScope::Workspace, WORKSPACE))
        .unwrap();
    publish(
        &root,
        &format!("{name}-written"),
        &serde_json::to_value(record).unwrap(),
    );
}

struct Frames {
    chunks: axum::body::BodyDataStream,
    buffered: String,
}

impl Frames {
    fn new(response: Response) -> Self {
        Self {
            chunks: response.into_body().into_data_stream(),
            buffered: String::new(),
        }
    }

    async fn next(&mut self, deadline: Duration) -> String {
        timeout(deadline, async {
            loop {
                if let Some(end) = self.buffered.find("\n\n") {
                    let frame = self.buffered[..end].to_owned();
                    self.buffered.drain(..end + 2);
                    return frame;
                }
                let bytes = self
                    .chunks
                    .next()
                    .await
                    .expect("live SSE must remain open")
                    .unwrap();
                self.buffered
                    .push_str(&String::from_utf8(bytes.to_vec()).unwrap());
            }
        })
        .await
        .expect("bounded external reader SSE delivery")
    }

    async fn record(&mut self) -> (String, serde_json::Value) {
        let frame = self.next(Duration::from_secs(3)).await;
        let id = frame
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .expect("data frame must have a durable cursor")
            .to_owned();
        let data = frame
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .expect("canonical durable envelope");
        (id, serde_json::from_str(data).unwrap())
    }

    async fn keepalive(&mut self) {
        let frame = self.next(Duration::from_secs(12)).await;
        assert!(
            frame.starts_with(':'),
            "trailing duplicate or error instead of real keepalive: {frame}"
        );
        assert!(!frame.lines().any(|line| line.starts_with("id:")));
    }
}

async fn consume_pair(
    global: &mut Frames,
    scoped: &mut Frames,
    event: &str,
    seq: i64,
) -> serde_json::Value {
    let ((global_id, global_record), (scoped_id, scoped_record)) =
        tokio::join!(global.record(), scoped.record());
    assert_eq!(
        global_record, scoped_record,
        "projection must retain original canonical envelope"
    );
    assert_eq!(global_record["event_id"], event);
    assert_eq!(global_record["scope"], "workspace");
    assert_eq!(global_record["scope_id"], WORKSPACE);
    assert_eq!(global_record["seq"], seq);
    let global_cursor = EventCursor::decode(&global_id).unwrap();
    let scoped_cursor = EventCursor::decode(&scoped_id).unwrap();
    assert_eq!(
        global_cursor.collection(),
        &EventCollection::stream(None).unwrap()
    );
    assert_eq!(
        scoped_cursor.collection(),
        &EventCollection::stream(Some((RuntimeEventScope::Workspace, WORKSPACE.into()))).unwrap()
    );
    assert_eq!(global_cursor.epoch(), scoped_cursor.epoch());
    global_cursor.validate_anchor(Some(event)).unwrap();
    scoped_cursor.validate_anchor(Some(event)).unwrap();
    assert_eq!(global_record["row_id"], global_cursor.position());
    assert_eq!(scoped_cursor.position(), seq);
    assert_ne!(global_cursor.position(), scoped_cursor.position());
    // Publish the exact wire bytes, never freshly encoded replacement tokens.
    serde_json::json!({"pid":std::process::id(),"global_cursor":global_id,
        "scoped_cursor":scoped_id,"record":global_record})
}

fn saved_cursor(name: &str) -> Option<EventCursor> {
    let raw = std::env::var(name).unwrap();
    if raw.is_empty() {
        None
    } else {
        Some(EventCursor::decode(&raw).unwrap())
    }
}

fn observed_position(report: &serde_json::Value, key: &str) -> i64 {
    EventCursor::decode(report[key].as_str().unwrap())
        .unwrap()
        .position()
}

#[tokio::test(flavor = "current_thread")]
async fn reader_child() {
    let Some(root) = child_root() else {
        return;
    };
    let role = std::env::var(ROLE_ENV).unwrap();
    assert!(["a", "b"].contains(&role.as_str()));
    let global_after = saved_cursor(GLOBAL_ENV);
    let scoped_after = saved_cursor(SCOPED_ENV);
    let store = Arc::new(SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: root.join("data/runtime.sqlite3"),
    }));
    // No initialize/schema operation and no writer in this process. Both
    // subscriptions bind the existing generation, including cursor checks.
    let headers = HeaderMap::new();
    let mut global = Frames::new(
        checked_durable_event_stream(
            store.clone(),
            EventCollection::stream(None).unwrap(),
            global_after,
            10,
            &headers,
        )
        .await
        .unwrap()
        .into_response(),
    );
    let mut scoped = Frames::new(
        checked_durable_event_stream(
            store,
            EventCollection::stream(Some((RuntimeEventScope::Workspace, WORKSPACE.into())))
                .unwrap(),
            scoped_after,
            10,
            &headers,
        )
        .await
        .unwrap()
        .into_response(),
    );
    publish(
        &root,
        &format!("{role}-opened"),
        &serde_json::json!({
            "pid":std::process::id(), "global_after":std::env::var(GLOBAL_ENV).unwrap(), "scoped_after":std::env::var(SCOPED_ENV).unwrap(),
        }),
    );
    if role == "a" {
        let consumed = consume_pair(&mut global, &mut scoped, "a", 1).await;
        // A later write must not rescue A's wake. An actual keepalive also
        // exposes any trailing duplicate before the reader is killed.
        tokio::join!(global.keepalive(), scoped.keepalive());
        publish(&root, "a-consumed", &consumed);
        tokio::time::sleep(Duration::from_secs(30)).await;
        panic!("parent did not terminate its owned reader after consuming A");
    }
    // A one-row first page (page_size=10) is drained on this current-thread
    // runtime before the parent is allowed to launch writer C.
    let caught_up = consume_pair(&mut global, &mut scoped, "b", 2).await;
    publish(&root, "b-caught-up", &caught_up);
    let live = consume_pair(&mut global, &mut scoped, "c", 3).await;
    tokio::join!(global.keepalive(), scoped.keepalive());
    publish(&root, "b-live-consumed", &live);
}

struct OwnedChild {
    child: Child,
    log: PathBuf,
}

impl OwnedChild {
    fn spawn(
        root: &Path,
        test: &str,
        role: &str,
        global: Option<&str>,
        scoped: Option<&str>,
    ) -> Self {
        let label = if test == READER { "reader" } else { "writer" };
        let log = root.join("control").join(format!("{label}-{role}.log"));
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&log)
            .unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env(ROOT_ENV, root)
            .env(ROLE_ENV, role)
            .env(GLOBAL_ENV, global.unwrap_or_default())
            .env(SCOPED_ENV, scoped.unwrap_or_default())
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap();
        Self { child, log }
    }

    async fn message(&mut self, root: &Path, name: &str) -> serde_json::Value {
        let path = root.join("control").join(format!("{name}.json"));
        timeout(Duration::from_secs(16), async {
            loop {
                if path.exists() {
                    return serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                }
                if let Some(status) = self.child.try_wait().unwrap() {
                    panic!(
                        "owned child exited {status} before {name}: {}",
                        self.log_text()
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("bounded child handshake {name}: {}", self.log_text()))
    }

    async fn success(&mut self) {
        let status = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("bounded owned child exit");
        assert!(status.success(), "owned child failed: {}", self.log_text());
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn kill_and_reap(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
        }
        self.child.wait().unwrap();
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

async fn write_external(root: &Path, event: &str) -> serde_json::Value {
    let mut writer = OwnedChild::spawn(root, WRITER, event, None, None);
    writer.success().await;
    serde_json::from_reader(
        File::open(root.join("control").join(format!("{event}-written.json"))).unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn killed_reader_process_resumes_consumed_global_and_scoped_cursors_then_receives_live_commit(
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    std::fs::create_dir(root.join("data")).unwrap();
    std::fs::create_dir(root.join("control")).unwrap();
    std::fs::write(root.join("fixture-marker"), MARKER).unwrap();
    let path = root.join("data/runtime.sqlite3");
    let store = SqliteRuntimeStore::new(SqliteStoreConfig {
        database_path: path.clone(),
    });
    store.initialize().await.unwrap();
    let repository = SqliteRuntimeRepository::new(path.clone());
    let seed = repository
        .append_runtime_event(&new_event(
            "unrelated_seed",
            RuntimeEventScope::System,
            "global",
        ))
        .unwrap();
    drop(store);
    let identity = std::fs::metadata(&path).unwrap();
    let identity = (identity.dev(), identity.ino());

    let seed_cursor = super::operator_event_test_support::fixture_cursor(
        &repository,
        &EventCollection::stream(None).unwrap(),
        &seed,
    );
    let mut first = OwnedChild::spawn(root, READER, "a", Some(&seed_cursor), None);
    let first_pid = first.child.id();
    let opened = first.message(root, "a-opened").await;
    assert_eq!(opened["pid"], first_pid);
    assert_eq!(opened["global_after"], seed_cursor);
    assert_eq!(opened["scoped_after"], "");
    let a = write_external(root, "a").await;
    let consumed = first.message(root, "a-consumed").await;
    assert_eq!(consumed["record"], a);
    first.kill_and_reap();
    assert!(first.child.try_wait().unwrap().is_some());

    let b = write_external(root, "b").await;
    let global_cursor = consumed["global_cursor"].as_str().unwrap();
    let scoped_cursor = consumed["scoped_cursor"].as_str().unwrap();
    assert_ne!(global_cursor, scoped_cursor);
    let mut second = OwnedChild::spawn(root, READER, "b", Some(global_cursor), Some(scoped_cursor));
    assert_ne!(second.child.id(), first_pid);
    let reopened = second.message(root, "b-opened").await;
    assert_eq!(reopened["global_after"], global_cursor);
    assert_eq!(reopened["scoped_after"], scoped_cursor);
    let catchup = second.message(root, "b-caught-up").await;
    assert_eq!(catchup["record"], b);
    assert_eq!(catchup["pid"], second.child.id());

    // C is allowed only after B has actually been consumed from both streams.
    // No local write or later sentinel can rescue the following live receive.
    let c = write_external(root, "c").await;
    let live = second.message(root, "b-live-consumed").await;
    assert_eq!(live["record"], c);
    second.success().await;
    let after = std::fs::metadata(&path).unwrap();
    assert_eq!((after.dev(), after.ino()), identity);
    let oracle = repository
        .list_runtime_events(None, Some(seed.row_id), 10)
        .unwrap();
    assert_eq!(
        oracle
            .iter()
            .map(|row| row.event_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    assert_eq!(
        [
            observed_position(&consumed, "global_cursor"),
            observed_position(&catchup, "global_cursor"),
            observed_position(&live, "global_cursor")
        ],
        oracle
            .iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>()
            .as_slice()
    );
    assert_eq!(
        [
            observed_position(&consumed, "scoped_cursor"),
            observed_position(&catchup, "scoped_cursor"),
            observed_position(&live, "scoped_cursor")
        ],
        [1, 2, 3]
    );
}
