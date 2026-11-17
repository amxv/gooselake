use super::support::*;
use crate::db::open_connection;
use crate::SqliteRuntimeRepository;
use runtime_core::{RuntimeError, StartupReattachAdmission};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const CHILD_TEST: &str = "tests::startup_reattachment::process::startup_sqlite_child";
const OWNER_MARKER: &str = "startup-sqlite-test-owned-v1";

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn child(
    root: &Path,
    name: &str,
    action: &str,
    after_effect: bool,
    expected_owner: &str,
) -> Command {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env("GOOSELAKE_STARTUP_SQLITE_OWNED_ROOT", root)
        .env("GOOSELAKE_STARTUP_SQLITE_CHILD", name)
        .env("GOOSELAKE_STARTUP_SQLITE_ACTION", action)
        .env(
            "GOOSELAKE_STARTUP_SQLITE_AFTER_EFFECT",
            if after_effect { "yes" } else { "no" },
        )
        .env("GOOSELAKE_STARTUP_SQLITE_OWNER", expected_owner)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    child
}

fn successful(output: &Output) {
    assert!(
        output.status.success(),
        "child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
        "child did not execute the selected test: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

fn effect_count(root: &Path) -> usize {
    match std::fs::read_to_string(root.join("fake-native-attempts")) {
        Ok(text) => text.lines().count(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!("read owned fake native counter: {error}"),
    }
}

#[test]
fn startup_sqlite_child() {
    let Some(root) = std::env::var_os("GOOSELAKE_STARTUP_SQLITE_OWNED_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    assert_eq!(
        std::fs::read_to_string(root.join("owned-fixture-marker")).unwrap(),
        OWNER_MARKER
    );
    assert!(root.join("runtime.sqlite3").is_file());
    let name = std::env::var("GOOSELAKE_STARTUP_SQLITE_CHILD").unwrap();
    assert!(
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
    let action = std::env::var("GOOSELAKE_STARTUP_SQLITE_ACTION").unwrap();
    let repository = SqliteRuntimeRepository::new(root.join("runtime.sqlite3"));
    let command = command(
        &repository,
        &format!("startup_process_{name}"),
        &format!("instance_{name}"),
        SESSION,
    );
    if action == "compete" {
        std::fs::write(root.join(format!("ready-{name}")), b"ready").unwrap();
        wait_for(&root.join("start-competition"));
        match repository.admit_startup_reattach(&command, 300) {
            Ok(StartupReattachAdmission::Execute(grant)) => {
                assert_attempt(&repository, &grant, "requested", "started", true);
                if std::env::var("GOOSELAKE_STARTUP_SQLITE_AFTER_EFFECT").unwrap() == "yes" {
                    // This owned local counter models one external side effect; no provider is called.
                    let mut counter = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(root.join("fake-native-attempts"))
                        .unwrap();
                    writeln!(counter, "{}", command.operation_id).unwrap();
                    counter.sync_all().unwrap();
                }
                // Simulate process death after durable admission, optionally after the modeled effect.
                // No finalizer, outcome receipt, or destructor writes are allowed to run.
                std::process::exit(73);
            }
            Err(RuntimeError::OperationUnresolved { operation_id, .. }) => {
                assert!(operation_id == "startup_process_a" || operation_id == "startup_process_b");
                assert_ne!(operation_id, command.operation_id);
            }
            other => panic!("independent process got unexpected admission: {other:?}"),
        }
    } else {
        assert_eq!(action, "restart");
        let owner = std::env::var("GOOSELAKE_STARTUP_SQLITE_OWNER").unwrap();
        let before = snapshot(&repository);
        assert_unresolved(repository.admit_startup_reattach(&command, 600), &owner);
        assert_eq!(snapshot(&repository), before);
    }
}

#[test]
fn independent_processes_never_repeat_a_granted_startup_after_unknown_exit() {
    for after_effect in [false, true] {
        let fixture = Fixture::new(true);
        let root = fixture.temp.path();
        std::fs::write(root.join("owned-fixture-marker"), OWNER_MARKER).unwrap();
        let first = child(root, "a", "compete", after_effect, "")
            .spawn()
            .unwrap();
        let second = child(root, "b", "compete", after_effect, "")
            .spawn()
            .unwrap();
        wait_for(&root.join("ready-a"));
        wait_for(&root.join("ready-b"));
        std::fs::write(root.join("start-competition"), b"go").unwrap();
        let results = [
            first.wait_with_output().unwrap(),
            second.wait_with_output().unwrap(),
        ];
        assert_eq!(
            results
                .iter()
                .filter(|result| result.status.code() == Some(73))
                .count(),
            1,
            "expected one process death after exclusive grant: {results:?}"
        );
        let loser = results
            .iter()
            .find(|result| result.status.code() != Some(73))
            .unwrap();
        successful(loser);
        assert_eq!(effect_count(root), usize::from(after_effect));
        let db = open_connection(&fixture.repository.database_path).unwrap();
        let owner: String = db
            .query_row(
                "SELECT owner_operation_id FROM runtime_operation_resource_claims
             WHERE resource_kind=?1 AND resource_id=?2",
                [RESUME_CLAIM, SESSION],
                |row| row.get(0),
            )
            .unwrap();
        let evidence: String = db
            .query_row(
                "SELECT evidence_json FROM runtime_operation_effects WHERE operation_id=?1",
                [&owner],
                |row| row.get(0),
            )
            .unwrap();
        let evidence: serde_json::Value = serde_json::from_str(&evidence).unwrap();
        let grant: runtime_core::StartupReattachGrant =
            serde_json::from_value(evidence["grant"].clone()).unwrap();
        assert_eq!(grant.intent.command.operation_id, owner);
        assert_attempt(&fixture.repository, &grant, "requested", "started", true);
        let before = snapshot(&fixture.repository);
        assert!(matches!(
            fixture.repository.admit_startup_reattach(&grant.intent.command, 500).unwrap(),
            StartupReattachAdmission::Unresolved { operation_id } if operation_id == owner
        ));
        successful(
            &child(root, "restart_before_hold", "restart", after_effect, &owner)
                .output()
                .unwrap(),
        );
        assert_eq!(snapshot(&fixture.repository), before);
        assert_eq!(effect_count(root), usize::from(after_effect));

        fixture
            .repository
            .hold_startup_reattach(&grant, None, "process exited with unknown outcome", 550)
            .unwrap();
        assert_attempt(
            &fixture.repository,
            &grant,
            "manual_review",
            "uncertain",
            true,
        );
        let before = snapshot(&fixture.repository);
        successful(
            &child(root, "restart_after_hold", "restart", after_effect, &owner)
                .output()
                .unwrap(),
        );
        assert_eq!(snapshot(&fixture.repository), before);
        assert_eq!(effect_count(root), usize::from(after_effect));

        // A different attachment is independent; no uncertain target is cleared.
        let (session, agent) = crate::tests::workspace_agent::workspace_agent_fixture(
            &fixture.workspace,
            "startup_disjoint_session",
            "startup-disjoint-heron",
        );
        fixture
            .repository
            .create_workspace_agent(&session, &agent)
            .unwrap();
        let disjoint = command(
            &fixture.repository,
            "startup_disjoint",
            "instance_disjoint",
            &session.id,
        );
        let separate = execute(&fixture.repository, &disjoint, 700);
        assert_attempt(&fixture.repository, &separate, "requested", "started", true);
        assert_attempt(
            &fixture.repository,
            &grant,
            "manual_review",
            "uncertain",
            true,
        );
    }
}
