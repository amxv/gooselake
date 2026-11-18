//! Exercise the shipped read-only observer against the actual runtime backup.
use super::image::{assert_no_sidecars, sha256};
use serde_json::Value;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output},
};

pub(super) struct Witness {
    pub path: PathBuf,
    pub sha256: String,
    pub document: Value,
}

fn command() -> Command {
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/audit-snapshot-manifest.py");
    let mut command = Command::new("/usr/bin/python3");
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .arg(script);
    command
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "manifest CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) fn capture(snapshot: &Path, destination: &Path) -> Witness {
    assert_no_sidecars(snapshot);
    let before = fs::read(snapshot).unwrap();
    let output = command()
        .args(["capture", "--snapshot"])
        .arg(snapshot)
        .output()
        .unwrap();
    assert_success(&output);
    let sha256 = sha256(&output.stdout);
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        format!("retain independently: manifest_sha256={sha256}\n")
    );
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        document["snapshot"]["sha256"],
        super::image::sha256(&before)
    );
    assert_eq!(document["inspection_only"], true);
    for field in [
        "cutover_authorized",
        "restore_authorized",
        "native_retry_authorized",
    ] {
        assert_eq!(document[field], false);
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .unwrap();
    file.write_all(&output.stdout).unwrap();
    file.sync_all().unwrap();
    // A separately retained byte hash is an input to verification, never a
    // signature or proof of acquisition from a live source.
    let mut pin = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination.with_extension("sha256"))
        .unwrap();
    writeln!(pin, "{sha256}").unwrap();
    pin.sync_all().unwrap();
    assert_eq!(fs::read(snapshot).unwrap(), before);
    assert_no_sidecars(snapshot);
    Witness {
        path: destination.to_path_buf(),
        sha256,
        document,
    }
}

pub(super) fn verify(snapshot: &Path, witness: &Witness, matches: bool) {
    assert_no_sidecars(snapshot);
    let before = fs::read(snapshot).unwrap();
    let manifest_before = fs::read(&witness.path).unwrap();
    let output = command()
        .args(["verify", "--snapshot"])
        .arg(snapshot)
        .arg("--manifest")
        .arg(&witness.path)
        .arg("--expected-manifest-sha256")
        .arg(&witness.sha256)
        .output()
        .unwrap();
    if matches {
        assert_success(&output);
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["manifest_match"], true);
        assert_eq!(report["retained_history_consistent"], true);
        for field in [
            "cutover_authorized",
            "restore_authorized",
            "native_retry_authorized",
        ] {
            assert_eq!(report[field], false);
        }
    } else {
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("differ from retained manifest"));
    }
    assert_eq!(fs::read(snapshot).unwrap(), before);
    assert_eq!(fs::read(&witness.path).unwrap(), manifest_before);
    assert_no_sidecars(snapshot);
}
