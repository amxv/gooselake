use std::path::Path;

use runtime_core::{ManagedProcessRecord, ProcessSummary, RuntimeError};
use serde_json::{json, Value};
use tokio::process::Command;

use crate::process::{COMPLETION_CORRELATION_PREFIX, COMPLETION_PREVIEW_BYTES_PER_STREAM};

pub(crate) fn summary_from_record(record: &ManagedProcessRecord) -> ProcessSummary {
    ProcessSummary {
        process_id: record.process_id.clone(),
        session_id: record.owner_session_id.clone(),
        pid: record.pid,
        status: record.status.clone(),
        command: record.command.clone(),
        cwd: record.cwd.clone(),
        started_at: record.execution_started_at.unwrap_or(record.admitted_at),
        ended_at: record.ended_at,
    }
}

pub(crate) fn command_text(command: &Value) -> Result<String, RuntimeError> {
    command
        .get("command")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| RuntimeError::InvalidState("process command payload is invalid".to_string()))
}

pub(crate) fn completion_correlation(process_id: &str) -> String {
    format!("{COMPLETION_CORRELATION_PREFIX}{process_id}")
}

pub(crate) fn build_completion_input(record: &ManagedProcessRecord) -> Vec<Value> {
    let stdout_preview =
        bounded_file_tail(&record.stdout_path, COMPLETION_PREVIEW_BYTES_PER_STREAM);
    let stderr_preview =
        bounded_file_tail(&record.stderr_path, COMPLETION_PREVIEW_BYTES_PER_STREAM);
    let payload = json!({
        "kind": "managed_process_completion",
        "process_id": record.process_id,
        "status": record.status,
        "command": record.command,
        "cwd": record.cwd,
        "pid": record.pid,
        "reason": record.terminal_reason,
        "exit_code": record.exit_code,
        "signal": record.signal,
        "timeout_ms": record.timeout_ms,
        "execution_duration_ms": record.execution_duration_ms,
        "stdout": {
            "captured_bytes": record.stdout_captured_bytes,
            "truncated": record.stdout_truncated,
            "log_path": record.stdout_path,
            "preview": stdout_preview,
        },
        "stderr": {
            "captured_bytes": record.stderr_captured_bytes,
            "truncated": record.stderr_truncated,
            "log_path": record.stderr_path,
            "preview": stderr_preview,
        }
    });
    vec![json!({
        "type": "text",
        "text": format!(
            "<managed_process_completion>
{}
</managed_process_completion>",
            serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string())
        )
    })]
}

fn bounded_file_tail(path: &str, max_bytes: usize) -> String {
    let bytes = std::fs::read(path).unwrap_or_default();
    let start = bytes.len().saturating_sub(max_bytes);
    String::from_utf8_lossy(&bytes[start..]).to_string()
}

pub(crate) fn file_len(path: &str) -> usize {
    std::fs::metadata(path)
        .ok()
        .and_then(|metadata| usize::try_from(metadata.len()).ok())
        .unwrap_or(0)
}

pub(crate) fn create_exclusive_log_bundle(
    stdout: &Path,
    stderr: &Path,
) -> Result<bool, RuntimeError> {
    use std::fs::OpenOptions;

    match OpenOptions::new().write(true).create_new(true).open(stdout) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
        Err(error) => {
            return Err(RuntimeError::Io(format!(
                "failed to create process stdout log {}: {error}",
                stdout.display()
            )))
        }
    }
    match OpenOptions::new().write(true).create_new(true).open(stderr) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = std::fs::remove_file(stdout);
            Ok(false)
        }
        Err(error) => {
            let _ = std::fs::remove_file(stdout);
            Err(RuntimeError::Io(format!(
                "failed to create process stderr log {}: {error}",
                stderr.display()
            )))
        }
    }
}

pub(crate) fn remove_log_bundle(stdout: &Path, stderr: &Path) {
    let _ = std::fs::remove_file(stdout);
    let _ = std::fs::remove_file(stderr);
}

pub(crate) fn build_process_command(
    command: &str,
    cwd: &str,
    launch_gate: &Path,
    allow_shell: bool,
) -> Result<Command, RuntimeError> {
    #[cfg(target_os = "windows")]
    {
        let mut process = if allow_shell {
            let mut process = Command::new("cmd");
            process.arg("/C").arg(command);
            process
        } else {
            let mut split = command.split_whitespace();
            let executable = split
                .next()
                .ok_or_else(|| RuntimeError::InvalidState("command cannot be empty".to_string()))?;
            let mut process = Command::new(executable);
            process.args(split);
            process
        };
        process.current_dir(cwd);
        process.stdin(std::process::Stdio::null());
        process.stdout(std::process::Stdio::piped());
        process.stderr(std::process::Stdio::piped());
        process.kill_on_drop(true);
        return Ok(process);
    }

    #[cfg(not(target_os = "windows"))]
    {
        const WATCHDOG_WRAPPER: &str = r#"
parent_pid="$GG_PROCESS_PARENT_PID"
process_group="$$"
launch_gate="$GG_PROCESS_LAUNCH_GATE"
while [ ! -e "$launch_gate" ]; do
  kill -0 "$parent_pid" 2>/dev/null || exit 125
  sleep 0.02
done
rm -f "$launch_gate"
(
  while kill -0 "$parent_pid" 2>/dev/null; do sleep 1; done
  kill -KILL -- "-$process_group" 2>/dev/null || true
) &
watchdog_pid=$!
cleanup_watchdog() {
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
}
trap cleanup_watchdog EXIT
if [ "$GG_PROCESS_USE_SHELL" = "1" ]; then
  sh -lc "$1"
else
  shift
  "$@"
fi
"#;
        let mut process = Command::new("sh");
        process
            .arg("-c")
            .arg(WATCHDOG_WRAPPER)
            .arg("gg-managed-process")
            .env("GG_PROCESS_PARENT_PID", std::process::id().to_string())
            .env("GG_PROCESS_LAUNCH_GATE", launch_gate)
            .env("GG_PROCESS_USE_SHELL", if allow_shell { "1" } else { "0" });

        if allow_shell {
            process.arg(command);
        } else {
            process.arg("unused");
            let mut split = command.split_whitespace();
            let executable = split
                .next()
                .ok_or_else(|| RuntimeError::InvalidState("command cannot be empty".to_string()))?;
            process.arg(executable).args(split);
        }

        process.process_group(0);
        process.current_dir(cwd);
        process.stdin(std::process::Stdio::null());
        process.stdout(std::process::Stdio::piped());
        process.stderr(std::process::Stdio::piped());
        process.kill_on_drop(true);
        Ok(process)
    }
}

pub(crate) fn tail_utf8_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let bytes = value.as_bytes();
    String::from_utf8_lossy(&bytes[bytes.len() - max_bytes..]).to_string()
}
