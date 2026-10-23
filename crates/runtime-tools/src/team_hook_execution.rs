//! Shell hook executor behind operator approval and immutable event receipts.
//! No HTTP/model path may invoke this with a fabricated event or command.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use runtime_core::team_hooks::validate_team_hook_script_output;
use runtime_core::{
    RuntimeError, RuntimeStore, RuntimeTeamHookExecutor, TeamHookEffectAdmission,
    TeamHookEffectPhase, TeamHookEffectReceipt, TeamHookEffectRecord,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::now_ms;

pub(crate) const MAX_SCRIPT_STDOUT: usize = 16 * 1024;
const MAX_SCRIPT_STDERR: usize = 4 * 1024;
pub(crate) const MAX_DURABLE_RECEIPT_BYTES: usize = 16 * 1024;

enum ScriptResult {
    Success(Vec<u8>),
    Failed(&'static str),
    TimedOut,
}

pub struct NativeTeamHookExecutor {
    store: Arc<dyn RuntimeStore>,
}

impl NativeTeamHookExecutor {
    pub fn new(store: Arc<dyn RuntimeStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl RuntimeTeamHookExecutor for NativeTeamHookExecutor {
    async fn execute(
        &self,
        admission: &TeamHookEffectAdmission,
    ) -> Result<TeamHookEffectRecord, RuntimeError> {
        execute_team_hook_effect(self.store.as_ref(), admission).await
    }
}

/// Admit the exact approved command against a real persisted parent event,
/// claim its external effect before spawning, and persist the sanitized receipt.
/// Already-started or terminal effects are returned without re-running shell.
/// Applying output is deliberately a *separate* scoped and durable operation.
pub async fn execute_team_hook_effect(
    store: &dyn RuntimeStore,
    admission: &TeamHookEffectAdmission,
) -> Result<TeamHookEffectRecord, RuntimeError> {
    let workspace = store
        .get_workspace(&admission.workspace_id)?
        .ok_or_else(|| RuntimeError::NotFound(format!("workspace {}", admission.workspace_id)))?;
    let root = Path::new(&workspace.canonical_root);
    if !root.is_absolute() || !root.is_dir() {
        return Err(RuntimeError::Configuration(
            "hook workspace root is missing or not absolute".into(),
        ));
    }
    let durable = store.admit_team_hook_effect(admission)?;
    if durable.phase != TeamHookEffectPhase::Intended {
        return Ok(durable);
    }
    let claimed = match store.claim_team_hook_effect(
        &admission.workspace_id,
        &admission.parent_event_id,
        admission.hook.event_name,
        &admission.hook.name,
        now_ms(),
    ) {
        Ok(claimed) => claimed,
        Err(error @ RuntimeError::Conflict(_)) => {
            let observed = store
                .get_team_hook_effect(
                    &admission.workspace_id,
                    &admission.parent_event_id,
                    admission.hook.event_name,
                    &admission.hook.name,
                )?
                .ok_or_else(|| RuntimeError::InvalidState("hook claim disappeared".into()))?;
            // A competing dispatcher may have won the claim. Revocation or
            // retirement leaves it intended, and must remain a hard denial.
            if observed.phase != TeamHookEffectPhase::Intended {
                return Ok(observed);
            }
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let input = serde_json::to_vec(&claimed.input)
        .map_err(|_| RuntimeError::InvalidState("hook input cannot serialize".into()))?;
    let result =
        run_approved_script(&claimed.hook.command, &input, root, claimed.hook.timeout_ms).await;
    let (phase, receipt) = match result {
        ScriptResult::Success(stdout) => {
            match validate_team_hook_script_output(claimed.hook.event_name, &stdout) {
                Ok(output) => {
                    let receipt = TeamHookEffectReceipt {
                        validated_output: Some(serde_json::to_value(output).map_err(|_| {
                            RuntimeError::Bootstrap("validated hook output cannot serialize".into())
                        })?),
                        error_code: None,
                    };
                    // The persisted receipt has a stricter size bound than
                    // script stdout: JSON re-encoding and envelope overhead
                    // can make a valid 16 KiB output impossible to commit.
                    // Never leave an already-run effect stuck in `started`.
                    if serde_json::to_vec(&receipt)
                        .map_err(|_| {
                            RuntimeError::Bootstrap("hook receipt cannot serialize".into())
                        })?
                        .len()
                        > MAX_DURABLE_RECEIPT_BYTES
                    {
                        (
                            TeamHookEffectPhase::Failed,
                            TeamHookEffectReceipt {
                                validated_output: None,
                                error_code: Some("output_too_large".into()),
                            },
                        )
                    } else {
                        (TeamHookEffectPhase::Succeeded, receipt)
                    }
                }
                Err(_) => (
                    TeamHookEffectPhase::Failed,
                    TeamHookEffectReceipt {
                        validated_output: None,
                        error_code: Some("invalid_output".into()),
                    },
                ),
            }
        }
        ScriptResult::Failed(code) => (
            TeamHookEffectPhase::Failed,
            TeamHookEffectReceipt {
                validated_output: None,
                error_code: Some(code.into()),
            },
        ),
        ScriptResult::TimedOut => (
            TeamHookEffectPhase::TimedOut,
            TeamHookEffectReceipt {
                validated_output: None,
                error_code: Some("timeout".into()),
            },
        ),
    };
    store.finish_team_hook_effect(
        &claimed.workspace_id,
        &claimed.parent_event_id,
        claimed.hook.event_name,
        &claimed.hook.name,
        phase,
        receipt,
        now_ms(),
    )
}

/// Settle immutable post-event intents and apply validated context notices.
/// Failed script effects never get a second native dispatch. The SQLite
/// application transaction provides the separate exactly-once notice receipt.
pub async fn dispatch_team_hook_effects(store: &dyn RuntimeStore) -> Result<usize, RuntimeError> {
    let mut settled = 0;
    for effect in store.list_dispatchable_team_hook_effects(64)? {
        let admission = TeamHookEffectAdmission {
            workspace_id: effect.workspace_id,
            parent_event_id: effect.parent_event_id,
            hook: effect.hook,
            input: effect.input,
            created_at_ms: effect.created_at_ms,
        };
        match execute_team_hook_effect(store, &admission).await {
            Ok(_) => settled += 1,
            // An approval or workspace can be revoked between admission and
            // claim. Do not run the command through an alternate path.
            Err(RuntimeError::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
    }
    for effect in store.list_unapplied_team_hook_effects(64)? {
        store.apply_team_hook_effect_output(&effect, now_ms())?;
        settled += 1;
    }
    Ok(settled)
}

/// A weak store reference allows a disposable runtime to shut down naturally
/// without an immortal background task holding its database open.
pub fn start_team_hook_worker(store: &Arc<dyn RuntimeStore>) {
    let weak = Arc::downgrade(store);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        loop {
            ticker.tick().await;
            let Some(store) = weak.upgrade() else { break };
            if let Err(error) = dispatch_team_hook_effects(store.as_ref()).await {
                // The script runner never returns raw script stdout/stderr.
                // Surface storage/dispatch faults instead of silently making
                // an approved hook appear to do nothing forever.
                eprintln!("team-hook worker failed to reconcile effects: {error}");
            }
        }
    });
}

async fn run_approved_script(
    command: &str,
    input: &[u8],
    cwd: &Path,
    timeout_ms: u64,
) -> ScriptResult {
    #[cfg(unix)]
    let mut process = Command::new("sh");
    #[cfg(windows)]
    let mut process = Command::new("cmd");
    #[cfg(unix)]
    process.arg("-lc");
    #[cfg(windows)]
    process.arg("/C");
    process
        .arg(command)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_clear()
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        process.as_std_mut().process_group(0);
        process.env("PATH", "/usr/bin:/bin");
        if let Some(home) = std::env::var_os("HOME") {
            process.env("HOME", home);
        }
    }
    #[cfg(windows)]
    {
        // cmd.exe needs SystemRoot and the operator's command lookup path.
        // Never forward an arbitrary model/provider environment to scripts.
        for key in ["SystemRoot", "PATH", "USERPROFILE"] {
            if let Some(value) = std::env::var_os(key) {
                process.env(key, value);
            }
        }
    }
    let Ok(mut child) = process.spawn() else {
        return ScriptResult::Failed("spawn_failed");
    };
    #[cfg(unix)]
    let process_group = child.id().and_then(|pid| i32::try_from(pid).ok());
    let Some(mut stdin) = child.stdin.take() else {
        return ScriptResult::Failed("stdin_unavailable");
    };
    let Some(stdout) = child.stdout.take() else {
        return ScriptResult::Failed("stdout_unavailable");
    };
    let Some(stderr) = child.stderr.take() else {
        return ScriptResult::Failed("stderr_unavailable");
    };
    let execution = async {
        stdin.write_all(input).await?;
        stdin.shutdown().await?;
        drop(stdin);
        let mut stdout_buffer = Vec::new();
        let mut stderr_buffer = Vec::new();
        let stdout_reader = async {
            stdout
                .take((MAX_SCRIPT_STDOUT + 1) as u64)
                .read_to_end(&mut stdout_buffer)
                .await
        };
        let stderr_reader = async {
            stderr
                .take((MAX_SCRIPT_STDERR + 1) as u64)
                .read_to_end(&mut stderr_buffer)
                .await
        };
        let (status, _, _) = tokio::try_join!(child.wait(), stdout_reader, stderr_reader)?;
        Ok::<_, std::io::Error>((status.success(), stdout_buffer, stderr_buffer))
    };
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), execution).await;
    match result {
        Err(_) => {
            // The shell owns a fresh process group on Unix. Reap children too
            // rather than allowing a timed-out background grandchild to live.
            #[cfg(unix)]
            if let Some(pid) = process_group {
                unsafe { libc::kill(-pid, libc::SIGKILL) };
            }
            let _ = child.kill().await;
            ScriptResult::TimedOut
        }
        Ok(Err(_)) => ScriptResult::Failed("io_failed"),
        Ok(Ok((success, stdout, stderr))) => {
            if !success {
                ScriptResult::Failed("exit_failure")
            } else if stdout.len() > MAX_SCRIPT_STDOUT || stderr.len() > MAX_SCRIPT_STDERR {
                ScriptResult::Failed("output_too_large")
            } else {
                ScriptResult::Success(stdout)
            }
        }
    }
}
