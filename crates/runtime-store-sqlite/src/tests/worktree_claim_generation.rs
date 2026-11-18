//! Same-value lifetime oracle through supported writers in a separate process.
use super::*;
use runtime_core::ManagedWorktreeClaimTransition::{Claim, Release};
use runtime_core::RuntimeError;

const CHILD_DB: &str = "GOOSELAKE_N06_SAME_VALUE_ABA_DB";
const CHILD_TERMINAL: &str = "GOOSELAKE_N06_SAME_VALUE_ABA_TERMINAL";

#[test]
fn same_value_aba_child() {
    let Some(path) = std::env::var_os(CHILD_DB) else {
        return;
    };
    let repository = SqliteRuntimeRepository::new(std::path::PathBuf::from(path));
    let old = repository
        .get_managed_worktree_claim("wt_claim_event", "session_1")
        .unwrap()
        .unwrap();
    assert_eq!(old.created_at, 100);
    let terminal = std::env::var_os(CHILD_TERMINAL).is_some();
    assert_eq!(old.released_at.is_some(), terminal);
    if !terminal {
        repository
            .transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(150),
                    ..old.clone()
                },
                Release,
            )
            .unwrap();
    }
    // Reuse every raw identity/role/time value. Only a durable lifetime token
    // can distinguish this supported new Claim from the parent's observation.
    let newer = repository
        .transition_managed_worktree_claim(
            &ManagedWorktreeClaimRecord {
                released_at: None,
                ..old
            },
            Claim,
        )
        .unwrap();
    if terminal {
        repository
            .transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(150),
                    ..newer
                },
                Release,
            )
            .unwrap();
    }
}

fn stale_same_value_release(explicit: bool, terminal: bool) {
    let (temp, repository, proposal) = super::worktree_claim_events::fixture();
    let mut observed = repository
        .transition_managed_worktree_claim(&proposal, Claim)
        .unwrap();
    if terminal {
        observed = repository
            .transition_managed_worktree_claim(
                &ManagedWorktreeClaimRecord {
                    released_at: Some(150),
                    ..observed
                },
                Release,
            )
            .unwrap();
    }
    let cwd = repository
        .get_managed_worktree(&observed.worktree_id)
        .unwrap()
        .unwrap()
        .worktree_cwd;
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "tests::worktree_claim_generation::same_value_aba_child",
            "--nocapture",
        ])
        .env(CHILD_DB, temp.path().join("runtime.sqlite3"));
    if terminal {
        child.env(CHILD_TERMINAL, "1");
    } else {
        child.env_remove(CHILD_TERMINAL);
    }
    let output = child.output().expect("independent fixture writer");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    let restarted = SqliteRuntimeRepository::new(temp.path().join("runtime.sqlite3"));
    let newer = restarted
        .get_managed_worktree_claim(&observed.worktree_id, &observed.session_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            &newer.worktree_id,
            &newer.session_id,
            &newer.claim_role,
            newer.created_at,
            newer.released_at
        ),
        (
            &observed.worktree_id,
            &observed.session_id,
            &observed.claim_role,
            observed.created_at,
            observed.released_at
        )
    );
    let before = restarted.list_runtime_events(None, None, 20).unwrap();
    assert_eq!(before.len(), if terminal { 4 } else { 3 });
    let stale = ManagedWorktreeClaimRecord {
        released_at: Some(300),
        ..observed.clone()
    };
    let result = if explicit {
        restarted.release_managed_worktree_claim_if_unbound(&stale, &observed, &cwd)
    } else {
        restarted.transition_managed_worktree_claim(&stale, Release)
    };
    assert!(
        matches!(result, Err(RuntimeError::Conflict(_))),
        "stale same-value lifetime must be rejected before mutation or terminal replay: {result:?}"
    );
    assert_eq!(
        restarted
            .get_managed_worktree_claim(&newer.worktree_id, &newer.session_id)
            .unwrap(),
        Some(newer)
    );
    assert_eq!(
        restarted.list_runtime_events(None, None, 20).unwrap(),
        before
    );
}

#[test]
fn same_value_aba_generic_active_lifetime_is_fenced() {
    stale_same_value_release(false, false);
}
#[test]
fn same_value_aba_explicit_active_lifetime_is_fenced() {
    stale_same_value_release(true, false);
}
#[test]
fn same_value_aba_generic_terminal_lifetime_is_fenced() {
    stale_same_value_release(false, true);
}
#[test]
fn same_value_aba_explicit_terminal_lifetime_is_fenced() {
    stale_same_value_release(true, true);
}
