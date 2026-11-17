use super::worktree_spawn_recovery::fixture;
use super::*;
use runtime_core::WorktreeService;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn shared_legacy_spawn_refuses_fresh_keys_before_provider_git_hooks_or_admission() {
    let fixture = fixture().await;
    let repo = fixture._temp.path();
    for args in [
        vec!["init", "--quiet"],
        vec![
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "initial",
        ],
    ] {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let inventory = || {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["worktree", "list", "--porcelain"])
            .output()
            .unwrap();
        assert!(output.status.success());
        output.stdout
    };
    let marker = repo.join("historical-hook-must-not-run");
    let script = repo.join(".agents/gg/worktree-init.sh");
    std::fs::create_dir_all(script.parent().unwrap()).unwrap();
    std::fs::write(
        &script,
        format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    for args in [
        vec!["add", "--", ".agents/gg/worktree-init.sh"],
        vec![
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "executable disposable hook",
        ],
    ] {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[cfg(unix)]
    {
        let tree = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["ls-tree", "HEAD", ".agents/gg/worktree-init.sh"])
            .output()
            .unwrap();
        assert!(tree.status.success());
        assert!(
            String::from_utf8_lossy(&tree.stdout).starts_with("100755 "),
            "hook must exist executable in the prospective checkout"
        );
    }
    let before = fixture.store.hydrate_runtime_state().unwrap();
    let git_before = inventory();
    let native_before = fixture.provider.effect_calls.load(Ordering::SeqCst);
    for keyed in [false, true] {
        for route in [false, true] {
            let mut request = fixture.request();
            request.metadata = None;
            request.idempotency_key = keyed.then(|| format!("fresh-refused-{route}"));
            if route {
                request.worktree = Some(runtime_core::TeamMemberSpawnWorktreeInput {
                    mode: Some("create".into()),
                    name: Some("must-not-be-created".into()),
                    branch_prefix: None,
                    base_ref: None,
                    run_init_script: Some(true),
                });
            }
            let error = fixture
                .service
                .spawn_team_member(request)
                .await
                .unwrap_err();
            assert!(matches!(error, RuntimeError::Unsupported(_)), "{error:?}");
            assert_eq!(fixture.store.hydrate_runtime_state().unwrap(), before);
            assert_eq!(
                fixture.provider.effect_calls.load(Ordering::SeqCst),
                native_before
            );
            assert_eq!(inventory(), git_before);
            assert!(!marker.exists());
        }
    }
}
