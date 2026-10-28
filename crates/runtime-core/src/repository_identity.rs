use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::RuntimeError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryIdentity {
    pub canonical_root: String,
    pub git_common_dir: String,
    pub fingerprint: String,
}

pub fn resolve_repository_identity(checkout: &Path) -> Result<RepositoryIdentity, RuntimeError> {
    let canonical_checkout = checkout.canonicalize().map_err(|error| {
        RuntimeError::InvalidState(format!(
            "failed to canonicalize repository checkout {}: {error}",
            checkout.display()
        ))
    })?;
    if !canonical_checkout.is_dir() {
        return Err(RuntimeError::InvalidState(format!(
            "repository checkout is not a directory: {}",
            canonical_checkout.display()
        )));
    }

    let common_dir = git_stdout(&canonical_checkout, &["rev-parse", "--git-common-dir"])?;
    let common_dir = PathBuf::from(common_dir);
    let common_dir = if common_dir.is_absolute() {
        common_dir
    } else {
        canonical_checkout.join(common_dir)
    }
    .canonicalize()
    .map_err(|error| {
        RuntimeError::InvalidState(format!(
            "failed to canonicalize Git common dir for {}: {error}",
            canonical_checkout.display()
        ))
    })?;

    let primary_root = primary_worktree_root(&canonical_checkout)?
        .unwrap_or_else(|| canonical_checkout.clone())
        .canonicalize()
        .map_err(|error| {
            RuntimeError::InvalidState(format!(
                "failed to canonicalize primary repository root for {}: {error}",
                canonical_checkout.display()
            ))
        })?;
    let canonical_root = primary_root.to_string_lossy().to_string();
    let git_common_dir = common_dir.to_string_lossy().to_string();
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in git_common_dir.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    Ok(RepositoryIdentity {
        canonical_root,
        git_common_dir,
        fingerprint: format!("repo_v2_{hash:016x}"),
    })
}

/// Inspect one Git worktree inventory rather than invoking `git rev-parse`
/// separately for every workspace that might share this repository. Includes
/// the primary checkout and linked checkouts, but never a bare checkout.
pub fn repository_checkout_roots(checkout: &Path) -> Result<Vec<PathBuf>, RuntimeError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(checkout)
        .args(["worktree", "list", "--porcelain", "-z"])
        .output()
        .map_err(|error| RuntimeError::Io(format!("failed to inspect Git worktrees: {error}")))?;
    if !output.status.success() {
        return Err(git_failure("git worktree list", output.stderr));
    }

    let mut roots = Vec::new();
    let mut current_path: Option<PathBuf> = None;
    let mut current_bare = false;
    for field in output.stdout.split(|byte| *byte == 0) {
        if field.is_empty() {
            continue;
        }
        let text = std::str::from_utf8(field).map_err(|error| {
            RuntimeError::InvalidState(format!("invalid Git worktree output: {error}"))
        })?;
        if let Some(path) = text.strip_prefix("worktree ") {
            if let Some(path) = current_path.take() {
                if !current_bare {
                    roots.push(path.canonicalize().unwrap_or(path));
                }
            }
            current_path = Some(PathBuf::from(path));
            current_bare = false;
        } else if text == "bare" {
            current_bare = true;
        }
    }
    if !current_bare {
        if let Some(path) = current_path {
            roots.push(path.canonicalize().unwrap_or(path));
        }
    }
    let mut seen = std::collections::HashSet::new();
    roots.retain(|path| seen.insert(path.clone()));
    // A bare Git repository has no checkout. Preserve the caller's prior
    // bare-repository fallback rather than inventing a worktree root.
    Ok(roots)
}

/// A nested independent Git repository is not part of its parent checkout,
/// even though its filesystem path has the parent's prefix. This check is
/// filesystem-only and cheap for ordinary nested workspace roots; native Git
/// still provides the authoritative checkout list above.
pub fn checkout_contains_workspace(checkout: &Path, workspace: &Path) -> bool {
    let mut cursor = workspace;
    while cursor != checkout {
        if !cursor.starts_with(checkout) || cursor.join(".git").exists() {
            return false;
        }
        let Some(parent) = cursor.parent() else {
            return false;
        };
        cursor = parent;
    }
    true
}

fn primary_worktree_root(checkout: &Path) -> Result<Option<PathBuf>, RuntimeError> {
    // Git lists the primary checkout first. All callers share this inventory
    // parser, so linked and nested workspace identity cannot drift.
    Ok(repository_checkout_roots(checkout)?.into_iter().next())
}

fn git_stdout(checkout: &Path, args: &[&str]) -> Result<String, RuntimeError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(checkout)
        .args(args)
        .output()
        .map_err(|error| {
            RuntimeError::Io(format!("failed to run git {}: {error}", args.join(" ")))
        })?;
    if !output.status.success() {
        return Err(git_failure(
            &format!("git {}", args.join(" ")),
            output.stderr,
        ));
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() {
        return Err(RuntimeError::InvalidState(format!(
            "git {} returned an empty value",
            args.join(" ")
        )));
    }
    Ok(value)
}

fn git_failure(command: &str, stderr: Vec<u8>) -> RuntimeError {
    let detail = String::from_utf8_lossy(&stderr).trim().to_string();
    if detail.is_empty() {
        RuntimeError::InvalidState(format!("{command} failed"))
    } else {
        RuntimeError::InvalidState(format!("{command} failed: {detail}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_shared_by_primary_and_linked_worktrees() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repo = temp.path().join("repo");
        let linked = temp.path().join("linked");
        std::fs::create_dir(&repo).expect("repo dir");
        run_git(&repo, &["init"]);
        run_git(&repo, &["config", "user.email", "runtime@example.invalid"]);
        run_git(&repo, &["config", "user.name", "Runtime Test"]);
        std::fs::write(repo.join("README.md"), "seed\n").expect("seed");
        run_git(&repo, &["add", "."]);
        run_git(&repo, &["commit", "-m", "seed"]);
        run_git(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "linked-test",
                linked.to_str().expect("linked path"),
            ],
        );

        let primary = resolve_repository_identity(&repo).expect("primary identity");
        let secondary = resolve_repository_identity(&linked).expect("linked identity");
        assert_eq!(primary, secondary);
        let inventory = repository_checkout_roots(&repo).expect("one Git checkout inventory");
        assert_eq!(inventory.len(), 2);
        assert_eq!(inventory[0], repo.canonicalize().unwrap());
        assert_eq!(inventory[1], linked.canonicalize().unwrap());
        let nested = repo.join("nested/workspace");
        std::fs::create_dir_all(&nested).unwrap();
        assert!(checkout_contains_workspace(&inventory[0], &nested));
        assert!(!checkout_contains_workspace(&inventory[1], &nested));
        let independent = repo.join("nested/separate");
        std::fs::create_dir_all(&independent).unwrap();
        run_git(&independent, &["init"]);
        let independent_project = independent.join("project");
        std::fs::create_dir_all(&independent_project).unwrap();
        assert!(!checkout_contains_workspace(
            &inventory[0],
            &independent_project
        ));
        assert_eq!(
            primary.canonical_root,
            repo.canonicalize()
                .expect("canonical repo")
                .to_string_lossy()
        );
        assert!(primary.fingerprint.starts_with("repo_v2_"));
    }

    fn run_git(cwd: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
