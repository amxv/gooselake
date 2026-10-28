//! Serialize native worktree mutations and provider route selection inside a
//! runtime process. SQLite claims remain the durable authority across restarts.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock, Weak};

use tokio::sync::{Mutex, RwLock};

type RepoLock = Mutex<()>;

static WORKTREE_LOCKS: OnceLock<Mutex<HashMap<String, Weak<RepoLock>>>> = OnceLock::new();

type LifecycleLock = RwLock<()>;
static LIFECYCLE_LOCKS: OnceLock<Mutex<HashMap<String, Weak<LifecycleLock>>>> = OnceLock::new();

fn canonical_key(repository_root: &str) -> String {
    // A workspace may be registered inside a repository or in a linked
    // checkout. Native Git effects and provider admission must use the same
    // lifecycle/worktree lock as retirement of any of those workspace roots.
    // The primary Git root is stable across linked checkouts; filesystem-only
    // canonicalization would allocate competing locks for the same repo.
    if let Ok(identity) = crate::resolve_repository_identity(Path::new(repository_root)) {
        return identity.canonical_root;
    }
    std::fs::canonicalize(Path::new(repository_root))
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| repository_root.to_string())
}

/// Creation obtains a shared guard before any native/provider work, allowing
/// parallel agent admission. Retirement takes an exclusive guard to prove a
/// quiescent snapshot, with the same canonical-root key across services.
pub async fn repository_lifecycle_lock(repository_root: &str) -> Arc<RwLock<()>> {
    let key = canonical_key(repository_root);
    let registry = LIFECYCLE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = registry.lock().await;
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(RwLock::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

/// Native Git mutation, route admission, and worktree claims must serialize
/// for the same canonical repository even when they enter through different
/// runtime services. Weak entries keep the registry bounded after shutdown.
pub async fn repository_worktree_lock(repository_root: &str) -> Arc<Mutex<()>> {
    let key = canonical_key(repository_root);
    let registry = WORKTREE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = registry.lock().await;
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nested_and_linked_git_checkouts_share_primary_repository_locks() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("repo");
        let nested = root.join("nested/workspace");
        let linked = temp.path().join("linked");
        std::fs::create_dir_all(&nested).unwrap();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(&root)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "--quiet"]);
        git(&[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "--quiet",
            "-m",
            "seed",
        ]);
        git(&[
            "worktree",
            "add",
            "--quiet",
            "-b",
            "linked-branch",
            linked.to_str().unwrap(),
        ]);

        let primary = repository_lifecycle_lock(root.to_str().unwrap()).await;
        let nested_lock = repository_lifecycle_lock(nested.to_str().unwrap()).await;
        let linked_lock = repository_lifecycle_lock(linked.to_str().unwrap()).await;
        assert!(Arc::ptr_eq(&primary, &nested_lock));
        assert!(Arc::ptr_eq(&primary, &linked_lock));
        let reader = nested_lock.read().await;
        assert!(primary.try_write().is_err());
        drop(reader);
        assert!(primary.try_write().is_ok());

        let primary = repository_worktree_lock(root.to_str().unwrap()).await;
        let nested_lock = repository_worktree_lock(nested.to_str().unwrap()).await;
        let linked_lock = repository_worktree_lock(linked.to_str().unwrap()).await;
        assert!(Arc::ptr_eq(&primary, &nested_lock));
        assert!(Arc::ptr_eq(&primary, &linked_lock));
    }

    #[tokio::test]
    async fn independent_services_share_a_canonical_repository_lock() {
        let root = std::env::current_dir().unwrap();
        let path = root.to_str().unwrap();
        let a = repository_worktree_lock(path).await;
        let b = repository_worktree_lock(path).await;
        assert!(Arc::ptr_eq(&a, &b));
        let guard = a.lock().await;
        assert!(b.try_lock().is_err());
        drop(guard);
        assert!(b.try_lock().is_ok());
    }

    #[tokio::test]
    async fn multiple_creators_can_proceed_but_retirement_waits_for_all_of_them() {
        let root = std::env::current_dir().unwrap();
        let a = repository_lifecycle_lock(root.to_str().unwrap()).await;
        let b = repository_lifecycle_lock(root.to_str().unwrap()).await;
        assert!(Arc::ptr_eq(&a, &b));
        let first = a.read().await;
        let second = b.read().await;
        assert!(b.try_write().is_err());
        drop(first);
        assert!(a.try_write().is_err());
        drop(second);
        assert!(b.try_write().is_ok());
    }
}
