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
