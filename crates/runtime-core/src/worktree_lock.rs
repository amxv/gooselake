//! Serialize native worktree mutations and provider route selection inside a
//! runtime process. SQLite claims remain the durable authority across restarts.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, OnceLock, Weak};

use tokio::sync::Mutex;

type RepoLock = Mutex<()>;

static WORKTREE_LOCKS: OnceLock<Mutex<HashMap<String, Weak<RepoLock>>>> = OnceLock::new();

/// Native Git mutation, route admission, and worktree claims must serialize
/// for the same canonical repository even when they enter through different
/// runtime services. Weak entries keep the registry bounded after shutdown.
pub async fn repository_worktree_lock(repository_root: &str) -> Arc<Mutex<()>> {
    let key = std::fs::canonicalize(Path::new(repository_root))
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| repository_root.to_string());
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
}
