//! An in-process wakeup for *committed* durable event rows. The SQLite journal,
//! not this notification channel, is the source of event payloads or ordering.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::broadcast;

#[derive(Debug)]
pub(crate) struct EventCommitNotifier {
    sender: broadcast::Sender<()>,
}

impl EventCommitNotifier {
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    pub(crate) fn wake(&self) {
        // No active subscribers is a normal state, not a persistence failure.
        let _ = self.sender.send(());
    }
}

fn database_identity(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    if let Some(parent) = path.parent() {
        if let Ok(canonical_parent) = parent.canonicalize() {
            if let Some(name) = path.file_name() {
                return canonical_parent.join(name);
            }
        }
    }
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

/// Independent SQLite store instances for one runtime database share a
/// notification channel. Weak registry entries do not retain old test/runtime
/// databases or their subscribers after the last store closes.
pub(crate) fn notifier_for(path: &Path) -> Arc<EventCommitNotifier> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<EventCommitNotifier>>>> = OnceLock::new();
    let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry.lock().expect("event notifier registry poisoned");
    registry.retain(|_, notifier| notifier.strong_count() > 0);
    let identity = database_identity(path);
    if let Some(notifier) = registry.get(&identity).and_then(Weak::upgrade) {
        return notifier;
    }
    let (sender, _) = broadcast::channel(256);
    let notifier = Arc::new(EventCommitNotifier { sender });
    registry.insert(identity, Arc::downgrade(&notifier));
    notifier
}
