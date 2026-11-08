use std::collections::HashMap;
use std::sync::Arc;

use crate::{ProviderKind, ProviderMetadata, RuntimeError, RuntimeProvider};

#[derive(Default)]
pub struct ProviderRegistry {
    providers: HashMap<ProviderKind, Arc<dyn RuntimeProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, provider: Arc<dyn RuntimeProvider>) -> Result<(), RuntimeError> {
        let kind = provider.kind();
        if self.providers.contains_key(&kind) {
            return Err(RuntimeError::ProviderAlreadyRegistered(
                kind.as_str().to_string(),
            ));
        }
        self.providers.insert(kind, provider);
        Ok(())
    }

    pub fn get(&self, kind: ProviderKind) -> Option<Arc<dyn RuntimeProvider>> {
        self.providers.get(&kind).cloned()
    }

    /// Registration and enablement are distinct: an adapter can remain
    /// addressable for diagnostics while being ineligible for new native
    /// work. Do not dispatch a newly-created session to a disabled adapter.
    pub fn get_enabled(&self, kind: ProviderKind) -> Option<Arc<dyn RuntimeProvider>> {
        self.get(kind).filter(|adapter| adapter.metadata().enabled)
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    pub fn metadata(&self) -> Vec<ProviderMetadata> {
        let mut items = self
            .providers
            .values()
            .map(|provider| provider.metadata())
            .collect::<Vec<_>>();
        items.sort_by(|left, right| left.display_name.cmp(&right.display_name));
        items
    }
}
