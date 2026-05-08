//! Relay class + instance registries.
//!
//! `RelayClassRegistry`: process-global; one entry per Relay class. Holds
//! the immutable `Arc<RelayTopology>` produced by `register_relay::<R>()`.
//! Looked up by controller-facing `factory_id` when `AddRelay` arrives.
//!
//! `RelayInstanceRegistry`: per-`ExecutorNode`; one entry per
//! `AddRelay` (instance_id). Reuses the DashMap pattern from the
//! pre-change `RelayRegistry` so concurrent reader/writer access is
//! sharded.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use lazy_static::lazy_static;

use crate::relay::instance::RelayInstance;
use crate::relay::topology::RelayTopology;

lazy_static! {
    static ref CLASS_REGISTRY: RwLock<HashMap<&'static str, Arc<RelayTopology>>> =
        RwLock::new(HashMap::new());
}

pub fn register_class(factory_id: &'static str, topology: Arc<RelayTopology>) {
    CLASS_REGISTRY.write().unwrap().insert(factory_id, topology);
}

pub fn get_class(factory_id: &str) -> Option<Arc<RelayTopology>> {
    CLASS_REGISTRY.read().unwrap().get(factory_id).cloned()
}

pub fn class_factory_ids() -> Vec<&'static str> {
    CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}

pub struct RelayInstanceRegistry {
    inner: dashmap::DashMap<String, Arc<RelayInstance>>,
}

impl RelayInstanceRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register(&self, instance: Arc<RelayInstance>) -> Arc<RelayInstance> {
        let id = instance.instance_id.to_string();
        self.inner.insert(id, Arc::clone(&instance));
        instance
    }

    pub fn get(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.inner.remove(instance_id).map(|(_, v)| v)
    }

    pub fn contains(&self, instance_id: &str) -> bool {
        self.inner.contains_key(instance_id)
    }

    pub fn snapshot(&self) -> HashMap<String, Arc<RelayInstance>> {
        self.inner
            .iter()
            .map(|r| (r.key().clone(), Arc::clone(r.value())))
            .collect()
    }

    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for RelayInstanceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Public-API view that mirrors today's `Flezha::get_relays()` shape.
/// Returned as `Arc<RwLock<HashMap<String, Arc<Mutex<…>>>>>` so external
/// consumers (test apps, observability wrappers) can iterate without
/// touching DashMap internals. The inner Mutex is a placeholder kept for
/// API compatibility — `RelayInstance` is itself shared via Arc and does
/// not require a Mutex on the read path.
// reason: return shape mirrors Flezha::get_relays() public API; splitting would break external inspectors
#[allow(clippy::type_complexity)]
pub fn snapshot_as_relay_view(
    registry: &RelayInstanceRegistry,
) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>> {
    let snapshot = registry.snapshot();
    let mapped: HashMap<String, Arc<Mutex<Arc<RelayInstance>>>> = snapshot
        .into_iter()
        .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
        .collect();
    Arc::new(RwLock::new(mapped))
}
