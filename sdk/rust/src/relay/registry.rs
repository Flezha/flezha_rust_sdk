//! Relay class + instance registries.
//!
//! `RelayClassRegistry`: process-global; one entry per Relay class. Holds
//! the immutable `Arc<RelayTopology>` produced by `register_relay::<R>()`.
//! Looked up by controller-facing `factory_id` when `AddRelay` arrives.
//!
//! `RelayInstanceRegistry`: per-`ExecutorNode`; one entry per
//! `AddRelay` (instance_id). Reuses the DashMap pattern from the
//! pre-change `RelayRegistry` so concurrent reader/writer access is
//! sharded. Values are a sealed [`UnitInstance`] — relay or task — so the
//! dispatch boundary resolves the variant once per frame; the `Arc` keeps
//! map values pointer-sized.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use lazy_static::lazy_static;

use crate::relay::instance::RelayInstance;
use crate::relay::topology::RelayTopology;

/// A control-plane-managed unit instance. Sealed two-variant enum: a new kind
/// must be handled at every match, and the 2-variant match keeps the relay hot
/// path branch-predictable rather than devirtualizing a single variant. A task
/// carries only its instance id — it has no per-instance runtime state.
pub enum UnitInstance {
    Relay(Arc<RelayInstance>),
    Task(Arc<str>),
}

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
    inner: dashmap::DashMap<String, Arc<UnitInstance>>,
}

impl RelayInstanceRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register_relay(&self, instance: Arc<RelayInstance>) -> Arc<RelayInstance> {
        let id = instance.instance_id.to_string();
        self.inner
            .insert(id, Arc::new(UnitInstance::Relay(Arc::clone(&instance))));
        instance
    }

    pub fn register_task(&self, instance_id: Arc<str>) {
        self.inner.insert(
            instance_id.to_string(),
            Arc::new(UnitInstance::Task(instance_id)),
        );
    }

    /// Resolve the unit variant for `instance_id` — the per-frame dispatch
    /// lookup.
    pub fn get_unit(&self, instance_id: &str) -> Option<Arc<UnitInstance>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    /// Resolve `instance_id` only when it is a relay; `None` for a task or
    /// an absent id.
    pub fn get_relay(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.inner.get(instance_id).and_then(|r| match &**r.value() {
            UnitInstance::Relay(ri) => Some(Arc::clone(ri)),
            UnitInstance::Task(_) => None,
        })
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<UnitInstance>> {
        self.inner.remove(instance_id).map(|(_, v)| v)
    }

    pub fn contains(&self, instance_id: &str) -> bool {
        self.inner.contains_key(instance_id)
    }

    /// Snapshot of the relay instances only — tasks carry no serializable
    /// state, so the shutdown snapshot and `get_relays` view skip them.
    pub fn snapshot_relays(&self) -> HashMap<String, Arc<RelayInstance>> {
        self.inner
            .iter()
            .filter_map(|r| match &**r.value() {
                UnitInstance::Relay(ri) => Some((r.key().clone(), Arc::clone(ri))),
                UnitInstance::Task(_) => None,
            })
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
    let snapshot = registry.snapshot_relays();
    let mapped: HashMap<String, Arc<Mutex<Arc<RelayInstance>>>> = snapshot
        .into_iter()
        .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
        .collect();
    Arc::new(RwLock::new(mapped))
}
