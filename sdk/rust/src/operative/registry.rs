//! ContinuousOperative class + instance registries.
//!
//! `OperativeClassRegistry`: process-global; one entry per ContinuousOperative class. Holds
//! the immutable `Arc<OperativeTopology>` produced by `register_continuous::<R>()`.
//! Looked up by controller-facing `factory_id` when `AddOperative` arrives.
//!
//! `OperativeRegistry`: per-`ExecutorNode`; one entry per
//! `AddOperative` (instance_id). Reuses the DashMap pattern from the
//! pre-change `OperativeRegistry` so concurrent reader/writer access is
//! operationed. Values are a sealed [`Operative`] — operative or bounded — so the
//! dispatch boundary resolves the variant once per frame; the `Arc` keeps
//! map values pointer-sized.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use lazy_static::lazy_static;

use crate::operative::instance::ContinuousInstance;
use crate::operative::topology::OperativeTopology;

/// A control-plane-managed unit instance. Sealed two-variant enum: a new kind
/// must be handled at every match, and the 2-variant match keeps the operative hot
/// path branch-predictable rather than devirtualizing a single variant. A bounded
/// carries only its instance id — it has no per-instance runtime state.
pub enum Operative {
    Continuous(Arc<ContinuousInstance>),
    Bounded(Arc<str>),
}

lazy_static! {
    static ref CLASS_REGISTRY: RwLock<HashMap<&'static str, Arc<OperativeTopology>>> =
        RwLock::new(HashMap::new());
}

pub fn register_class(factory_id: &'static str, topology: Arc<OperativeTopology>) {
    CLASS_REGISTRY.write().unwrap().insert(factory_id, topology);
}

pub fn get_class(factory_id: &str) -> Option<Arc<OperativeTopology>> {
    CLASS_REGISTRY.read().unwrap().get(factory_id).cloned()
}

pub fn class_factory_ids() -> Vec<&'static str> {
    CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}

pub struct OperativeRegistry {
    inner: dashmap::DashMap<String, Arc<Operative>>,
}

impl OperativeRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register_continuous(
        &self,
        instance: Arc<ContinuousInstance>,
    ) -> Arc<ContinuousInstance> {
        let id = instance.instance_id.to_string();
        self.inner
            .insert(id, Arc::new(Operative::Continuous(Arc::clone(&instance))));
        instance
    }

    pub fn register_bounded(&self, instance_id: Arc<str>) {
        self.inner.insert(
            instance_id.to_string(),
            Arc::new(Operative::Bounded(instance_id)),
        );
    }

    /// Resolve the unit variant for `instance_id` — the per-frame dispatch
    /// lookup.
    pub fn get_operative(&self, instance_id: &str) -> Option<Arc<Operative>> {
        self.inner.get(instance_id).map(|r| Arc::clone(r.value()))
    }

    /// Resolve `instance_id` only when it is a operative; `None` for a bounded or
    /// an absent id.
    pub fn get_continuous(&self, instance_id: &str) -> Option<Arc<ContinuousInstance>> {
        self.inner
            .get(instance_id)
            .and_then(|r| match &**r.value() {
                Operative::Continuous(ri) => Some(Arc::clone(ri)),
                Operative::Bounded(_) => None,
            })
    }

    pub fn remove(&self, instance_id: &str) -> Option<Arc<Operative>> {
        self.inner.remove(instance_id).map(|(_, v)| v)
    }

    pub fn contains(&self, instance_id: &str) -> bool {
        self.inner.contains_key(instance_id)
    }

    /// Snapshot of the operative instances only — bounded carry no serializable
    /// state, so the shutdown snapshot and `get_operatives` view skip them.
    pub fn snapshot_continuous(&self) -> HashMap<String, Arc<ContinuousInstance>> {
        self.inner
            .iter()
            .filter_map(|r| match &**r.value() {
                Operative::Continuous(ri) => Some((r.key().clone(), Arc::clone(ri))),
                Operative::Bounded(_) => None,
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

impl Default for OperativeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Public-API view that mirrors today's `Flezha::get_operatives()` shape.
/// Returned as `Arc<RwLock<HashMap<String, Arc<Mutex<…>>>>>` so external
/// consumers (test apps, observability wrappers) can iterate without
/// touching DashMap internals. The inner Mutex is a placeholder kept for
/// API compatibility — `ContinuousInstance` is itself shared via Arc and does
/// not require a Mutex on the read path.
// reason: return shape mirrors Flezha::get_operatives() public API; splitting would break external inspectors
#[allow(clippy::type_complexity)]
pub fn snapshot_as_operative_view(
    registry: &OperativeRegistry,
) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<ContinuousInstance>>>>>> {
    let snapshot = registry.snapshot_continuous();
    let mapped: HashMap<String, Arc<Mutex<Arc<ContinuousInstance>>>> = snapshot
        .into_iter()
        .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
        .collect();
    Arc::new(RwLock::new(mapped))
}
