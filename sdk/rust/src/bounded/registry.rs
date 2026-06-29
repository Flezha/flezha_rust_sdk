//! BoundedOperative class registry: process-global, one entry per bounded class. Mirrors
//! `operative::registry`'s `OperativeClassRegistry` — keyed by the controller-facing
//! `factory_id`, looked up when `AddOperative --kind bounded` arrives.

use std::collections::HashMap;
use std::sync::RwLock;

use lazy_static::lazy_static;

use super::BoundedRunFn;

lazy_static! {
    static ref BOUNDED_CLASS_REGISTRY: RwLock<HashMap<&'static str, BoundedRunFn>> =
        RwLock::new(HashMap::new());
}

pub fn register_class(factory_id: &'static str, run: BoundedRunFn) {
    BOUNDED_CLASS_REGISTRY.write().unwrap().insert(factory_id, run);
}

pub fn get_class(factory_id: &str) -> Option<BoundedRunFn> {
    BOUNDED_CLASS_REGISTRY.read().unwrap().get(factory_id).copied()
}

/// Factory ids of every registered bounded class. Advertised to the controller
/// on connect alongside operative factories so `AddOperative --kind bounded` validates.
pub fn bounded_factory_ids() -> Vec<&'static str> {
    BOUNDED_CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}
