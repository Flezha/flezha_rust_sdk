//! Bounded class registry: process-global, one entry per bounded class. Mirrors
//! `operative::registry`'s `OperativeClassRegistry` — keyed by the controller-facing
//! `factory_id`, looked up when `AddOperative --kind bounded` arrives. Holds the
//! frozen [`BoundedSpec`] (run-sugar fn + optional structured topology).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use lazy_static::lazy_static;

use super::spec::BoundedSpec;

lazy_static! {
    static ref BOUNDED_CLASS_REGISTRY: RwLock<HashMap<&'static str, Arc<BoundedSpec>>> =
        RwLock::new(HashMap::new());
}

pub(crate) fn register_class(factory_id: &'static str, spec: BoundedSpec) {
    BOUNDED_CLASS_REGISTRY
        .write()
        .unwrap()
        .insert(factory_id, Arc::new(spec));
}

pub(crate) fn get_class(factory_id: &str) -> Option<Arc<BoundedSpec>> {
    BOUNDED_CLASS_REGISTRY
        .read()
        .unwrap()
        .get(factory_id)
        .cloned()
}

/// Factory ids of every registered bounded class. Advertised to the controller
/// on connect alongside operative factories so `AddOperative --kind bounded` validates.
pub fn bounded_factory_ids() -> Vec<&'static str> {
    BOUNDED_CLASS_REGISTRY
        .read()
        .unwrap()
        .keys()
        .copied()
        .collect()
}
