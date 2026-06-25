//! Task class registry: process-global, one entry per task class. Mirrors
//! `relay::registry`'s `RelayClassRegistry` — keyed by the controller-facing
//! `factory_id`, looked up when `AddRelay --kind task` arrives.

use std::collections::HashMap;
use std::sync::RwLock;

use lazy_static::lazy_static;

use super::TaskRunFn;

lazy_static! {
    static ref TASK_CLASS_REGISTRY: RwLock<HashMap<&'static str, TaskRunFn>> =
        RwLock::new(HashMap::new());
}

pub fn register_class(factory_id: &'static str, run: TaskRunFn) {
    TASK_CLASS_REGISTRY.write().unwrap().insert(factory_id, run);
}

pub fn get_class(factory_id: &str) -> Option<TaskRunFn> {
    TASK_CLASS_REGISTRY.read().unwrap().get(factory_id).copied()
}

/// Factory ids of every registered task class. Advertised to the controller
/// on connect alongside relay factories so `AddRelay --kind task` validates.
pub fn task_factory_ids() -> Vec<&'static str> {
    TASK_CLASS_REGISTRY.read().unwrap().keys().copied().collect()
}
