use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::computation::{Computation, ComputationError, ComputationInput, ComputationResult};

// Type erased computation for registration. All computations implement Computation.
pub type ComputationBox = Box<dyn Computation + Send + Sync>;

// Factory function type that can create new computation instances
pub type ComputationFactory = Box<dyn Fn(String) -> ComputationBox + Send + Sync>;

// Use Arc<Mutex<ComputationBox>> for each computation
lazy_static! {
    static ref FACTORY_REGISTRY: RwLock<HashMap<String, Arc<ComputationFactory>>> =
        RwLock::new(HashMap::new());
}

/// Register a computation factory with a unique factory ID
pub fn register_computation_factory(factory_id: String, factory: ComputationFactory) {
    FACTORY_REGISTRY
        .write()
        .unwrap()
        .insert(factory_id, Arc::new(factory));
}

pub fn get_factory(factory_id: String) -> Option<Arc<ComputationFactory>> {
    FACTORY_REGISTRY.read().unwrap().get(&factory_id).cloned()
}

pub fn get_factory_ids() -> Vec<String> {
    FACTORY_REGISTRY.read().unwrap().keys().cloned().collect()
}

/// A wrapper for a computation that handles locking
pub struct ComputationHandle {
    pub computation: Arc<Mutex<ComputationBox>>,
}

impl ComputationHandle {
    /// Get mutable access to the computation
    pub fn computation_mut(&self) -> std::sync::MutexGuard<ComputationBox> {
        self.computation.lock().unwrap()
    }

    /// Execute the computation with the given input
    pub fn run(&self, input: ComputationInput) -> Result<ComputationResult, ComputationError> {
        self.computation_mut().run(input)
    }
}
