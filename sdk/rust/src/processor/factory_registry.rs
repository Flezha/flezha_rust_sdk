use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::processor::{Processor, ProcessorError, ProcessorInput, ProcessorResult};

// Type erased processor for registration. All processors implement Processor.
pub type ProcessorBox = Box<dyn Processor + Send + Sync>;

// Factory function type that can create new processor instances
pub type ProcessorFactory = Box<dyn Fn(String) -> ProcessorBox + Send + Sync>;

// Use Arc<Mutex<ProcessorBox>> for each processor
lazy_static! {
    static ref FACTORY_REGISTRY: RwLock<HashMap<String, Arc<ProcessorFactory>>> =
        RwLock::new(HashMap::new());
}

/// Register a processor factory with a unique factory ID
pub fn register_processor_factory(factory_id: String, factory: ProcessorFactory) {
    FACTORY_REGISTRY
        .write()
        .unwrap()
        .insert(factory_id, Arc::new(factory));
}

pub fn get_factory(factory_id: String) -> Option<Arc<ProcessorFactory>> {
    FACTORY_REGISTRY.read().unwrap().get(&factory_id).cloned()
}

pub fn get_factory_ids() -> Vec<String> {
    FACTORY_REGISTRY.read().unwrap().keys().cloned().collect()
}

/// A wrapper for a processor that handles locking
pub struct ProcessorHandle {
    pub processor: Arc<Mutex<ProcessorBox>>,
}

impl ProcessorHandle {
    /// Get mutable access to the processor
    pub fn processor_mut(&self) -> std::sync::MutexGuard<'_, ProcessorBox> {
        self.processor.lock().unwrap()
    }

    /// Execute the processor with the given input
    pub fn run(&self, input: ProcessorInput) -> Result<ProcessorResult, ProcessorError> {
        self.processor_mut().run(input)
    }
}
