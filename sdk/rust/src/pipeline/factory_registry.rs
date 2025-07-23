use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::pipeline::{Pipeline, ComputationInput, ComputationResult};

// Type erased pipeline for registration. All pipelines implement Pipeline.
pub type PipelineBox = Box<dyn Pipeline + Send + Sync>;

// Factory function type that can create new pipeline instances
pub type ComputationFactory = Box<dyn Fn(String) -> PipelineBox + Send + Sync>;

// Use Arc<Mutex<PipelineBox>> for each pipeline
lazy_static! {
    static ref FACTORY_REGISTRY: RwLock<HashMap<String, Arc<ComputationFactory>>> =
        RwLock::new(HashMap::new());
}

/// Register a pipeline factory with a unique factory ID
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

/// A wrapper for a pipeline that handles locking
pub struct PipelineHandle {
    pub pipeline: Arc<Mutex<PipelineBox>>,
}

impl PipelineHandle {
    /// Get mutable access to the pipeline
    pub fn pipeline_mut(&self) -> std::sync::MutexGuard<PipelineBox> {
        self.pipeline.lock().unwrap()
    }

    /// Execute the pipeline with the given input
    pub fn run(&self, input: ComputationInput) -> ComputationResult {
        self.pipeline_mut().run(input)
    }
}
