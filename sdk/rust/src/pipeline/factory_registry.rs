use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use lazy_static::lazy_static;

use crate::pipeline::{Pipeline, PipelineInput, PipelineResult};

// Type erased pipeline for registration. All pipelines implement Pipeline.
pub type PipelineBox = Box<dyn Pipeline + Send + Sync>;

// Factory function type that can create new pipeline instances
pub type PipelineFactory = Box<dyn Fn(String) -> PipelineBox + Send + Sync>;

// Use Arc<Mutex<PipelineBox>> for each pipeline
lazy_static! {
    static ref FACTORY_REGISTRY: RwLock<HashMap<String, Arc<PipelineFactory>>> = RwLock::new(HashMap::new());
}


/// Register a pipeline factory with a unique factory ID
pub fn register_pipeline_factory(factory_id: String, factory: PipelineFactory) {
    FACTORY_REGISTRY.write().unwrap().insert(factory_id, Arc::new(factory));
}

pub fn get_factory(factory_id: String) -> Option<Arc<PipelineFactory>> {
    FACTORY_REGISTRY.read().unwrap().get(&factory_id).cloned()
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
    pub fn run(&self, input: PipelineInput) -> PipelineResult {
        self.pipeline_mut().run(input)
    }
}
