mod command;
mod node;
pub mod pipeline;
mod service;
mod utility;

use std::{
    collections::HashMap,
    os::unix::net::UnixStream,
    sync::{Arc, Mutex, RwLock},
};

use node::OperatorNode;

use pipeline::factory_registry::ComputationBox;
use pipeline::runner;
pub use pipeline::{
    register_factory, ComputationInput, ComputationOutput, ComputationResult, ComputationSettings,
    ComputationHandler, VirtualChannel,
};

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/operator_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/operator_commands.rs");
}
pub struct Flezha {
    _id: String,
    pipelines: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u32, UnixStream>>>,
    _operator: Arc<Mutex<OperatorNode>>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let pipelines = Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        Flezha {
            _id: id.clone(),
            pipelines: Arc::clone(&pipelines),
            shared_io_streams: Arc::clone(&shared_io_streams),
            _operator: OperatorNode::new(id.clone(), pipelines, shared_io_streams),
        }
    }

    pub fn get_pipeline(&self, pipeline_id: &str) -> Option<Arc<Mutex<ComputationBox>>> {
        self.pipelines.read().unwrap().get(pipeline_id).cloned()
    }

    pub fn get_pipelines(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>> {
        Arc::clone(&self.pipelines)
    }

    pub fn run(&mut self, pipeline_id: String, input: ComputationInput) -> ComputationResult {
        runner::exec(&pipeline_id, &self.pipelines, input)
    }

    pub fn pipe_to_agent(
        &mut self,
        pipeline_id: String,
        result: ComputationOutput,
        virtual_channel: VirtualChannel,
    ) {
        let mut io_stream = self
            .shared_io_streams
            .write()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .try_clone()
            .unwrap();

        runner::pipe_to_agent(&mut io_stream, &pipeline_id, &result, virtual_channel);
    }
}

pub fn new(group_id: String) -> Flezha {
    Flezha::new(group_id)
}
