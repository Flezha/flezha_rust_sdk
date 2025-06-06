pub mod pipeline;
mod node;
mod service;
mod command;
mod utility;

use std::{collections::HashMap, os::unix::net::UnixStream, sync::{Arc, Mutex, RwLock}};

use node::OperatorNode;

pub use pipeline::{PipelineInput, PipelineOutput, PipelineResult, PipelineResultType};
use pipeline::factory_registry::PipelineBox;
use pipeline::runner;

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!(concat!(env!("OUT_DIR"), "/operator_commands.rs"));
}
pub struct Flezha {
    _id: String,
    pipelines: Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u32, UnixStream>>>,
    _operator: Arc<Mutex<OperatorNode>>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let pipelines =  Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        Flezha {
            _id: id.clone(),
            pipelines: Arc::clone(&pipelines),
            shared_io_streams: Arc::clone(&shared_io_streams),
            _operator: OperatorNode::new(
                id.clone(),
                pipelines,
                shared_io_streams,
            ),
        }
    }

    pub fn run(&mut self, pipeline_id: String, input: PipelineInput) -> PipelineResult {
        runner::exec(
            &pipeline_id, 
            &self.pipelines,
            input
        )
    }

    pub fn pipe_to_agent(&mut self, pipeline_id: String, result: PipelineOutput) {
        let mut io_stream = self.shared_io_streams.write().unwrap().values().next().unwrap().try_clone().unwrap();

        runner::pipe_to_agent(&mut io_stream, &pipeline_id, &result);
    }
}

pub fn new(group_id: String) -> Flezha {
    Flezha::new(group_id)
}