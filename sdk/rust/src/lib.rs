mod command;
mod node;
pub mod computation;
mod service;
mod utility;

use std::{
    collections::HashMap,
    os::unix::net::UnixStream,
    sync::{Arc, Mutex, RwLock},
};

use node::OperatorNode;

use computation::factory_registry::ComputationBox;
use computation::runner;
pub use computation::{
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
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u32, UnixStream>>>,
    _operator: Arc<Mutex<OperatorNode>>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let computations = Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        Flezha {
            _id: id.clone(),
            computations: Arc::clone(&computations),
            shared_io_streams: Arc::clone(&shared_io_streams),
            _operator: OperatorNode::new(id.clone(), computations, shared_io_streams),
        }
    }

    pub fn get_computation(&self, computation_id: &str) -> Option<Arc<Mutex<ComputationBox>>> {
        self.computations.read().unwrap().get(computation_id).cloned()
    }

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn run(&mut self, computation_id: String, input: ComputationInput) -> ComputationResult {
        runner::exec(&computation_id, &self.computations, input)
    }

    pub fn pipe_to_agent(
        &mut self,
        computation_id: String,
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

        runner::pipe_to_agent(&mut io_stream, &computation_id, &result, virtual_channel);
    }
}

pub fn new(group_id: String) -> Flezha {
    Flezha::new(group_id)
}
