mod computation;
mod node;
mod service;
mod command;
mod utility;

use std::{collections::HashMap, os::unix::net::UnixStream, sync::{Arc, Mutex, RwLock}};

use node::WorkerNode;

pub use computation::{
    ComputationOutput,
    ComputationResult,
    Computation,
    SerializableComputation,
    ComputationInput
};

pub struct Aflecha {
    _id: String,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Computation>>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u8, UnixStream>>>,
    operator: Arc<Mutex<WorkerNode>>,
}

impl Aflecha {
    fn new(id: String) -> Aflecha {
        let computations =  Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        Aflecha {
            _id: id.clone(),
            computations: Arc::clone(&computations),
            shared_io_streams: Arc::clone(&shared_io_streams),
            operator: WorkerNode::new(
                id.clone(),
                computations,
                shared_io_streams,
            ),
        }
    }
    
    pub fn register_factory(&mut self, name: String, factory: fn() -> Box<dyn Computation>) {
        self.operator.lock().unwrap().register_factory(name, factory);
    }

    pub fn run(&mut self, computation_id: String, input: ComputationInput) -> Option<ComputationResult> {
        // Take random io_stream.
        let mut io_stream = self.shared_io_streams.write().unwrap().values().next().unwrap().try_clone().unwrap();

        computation::exec(
            &computation_id, 
            &self.computations, 
            &mut io_stream, 
            input
        )
    }
}

pub fn new(id: String) -> Aflecha {
    Aflecha::new(id)
}