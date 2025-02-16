mod worker;
mod node;
mod service;
mod command;
mod utility;

use std::{collections::HashMap, os::unix::net::UnixStream, sync::{Arc, Mutex, RwLock}};

use node::OperatorNode;

pub use worker::{
    WorkerOutput,
    WorkerResult,
    Worker,
    SerializableWorker,
    WorkerInput
};

pub struct Aflecha {
    _id: String,
    workers: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Worker>>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u8, UnixStream>>>,
    operator: Arc<Mutex<OperatorNode>>,
}

impl Aflecha {
    fn new(id: String) -> Aflecha {
        let workers =  Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        Aflecha {
            _id: id.clone(),
            workers: Arc::clone(&workers),
            shared_io_streams: Arc::clone(&shared_io_streams),
            operator: OperatorNode::new(
                id.clone(),
                workers,
                shared_io_streams,
            ),
        }
    }
    
    pub fn register_factory(&mut self, name: String, factory: fn() -> Box<dyn Worker>) {
        self.operator.lock().unwrap().register_factory(name, factory);
    }

    pub fn run(&mut self, worker_id: String, input: WorkerInput) -> Option<WorkerResult> {
        // Take random io_stream.
        let mut io_stream = self.shared_io_streams.write().unwrap().values().next().unwrap().try_clone().unwrap();

        worker::exec(
            &worker_id, 
            &self.workers, 
            &mut io_stream, 
            input
        )
    }
}

pub fn new(group_id: String) -> Aflecha {
    Aflecha::new(group_id)
}