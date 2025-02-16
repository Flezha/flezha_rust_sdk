pub mod action_receiver;
mod control_unit;

use crate::worker::Worker;
use crate::service::payloader;

use std::io::Write;
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::{self};
use std::io::{self, Read};
use std::time::Duration;

pub struct OperatorNode {
    operator_id: String,
    _namespace: String,
    worker_factory: HashMap<String, fn() -> Box<dyn Worker>>,
    workers: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Worker>>>>>>,
    control_stream: Arc<UnixStream>,
    shared_io_streams: Arc<RwLock<HashMap<u8, UnixStream>>>,
}

pub enum NodeAction {
    ControlCommand(String, Vec<u8>),
    EngineIdConfirmation
}

impl OperatorNode {
    pub fn new(
        namespace: String,
        workers: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Worker>>>>>>,
        shared_io_streams: Arc<RwLock<HashMap<u8, UnixStream>>>,
    ) -> Arc<Mutex<OperatorNode>> {
        let (tx, rx): (mpsc::Sender<NodeAction>, mpsc::Receiver<NodeAction>) = mpsc::channel();
        let tx = Arc::new(Mutex::new(tx));
        let rx = Arc::new(Mutex::new(rx));
    
        let (_control_thread, control_stream) = control_unit::connect_control_socket(
            namespace.clone(),
            Arc::clone(&tx)
        );

        // Make sure the namespace is passed to the Aflecha.
        match rx.lock().unwrap().recv().expect("Failed to receive namespace confirmation") {
            NodeAction::EngineIdConfirmation => {},
            _ => panic!("Wrong order of messages"),
        }

        let worker = OperatorNode {
            operator_id: "undefined".to_string(),
            _namespace: namespace,
            control_stream,
            workers,
            shared_io_streams,
            worker_factory: HashMap::new(),
        };
    
        let worker_arc = Arc::new(Mutex::new(worker));
            
        let _action_receiver = action_receiver::start_action_receiver(
            Arc::clone(&worker_arc),
            Arc::clone(&rx)
        );
    
        let _stdio_thread = start_stdio_thread();
    
        worker_arc
    }

    pub fn register_factory(
        &mut self,
        factory_id: String,
        factory: fn() -> Box<dyn Worker>) {
        let mut core_data = HashMap::new();
        
        core_data.insert("factory_id".to_string(), factory_id.clone());

        let json = serde_json::to_string(&core_data).unwrap();
        let payload = payloader::build_from_str(&"register_factory".to_string(), &json);

        self.control_stream.try_clone().unwrap().write_all(&payload).unwrap();
        self.worker_factory.insert(factory_id.clone(), factory);
    }

    pub fn get_factory(&self, factory_id: String) -> Option<&fn() -> Box<dyn Worker>> {
        self.worker_factory.get(&factory_id)
    }

    pub fn add_worker(&self, id: String, worker: Box<dyn Worker>) {
        let worker = Arc::new(Mutex::new(worker));
        self.workers.write().unwrap().insert(id, worker);
    }

    pub fn rem_worker(&self, id: String) {
        self.workers.write().unwrap().remove(&id);
    }

    pub fn get_control_stream(&self) -> Arc<UnixStream> {
        Arc::clone(&self.control_stream)
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    pub fn get_workers(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Worker>>>>>> {
        Arc::clone(&self.workers)
    }

    pub fn get_worker(&self, id: String) -> Option<Arc<Mutex<Box<dyn Worker>>>>
    {
        self.workers.read().unwrap().get(&id).map(|worker| Arc::clone(worker))
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u8, UnixStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_operator_id(&self) -> String {
        self.operator_id.clone()
    }

    pub fn assign_operator_id(&mut self, operator_id: String) {
        self.operator_id = operator_id;
    }
}

// Start a thread to monitor stdout and exit when stdin is closed. TODO: Maybe remove this?
fn start_stdio_thread() -> thread::JoinHandle<()> {
       // Start a thread to monitor stdout
       thread::spawn(|| {
        let mut buf = [0u8; 1]; // Buffer to read from stdout

        loop {
            let result = io::stdin().read(&mut buf); // Attempt to read from stdin
            match result {
                Ok(0) => {
                    // TODO: Log that stdin has been closed and exit.
                    std::process::exit(0);
                }
                Ok(_) => {
                    // Data available; continue monitoring
                    println!("Data available: {:?}", buf);
                }
                Err(_e) => {
                    // TODO: Log the error.
                }
            }

            thread::sleep(Duration::from_secs(1));
        }
    })
}