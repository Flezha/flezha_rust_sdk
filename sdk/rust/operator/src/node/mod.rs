pub mod action_receiver;
mod control_unit;

use crate::computation::Computation;
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
    namespace: String,
    computation_factory: HashMap<String, fn() -> Box<dyn Computation>>,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Computation>>>>>>,
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
        computations: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Computation>>>>>>,
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
            namespace,
            control_stream,
            computations,
            shared_io_streams,
            computation_factory: HashMap::new(),
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
        factory: fn() -> Box<dyn Computation>) {
        let mut core_data = HashMap::new();
        
        core_data.insert("factory_id".to_string(), factory_id.clone());

        let json = serde_json::to_string(&core_data).unwrap();
        let payload = payloader::build_from_str(&"register_factory".to_string(), &json);

        self.control_stream.try_clone().unwrap().write_all(&payload).unwrap();
        self.computation_factory.insert(factory_id.clone(), factory);
    }

    pub fn get_factory(&self, factory_id: String) -> Option<&fn() -> Box<dyn Computation>> {
        self.computation_factory.get(&factory_id)
    }

    pub fn add_computation(&self, id: String, computation: Box<dyn Computation>) {
        let computation = Arc::new(Mutex::new(computation));
        self.computations.write().unwrap().insert(id, computation);
    }

    pub fn rem_computation(&self, id: String) {
        self.computations.write().unwrap().remove(&id);
    }

    pub fn get_control_stream(&self) -> Arc<UnixStream> {
        Arc::clone(&self.control_stream)
    }

    pub fn get_namespace(&self) -> String {
        self.namespace.clone()
    }

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Computation>>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn get_computation(&self, id: String) -> Option<Arc<Mutex<Box<dyn Computation>>>>
    {
        self.computations.read().unwrap().get(&id).map(|computation| Arc::clone(computation))
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u8, UnixStream>>> {
        Arc::clone(&self.shared_io_streams)
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