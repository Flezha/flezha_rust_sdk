pub mod action_receiver;
mod control_unit;

use crate::pipeline::factory_registry::{get_factory, PipelineBox, PipelineHandle};

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::{mpsc, Arc, Mutex, RwLock};

pub struct OperatorNode {
    operator_id: String,
    _namespace: String,
    pipelines: Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>>,
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
        pipelines: Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>>,
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

        let pipeline = OperatorNode {
            operator_id: "undefined".to_string(),
            _namespace: namespace,
            control_stream,
            pipelines,
            shared_io_streams
        };
    
        let pipeline_arc = Arc::new(Mutex::new(pipeline));
            
        let _action_receiver = action_receiver::start_action_receiver(
            Arc::clone(&pipeline_arc),
            Arc::clone(&rx)
        );
        
        pipeline_arc
    }

    pub fn add_pipeline(&self, factory_id: String, pipeline_id: String) -> Option<PipelineHandle> {
        if let Some(factory) = get_factory(factory_id) {
            let pipeline = factory(pipeline_id.clone());
            self.register_pipeline(pipeline_id.clone(), pipeline);
            self.get_pipeline(pipeline_id)
        } else {
            None
        }
    }

    fn register_pipeline(&self, id: String, pipeline: PipelineBox) {
        self.pipelines.write().unwrap().insert(id, Arc::new(Mutex::new(pipeline)));
    }

    pub fn rem_pipeline(&self, id: String) {
        self.pipelines.write().unwrap().remove(&id);
    }

    pub fn get_control_stream(&self) -> Arc<UnixStream> {
        Arc::clone(&self.control_stream)
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    pub fn get_pipelines(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>> {
        Arc::clone(&self.pipelines)
    }

    pub fn get_pipeline(&self, id: String) -> Option<PipelineHandle> {    
        self.pipelines.read().unwrap().get(&id).cloned().map(|pipeline| PipelineHandle { pipeline })
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

    pub fn has_pipeline(&self, id: String) -> bool {
        self.pipelines.read().unwrap().contains_key(&id)
    }
}

