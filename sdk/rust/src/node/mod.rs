mod control_unit;
pub(crate) mod coordinator;
mod worker_pool;

use crate::node::coordinator::CoordinatorMessage;
use crate::computation::factory_registry::{get_factory, ComputationBox, ComputationHandle};

use crossbeam::channel::{Receiver, Sender};
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, RwLock};

pub enum OperatorStatus {
    Starting,
    Running,
    Stopped,
}

pub struct OperatorNode {
    operator_id: String,
    _namespace: String,
    pipelines: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u32, UnixStream>>>,
    coordinator_channel: Arc<Mutex<Sender<CoordinatorMessage>>>,
    status: OperatorStatus,
}

pub enum NodeAction {
    EngineIdConfirmation,
}

impl OperatorNode {
    pub fn new(
        namespace: String,
        pipelines: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
        shared_io_streams: Arc<RwLock<HashMap<u32, UnixStream>>>,
    ) -> Arc<Mutex<OperatorNode>> {
        let (itx, irx): (Sender<NodeAction>, Receiver<NodeAction>) =
            crossbeam::channel::unbounded();

        let (ctx, crx): (Sender<CoordinatorMessage>, Receiver<CoordinatorMessage>) =
            crossbeam::channel::unbounded();

        let operator = OperatorNode {
            operator_id: "undefined".to_string(),
            _namespace: namespace.clone(),
            pipelines,
            shared_io_streams: shared_io_streams,
            coordinator_channel: Arc::new(Mutex::new(ctx)),
            status: OperatorStatus::Starting,
        };

        let operator_arc = Arc::new(Mutex::new(operator));

        let _coordinator_thread = coordinator::start(
            namespace,
            Arc::new(Mutex::new(crx)),
            Arc::new(Mutex::new(itx)),
            operator_arc.clone(),
        );

        // Make sure the namespace is passed to the agent.
        match irx
            .recv()
            .expect("Failed to receive namespace confirmation")
        {
            NodeAction::EngineIdConfirmation => {}
        }

        operator_arc
    }

    pub fn start_pipeline(
        &self,
        factory_id: String,
        computation_id: String,
    ) -> Option<ComputationHandle> {
        if let Some(factory) = get_factory(factory_id) {
            let pipeline = factory(computation_id.clone());
            self.register_pipeline(computation_id.clone(), pipeline);
            self.get_pipeline(computation_id)
        } else {
            None
        }
    }

    fn register_pipeline(&self, id: String, pipeline: ComputationBox) {
        self.pipelines
            .write()
            .unwrap()
            .insert(id, Arc::new(Mutex::new(pipeline)));
    }

    pub fn stop_pipeline(&self, id: String) {
        self.pipelines.write().unwrap().remove(&id);
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    pub fn get_pipelines(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>> {
        Arc::clone(&self.pipelines)
    }

    pub fn get_pipeline(&self, id: String) -> Option<ComputationHandle> {
        self.pipelines
            .read()
            .unwrap()
            .get(&id)
            .cloned()
            .map(|pipeline| ComputationHandle { pipeline })
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u32, UnixStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_operator_id(&self) -> String {
        self.operator_id.clone()
    }

    pub fn assign_operator_id(&mut self, operator_id: String) {
        self.operator_id = operator_id;
    }

    pub fn set_status(&mut self, status: OperatorStatus) {
        self.status = status;
    }

    pub fn has_pipeline(&self, id: String) -> bool {
        self.pipelines.read().unwrap().contains_key(&id)
    }

    pub fn get_coordinator_channel(&self) -> Arc<Mutex<Sender<CoordinatorMessage>>> {
        Arc::clone(&self.coordinator_channel)
    }
}
