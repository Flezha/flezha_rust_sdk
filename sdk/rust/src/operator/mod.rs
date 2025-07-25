mod control_unit;
pub(crate) mod coordinator;
mod worker_pool;

use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, VecDeque};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crate::computation::factory_registry::{get_factory, ComputationBox, ComputationHandle};

use crate::computation::{executor, ComputationRunner};
use crate::operator::worker_pool::{init_worker_pool, pool_worker_delegate, WorkerPoolMessage};
use crate::{command, ComputationInput};

type WorkItem = (ComputationInput, UnixStream);

pub enum OperatorMessage {
    WorkerInitCheckIn(u16),
    WorkerCheckIn(u16, String),
    WorkerCheckOut(String, ComputationInput, UnixStream),
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnComputations,
}

pub enum OperatorStatus {
    Starting,
    Running,
    Stopped,
}

pub enum ClientResponseMessage {
    OperatorIdConfirmation,
    ReturnComputations(Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>),
}

pub struct OperatorNode {
    operator_id: String,
    _namespace: String,
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u32, UnixStream>>>,
    workers_pool_tx: HashMap<u16, Sender<WorkerPoolMessage>>,
    computation_queues: HashMap<String, VecDeque<WorkItem>>,
    computation_workers: HashMap<String, u16>,
    available_workers: VecDeque<u16>,
    pending_computations: VecDeque<String>,
    operator_channel: Arc<Mutex<Sender<OperatorMessage>>>,
    status: OperatorStatus,
}

impl OperatorNode {
    pub fn new(namespace: String, otx: Sender<OperatorMessage>) -> OperatorNode {
        let computations = Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));
        let otx_arc = Arc::new(Mutex::new(otx));

        OperatorNode {
            operator_id: "undefined".to_string(),
            _namespace: namespace.clone(),
            computations: computations.clone(),
            shared_io_streams: shared_io_streams,
            workers_pool_tx: init_worker_pool(computations, otx_arc.clone()),
            computation_queues: HashMap::new(),
            computation_workers: HashMap::new(),
            available_workers: VecDeque::new(),
            pending_computations: VecDeque::new(),
            operator_channel: otx_arc.clone(),
            status: OperatorStatus::Starting,
        }
    }

    pub fn start_computation(
        &self,
        factory_id: String,
        computation_id: String,
    ) -> Option<ComputationHandle> {
        if let Some(factory) = get_factory(factory_id) {
            let computation = factory(computation_id.clone());
            self.register_computation(computation_id.clone(), computation);
            self.get_computation(computation_id)
        } else {
            None
        }
    }

    fn register_computation(&self, id: String, computation: ComputationBox) {
        self.computations
            .write()
            .unwrap()
            .insert(id, Arc::new(Mutex::new(computation)));
    }

    pub fn stop_computation(&self, id: String) {
        self.computations.write().unwrap().remove(&id);
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>> {
        Arc::clone(&self.computations)
    }

    pub fn get_computation(&self, id: String) -> Option<ComputationHandle> {
        self.computations
            .read()
            .unwrap()
            .get(&id)
            .cloned()
            .map(|computation| ComputationHandle { computation })
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

    pub fn has_computation(&self, id: String) -> bool {
        self.computations.read().unwrap().contains_key(&id)
    }

    pub fn get_operator_channel(&self) -> Arc<Mutex<Sender<OperatorMessage>>> {
        Arc::clone(&self.operator_channel)
    }
}

pub fn start(
    namespace: String,
    (otx, orx): (Sender<OperatorMessage>, Receiver<OperatorMessage>),
    ctx: Sender<ClientResponseMessage>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut operator = OperatorNode::new(namespace.clone(), otx.clone());
        let ctx_arc = Arc::new(Mutex::new(ctx));
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, control_stream) =
            control_unit::open(namespace, ctx_arc.clone(), otx_arc);

        loop {
            let message = orx.recv().expect("Failed to receive coordinator message");

            match message {
                OperatorMessage::WorkerInitCheckIn(worker_id) => {
                    operator.available_workers.push_back(worker_id);
                }
                OperatorMessage::WorkerCheckOut(computation_id, computation_input, stream) => {
                    handle_checkout(&mut operator, computation_id, computation_input, stream);
                }
                OperatorMessage::WorkerCheckIn(worker_id, worked_computation_id) => {
                    handle_checkin(&mut operator, worker_id, worked_computation_id);
                }
                // TODO: Do anything with metadata?
                OperatorMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(&mut operator, &control_stream, &payload_id, &message);
                }
                OperatorMessage::ReturnComputations => {
                    return_computations(&operator, ctx_arc.clone());
                }
            }
        }
    })
}

fn return_computations(
    operator: &OperatorNode,
    ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>,
) {
    let computations = operator.get_computations();

    ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnComputations(computations))
        .expect("Failed to send computations response");
}

fn handle_checkin(operator: &mut OperatorNode, worker_id: u16, worked_computation_id: String) {
    // Remove computation from active processing
    operator.computation_workers.remove(&worked_computation_id);

    // TODO: handle different worker types.
    // Check if this computation has queued work
    if let Some(queue) = operator.computation_queues.get_mut(&worked_computation_id) {
        if let Some((computation_input, stream)) = queue.pop_front() {
            // Process next item in this computation's queue with same worker (sticky assignment)
            operator
                .computation_workers
                .insert(worked_computation_id.clone(), worker_id);
            pool_worker_delegate(
                &operator.workers_pool_tx,
                worked_computation_id,
                computation_input,
                worker_id,
                stream,
            );
            return;
        } else {
            // Queue is empty, remove it
            operator.computation_queues.remove(&worked_computation_id);
        }
    }

    // No work for this computation, make worker available for other computations
    operator.available_workers.push_back(worker_id);

    // Optimized: Only check computations that have pending work
    while let Some(computation_id) = operator.pending_computations.pop_front() {
        // Check if computation is still idle and has work
        if !operator.computation_workers.contains_key(&computation_id) {
            // TODO: handle different worker types.
            if let Some(queue) = operator.computation_queues.get_mut(&computation_id) {
                if let Some((computation_input, stream)) = queue.pop_front() {
                    // Remove worker from available workers since we're assigning it
                    operator.available_workers.pop_back();
                    operator
                        .computation_workers
                        .insert(computation_id.clone(), worker_id);
                    pool_worker_delegate(
                        &operator.workers_pool_tx,
                        computation_id.clone(),
                        computation_input,
                        worker_id,
                        stream,
                    );

                    // If queue still has work, put it back in pending list
                    if !queue.is_empty() {
                        operator
                            .pending_computations
                            .push_back(computation_id.clone());
                    } else {
                        // Queue is empty, remove it
                        operator.computation_queues.remove(&computation_id);
                    }
                    return;
                }
            }
        } else {
            // Computation is busy, put it back for later
            operator.pending_computations.push_back(computation_id);
            break;
        }
    }
}

fn handle_checkout(
    operator: &mut OperatorNode,
    computation_id: String,
    computation_input: ComputationInput,
    stream: UnixStream,
) {
    let computations_read = operator.computations.read().unwrap();

    if let Some(c) = computations_read.get(computation_id.as_str()) {
        let computation = c.lock().unwrap();
        let runner_type = computation.get_runner_type();

        match runner_type {
            ComputationRunner::ThreadPool => {
                // Check if computation is already being processed
                if operator.computation_workers.contains_key(&computation_id) {
                    // Computation busy, add to its specific queue
                    let queue = operator
                        .computation_queues
                        .entry(computation_id.clone())
                        .or_insert_with(VecDeque::new);
                    let is_first_item = queue.is_empty();
                    queue.push_back((computation_input, stream));

                    // Track that this computation has pending work
                    if is_first_item {
                        operator.pending_computations.push_back(computation_id);
                    }
                } else if let Some(worker_id) = operator.available_workers.pop_front() {
                    // Computation available and worker available, assign immediately
                    operator
                        .computation_workers
                        .insert(computation_id.clone(), worker_id);
                    pool_worker_delegate(
                        &operator.workers_pool_tx,
                        computation_id,
                        computation_input,
                        worker_id,
                        stream,
                    );
                } else {
                    // No available workers, add to computation queue
                    let queue = operator
                        .computation_queues
                        .entry(computation_id.clone())
                        .or_insert_with(VecDeque::new);
                    let is_first_item = queue.is_empty();
                    queue.push_back((computation_input, stream));

                    // Track that this computation has pending work
                    if is_first_item {
                        operator.pending_computations.push_back(computation_id);
                    }
                }
            }
            ComputationRunner::ThreadSpawn => {
                executor::exec_agent(
                    stream,
                    &computation_id,
                    &operator.get_computations(),
                    computation_input,
                );
            }
        }
    } else {
        eprint!(
            "Computation {} not found in operator {}",
            computation_id, operator.operator_id
        );
    }
}
