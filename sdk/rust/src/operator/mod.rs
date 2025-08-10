mod control_unit;
pub(crate) mod coordinator;
mod worker_pool;

use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, VecDeque};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crate::processor::factory_registry::{get_factory, ProcessorBox, ProcessorHandle};

use crate::operator::worker_pool::{init_worker_pool, pool_worker_delegate, WorkerPoolMessage};
use crate::processor::{executor, ProcessorRunner};
use crate::{command, ProcessorInput};

type WorkItem = (ProcessorInput, UnixStream);

pub enum OperatorMessage {
    WorkerInitCheckIn(u16),
    WorkerCheckIn(u16, String),
    WorkerCheckOut(String, ProcessorInput, UnixStream),
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnProcessors,
}

pub enum OperatorStatus {
    Starting,
    Running,
    Stopped,
}

pub enum ClientResponseMessage {
    OperatorIdConfirmation,
    ReturnProcessors(Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>),
}

pub struct OperatorNode {
    operator_id: String,
    _namespace: String,
    processors: Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u32, UnixStream>>>,
    workers_pool_tx: HashMap<u16, Sender<WorkerPoolMessage>>,
    processor_queues: HashMap<String, VecDeque<WorkItem>>,
    processor_workers: HashMap<String, u16>,
    available_workers: VecDeque<u16>,
    pending_processors: VecDeque<String>,
    operator_channel: Arc<Mutex<Sender<OperatorMessage>>>,
    status: OperatorStatus,
}

impl OperatorNode {
    pub fn new(namespace: String, otx: Sender<OperatorMessage>) -> OperatorNode {
        let processors = Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));
        let otx_arc = Arc::new(Mutex::new(otx));

        OperatorNode {
            operator_id: "undefined".to_string(),
            _namespace: namespace.clone(),
            processors: processors.clone(),
            shared_io_streams: shared_io_streams,
            workers_pool_tx: init_worker_pool(processors, otx_arc.clone()),
            processor_queues: HashMap::new(),
            processor_workers: HashMap::new(),
            available_workers: VecDeque::new(),
            pending_processors: VecDeque::new(),
            operator_channel: otx_arc.clone(),
            status: OperatorStatus::Starting,
        }
    }

    pub fn start_processor(
        &self,
        factory_id: String,
        processor_id: String,
    ) -> Option<ProcessorHandle> {
        if let Some(factory) = get_factory(factory_id) {
            let processor = factory(processor_id.clone());
            self.register_processor(processor_id.clone(), processor);
            self.get_processor(processor_id)
        } else {
            None
        }
    }

    fn register_processor(&self, id: String, processor: ProcessorBox) {
        self.processors
            .write()
            .unwrap()
            .insert(id, Arc::new(Mutex::new(processor)));
    }

    pub fn stop_processor(&self, id: String) {
        self.processors.write().unwrap().remove(&id);
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    pub fn get_processors(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>> {
        Arc::clone(&self.processors)
    }

    pub fn get_processor(&self, id: String) -> Option<ProcessorHandle> {
        self.processors
            .read()
            .unwrap()
            .get(&id)
            .cloned()
            .map(|processor| ProcessorHandle { processor })
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

    pub fn has_processor(&self, id: String) -> bool {
        self.processors.read().unwrap().contains_key(&id)
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
                OperatorMessage::WorkerCheckOut(processor_id, processor_input, stream) => {
                    handle_checkout(&mut operator, processor_id, processor_input, stream);
                }
                OperatorMessage::WorkerCheckIn(worker_id, worked_processor_id) => {
                    handle_checkin(&mut operator, worker_id, worked_processor_id);
                }
                // TODO: Do anything with metadata?
                OperatorMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(&mut operator, &control_stream, &payload_id, &message);
                }
                OperatorMessage::ReturnProcessors => {
                    return_processors(&operator, ctx_arc.clone());
                }
            }
        }
    })
}

fn return_processors(operator: &OperatorNode, ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>) {
    let processors = operator.get_processors();

    ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnProcessors(processors))
        .expect("Failed to send processors response");
}

fn handle_checkin(operator: &mut OperatorNode, worker_id: u16, worked_processor_id: String) {
    // Remove processor from active processing
    operator.processor_workers.remove(&worked_processor_id);

    // TODO: handle different worker types.
    // Check if this processor has queued work
    if let Some(queue) = operator.processor_queues.get_mut(&worked_processor_id) {
        if let Some((processor_input, stream)) = queue.pop_front() {
            // Process next item in this processor's queue with same worker (sticky assignment)
            operator
                .processor_workers
                .insert(worked_processor_id.clone(), worker_id);
            pool_worker_delegate(
                &operator.workers_pool_tx,
                worked_processor_id,
                processor_input,
                worker_id,
                stream,
            );
            return;
        } else {
            // Queue is empty, remove it
            operator.processor_queues.remove(&worked_processor_id);
        }
    }

    // No work for this processor, make worker available for other processors
    operator.available_workers.push_back(worker_id);

    // Optimized: Only check processors that have pending work
    while let Some(processor_id) = operator.pending_processors.pop_front() {
        // Check if processor is still idle and has work
        if !operator.processor_workers.contains_key(&processor_id) {
            // TODO: handle different worker types.
            if let Some(queue) = operator.processor_queues.get_mut(&processor_id) {
                if let Some((processor_input, stream)) = queue.pop_front() {
                    // Remove worker from available workers since we're assigning it
                    operator.available_workers.pop_back();
                    operator
                        .processor_workers
                        .insert(processor_id.clone(), worker_id);
                    pool_worker_delegate(
                        &operator.workers_pool_tx,
                        processor_id.clone(),
                        processor_input,
                        worker_id,
                        stream,
                    );

                    // If queue still has work, put it back in pending list
                    if !queue.is_empty() {
                        operator.pending_processors.push_back(processor_id.clone());
                    } else {
                        // Queue is empty, remove it
                        operator.processor_queues.remove(&processor_id);
                    }
                    return;
                }
            }
        } else {
            // Processor is busy, put it back for later
            operator.pending_processors.push_back(processor_id);
            break;
        }
    }
}

fn handle_checkout(
    operator: &mut OperatorNode,
    processor_id: String,
    processor_input: ProcessorInput,
    stream: UnixStream,
) {
    let processors_read = operator.processors.read().unwrap();

    if let Some(c) = processors_read.get(processor_id.as_str()) {
        let processor = c.lock().unwrap();
        let runner_type = processor.get_runner_type();

        match runner_type {
            ProcessorRunner::ThreadPool => {
                // Check if processor is already being processed
                if operator.processor_workers.contains_key(&processor_id) {
                    // Processor busy, add to its specific queue
                    let queue = operator
                        .processor_queues
                        .entry(processor_id.clone())
                        .or_insert_with(VecDeque::new);
                    let is_first_item = queue.is_empty();
                    queue.push_back((processor_input, stream));

                    // Track that this processor has pending work
                    if is_first_item {
                        operator.pending_processors.push_back(processor_id);
                    }
                } else if let Some(worker_id) = operator.available_workers.pop_front() {
                    // Processor available and worker available, assign immediately
                    operator
                        .processor_workers
                        .insert(processor_id.clone(), worker_id);
                    pool_worker_delegate(
                        &operator.workers_pool_tx,
                        processor_id,
                        processor_input,
                        worker_id,
                        stream,
                    );
                } else {
                    // No available workers, add to processor queue
                    let queue = operator
                        .processor_queues
                        .entry(processor_id.clone())
                        .or_insert_with(VecDeque::new);
                    let is_first_item = queue.is_empty();
                    queue.push_back((processor_input, stream));

                    // Track that this processor has pending work
                    if is_first_item {
                        operator.pending_processors.push_back(processor_id);
                    }
                }
            }
            ProcessorRunner::ThreadSpawn => {
                executor::exec_agent(
                    stream,
                    &processor_id,
                    &operator.get_processors(),
                    processor_input,
                );
            }
        }
    } else {
        eprint!(
            "Processor {} not found in operator {}",
            processor_id, operator.operator_id
        );
    }
}
