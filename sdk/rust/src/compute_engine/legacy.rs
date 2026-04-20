//! Pre-change ("legacy") dispatch path. Kept byte-for-byte behind
//! `#[cfg(not(feature = "fast-dispatch"))]` during rollout (see tasks 5.4,
//! 10.x). All logic here matches what shipped prior to the optimize-rust-sdk-
//! dispatch change.

#![cfg(not(feature = "fast-dispatch"))]

use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use flz_transport::transport::FlezhaStream;
use flz_transport::transport::quic::QuicClient;

use crate::processor::factory_registry::{get_factory, ProcessorBox, ProcessorHandle};

use crate::compute_engine::worker_pool::{
    init_worker_pool, pool_worker_delegate, WorkerPoolMessage,
};
use crate::processor::{executor, ProcessorRunner};
use crate::{command, ProcessorInput};

use super::{ClientResponseMessage, ComputeEngineMessage, ComputeEngineStatus};
use super::control_unit;

type WorkItem = (ProcessorInput, Arc<FlezhaStream>);

pub struct ComputeEngineNode {
    compute_engine_id: String,
    _namespace: String,
    processors: Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    workers_pool_tx: HashMap<u16, Sender<WorkerPoolMessage>>,
    processor_queues: HashMap<String, VecDeque<WorkItem>>,
    processor_workers: HashMap<String, u16>,
    available_workers: VecDeque<u16>,
    pending_processors: VecDeque<String>,
    compute_engine_channel: Arc<Mutex<Sender<ComputeEngineMessage>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: ComputeEngineStatus,
}

impl ComputeEngineNode {
    pub fn new(namespace: String, otx: Sender<ComputeEngineMessage>) -> ComputeEngineNode {
        let processors = Arc::new(RwLock::new(HashMap::new()));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));
        let otx_arc = Arc::new(Mutex::new(otx));

        ComputeEngineNode {
            compute_engine_id: "undefined".to_string(),
            _namespace: namespace.clone(),
            processors: processors.clone(),
            shared_io_streams,
            workers_pool_tx: init_worker_pool(processors, otx_arc.clone()),
            processor_queues: HashMap::new(),
            processor_workers: HashMap::new(),
            available_workers: VecDeque::new(),
            pending_processors: VecDeque::new(),
            compute_engine_channel: otx_arc.clone(),
            quic_client: None,
            status: ComputeEngineStatus::Starting,
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

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u32, FlezhaStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_compute_engine_id(&self) -> String {
        self.compute_engine_id.clone()
    }

    pub fn assign_compute_engine_id(&mut self, compute_engine_id: String) {
        self.compute_engine_id = compute_engine_id;
    }

    pub fn set_status(&mut self, status: ComputeEngineStatus) {
        self.status = status;
    }

    pub fn has_processor(&self, id: String) -> bool {
        self.processors.read().unwrap().contains_key(&id)
    }

    pub fn get_compute_engine_channel(&self) -> Arc<Mutex<Sender<ComputeEngineMessage>>> {
        Arc::clone(&self.compute_engine_channel)
    }

    pub fn get_quic_client(&self) -> Option<Arc<QuicClient>> {
        self.quic_client.clone()
    }

    pub fn set_quic_client(&mut self, client: Option<Arc<QuicClient>>) {
        self.quic_client = client;
    }
}

pub fn start(
    namespace: String,
    (otx, orx): (Sender<ComputeEngineMessage>, Receiver<ComputeEngineMessage>),
    ctx: Sender<ClientResponseMessage>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut compute_engine = ComputeEngineNode::new(namespace.clone(), otx.clone());
        let ctx_arc = Arc::new(Mutex::new(ctx));
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, mut control_stream, quic_client) =
            control_unit::open(namespace.clone(), ctx_arc.clone(), otx_arc.clone());
        compute_engine.set_quic_client(quic_client);

        loop {
            let message = orx.recv().expect("Failed to receive coordinator message");

            match message {
                ComputeEngineMessage::WorkerInitCheckIn(worker_id) => {
                    compute_engine.available_workers.push_back(worker_id);
                }
                ComputeEngineMessage::WorkerCheckOut(processor_id, processor_input, stream) => {
                    handle_checkout(&mut compute_engine, processor_id, processor_input, stream);
                }
                ComputeEngineMessage::WorkerCheckIn(worker_id, worked_processor_id) => {
                    handle_checkin(&mut compute_engine, worker_id, worked_processor_id);
                }
                ComputeEngineMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(
                        &mut compute_engine,
                        &control_stream,
                        &payload_id,
                        &message,
                    );
                }
                ComputeEngineMessage::ReturnProcessors => {
                    return_processors(&compute_engine, ctx_arc.clone());
                }
                ComputeEngineMessage::Reconnect => {
                    if matches!(compute_engine.status, ComputeEngineStatus::Stopped) {
                        continue;
                    }
                    handle_reconnect(
                        &mut compute_engine,
                        &mut control_stream,
                        &namespace,
                        &otx_arc,
                    );
                }
            }
        }
    })
}

fn return_processors(
    compute_engine: &ComputeEngineNode,
    ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>,
) {
    let processors = compute_engine.get_processors();

    ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnProcessors(processors))
        .expect("Failed to send processors response");
}

fn handle_reconnect(
    compute_engine: &mut ComputeEngineNode,
    control_stream: &mut Arc<FlezhaStream>,
    namespace: &str,
    otx_arc: &Arc<Mutex<Sender<ComputeEngineMessage>>>,
) {
    compute_engine.set_status(ComputeEngineStatus::Stopped);

    compute_engine.get_io_streams().write().unwrap().clear();
    compute_engine.get_processors().write().unwrap().clear();
    compute_engine.processor_queues.clear();
    compute_engine.processor_workers.clear();
    compute_engine.pending_processors.clear();
    compute_engine.assign_compute_engine_id("undefined".to_string());

    let (_, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), otx_arc.clone());

    *control_stream = new_stream;
    compute_engine.set_quic_client(new_quic);
}

fn handle_checkin(
    compute_engine: &mut ComputeEngineNode,
    worker_id: u16,
    worked_processor_id: String,
) {
    compute_engine
        .processor_workers
        .remove(&worked_processor_id);

    if let Some(queue) = compute_engine
        .processor_queues
        .get_mut(&worked_processor_id)
    {
        if let Some((processor_input, stream)) = queue.pop_front() {
            compute_engine
                .processor_workers
                .insert(worked_processor_id.clone(), worker_id);
            pool_worker_delegate(
                &compute_engine.workers_pool_tx,
                worked_processor_id,
                processor_input,
                worker_id,
                stream,
            );
            return;
        } else {
            compute_engine.processor_queues.remove(&worked_processor_id);
        }
    }

    compute_engine.available_workers.push_back(worker_id);

    while let Some(processor_id) = compute_engine.pending_processors.pop_front() {
        if !compute_engine.processor_workers.contains_key(&processor_id) {
            if let Some(queue) = compute_engine.processor_queues.get_mut(&processor_id) {
                if let Some((processor_input, stream)) = queue.pop_front() {
                    compute_engine.available_workers.pop_back();
                    compute_engine
                        .processor_workers
                        .insert(processor_id.clone(), worker_id);
                    pool_worker_delegate(
                        &compute_engine.workers_pool_tx,
                        processor_id.clone(),
                        processor_input,
                        worker_id,
                        stream,
                    );

                    if !queue.is_empty() {
                        compute_engine
                            .pending_processors
                            .push_back(processor_id.clone());
                    } else {
                        compute_engine.processor_queues.remove(&processor_id);
                    }
                    return;
                }
            }
        } else {
            compute_engine.pending_processors.push_back(processor_id);
            break;
        }
    }
}

fn handle_checkout(
    compute_engine: &mut ComputeEngineNode,
    processor_id: String,
    processor_input: ProcessorInput,
    stream: Arc<FlezhaStream>,
) {
    let processors_read = compute_engine.processors.read().unwrap();

    if let Some(c) = processors_read.get(processor_id.as_str()) {
        let processor = c.lock().unwrap();
        let runner_type = processor.get_runner_type();

        match runner_type {
            ProcessorRunner::ThreadPool => {
                if compute_engine.processor_workers.contains_key(&processor_id) {
                    let queue = compute_engine
                        .processor_queues
                        .entry(processor_id.clone())
                        .or_insert_with(VecDeque::new);
                    let is_first_item = queue.is_empty();
                    queue.push_back((processor_input, stream));

                    if is_first_item {
                        compute_engine.pending_processors.push_back(processor_id);
                    }
                } else if let Some(worker_id) = compute_engine.available_workers.pop_front() {
                    compute_engine
                        .processor_workers
                        .insert(processor_id.clone(), worker_id);
                    pool_worker_delegate(
                        &compute_engine.workers_pool_tx,
                        processor_id,
                        processor_input,
                        worker_id,
                        stream,
                    );
                } else {
                    let queue = compute_engine
                        .processor_queues
                        .entry(processor_id.clone())
                        .or_insert_with(VecDeque::new);
                    let is_first_item = queue.is_empty();
                    queue.push_back((processor_input, stream));

                    if is_first_item {
                        compute_engine.pending_processors.push_back(processor_id);
                    }
                }
            }
            ProcessorRunner::ThreadSpawn => {
                executor::exec_agent(
                    stream,
                    &processor_id,
                    &compute_engine.get_processors(),
                    processor_input,
                );
            }
        }
    } else {
        eprint!(
            "Processor {} not found in compute_engine {}",
            processor_id, compute_engine.compute_engine_id
        );
    }
}
