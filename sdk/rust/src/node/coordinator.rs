use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, VecDeque};
use std::os::unix::net::UnixStream;
use std::{
    sync::{Arc, Mutex},
    thread,
};

use crate::computation::runner;
use crate::{
    node::{
        control_unit,
        worker_pool::{init_worker_pool, pool_worker_delegate, WorkerPoolMessage},
        NodeAction, OperatorNode,
    },
    ComputationInput,
};

pub enum WorkerType {
    ThreadPool,
    ThreadSpawn,
}

pub enum CoordinatorMessage {
    WorkerInitCheckIn(u16),
    WorkerCheckIn(u16, String),
    WorkerCheckOut(String, ComputationInput, WorkerType, UnixStream),
}

type WorkItem = (ComputationInput, WorkerType, UnixStream);

struct Coordinator {
    operator: Arc<Mutex<OperatorNode>>,
    workers_pool_tx: HashMap<u16, Sender<WorkerPoolMessage>>,
    computation_queues: HashMap<String, VecDeque<WorkItem>>,
    computation_workers: HashMap<String, u16>, // Track which worker is processing each computation
    available_workers: VecDeque<u16>,
    // Optimization: Track which computations have work to avoid full iteration
    pending_computations: VecDeque<String>,
}

pub fn start(
    namespace: String,
    crx: Arc<Mutex<Receiver<CoordinatorMessage>>>,
    itx: Arc<Mutex<Sender<NodeAction>>>,
    operator_arc: Arc<Mutex<OperatorNode>>,
) {
    thread::spawn(move || {
        let _control_thread = control_unit::open(namespace, itx, operator_arc.clone());
        let ctx = operator_arc.lock().unwrap().coordinator_channel.clone();
        let computations = operator_arc.lock().unwrap().get_computations();
        let crx_guard = crx.lock().unwrap();

        let mut coordinator = Coordinator {
            operator: operator_arc,
            workers_pool_tx: init_worker_pool(computations, ctx),
            computation_queues: HashMap::new(),
            computation_workers: HashMap::new(),
            available_workers: VecDeque::new(),
            pending_computations: VecDeque::new(),
        };

        loop {
            let message = crx_guard
                .recv()
                .expect("Failed to receive coordinator message");

            match message {
                CoordinatorMessage::WorkerInitCheckIn(worker_id) => {
                    coordinator.available_workers.push_back(worker_id);
                }
                CoordinatorMessage::WorkerCheckOut(
                    computation_id,
                    computation_input,
                    worker_type,
                    stream,
                ) => {
                    handle_checkout(
                        &mut coordinator,
                        computation_id,
                        computation_input,
                        worker_type,
                        stream,
                    );
                }
                CoordinatorMessage::WorkerCheckIn(worker_id, worked_computation_id) => {
                    handle_checkin(&mut coordinator, worker_id, worked_computation_id);
                }
            }
        }
    });
}

fn handle_checkout(
    coordinator: &mut Coordinator,
    computation_id: String,
    computation_input: ComputationInput,
    worker_type: WorkerType,
    stream: UnixStream,
) {
    match worker_type {
        WorkerType::ThreadPool => {
            // Check if computation is already being processed
            if coordinator.computation_workers.contains_key(&computation_id) {
                // Computation busy, add to its specific queue
                let queue = coordinator
                    .computation_queues
                    .entry(computation_id.clone())
                    .or_insert_with(VecDeque::new);
                let is_first_item = queue.is_empty();
                queue.push_back((computation_input, worker_type, stream));
                
                // Track that this computation has pending work
                if is_first_item {
                    coordinator.pending_computations.push_back(computation_id);
                }
            } else if let Some(worker_id) = coordinator.available_workers.pop_front() {
                // Computation available and worker available, assign immediately
                coordinator.computation_workers.insert(computation_id.clone(), worker_id);
                pool_worker_delegate(
                    &coordinator.workers_pool_tx,
                    computation_id,
                    computation_input,
                    worker_id,
                    stream,
                );
            } else {
                // No available workers, add to computation queue
                let queue = coordinator
                    .computation_queues
                    .entry(computation_id.clone())
                    .or_insert_with(VecDeque::new);
                let is_first_item = queue.is_empty();
                queue.push_back((computation_input, worker_type, stream));
                
                // Track that this computation has pending work
                if is_first_item {
                    coordinator.pending_computations.push_back(computation_id);
                }
            }
        }
        WorkerType::ThreadSpawn => {
            let computations = coordinator.operator.lock().unwrap().get_computations();
            thread::spawn(move || {
                runner::exec_agent(stream, &computation_id, &computations, computation_input)
            });
        }
    }
}

fn handle_checkin(coordinator: &mut Coordinator, worker_id: u16, worked_computation_id: String) {
    // Remove computation from active processing
    coordinator.computation_workers.remove(&worked_computation_id);

    // TODO: handle different worker types.
    // Check if this computation has queued work
    if let Some(queue) = coordinator.computation_queues.get_mut(&worked_computation_id) {
        if let Some((computation_input, _worker_type, stream)) = queue.pop_front() {
            // Process next item in this computation's queue with same worker (sticky assignment)
            coordinator.computation_workers.insert(worked_computation_id.clone(), worker_id);
            pool_worker_delegate(
                &coordinator.workers_pool_tx,
                worked_computation_id,
                computation_input,
                worker_id,
                stream,
            );
            return;
        } else {
            // Queue is empty, remove it
            coordinator.computation_queues.remove(&worked_computation_id);
        }
    }

    // No work for this computation, make worker available for other computations
    coordinator.available_workers.push_back(worker_id);

    // Optimized: Only check computations that have pending work
    while let Some(computation_id) = coordinator.pending_computations.pop_front() {
        // Check if computation is still idle and has work
        if !coordinator.computation_workers.contains_key(&computation_id) {
            // TODO: handle different worker types.
            if let Some(queue) = coordinator.computation_queues.get_mut(&computation_id) {
                if let Some((computation_input, _worker_type, stream)) = queue.pop_front() {
                    // Remove worker from available workers since we're assigning it
                    coordinator.available_workers.pop_back();
                    coordinator.computation_workers.insert(computation_id.clone(), worker_id);
                    pool_worker_delegate(
                        &coordinator.workers_pool_tx,
                        computation_id.clone(),
                        computation_input,
                        worker_id,
                        stream,
                    );
                    
                    // If queue still has work, put it back in pending list
                    if !queue.is_empty() {
                        coordinator.pending_computations.push_back(computation_id.clone());
                    } else {
                        // Queue is empty, remove it
                        coordinator.computation_queues.remove(&computation_id);
                    }
                    return;
                }
            }
        } else {
            // Computation is busy, put it back for later
            coordinator.pending_computations.push_back(computation_id);
            break;
        }
    }
}
