use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, HashSet, VecDeque};
use std::os::unix::net::UnixStream;
use std::{
    sync::{Arc, Mutex},
    thread,
};

use crate::pipeline::runner;
use crate::{
    node::{
        control_unit,
        worker_pool::{init_worker_pool, pool_worker_delegate, WorkerPoolMessage},
        NodeAction, OperatorNode,
    },
    PipelineInput,
};

pub enum WorkerType {
    ThreadPool,
    ThreadSpawn,
}

pub enum CoordinatorMessage {
    WorkerInitCheckIn(u16),
    WorkerCheckIn(u16, String),
    WorkerCheckOut(String, PipelineInput, WorkerType, UnixStream),
}

type WorkItem = (PipelineInput, WorkerType, UnixStream);

struct Coordinator {
    operator: Arc<Mutex<OperatorNode>>,
    workers_pool_tx: HashMap<u16, Sender<WorkerPoolMessage>>,
    pipeline_queues: HashMap<String, VecDeque<WorkItem>>,
    pipeline_workers: HashMap<String, u16>, // Track which worker is processing each pipeline
    available_workers: VecDeque<u16>,
    // Optimization: Track which pipelines have work to avoid full iteration
    pending_pipelines: VecDeque<String>,
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
        let pipelines = operator_arc.lock().unwrap().get_pipelines();
        let crx_guard = crx.lock().unwrap();

        let mut coordinator = Coordinator {
            operator: operator_arc,
            workers_pool_tx: init_worker_pool(pipelines, ctx),
            pipeline_queues: HashMap::new(),
            pipeline_workers: HashMap::new(),
            available_workers: VecDeque::new(),
            pending_pipelines: VecDeque::new(),
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
                    pipeline_id,
                    pipeline_input,
                    worker_type,
                    stream,
                ) => {
                    handle_checkout(
                        &mut coordinator,
                        pipeline_id,
                        pipeline_input,
                        worker_type,
                        stream,
                    );
                }
                CoordinatorMessage::WorkerCheckIn(worker_id, worked_pipeline_id) => {
                    handle_checkin(&mut coordinator, worker_id, worked_pipeline_id);
                }
            }
        }
    });
}

fn handle_checkout(
    coordinator: &mut Coordinator,
    pipeline_id: String,
    pipeline_input: PipelineInput,
    worker_type: WorkerType,
    stream: UnixStream,
) {
    match worker_type {
        WorkerType::ThreadPool => {
            // Check if pipeline is already being processed
            if coordinator.pipeline_workers.contains_key(&pipeline_id) {
                // Pipeline busy, add to its specific queue
                let queue = coordinator
                    .pipeline_queues
                    .entry(pipeline_id.clone())
                    .or_insert_with(VecDeque::new);
                let is_first_item = queue.is_empty();
                queue.push_back((pipeline_input, worker_type, stream));
                
                // Track that this pipeline has pending work
                if is_first_item {
                    coordinator.pending_pipelines.push_back(pipeline_id);
                }
            } else if let Some(worker_id) = coordinator.available_workers.pop_front() {
                // Pipeline available and worker available, assign immediately
                coordinator.pipeline_workers.insert(pipeline_id.clone(), worker_id);
                pool_worker_delegate(
                    &coordinator.workers_pool_tx,
                    pipeline_id,
                    pipeline_input,
                    worker_id,
                    stream,
                );
            } else {
                // No available workers, add to pipeline queue
                let queue = coordinator
                    .pipeline_queues
                    .entry(pipeline_id.clone())
                    .or_insert_with(VecDeque::new);
                let is_first_item = queue.is_empty();
                queue.push_back((pipeline_input, worker_type, stream));
                
                // Track that this pipeline has pending work
                if is_first_item {
                    coordinator.pending_pipelines.push_back(pipeline_id);
                }
            }
        }
        WorkerType::ThreadSpawn => {
            let pipelines = coordinator.operator.lock().unwrap().get_pipelines();
            thread::spawn(move || {
                runner::exec_agent(stream, &pipeline_id, &pipelines, pipeline_input)
            });
        }
    }
}

fn handle_checkin(coordinator: &mut Coordinator, worker_id: u16, worked_pipeline_id: String) {
    // Remove pipeline from active processing
    coordinator.pipeline_workers.remove(&worked_pipeline_id);

    // Check if this pipeline has queued work
    if let Some(queue) = coordinator.pipeline_queues.get_mut(&worked_pipeline_id) {
        if let Some((pipeline_input, worker_type, stream)) = queue.pop_front() {
            // Process next item in this pipeline's queue with same worker (sticky assignment)
            coordinator.pipeline_workers.insert(worked_pipeline_id.clone(), worker_id);
            pool_worker_delegate(
                &coordinator.workers_pool_tx,
                worked_pipeline_id,
                pipeline_input,
                worker_id,
                stream,
            );
            return;
        } else {
            // Queue is empty, remove it
            coordinator.pipeline_queues.remove(&worked_pipeline_id);
        }
    }

    // No work for this pipeline, make worker available for other pipelines
    coordinator.available_workers.push_back(worker_id);

    // Optimized: Only check pipelines that have pending work
    while let Some(pipeline_id) = coordinator.pending_pipelines.pop_front() {
        // Check if pipeline is still idle and has work
        if !coordinator.pipeline_workers.contains_key(&pipeline_id) {
            if let Some(queue) = coordinator.pipeline_queues.get_mut(&pipeline_id) {
                if let Some((pipeline_input, worker_type, stream)) = queue.pop_front() {
                    // Remove worker from available workers since we're assigning it
                    coordinator.available_workers.pop_back();
                    coordinator.pipeline_workers.insert(pipeline_id.clone(), worker_id);
                    pool_worker_delegate(
                        &coordinator.workers_pool_tx,
                        pipeline_id.clone(),
                        pipeline_input,
                        worker_id,
                        stream,
                    );
                    
                    // If queue still has work, put it back in pending list
                    if !queue.is_empty() {
                        coordinator.pending_pipelines.push_back(pipeline_id.clone());
                    } else {
                        // Queue is empty, remove it
                        coordinator.pipeline_queues.remove(&pipeline_id);
                    }
                    return;
                }
            }
        } else {
            // Pipeline is busy, put it back for later
            coordinator.pending_pipelines.push_back(pipeline_id);
            break;
        }
    }
}
