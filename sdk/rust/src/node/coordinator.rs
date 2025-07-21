use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, VecDeque};
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

struct Coordinator {
    operator: Arc<Mutex<OperatorNode>>,
    workers_pool_tx: HashMap<u16, Sender<WorkerPoolMessage>>,
    occupied_pipelines: Vec<(String, std::time::Instant)>,
    available_workers: VecDeque<u16>,
    work_queue: VecDeque<(String, PipelineInput, WorkerType, UnixStream)>,
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
            occupied_pipelines: vec![],
            available_workers: VecDeque::new(),
            work_queue: VecDeque::new(),
        };

        loop {
            let message = crx_guard
                .recv()
                .expect("Failed to receive coordinator message");

            // TODO: remove
            let start = std::time::Instant::now();

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

            // println!(
            //     "Coordinator processed message in {:?}",
            //     start.elapsed()
            // );
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
    // Make sure the pipeline is not already occupied to keep the order of execution.
    // TODO: replace back with contains
    if !coordinator.occupied_pipelines.iter().any(|(id, _)| id == &pipeline_id) {
        match worker_type {
            WorkerType::ThreadPool => {
                // If there are available workers and the pipeline is not already occupied, assign a worker.
                if let Some(worker_id) = coordinator.available_workers.pop_front() {
                    pool_worker_delegate(
                        &mut coordinator.occupied_pipelines,
                        &coordinator.workers_pool_tx,
                        pipeline_id,
                        pipeline_input,
                        worker_id,
                        stream,
                    )
                } else {
                    println!(
                        "No available workers for pipeline {}. Adding to work queue.",
                        pipeline_id
                    );

                    // No available workers, add to work queue.
                    coordinator.work_queue.push_back((
                        pipeline_id,
                        pipeline_input,
                        worker_type,
                        stream,
                    ));
                }
            }
            WorkerType::ThreadSpawn => {
                let pipelines = coordinator.operator.lock().unwrap().get_pipelines();
                thread::spawn(move || {
                    runner::exec_agent(stream, &pipeline_id, &pipelines, pipeline_input)
                });
            }
        }
    } else {
        // If the pipeline is already occupied, add to work queue.
        coordinator
            .work_queue
            .push_back((pipeline_id, pipeline_input, worker_type, stream));
    }
}

fn handle_checkin(coordinator: &mut Coordinator, worker_id: u16, worked_pipeline_id: String) {
    // Append the worker back to available workers.
    coordinator.available_workers.push_back(worker_id);

    // Remove the pipeline from occupied pipelines.
    if let Some(pos) = coordinator
        .occupied_pipelines
        .iter()
        .position(|(x, y)| *x == worked_pipeline_id)
    {

        // TODO: remove
        let elapsed = coordinator.occupied_pipelines[pos].1.elapsed().as_millis();

        if elapsed >= 30 {
        // TODO: remove the instant printing.
        println!(
            "Pipeline {} finished processing in {:?}",
            worked_pipeline_id,
            elapsed
        );
        }

        coordinator.occupied_pipelines.remove(pos);
    }

    // TODO: replace with contains
    // Find the next available work in the queue.
    let next_work = coordinator
        .work_queue
        .iter()
        .position(|(pipe_id, _, _, _)| !coordinator.occupied_pipelines.iter().any(|(id, _)| id == pipe_id));

    if let Some(index) = next_work {
        let removed_item = coordinator.work_queue.remove(index);

        if let Some((pipe_id, pipe_input, processing_type, stream)) = removed_item {
            handle_checkout(coordinator, pipe_id, pipe_input, processing_type, stream);
        }
    }
}
