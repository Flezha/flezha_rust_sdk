use std::os::unix::net::UnixStream;
use std::primitive::u16;
use std::sync::RwLock;
use std::thread;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
};
use crossbeam::channel::Sender;

use crate::pipeline::factory_registry::PipelineBox;
use crate::pipeline::runner;
use crate::{node::coordinator::CoordinatorMessage, PipelineInput};

pub enum WorkerPoolMessage {
    HandleWork(String, PipelineInput, UnixStream),
}

pub fn init_worker_pool(
    pipelines: Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>>,
    ctx: Arc<Mutex<Sender<CoordinatorMessage>>>,
) -> HashMap<u16, Sender<WorkerPoolMessage>> {
    let worker_count = number_of_workers();
    let mut worker_channels: HashMap<u16, Sender<WorkerPoolMessage>> = HashMap::new();

    for i in 0..worker_count {
        let pipelines = Arc::clone(&pipelines);
        let ctx_clone = ctx.lock().unwrap().clone();
        let (wtx, wrx) = crossbeam::channel::unbounded();
        worker_channels.insert(i, wtx);

        thread::spawn(move || {
            let worker_id = i;

            check_in(&ctx_clone, CoordinatorMessage::WorkerInitCheckIn(worker_id));

            loop {
                let message = wrx.recv().expect("Failed to receive message");

                match message {
                    WorkerPoolMessage::HandleWork(pipeline_id, pipeline_input, unix_stream) => {
                        runner::exec_agent(unix_stream, &pipeline_id, &pipelines, pipeline_input);
                        check_in(&ctx_clone, CoordinatorMessage::WorkerCheckIn(worker_id, pipeline_id));
                    }
                }
            }
        });
    }

    worker_channels
}

pub fn pool_worker_delegate(
    occupied_pipelines: &mut Vec<String>,
    workers_pool_tx: &HashMap<u16, Sender<WorkerPoolMessage>>,
    pipeline_id: String,
    pipeline_input: PipelineInput,
    worker_id: u16,
    stream: UnixStream,
) {
    // TODO: remove instant
    occupied_pipelines.push(pipeline_id.clone());
    workers_pool_tx
        .get(&worker_id)
        .unwrap()
        .send(WorkerPoolMessage::HandleWork(
            pipeline_id,
            pipeline_input,
            stream,
        ))
        .expect("Failed to send work to worker");
}

fn check_in(ctx: &Sender<CoordinatorMessage>, message: CoordinatorMessage) {
    ctx.send(message).expect("Failed to send message to coordinator");
}

fn number_of_workers() -> u16 {
    let key = "FLEZHA_WORKER_POOL_SIZE";
    let numb_of_worker = match env::var(key) {
        Ok(val) => val.parse().unwrap_or(10),
        Err(_) => 10,
    };

    if numb_of_worker < 1 {
        panic!(
            "Invalid number of workers specified in {}: {}",
            key, numb_of_worker
        );
    }

    if numb_of_worker > u16::MAX as u32 {
        panic!("Number of workers exceeds maximum limit: {}", u16::MAX);
    }

    numb_of_worker as u16
}
