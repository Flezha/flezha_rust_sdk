use crossbeam::channel::Sender;
use std::os::unix::net::UnixStream;
use std::primitive::u16;
use std::sync::RwLock;
use std::thread;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
};

use crate::computation::factory_registry::ComputationBox;
use crate::computation::runner;
use crate::{node::coordinator::CoordinatorMessage, ComputationInput};

pub enum WorkerPoolMessage {
    HandleWork(String, ComputationInput, UnixStream),
}

pub fn init_worker_pool(
    computations: Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
    ctx: Arc<Mutex<Sender<CoordinatorMessage>>>,
) -> HashMap<u16, Sender<WorkerPoolMessage>> {
    let worker_count = number_of_workers();
    let mut worker_channels: HashMap<u16, Sender<WorkerPoolMessage>> = HashMap::new();

    for i in 0..worker_count {
        let computations = Arc::clone(&computations);
        let ctx_clone = ctx.lock().unwrap().clone();
        let (wtx, wrx) = crossbeam::channel::unbounded();
        worker_channels.insert(i, wtx);

        thread::spawn(move || {
            let worker_id = i;

            check_in(&ctx_clone, CoordinatorMessage::WorkerInitCheckIn(worker_id));

            loop {
                let message = wrx.recv().expect("Failed to receive message");

                match message {
                    WorkerPoolMessage::HandleWork(computation_id, computation_input, unix_stream) => {
                        runner::exec_agent(unix_stream, &computation_id, &computations, computation_input);
                        check_in(
                            &ctx_clone,
                            CoordinatorMessage::WorkerCheckIn(worker_id, computation_id),
                        );
                    }
                }
            }
        });
    }

    worker_channels
}

pub fn pool_worker_delegate(
    workers_pool_tx: &HashMap<u16, Sender<WorkerPoolMessage>>,
    computation_id: String,
    computation_input: ComputationInput,
    worker_id: u16,
    stream: UnixStream,
) {
    workers_pool_tx
        .get(&worker_id)
        .unwrap()
        .send(WorkerPoolMessage::HandleWork(
            computation_id,
            computation_input,
            stream,
        ))
        .expect("Failed to send work to worker");
}

fn check_in(ctx: &Sender<CoordinatorMessage>, message: CoordinatorMessage) {
    ctx.send(message)
        .expect("Failed to send message to coordinator");
}

/// Determines the number of workers for the pool.
///
/// The number of workers is determined in the following order:
/// 1. The value of the `FLEZHA_WORKER_POOL_SIZE` environment variable, if set and valid.
/// 2. The number of available CPU cores, as a fallback.
///
/// # Panics
///
/// This function will panic if the determined number of workers is less than 1
/// or greater than `u16::MAX`.
fn number_of_workers() -> u16 {
    let key = "FLEZHA_WORKER_POOL_SIZE";
    let default_workers = std::thread::available_parallelism().map_or(2, |v| v.get());

    let numb_of_worker = match env::var(key) {
        Ok(val) => val.parse().unwrap_or(default_workers),
        Err(_) => default_workers,
    };

    if numb_of_worker < 1 {
        panic!(
            "Invalid number of workers specified in {}: {}",
            key, numb_of_worker
        );
    }

    if numb_of_worker > u16::MAX as usize {
        panic!("Number of workers exceeds maximum limit: {}", u16::MAX);
    }

    numb_of_worker as u16
}
