use crossbeam::channel::Sender;
use std::primitive::u16;
use std::sync::RwLock;
use std::thread;
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
};

use crate::compute_engine::ComputeEngineMessage;
use crate::processor::executor;
use crate::processor::factory_registry::ProcessorBox;
use flz_transport::transport::FlezhaStream;
use crate::ProcessorInput;

pub enum WorkerPoolMessage {
    HandleWork(String, ProcessorInput, Arc<FlezhaStream>),
}

pub fn init_worker_pool(
    processors: Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>,
    ctx: Arc<Mutex<Sender<ComputeEngineMessage>>>,
) -> HashMap<u16, Sender<WorkerPoolMessage>> {
    let worker_count = number_of_workers();
    let mut worker_channels: HashMap<u16, Sender<WorkerPoolMessage>> = HashMap::new();

    for i in 0..worker_count {
        let processors = Arc::clone(&processors);
        let ctx_clone = ctx.lock().unwrap().clone();
        let (wtx, wrx) = crossbeam::channel::unbounded();
        worker_channels.insert(i, wtx);

        thread::spawn(move || {
            let worker_id = i;

            check_in(
                &ctx_clone,
                ComputeEngineMessage::WorkerInitCheckIn(worker_id),
            );

            loop {
                let message = wrx.recv().expect("Failed to receive message");

                match message {
                    WorkerPoolMessage::HandleWork(processor_id, processor_input, stream) => {
                        executor::exec_agent(
                            stream,
                            &processor_id,
                            &processors,
                            processor_input,
                        );
                        check_in(
                            &ctx_clone,
                            ComputeEngineMessage::WorkerCheckIn(worker_id, processor_id),
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
    processor_id: String,
    processor_input: ProcessorInput,
    worker_id: u16,
    stream: Arc<FlezhaStream>,
) {
    workers_pool_tx
        .get(&worker_id)
        .unwrap()
        .send(WorkerPoolMessage::HandleWork(
            processor_id,
            processor_input,
            stream,
        ))
        .expect("Failed to send work to worker");
}

fn check_in(ctx: &Sender<ComputeEngineMessage>, message: ComputeEngineMessage) {
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
