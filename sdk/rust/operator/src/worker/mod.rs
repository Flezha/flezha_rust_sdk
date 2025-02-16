mod worker_input;

use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};
use std::io::Write;
use serde_json::Value;

use crate::service::payloader;

pub use worker_input::WorkerInput;

pub enum WorkerOutput {
    None,
    Forward(WorkerResult),
    Return(WorkerResult),
}

pub enum WorkerResult {
    Text(String),
    Binary(Vec<u8>),
}

pub trait Worker: SerializableWorker  {
    fn run(&mut self, input: WorkerInput) -> WorkerOutput;
}

pub trait SerializableWorker: Send + Sync  {
    fn update_worker(&mut self, map: &HashMap<String, Value>);
    fn serialize_worker(&self) -> Vec<u8>;
    fn deserialize_worker(&self, data: &[u8]) -> Box<dyn SerializableWorker>;
    fn to_json_string(&self) -> String;
}

pub fn exec(
    worker_id: &String, 
    workers: &Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Worker>>>>>>,
    stream: &mut UnixStream,
    input: WorkerInput,
) -> Option<WorkerResult> {
    let workers_read = workers.read().unwrap();
                
    if let Some(c) = workers_read.get(worker_id) {
        let mut worker = c.lock().unwrap();
        let worker_result= worker.run(input);

        handle_worker_result(worker_result, stream, worker_id)
    } else {
        panic!("Worker with given ID not found {}", worker_id);
    }
}

fn handle_worker_result(worker_result: WorkerOutput, stream: &mut UnixStream, worker_id: &String) -> Option<WorkerResult> {
    match worker_result {
        WorkerOutput::None => None,
        WorkerOutput::Return(res) => Some(res),
        WorkerOutput::Forward(res) => {
            send_result(stream, &worker_id, &res);
            Some(res)
        }
    }
}

fn send_result(stream: &mut UnixStream, worker_id: &String, result: &WorkerResult) {
    let payload = match result {
        WorkerResult::Text(text) => {
            payloader::build_from_str(&worker_id, text)
        }
        WorkerResult::Binary(binary) => {
            payloader::build_from_binary(&worker_id, binary)
        }
    };

    if let Err(err) = stream.write_all(&payload) {
        eprintln!("Error writing to stream: {}", err);
    }
}