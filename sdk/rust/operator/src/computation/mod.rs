mod computation_input;

use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};
use std::io::Write;
use serde_json::Value;

use crate::service::payloader;

pub use computation_input::ComputationInput;

pub enum ComputationOutput {
    None,
    Forward(ComputationResult),
    Return(ComputationResult),
}

pub enum ComputationResult {
    Text(String),
    Binary(Vec<u8>),
}

pub trait Computation: SerializableComputation  {
    fn run(&mut self, input: ComputationInput) -> ComputationOutput;
}

pub trait SerializableComputation: Send + Sync  {
    fn update_computation(&mut self, map: &HashMap<String, Value>);
    fn serialize_computation(&self) -> Vec<u8>;
    fn deserialize_computation(&self, data: &[u8]) -> Box<dyn SerializableComputation>;
    fn to_json_string(&self) -> String;
}

pub fn exec(
    computation_id: &String, 
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Computation>>>>>>,
    stream: &mut UnixStream,
    input: ComputationInput,
) -> Option<ComputationResult> {
    let computations_read = computations.read().unwrap();
                
    if let Some(c) = computations_read.get(computation_id) {
        let mut computation = c.lock().unwrap();
        let computation_result= computation.run(input);

        handle_computation_result(computation_result, stream, computation_id)
    } else {
        panic!("Computation with given ID not found {}", computation_id);
    }
}

fn handle_computation_result(computation_result: ComputationOutput, stream: &mut UnixStream, computation_id: &String) -> Option<ComputationResult> {
    match computation_result {
        ComputationOutput::None => None,
        ComputationOutput::Return(res) => Some(res),
        ComputationOutput::Forward(res) => {
            send_result(stream, &computation_id, &res);
            Some(res)
        }
    }
}

fn send_result(stream: &mut UnixStream, computation_id: &String, result: &ComputationResult) {
    let payload = match result {
        ComputationResult::Text(text) => {
            payloader::build_from_str(&computation_id, text)
        }
        ComputationResult::Binary(binary) => {
            payloader::build_from_binary(&computation_id, binary)
        }
    };

    if let Err(err) = stream.write_all(&payload) {
        eprintln!("Error writing to stream: {}", err);
    }
}