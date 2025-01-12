mod add_computation;
mod rem_computation;
mod connect_streams;
mod serialize_computation;
mod deserialize_computation;
mod computation_state;

use std::sync::{Arc, Mutex};

use connect_streams::ConnectSocketCommand;
use serialize_computation::SerializeComputationCommand;
use deserialize_computation::DeSerializeComputationCommand;
use computation_state::ComputationStateCommand;

use crate::node::WorkerNode;

use self::add_computation::AddComputationCommand;
use self::rem_computation::RemComputationCommand;

use serde::Deserialize;
use serde_json::Value;

pub trait CommandAction {
    fn handle(
        &self,
        operator: &Arc<Mutex<WorkerNode>>,
        command_struct: &String,
        reference_id: String,
    );
}

#[derive(Deserialize)]
struct Command {
    reference_id: String,
    data: Value,
}

pub fn handle_action(
    operator: &Arc<Mutex<WorkerNode>>,
    payload_id: &String,
    message: &String
) {
    // Deserialize the command from the JSON message
    let command: Command = serde_json::from_str(message).expect("Failed to parse command");
    let reference_id = command.reference_id;

    let command_action: Box<dyn CommandAction> = match payload_id.as_str() {
        "connect_sockets" => {
            let command: ConnectSocketCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize OpenSocketCommand");
            Box::new(command)
        }
        "add_computation" => {
            let command: AddComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddComputationCommand");
            Box::new(command)
        }
        "rem_computation" => {
            let command: RemComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddComputationCommand");
            Box::new(command)
        }
        "serialize_computation" => {
            let command: SerializeComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize SerializeComputationCommand");
            Box::new(command)
        }
        "deserialize_computation" => {
            let command: DeSerializeComputationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize DeSerializeComputationCommand");
            Box::new(command)
        }
        "computation_state" => {
            let command: ComputationStateCommand = serde_json::from_value(command.data)
            .expect("Failed to deserialize ComputationStateCommand");
        Box::new(command)
        }
        _ => {
            panic!("Unknown command payload_id: {}", payload_id);
        }
    };

    command_action.handle(operator, message, reference_id);
}