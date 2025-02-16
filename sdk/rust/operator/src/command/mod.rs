mod operator_assign;
mod worker_add;
mod worker_rem;
mod streams_connect;
mod worker_serialize;
mod worker_deserialize;
mod worker_state;

use std::sync::{Arc, Mutex};

use streams_connect::ConnectSocketCommand;
use worker_serialize::SerializeWorkerCommand;
use worker_deserialize::DeSerializeWorkerCommand;
use worker_state::WorkerStateCommand;

use crate::node::OperatorNode;

use self::worker_add::AddWorkerCommand;
use self::worker_rem::RemWorkerCommand;

use serde::Deserialize;
use serde_json::Value;

pub trait CommandAction {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
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
    operator: &Arc<Mutex<OperatorNode>>,
    payload_id: &String,
    message: &String
) {
    // Deserialize the command from the JSON message
    let command: Command = serde_json::from_str(message).expect("Failed to parse command");
    let reference_id = command.reference_id;

    let command_action: Box<dyn CommandAction> = match payload_id.as_str() {
        "assign_operator_id" => {
            let command: operator_assign::AssignOperatorIdCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AssignOperatorIdCommand");
            Box::new(command)
        }
        "connect_sockets" => {
            let command: ConnectSocketCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize OpenSocketCommand");
            Box::new(command)
        }
        "add_worker" => {
            let command: AddWorkerCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddWorkerCommand");
            Box::new(command)
        }
        "rem_worker" => {
            let command: RemWorkerCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddWorkerCommand");
            Box::new(command)
        }
        "serialize_worker" => {
            let command: SerializeWorkerCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize SerializeWorkerCommand");
            Box::new(command)
        }
        "deserialize_worker" => {
            let command: DeSerializeWorkerCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize DeSerializeWorkerCommand");
            Box::new(command)
        }
        "worker_state" => {
            let command: WorkerStateCommand = serde_json::from_value(command.data)
            .expect("Failed to deserialize WorkerStateCommand");
        Box::new(command)
        }
        _ => {
            panic!("Unknown command payload_id: {}", payload_id);
        }
    };

    command_action.handle(operator, message, reference_id);
}