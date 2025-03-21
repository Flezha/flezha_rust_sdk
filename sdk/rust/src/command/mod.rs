mod operator_assign;
mod pipeline_add;
mod pipeline_rem;
mod streams_connect;
mod pipeline_serialize;
mod pipeline_deserialize;
// TODO: mod pipeline_state;

use std::sync::{Arc, Mutex};

use streams_connect::ConnectSocketCommand;
use pipeline_serialize::SerializePipelineCommand;
use pipeline_deserialize::DeSerializePipelineCommand;
// TODO: use pipeline_state::WorkerStateCommand;

use crate::node::OperatorNode;

use self::pipeline_add::AddPipelineCommand;
use self::pipeline_rem::RemWorkerCommand;

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
        "add_pipeline" => {
            let command: AddPipelineCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddPipelineCommand");
            Box::new(command)
        }
        "rem_pipeline" => {
            let command: RemWorkerCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddPipelineCommand");
            Box::new(command)
        }
        // TODO: 
        "serialize_pipeline" => {
            let command: SerializePipelineCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize SerializePipelineCommand");
            Box::new(command)
        }
        "deserialize_pipeline" => {
            let command: DeSerializePipelineCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize DeSerializePipelineCommand");
            Box::new(command)
        }
        // TODO:
        // "pipeline_state" => {
        //     let command: WorkerStateCommand = serde_json::from_value(command.data)
        //     .expect("Failed to deserialize WorkerStateCommand");
        // Box::new(command)
        // }
        _ => {
            panic!("Unknown command payload_id: {}", payload_id);
        }
    };

    command_action.handle(operator, message, reference_id);
}