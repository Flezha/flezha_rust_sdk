mod operator_assign;
mod pipeline_start;
mod pipeline_stop;
mod streams_connect;
mod migration_serialization;
mod migration_deserialization;
mod pipeline_state;
mod operator_shutdown;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use streams_connect::ConnectSocketCommand;
use migration_serialization::MigrationSerializationCommand;
use migration_deserialization::MigrationDeserializationCommand;
use pipeline_state::PipelineStateCommand;

use crate::node::OperatorNode;
use crate::service::payloader;

use self::pipeline_start::AddPipelineCommand;
use self::pipeline_stop::RemPipelineCommand;

use serde::Deserialize;
use serde_json::Value;

pub trait CommandAction {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        command_struct: &String,
        errors: &mut Vec<String>
    ) -> (String, String);
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
) -> Vec<u8> {
    // Deserialize the command from the JSON message
    let command: Command = serde_json::from_str(message).expect("Failed to parse command");
    let reference_id = command.reference_id;
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

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
        "start_pipeline" => {
            let command: AddPipelineCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddPipelineCommand");
            Box::new(command)
        }
        "stop_pipeline" => {
            let command: RemPipelineCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddPipelineCommand");
            Box::new(command)
        }
        // TODO: 
        "migration_serialization" => {
            let command: MigrationSerializationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize MigrationSerializationCommand");
            Box::new(command)
        }
        "migration_deserialization" => {
            let command: MigrationDeserializationCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize MigrationDeserializationCommand");
            Box::new(command)
        }
        "pipeline_state" => {
            let command: PipelineStateCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize PipelineStateCommand");
            Box::new(command)
        }
        "operator_shutdown" => {
            let command: operator_shutdown::OperatorShutdownCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize OperatorShutdownCommand");
            Box::new(command)
        }
        _ => {
            panic!("Unknown command payload_id: {}", payload_id);
        }
    };

    let (payload_id, info) = command_action.handle(
        operator,
        message,
        &mut errors
    );

    get_reply(reference_id, payload_id, info, &mut status, &mut errors)
}

fn get_reply(
    reference_id: String,
    payload_id: String,
    info_msg: String,
    status: &mut String,
    errors: &mut Vec<String>
) -> Vec<u8> {
    let mut hashmap: HashMap<String, Value> = HashMap::new();

    if errors.len() > 0 {
        *status = "error".to_string();
    }
                    
    hashmap.insert("reference_id".to_string(), Value::String(reference_id));
    hashmap.insert("status".to_string(), Value::String(status.to_string()));
    hashmap.insert("info".to_string(), Value::String(info_msg));
    hashmap.insert("errors".to_string(), Value::Array(errors.iter().map(|e| Value::String(e.clone())).collect()));

    let json_string = serde_json::to_string(&hashmap).unwrap();
    
    payloader::build_from_str(&payload_id, &json_string)
}