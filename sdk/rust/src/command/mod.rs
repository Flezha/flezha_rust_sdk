mod operator_assign;
mod pipeline_add;
mod pipeline_rem;
mod streams_connect;
mod pipeline_serialize;
mod pipeline_deserialize;
mod pipeline_state;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use streams_connect::ConnectSocketCommand;
use pipeline_serialize::SerializePipelineCommand;
use pipeline_deserialize::DeSerializePipelineCommand;
use pipeline_state::PipelineStateCommand;

use crate::node::OperatorNode;
use crate::service::payloader;

use self::pipeline_add::AddPipelineCommand;
use self::pipeline_rem::RemPipelineCommand;

use serde::Deserialize;
use serde_json::Value;
use std::io::Write;

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
) {
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
        "add_pipeline" => {
            let command: AddPipelineCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize AddPipelineCommand");
            Box::new(command)
        }
        "rem_pipeline" => {
            let command: RemPipelineCommand = serde_json::from_value(command.data)
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
        "pipeline_state" => {
            let command: PipelineStateCommand = serde_json::from_value(command.data)
                .expect("Failed to deserialize PipelineStateCommand");
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

    reply_agent(operator, reference_id, payload_id, info, &mut status, &mut errors);
}

fn reply_agent(
    operator: &Arc<Mutex<OperatorNode>>,
    reference_id: String,
    payload_id: String,
    info_msg: String,
    status: &mut String,
    errors: &mut Vec<String>
) {
    let mut hashmap: HashMap<String, Value> = HashMap::new();
    let operator_guard = operator.lock().unwrap();

    if errors.len() > 0 {
        *status = "error".to_string();
    }
                    
    hashmap.insert("reference_id".to_string(), Value::String(reference_id));
    hashmap.insert("status".to_string(), Value::String(status.to_string()));
    hashmap.insert("info".to_string(), Value::String(info_msg));
    hashmap.insert("errors".to_string(), Value::Array(errors.iter().map(|e| Value::String(e.clone())).collect()));

    let json_string = serde_json::to_string(&hashmap).unwrap();
    let payload = payloader::build_from_str(&payload_id, &json_string);

    operator_guard
        .get_control_stream()
        .try_clone()
        .unwrap()
        .write_all(&payload)
        .unwrap();
}