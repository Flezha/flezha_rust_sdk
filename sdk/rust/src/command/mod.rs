use crate::pb::CommandRequest;
use crate::pb::command_request::{Command};

mod operator_assign;
mod pipeline_start;
mod pipeline_stop;
mod migration_serialization;
mod migration_deserialization;
mod pipeline_state;
mod operator_shutdown;
mod streams_connect;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use operator_shutdown::OperatorShutdownCommand;
use migration_serialization::MigrationSerializationCommand;
use migration_deserialization::MigrationDeserializationCommand;
use pipeline_state::PipelineStateCommand;

use crate::node::OperatorNode;
use crate::service::payloader;

use self::pipeline_start::AddPipelineCommand;
use self::pipeline_stop::RemPipelineCommand;

use serde_json::Value;
use prost::Message;

pub trait CommandAction {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        command_struct: &String,
        errors: &mut Vec<String>
    ) -> (String, String);
}

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    OperatorShutdown(Box<OperatorShutdownCommand>),
}


pub fn get_handler(    
    _payload_id: &String,
    message: &String
) -> (CommandActionType, String) {
    let wrapper_command = CommandRequest::decode(message.as_bytes())
        .expect("Failed to decode message");

    let reference_id = wrapper_command.reference_id;

    let command_handler = match wrapper_command.command.unwrap() {
        Command::AssignOperatorId(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        },
        Command::ConnectSocket(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        },
    };

    (command_handler, reference_id)
}

    // let command: Command = serde_json::from_str(message).expect("Failed to parse command");
    // let reference_id = command.reference_id;

    // let handler = match payload_id.as_str() {
    //     "assign_operator_id" => {
    //         let command: AssignOperatorIdCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize AssignOperatorIdCommand");
    //         CommandActionType::Regular(Box::new(command))
    //     }
    //     "connect_sockets" => {
    //         let command: ConnectSocketCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize OpenSocketCommand");
    //         CommandActionType::Regular(Box::new(command))
    //     }
    //     "start_pipeline" => {
    //         let command: AddPipelineCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize AddPipelineCommand");
    //         CommandActionType::Regular(Box::new(command))    
    //     }
    //     "stop_pipeline" => {
    //         let command: RemPipelineCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize AddPipelineCommand");
    //         CommandActionType::Regular(Box::new(command))
    //     }
    //     "migration_serialization" => {
    //         let command: MigrationSerializationCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize MigrationSerializationCommand");
    //         CommandActionType::Regular(Box::new(command))
    //     }
    //     "migration_deserialization" => {
    //         let command: MigrationDeserializationCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize MigrationDeserializationCommand");
    //         CommandActionType::Regular(Box::new(command))
    //     }
    //     "pipeline_state" => {
    //         let command: PipelineStateCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize PipelineStateCommand");
    //         CommandActionType::Regular(Box::new(command))
    //     }
    //     "operator_disconnect" => {
    //         let command: OperatorShutdownCommand = serde_json::from_value(command.data)
    //             .expect("Failed to deserialize OperatorShutdownCommand");
    //         CommandActionType::OperatorShutdown(Box::new(command))
    //     }
    //     _ => {
    //         panic!("Unknown command payload_id: {}", payload_id);
    //     }
    // };

    // (command_handler, reference_id)
// }

pub fn handle_action(
    operator: &Arc<Mutex<OperatorNode>>,
    handler: Box<dyn CommandAction>,
    payload_id: &String,
    message: &String,
    reference_id: String,
) -> Vec<u8> {
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

    let (_payload_id, info_msg) = handler.handle(
        operator,
        message,
        &mut errors
    );

    let mut hashmap: HashMap<String, Value> = HashMap::new();

    if errors.len() > 0 {
        status = "error".to_string();
    }
                    
    hashmap.insert("reference_id".to_string(), Value::String(reference_id));
    hashmap.insert("status".to_string(), Value::String(status));
    hashmap.insert("info".to_string(), Value::String(info_msg));
    hashmap.insert("errors".to_string(), Value::Array(errors.iter().map(|e| Value::String(e.clone())).collect()));

    let json_string = serde_json::to_string(&hashmap).unwrap();
    
    payloader::build_from_str(&payload_id, &json_string)
}
