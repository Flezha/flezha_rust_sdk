use crate::pb::{CommandRequest, CommandResponse};
use crate::pb::command_request::{Command};
use crate::node::OperatorNode;
use crate::service::payloader;
use std::sync::{Arc, Mutex};
use prost::Message;

mod operator_assign;
mod pipeline_start;
mod pipeline_stop;
mod migration_serialization;
mod migration_deserialization;
mod pipeline_state;
mod operator_disconnect;
mod streams_connect;

pub trait CommandAction {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        command_struct: &String,
        errors: &mut Vec<String>
    ) -> (String, Vec<u8>);
}

pub enum CommandActionType {
    Regular(Box<dyn CommandAction>),
    OperatorDisconnect(Box<dyn CommandAction>),
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
        Command::AddPipeline(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        },
        Command::RemPipeline(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        },
        Command::MigrationSerialization(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        },
        Command::MigrationDeserialization(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        },
        Command::PipelineState(proto_ch) => {
            CommandActionType::Regular(Box::new(proto_ch))
        },
        Command::OperatorDisconnect(proto_ch) => {
            CommandActionType::OperatorDisconnect(Box::new(proto_ch))
        },
    };

    (command_handler, reference_id)
}

pub fn handle_action(
    operator: &Arc<Mutex<OperatorNode>>,
    handler: Box<dyn CommandAction>,
    payload_id: &String,
    message: &String,
    reference_id: String,
) -> Vec<u8> {
    let mut errors: Vec<String> = vec![];
    let mut status = String::from("success");

    let (_payload_id, raw_data) = handler.handle(
        operator,
        message,
        &mut errors
    );

    if errors.len() > 0 {
        status = "error".to_string();
    }

    let command_response: CommandResponse = CommandResponse {
        reference_id,
        status,
        errors,
        raw_data
    };

    let command_response_bytes = command_response.encode_to_vec();
    
    payloader::build_from_binary(&payload_id, &command_response_bytes)
}
